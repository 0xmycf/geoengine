// TODO (low): handle all panics!
#![allow(unused)]
use std::{
    fmt::Write,
    fs,
    io::{self, ErrorKind, Write as IoWrite},
    ops::Deref,
    path::PathBuf,
    result,
    str::FromStr,
    sync::OnceLock,
};
#[cfg(test)]
use std::{sync::LazyLock, thread::ThreadId};

use chrono::offset;
#[cfg(test)]
use futures::executor::block_on;
use futures::task::UnsafeFutureObj;
use geoengine_datatypes::{
    primitives::{CacheHint, DateTimeParseFormat},
    raster::{
        EmptyGrid, GeoTransform, Grid, Grid2D, GridIdx, GridOrEmpty, GridShapeAccess, GridSize,
        MaskedGrid2D, RasterDataType, RasterProperties, TilesEqualIgnoringCacheHint, TypedGrid,
        TypedGrid2D, arrow_ipc_file_to_raster_tile_2d_for_ipc_channel,
        raster_tile_2d_to_arrow_ipc_file_for_ipc_channel,
    },
    util::ByteSize,
};
use serde_json::{Value, json};
use std::sync::RwLock as StdRwLock;
#[cfg(test)]
use tokio::sync::Mutex;
use tokio::{
    fs::{create_dir, metadata},
    io::AsyncReadExt,
    sync::RwLock,
};
use uuid::Uuid;
#[cfg(test)]
use zarrs::array::CodecError::ExpectedFixedLengthBytes;
use zarrs::{
    array::{
        Array, ArrayBuilder, ArrayBytes, ArrayBytesOffsets, ArrayCreateError, ArrayShape, DataType,
        ElementOwned, FromArrayBytes, IntoArrayBytes,
        builder::{ArrayBuilderChunkGridMetadata, ArrayBuilderFillValue},
        chunk_shape_to_array_shape,
        data_type::{self, BoolDataType},
    },
    filesystem::FilesystemStore,
    group::GroupBuilder,
};

use crate::{
    cache::{
        self,
        new_raster_cache::{
            on_disk::{OnDiskStorageFormat, cache_dir, time_interval_to_path_safe_iso},
            *,
        },
    },
    engine::OperatorName,
    error::{
        Error,
        WhichTile::{Metadata, Tiles, Validity},
    },
    util::gdal::create_ndvi_meta_data,
};

/*
    // TODO (low): revisit the visibility modifiers


    NOTE: This keeps the RasterTile both in memory and on the disk,
    which means the cache (might) be persistent.
*/
pub struct ArrowIpcStorageFormat {
    pub path: PathBuf,
    tile: TypedRasterTile2D, // TODO (low): make this optional and implement proper ways of not having it in RAM
    /// The cached bytesize of the file ins storage
    byte_size: usize,
    /// the datatype of the TypedRasterTile2D
    data_type: RasterDataType,
    /// The cache_hint from the tile, only used for [`StorageFormat::is_expired`]
    cache_hint: CacheHint,
}

impl OnDiskStorageFormat for ArrowIpcStorageFormat {
    fn cleanup(&mut self) {
        tokio::fs::remove_file(&self.path);
    }
}

/// returns the path to the directory (does not create) as the first element
///         and the filepath as the second element.
///
/// does not create create the file nor the directory.
async fn path_for_cache_key(key: &CacheKey) -> Result<(PathBuf, PathBuf)> {
    let cache_dir = cache_dir();
    let ops_dir_name = key.0.as_sha256();
    let file_name = format!(
        "band_{}-timeinterval_{}-grididx_{}.raster_cache.ipc",
        key.1,
        time_interval_to_path_safe_iso(&key.2),
        index_to_string(&key.3),
    );
    Ok((
        cache_dir.join(ops_dir_name),
        PathBuf::from_str(&file_name).expect("it to be a valid pathbuf"),
    ))
}

#[async_trait]
impl StorageFormat for ArrowIpcStorageFormat {
    async fn store(
        tile: TypedRasterTile2D,
        key: Arc<CacheKey>,
        _result_descriptor: /* TODO (crit): check if this must be used after the rebase! */ &RasterResultDescriptor,
    ) -> Result<Self> {
        let (cache_dir, file_path) = path_for_cache_key(&key).await?;
        tokio::fs::create_dir_all(cache_dir.clone()).await?;
        let cache_path = cache_dir.join(file_path);

        let bytes = typed_raster_tile_to_arrow_ipc(tile.clone())?; // is cloning here necessary?
        let byte_size = bytes.len();
        let mut file = File::create(cache_path.clone())?;
        let _ = file.write_all(&bytes)?;

        let data_type = tile.raster_data_type();
        let cache_hint = call_generic_raster_tile_2d!(&tile, tile => tile.cache_hint);
        Ok(Self {
            path: cache_path,
            tile,
            byte_size,
            data_type,
            cache_hint,
        })
    }

    async fn load(&self) -> Result<TypedRasterTile2D> {
        let mut file = tokio::fs::File::open(self.path.clone()).await?;
        let mut buf = vec![];

        let _ = file.read_to_end(&mut buf).await?;
        match self.data_type {
            RasterDataType::I8 => arrow_ipc_tile_to_typed_raster::<i8>(buf),
            RasterDataType::I16 => arrow_ipc_tile_to_typed_raster::<i16>(buf),
            RasterDataType::I32 => arrow_ipc_tile_to_typed_raster::<i32>(buf),
            RasterDataType::I64 => arrow_ipc_tile_to_typed_raster::<i64>(buf),
            RasterDataType::U8 => arrow_ipc_tile_to_typed_raster::<u8>(buf),
            RasterDataType::U16 => arrow_ipc_tile_to_typed_raster::<u16>(buf),
            RasterDataType::U32 => arrow_ipc_tile_to_typed_raster::<u32>(buf),
            RasterDataType::U64 => arrow_ipc_tile_to_typed_raster::<u64>(buf),
            RasterDataType::F32 => arrow_ipc_tile_to_typed_raster::<f32>(buf),
            RasterDataType::F64 => arrow_ipc_tile_to_typed_raster::<f64>(buf),
        }
    }

    async fn byte_size(&self) -> Result<usize> {
        Ok(self.byte_size)
    }

    async fn is_expired(&self) -> bool {
        self.cache_hint.is_expired()
    }
}

fn typed_raster_tile_to_arrow_ipc(tile: TypedRasterTile2D) -> Result<Vec<u8>> {
    call_generic_raster_tile_2d!(tile, tile => {
        raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(tile)
    })
    .map_err(|err| crate::error::Error::DataType { source: err })
}

fn arrow_ipc_tile_to_typed_raster<P>(tile: Vec<u8>) -> Result<TypedRasterTile2D>
where
    P: Pixel + SupportedRasterDataType,
{
    let mut raster_tile = arrow_ipc_file_to_raster_tile_2d_for_ipc_channel::<P>(tile)?;
    Ok(SupportedRasterDataType::map_tile_to_enum(raster_tile))
}

fn index_to_string(idx: &TileIndex) -> String {
    let mut sb = String::new();
    let mut iter = idx.as_slice().iter();
    while let Some(value) = iter.next() {
        sb.write_str(&format!("{}-", value));
    }
    sb[0..sb.len() - 1].to_string()
}

#[cfg(test)]
mod tests {

    use geoengine_datatypes::hashmap;

    use super::*;

    #[test]
    fn cache_key_formatting() {
        let cases = hashmap! {
            [1, 2].into() => "1-2".to_string(),
            [-1, -2].into() => "-1--2".to_string(),
            [0, -1].into() => "0--1".to_string(),
            [0, 1].into() => "0-1".to_string(),
        };
        for case in cases {
            let actual = index_to_string(&case.0);
            let expected = case.1;
            assert_eq!(expected, actual)
        }
    }
}
