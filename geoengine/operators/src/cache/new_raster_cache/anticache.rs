#![allow(unused)]
use std::{
    fmt::Write,
    fs,
    io::{self, ErrorKind, Write as IoWrite},
    ops::Deref,
    path::PathBuf,
    sync::OnceLock,
};

use geoengine_datatypes::raster::{
    arrow_ipc_file_to_raster_tile_2d_for_ipc_channel,
    raster_tile_2d_to_arrow_ipc_file_for_ipc_channel,
};
use tokio::{
    fs::{create_dir, remove_file},
    io::AsyncReadExt,
    sync::RwLock,
};
use uuid::Uuid;

use crate::{cache::new_raster_cache::*, error::Error};

pub struct OnDiskStore<SF: StorageFormat, ES: EvictionStrategy> {
    pub(crate) cache: RwLock<HashMap<CacheKey, Arc<SF>>>,
    pub(crate) eviction_strategy: RwLock<ES>,
}

pub struct PendingFile<SF: OnDiskStorageFormat> {
    inner: Option<SF>,
    commited: bool,
}

impl<SF: OnDiskStorageFormat> PendingFile<SF> {
    pub fn new(inner: SF) -> Self {
        let inner = Some(inner);
        Self {
            inner,
            commited: false,
        }
    }

    pub fn commit(mut self) -> SF {
        self.commited = true;
        self.inner.take().unwrap()
    }
}

impl<SF: OnDiskStorageFormat> Deref for PendingFile<SF> {
    type Target = SF;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl<SF: OnDiskStorageFormat> Drop for PendingFile<SF> {
    fn drop(&mut self) {
        if !self.commited {
            self.inner.as_mut().map(|f| f.cleanup());
        }
    }
}


#[async_trait]
pub trait OnDiskStorageFormat: StorageFormat {
    async fn write(tile: TypedRasterTile2D) -> Result<PendingFile<Self>> {
        let store = Self::store(tile).await?;
        let pending_file = PendingFile::new(store);
        Ok(pending_file)
    }

    fn cleanup(&mut self);
}

// Currently just a copy of ["MockOnDiskCacheStore"] in  ["super;"].
#[async_trait]
impl<SF, ES> CacheStore for OnDiskStore<SF, ES>
where
    SF: StorageFormat + OnDiskStorageFormat,
    ES: EvictionStrategy,
{
    type SF = SF;
    type ES = ES;

    async fn get(&self, key: &CacheKey) -> Result<Arc<Self::SF>> {
        self.cache
            .read()
            .await
            .get(key)
            .map(Arc::clone)
            .ok_or(crate::error::Error::Cache {
                source: CacheError::Unspecified,
            })
    }

    async fn insert(&self, key: CacheKey, tile: TypedRasterTile2D) -> Result<()> {
        /*
        We need to make sure that the file is not just written to the disk
        without actually being inserted in the cache / index later on.
        Otherwise we will have orphaned files.
        */
        let stored_tile = SF::write(tile).await?;
        let required_space = stored_tile.byte_size().await?;

        let mut cache = self.cache.write().await;
        let mut eviction_strategy = self.eviction_strategy.write().await;
        let eviction_plan = eviction_strategy.plan_eviction(required_space, f64::MAX, |key| {
            cache.get(key).map_or(true, |sf| Arc::strong_count(sf) > 1)
        })?;

        if eviction_plan.freed_bytes < required_space {
            return Err(crate::error::Error::Cache {
                source: CacheError::Unspecified,
            });
        }

        for key in eviction_plan.keys_to_remove {
            cache.remove(&key);
            eviction_strategy.record_removal(&key);
        }

        let cache_obj = stored_tile.commit();
        cache.insert(key, Arc::new(cache_obj));
        Ok(())
    }
}

/*
    NOTE: This keeps the RasterTile both in memory and on the disk,
    which means the cache (might) be persistent.
*/
pub struct ArrowIpcStorageFormat {
    path: PathBuf,
    tile: TypedRasterTile2D,
    /// The cached bytesize of the file ins storage
    byte_size: usize,
}

static RASTER_CACHE_DIR: OnceLock<PathBuf> = OnceLock::new();

pub fn set_raster_cache_dir(path: PathBuf) -> Result<()> {
    RASTER_CACHE_DIR.set(path).map_err(|err| Error::MustNotHappen { message: "RASTER_CACHE_DIR should only be set once during startup".to_string() })?;
    Ok(())
}

/// Returns the `RASTER_CACHE_DIR` config setting if it is set,
/// if is not, then return then return the xdg-cache dir
fn cache_dir() -> PathBuf {
    RASTER_CACHE_DIR
        .get()
        .map(|x| x.to_path_buf())
        .unwrap_or_else(|| {
            let home = std::env::home_dir()
                .expect("Either 'RASTER_CACHE_DIR' or 'HOME' dir should be set");
            home.join(".cache").join("geoengine")
        })
}

impl OnDiskStorageFormat for ArrowIpcStorageFormat {
    fn cleanup(&mut self) {
        remove_file(&self.path);
    }
}

#[async_trait]
impl StorageFormat for ArrowIpcStorageFormat {
    async fn store(tile: TypedRasterTile2D) -> Result<Self> {
        let cache_dir = cache_dir();
        tokio::fs::create_dir_all(&cache_dir).await?;
        let uuid = Uuid::new_v4();
        let file_path = PathBuf::from(format!("{uuid}.raster_cache.ipc"));
        let cache_path = cache_dir.join(file_path);

        let bytes = typed_raster_tile_to_arrow_ipc(tile.clone())?; // is cloning here necessary?
        let byte_size = bytes.len();
        let mut file = File::create(cache_path.clone())?;
        file.write_all(&bytes)?;

        Ok(Self {
            path: cache_path,
            tile,
            byte_size,
        })
    }

    async fn load(&self) -> Result<TypedRasterTile2D> {
        let mut file = tokio::fs::File::open(self.path.clone()).await?;
        let mut buf = vec![];

        file.read_to_end(&mut buf).await?;
        Ok(self.tile.clone())
    }

    async fn byte_size(&self) -> Result<usize> {
        Ok(self.byte_size)
    }
}

fn typed_raster_tile_to_arrow_ipc(tile: TypedRasterTile2D) -> Result<Vec<u8>> {
    match tile {
        TypedRasterTile2D::I8(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::I16(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::I32(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::I64(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::U8(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::U16(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::U32(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::U64(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::F32(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
        TypedRasterTile2D::F64(base_tile) => {
            raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(base_tile)
        }
    }
    .map_err(|err| crate::error::Error::DataType { source: err })
}

fn arrow_ipc_tile_to_typed_raster<P>(tile: Vec<u8>) -> Result<TypedRasterTile2D>
where
    P: Pixel + SupportedRasterDataType,
{
    let raster_tile = arrow_ipc_file_to_raster_tile_2d_for_ipc_channel::<P>(tile)?;
    Ok(SupportedRasterDataType::map_tile_to_enum(raster_tile))
}

#[cfg(test)]
mod tests {
    use crate::util::test::assert_eq_raster_operator_res_and_list_of_tiles_u8;

    use super::*;
    use gdal_sys::{CPLFormCIFilename, VSIGetCanonicalFilename};
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
    use geoengine_datatypes::raster::{Grid2D, RasterTile2D, TileInformation};
    use geoengine_datatypes::util::test::TestDefault;
    use serde::de::Expected;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::str::FromStr;

    use tokio::sync::RwLock;

    fn test_key(idx: i32) -> CacheKey {
        (
            CanonicOperatorName::new_unchecked(&json!({ "operator_name": "CacheOperator" })),
            idx as u32,
            TimeInterval::default(),
            [idx as isize, 0].into(),
        )
    }

    fn test_tile_u8() -> TypedRasterTile2D {
        let values: Vec<u8> = (0_u8..64).collect();
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [8, 8].into(),
            },
            0,
            Grid2D::new([8, 8].into(), values).unwrap().into(),
            CacheHint::default(),
        );

        TypedRasterTile2D::U8(raster_tile)
    }

    #[test]
    fn arrow_ipc_storage_format_store_and_load() {
        panic!("not implemented");
        // let cache_root = cache_dir();
        // fs::create_dir_all(&cache_root).unwrap();

        // let tile = test_tile_u8();
        // let stored = ArrowIpcStorageFormat::store(tile.clone())
        //     .expect("it should be possible to store the tile.");

        // let path = &stored.path;
        // let metadata = fs::metadata(&stored.path).expect(&format!("File should be at {path:#?}"));
        // assert_eq!(stored.byte_size().unwrap() as u64, metadata.len());

        // let loaded = stored
        //     .load()
        //     .expect(&format!("File '{path:#?}' should loadable as a RasterTile"));
        // match loaded {
        //     TypedRasterTile2D::U8(loaded_tile) => {
        //         if let TypedRasterTile2D::U8(original_tile) = tile {
        //             assert_eq!(
        //                 loaded_tile, original_tile,
        //                 "Tiles are not the same after on-disk cache"
        //             );
        //         } else {
        //             panic!("expected u8 tile");
        //         }
        //     }
        //     _ => panic!("expected u8 tile"),
        // }
        // fs::remove_file(&stored.path)
        //     .expect("it should be possible to delete the file from the disk");
    }

    async fn on_disk_storage_insert_and_get_with_arrow_ipc() {
        let cache_root = cache_dir();
        fs::create_dir_all(&cache_root).unwrap();

        let store = OnDiskStore {
            cache: RwLock::new(HashMap::new()),
            eviction_strategy: RwLock::new(FifoEvictionStrategy::new(usize::MAX)),
        };

        let key = test_key(1);
        let tile = test_tile_u8();
        let expected_tile = tile.clone();

        store
            .insert(key.clone(), tile)
            .await
            .expect("it should be possible to insert the tile into the OnDiskStorage.");
        let stored: Arc<ArrowIpcStorageFormat> = store
            .get(&key)
            .await
            .expect("it should be possible to extract the tile-ref from the cache.");
        let loaded = stored
            .load()
            .await
            .expect("it should be possible to load the tile via the tile-ref.");

        match loaded {
            TypedRasterTile2D::U8(loaded_tile) => {
                if let TypedRasterTile2D::U8(original_tile) = expected_tile {
                    assert_eq!(loaded_tile, original_tile, "Tiles differ after cache.");
                } else {
                    panic!("expected u8 tile");
                }
            }
            _ => panic!("expected u8 tile"),
        }

        let path = stored.path.clone();
        drop(stored);
        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn cache_dir_from_home() {
        let dir = cache_dir();
        let home = std::env::home_dir().expect("HOME should be set");
        assert_eq!(format!("{}/.cache/geoengine/", home.to_str().unwrap()), dir);
    }

    #[test]
    fn set_then_read_raster_cache_dir() {
        let expected = PathBuf::from_str("/tmp/something").unwrap();
        if let Err(err) = set_raster_cache_dir(expected.clone()) {
            panic!("{err}");
        }
        let actual = cache_dir();
        assert_eq!(expected, actual);
    }
}
