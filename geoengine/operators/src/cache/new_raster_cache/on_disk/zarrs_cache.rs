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
use geo::k_nearest_concave_hull;
use geoengine_datatypes::{
    call_generic_grid_2d,
    primitives::{CacheHint, Coordinate2D, DateTimeParseFormat},
    raster::{
        EmptyGrid, GeoTransform, Grid, Grid2D, GridBounds, GridIdx, GridOrEmpty, GridShapeAccess,
        GridSize, MaskedGrid2D, RasterDataType, RasterProperties, TilesEqualIgnoringCacheHint,
        TilingStrategy, TypedGrid, TypedGrid2D, arrow_ipc_file_to_raster_tile_2d_for_ipc_channel,
        raster_tile_2d_to_arrow_ipc_file_for_ipc_channel,
    },
    util::ByteSize,
};
use ipc_channel::ipc::bytes_channel;
use itertools::Itertools;
use num::ToPrimitive;
use opentelemetry::global;
use serde_json::{Value, json};
use std::sync::RwLock as StdRwLock;
#[cfg(test)]
use tokio::sync::Mutex;
use tokio::{
    fs::{create_dir, metadata, remove_file},
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
    storage::{
        ListableStorageTraits, ReadableStorageTraits, ReadableWritableStorageTraits, StoreKey,
        WritableStorageTraits, store::MemoryStore,
    },
};

use crate::{
    cache::{
        self,
        new_raster_cache::{
            on_disk::{
                CommonOnDiskStorageData, DiskEntry, GLOBAL_GEOTRANSFORM_KEY, OnDiskStorageFormat,
                cache_dir, time_interval_to_path_safe_iso,
            },
            *,
        },
    },
    engine::OperatorName,
    error::{
        Error,
        WhichTile::{self, Metadata, Tiles, Validity},
        ZarrArray, ZarrFilesystem,
    },
    util::gdal::create_ndvi_meta_data,
};

pub struct ZarrDiskEntry {
    // Abstraction over the filepaths to the different arrays used for serialising from/into Zarrs
    paths: ZarrsArrayPath,
    // The datatype of the tile, incase the tile is not present
    // (future work (TODO (low): {9B0394FE-14E0-4B82-9BC6-E2EE119D86DF} fix this comment once resolved))
    data_type: RasterDataType,
    // the indicies in the Zarrs arrays of this tile, validity mask, metadata etc...
    chunk_indices: [u64; 2],
    // the cache hint for the tile
    cache_hint: CacheHint,
}

#[async_trait]
impl DiskEntry for ZarrDiskEntry {
    // type Prepared = New__ZarrsStorageFormat;

    async fn read(&self) -> Result<TypedRasterTile2D> {
        let store = open_zarrs_filesystemstore(&self.paths)?;
        let arrays = self.paths.open(store)?;
        arrays.retrieve_tile(
            &self.chunk_indices,
            &self.paths,
            self.data_type,
            self.cache_hint,
        )
    }
}

#[derive(Debug)]
struct ZarrStoreEntry {
    store_key: StoreKey,
    /// The bytes are optional, for example for empty tiles
    bytes: Option<bytes::Bytes>,
}

impl ZarrStoreEntry {
    fn new((store_key, bytes): (StoreKey, Option<bytes::Bytes>)) -> Self {
        Self { store_key, bytes }
    }

    fn set_in_store<TStore>(&self, store: &TStore) -> Result<()>
    where
        TStore: WritableStorageTraits,
    {
        if let Some(bytes) = self.bytes.clone() {
            store
                .set(&self.store_key, bytes)
                .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct PreparedZarrStorageFormat {
    // the zarr array definitions
    layout: [(StoreKey, bytes::Bytes); 4],
    tile_data: ZarrStoreEntry,
    validity_data: ZarrStoreEntry,
    metadata: ZarrStoreEntry,

    byte_size: u64,
    paths: ZarrsArrayPath,

    chunk_indices: [u64; 2],

    common: CommonOnDiskStorageData,
}

impl PreparedZarrStorageFormat {
    const METADATA: &str = "metadata";
    const VALIDITY: &str = "validity";
    const TILES: &str = "tiles";
}

fn key_to_array<'a, 'b>(key: &'a StoreKey) -> Option<&'b str> {
    match key.as_str().split('/').next() {
        Some("metadata") => Some(PreparedZarrStorageFormat::METADATA),
        Some("validity") => Some(PreparedZarrStorageFormat::VALIDITY),
        Some("tiles") => Some(PreparedZarrStorageFormat::TILES),
        _ => None,
    }
}

#[async_trait]
impl StorageFormat for PreparedZarrStorageFormat {
    // TODO (high): remove the unwraps and expects.
    async fn store(
        tile: TypedRasterTile2D,
        cache_key: Arc<CacheKey>,
        result_descriptor: &RasterResultDescriptor,
    ) -> Result<Self> {
        let store = Arc::new(MemoryStore::new());
        let group = GroupBuilder::new()
            .build(Arc::clone(&store), "/")
            .map_err(|source| Error::ZarrGroupCreateError { source })?;
        group
            .store_metadata()
            .map_err(|err| Error::ZarrArrayCreateError { source: err.into() })?;

        let chunk_indices = {
            let global_geo_transform =
                call_generic_raster_tile_2d!(&tile, tile => tile.global_geo_transform);
            let tile_bounds = raster_tile_bounds(
                result_descriptor,
                global_geo_transform,
                call_generic_raster_tile_2d!(&tile, tile => tile.grid_shape_array()).into(),
            );

            tile_position_to_chunk_indices(
                call_generic_raster_tile_2d!(&tile, tile => tile.tile_position),
                tile_bounds.min_index(),
            )
        }?;
        let common = {
            let cache_hint = call_generic_raster_tile_2d!(&tile, tile => tile.cache_hint.clone());

            CommonOnDiskStorageData::new(
                tile.raster_data_type(),
                cache_hint,
                Arc::clone(&cache_key),
            )
        };

        let paths = ZarrsArrayPath::from_cache_key(&cache_key);
        let arrays = build_array_and_prepare_insert(
            //
            &cache_key,
            &paths,
            Arc::clone(&store),
            &tile,
            result_descriptor,
        )?;

        let _ = {
            let mut writer = ZarrChunkWriter::new(&arrays);
            match writer.store_tile_chunks(&tile, chunk_indices) {
                Ok(sizes) => sizes,
                Err(err) => {
                    for completed in writer.attempted() {
                        let (name, array) = match completed {
                            CompletedZarrWrite::Tile => ("tiles", &arrays.tiles),
                            CompletedZarrWrite::Validity => ("validity", &arrays.validity),
                            CompletedZarrWrite::Metadata => ("metadata", &arrays.metadata),
                        };
                        // TODO (crit): this is not even neccessary here.
                        if let Err(error) = array.erase_chunk(&chunk_indices) {
                            tracing::warn!(
                                %error,
                                array = name,
                                chunk_indices = ?chunk_indices,
                                "Failed to erase Zarr cache chunk after write failure"
                            );
                        }
                    }
                    return Err(err);
                }
            }
        };

        let mut layout_builder = Vec::with_capacity(4);
        let mut data: HashMap<&str, (StoreKey, Option<bytes::Bytes>)> = HashMap::with_capacity(3);
        let mut byte_size = 0_u64;
        for key in store
            .list()
            .map_err(|err| Error::ZarrArrayError { source: err.into() })?
        {
            let is_json = key.as_str().ends_with(".json");

            let bytes = store.get(&key).expect("The key to be present");

            byte_size += bytes.as_ref().map(|x| x.len()).unwrap_or(0) as u64;
            if is_json {
                layout_builder.push((key, bytes.expect("the bytes to be present")));
            } else {
                data.insert(
                    key_to_array(&key).ok_or(Error::ZarrsGeoengineCacheArrayMappingError {
                        key: key.to_string(),
                    })?,
                    (key, bytes),
                );
            }
        }
        Ok(PreparedZarrStorageFormat {
            layout: layout_builder
                .try_into()
                .expect("the layout to have 4 entries"),
            tile_data: ZarrStoreEntry::new(
                data.remove(PreparedZarrStorageFormat::TILES)
                    .expect("tiles component to be present"),
            ),
            validity_data: ZarrStoreEntry::new(
                data.remove(PreparedZarrStorageFormat::VALIDITY)
                    .expect("validity component to be present"),
            ),
            metadata: ZarrStoreEntry::new(
                data.remove(PreparedZarrStorageFormat::METADATA)
                    .expect("metadata component to be present"),
            ),

            paths,
            chunk_indices,
            byte_size,
            common,
        })
    }

    async fn load(&self) -> Result<TypedRasterTile2D> {
        let store = Arc::new(MemoryStore::new());
        write_to_store(&self, &store, true /* write jsons too */)?;
        let arrays = self.paths.open(Arc::clone(&store))?;
        arrays.retrieve_tile(
            &self.chunk_indices,
            &self.paths,
            self.common.data_type,
            self.common.cache_hint,
        )
    }

    async fn byte_size(&self) -> Result<usize> {
        Ok(self.byte_size as usize)
    }

    async fn is_expired(&self) -> bool {
        self.common.cache_hint.is_expired()
    }
}

fn write_to_store<TStore>(
    data: &PreparedZarrStorageFormat,
    store: &TStore,
    write_json: bool,
) -> Result<()>
where
    TStore: WritableStorageTraits,
{
    if write_json {
        for (key, bytes) in &data.layout {
            store
                .set(&key, bytes.clone())
                .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
        }
    }
    data.tile_data.set_in_store(store)?;
    data.validity_data.set_in_store(store)?;
    data.metadata.set_in_store(store)?;
    Ok(())
}

fn ensure_layout<TStore>(store: &TStore, layout: &[(StoreKey, bytes::Bytes); 4]) -> Result<()>
where
    TStore: ReadableWritableStorageTraits,
{
    for (key, expected_bytes) in layout {
        match store
            .get(key)
            .map_err(|err| Error::ZarrArrayError { source: err.into() })?
        {
            None => {
                // Definition is absent: write it.
                store
                    .set(key, expected_bytes.clone())
                    .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
            }
            Some(actual) => {
                check_metadata_matches(key, &actual, expected_bytes)?;
            }
        }
    }
    Ok(())
}

fn check_metadata_matches(key: &StoreKey, actual: &[u8], expected: &[u8]) -> Result<()> {
    let actual: Value = serde_json::from_slice(actual)?;
    let expected: Value = serde_json::from_slice(expected)?;

    if actual != expected {
        return Err(Error::ZarrCacheMetadataMismatch {
            key: key.to_string(),
        });
    }

    Ok(())
}

#[async_trait]
impl OnDiskStorageFormat for PreparedZarrStorageFormat {
    type Written = ZarrDiskEntry;

    async fn write(self) -> Result<Self::Written> {
        let store = open_zarrs_filesystemstore(&self.paths)?;
        ensure_layout(&store, &self.layout)?;

        write_to_store(&self, &store, false /* dont write zarr-layout jsons */)?;
        Ok(ZarrDiskEntry {
            paths: self.paths,
            data_type: self.common.data_type,
            chunk_indices: self.chunk_indices,
            cache_hint: self.common.cache_hint,
        })
    }
}

#[derive(Debug)]
struct ZarrTileData {
    /// None iff empty tile
    tile_grid: Option<TypedGrid2D>,
    /// None iff empty tile
    validity_mask: Option<Vec<bool>>,
    zarr_metadata_chunk: ZarrsMetaDataChunk,
}

#[derive(Debug)]
struct ZarrArrayData {
    attributes: serde_json::Map<String, Value>,
    tile_shape: [u64; 2],
    tile_bounds: GridBoundingBox2D,
    tile_counts: [u64; 2],
    array_shape: [u64; 2],
    zarr_data_type: zarrs::array::DataType,
    fill_value: ArrayBuilderFillValue,
}

impl ZarrArrayData {
    fn build_arrays<TStore>(
        self,
        paths: &ZarrsArrayPath,
        store: Arc<TStore>,
    ) -> Result<ZarrsArrayAccess<TStore>>
    where
        TStore: ReadableWritableStorageTraits + 'static,
    {
        let self_arr = open_or_create_new_zarrs_array(
            &paths.tiles(),
            store.clone(),
            self.array_shape.clone(),
            self.tile_shape.clone(),
            self.zarr_data_type,
            self.fill_value,
            Some(self.attributes),
        )
        .map_err(|source| Error::ZarrArrayCreateError { source })?;

        let validity_arr = open_or_create_new_zarrs_array(
            &paths.validity(),
            store.clone(),
            self.array_shape.clone(),
            self.tile_shape.clone(),
            data_type::bool(),
            zarrs::array::FillValue::from(false),
            None,
        )
        .map_err(|source| Error::ZarrArrayCreateError { source })?;

        let metadata_arr = open_or_create_new_zarrs_array(
            &paths.metadata(),
            store.clone(),
            self.tile_counts,
            vec![1, 1],
            zarrs::array::data_type::bytes(),
            zarrs::array::FillValue::from(Vec::<u8>::new()),
            None,
        )
        .map_err(|source| Error::ZarrArrayCreateError { source })?;

        Ok(ZarrsArrayAccess {
            tiles: self_arr,
            validity: validity_arr,
            metadata: metadata_arr,
        })
    }
}

fn geotransform_to_attributes_map(transform: &GeoTransform) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    map.insert(
        GLOBAL_GEOTRANSFORM_KEY.to_string(),
        serde_json::to_value(transform).expect("it to be serialisable"),
    );
    map
}

/// using the `time_interval` in the `key` field does not work in all cases
/// there are instances where the tile has a different time_interval:
///
///**** [operators/src/cache/new_raster_cache/mod.rs:702:33] tile.time == time_interval = false
///**** [operators/src/cache/new_raster_cache/mod.rs:702:33] &tile.time = TimeInterval [1404172800000, 8210266876799999)
///**** [operators/src/cache/new_raster_cache/mod.rs:702:33] time_interval = TimeInterval [1417392000000, 1420070400000)
///
#[derive(Debug, Clone)]
struct ZarrsArrayPath {
    operator_name: CanonicOperatorName,
    band: Band,
    time_interval: TimeInterval,
}

impl From<&CacheKey> for ZarrsArrayPath {
    fn from(value: &CacheKey) -> Self {
        Self::from_cache_key(value)
    }
}

impl ZarrsArrayPath {
    pub fn from_cache_key((operator_name, band, time_interval, _): &CacheKey) -> Self {
        ZarrsArrayPath {
            operator_name: operator_name.clone(),
            band: *band,
            time_interval: *time_interval,
        }
    }

    pub fn band(&self) -> Band {
        self.band
    }

    pub fn operator_name(&self) -> CanonicOperatorName {
        self.operator_name.clone()
    }

    fn operator_name_path_safe(&self) -> String {
        self.operator_name().as_sha256()
    }

    #[inline]
    fn path(suffix: &str) -> String {
        format!("/{suffix}")
    }

    pub fn tiles(&self) -> String {
        Self::path("tiles")
    }

    pub fn metadata(&self) -> String {
        Self::path("metadata")
    }

    pub fn validity(&self) -> String {
        Self::path("validity")
    }

    /// Fails if the arrays where not created yet, or whenever [`zarrs:array::Array::open`] fails.
    fn open<TStore>(&self, store: Arc<TStore>) -> Result<ZarrsArrayAccess<TStore>>
    where
        TStore: ReadableStorageTraits + 'static,
    {
        let tiles_array = Array::open(store.clone(), &self.tiles())
            .map_err(|source| Error::ZarrArrayCreateError { source })?;
        let metadata_array = Array::open(store.clone(), &self.metadata())
            .map_err(|source| Error::ZarrArrayCreateError { source })?;
        let validity_array = Array::open(store.clone(), &self.validity())
            .map_err(|source| Error::ZarrArrayCreateError { source })?;

        Ok(ZarrsArrayAccess {
            tiles: tiles_array,
            validity: validity_array,
            metadata: metadata_array,
        })
    }
}

type TileArray<TStore> = zarrs::array::Array<TStore>;
type MetaDataStore<TStore> = zarrs::array::Array<TStore>;
type ValidityMaskStore<TStore> = zarrs::array::Array<TStore>;

/// Bundles the different [`FilesystemStore`]s into one struct, so that
/// the access is always well-defined.
struct ZarrsArrayAccess<TStore> {
    /// The store in which the actual tile data is stored
    tiles: TileArray<TStore>,
    /// the store in which the validity mask is stored
    validity: ValidityMaskStore<TStore>,
    /// the store in which the metadata relevant for a single tile is stored
    metadata: MetaDataStore<TStore>,
}

// TODO (low): more cleanup with using this!
impl<TStore> ZarrsArrayAccess<TStore>
where
    TStore: ReadableWritableStorageTraits + 'static,
{
    /// Retrieve a single metadata chunk.
    fn retrieve_metadata_chunk(
        &self,
        chunk_indices: &[u64],
    ) -> std::result::Result<ZarrsMetaDataChunk, zarrs::array::ArrayError> {
        self.metadata.retrieve_chunk(chunk_indices)
    }

    /// Retrieve a validity mask.
    fn retrieve_validity_mask(
        &self,
        chunk_indices: &[u64],
    ) -> std::result::Result<Vec<bool>, zarrs::array::ArrayError> {
        self.validity.retrieve_chunk(chunk_indices)
    }

    fn make_tile<T: Pixel + zarrs::array::ElementOwned>(
        &self,
        chunk_indices: &[u64],
        time: TimeInterval,
        tile_position: GridIdx2D,
        band: u32,
        global_geo_transform: GeoTransform,
        cache_hint: CacheHint,
        properties: RasterProperties,
    ) -> Result<RasterTile2D<T>> {
        let raw_data: Vec<T> = self
            .tiles
            .retrieve_chunk(chunk_indices)
            .map_err(|source| Error::ZarrArrayError { source })?;
        let validity_mask: Vec<bool> = self
            .retrieve_validity_mask(chunk_indices)
            .map_err(|source| Error::ZarrArrayError { source })?;
        let tile_shape = self.single_tile_shape(chunk_indices);
        let data = GridOrEmpty::new_grid(
            MaskedGrid2D::new(
                Grid::new(
                    // TODO (low): do we care about 32 bit systems at all? <- why is that even relevant?
                    tile_shape, raw_data,
                )
                .expect("it to have the correct shape"),
                Grid::new(tile_shape, validity_mask).expect("it to have the correct shape"),
            )
            .expect("it to be a valid input"),
        );
        Ok((RasterTile2D::new_with_properties(
            time,
            tile_position,
            band,
            global_geo_transform,
            data,
            properties,
            cache_hint,
        )))
    }

    /// Retrieve a tile.
    fn retrieve_tile(
        &self,
        chunk_indices: &[u64],
        paths: &ZarrsArrayPath,
        data_type: RasterDataType,
        cache_hint: CacheHint,
    ) -> Result<TypedRasterTile2D> {
        let metadata_chunk: ZarrsMetaDataChunk = self
            .retrieve_metadata_chunk(chunk_indices)
            .map_err(|source| Error::ZarrArrayError { source })?;
        let time = metadata_chunk.time_interval;
        let tile_position = metadata_chunk.tile_position;
        let band = paths.band();
        let global_geo_transform = self.read_global_geo_transform();

        macro_rules! dispatch {
            ($variant:ident, $ty:ty) => {{
                if metadata_chunk.is_no_data {
                    Ok(TypedRasterTile2D::$variant(
                        RasterTile2D::new_with_properties(
                            time,
                            tile_position,
                            band,
                            global_geo_transform,
                            // TODO (mid): do the tiles actually all have the same shape?
                            GridOrEmpty::new_empty(EmptyGrid::new(
                                self.single_tile_shape(chunk_indices),
                            )),
                            metadata_chunk.properties,
                            cache_hint,
                        ),
                    ))
                } else {
                    let inner_tile = self.make_tile::<$ty>(
                        chunk_indices,
                        time,
                        tile_position,
                        band,
                        global_geo_transform,
                        cache_hint,
                        metadata_chunk.properties,
                    )?;
                    Ok(TypedRasterTile2D::$variant(inner_tile))
                }
            }};
        }

        match data_type {
            RasterDataType::U8 => dispatch!(U8, u8),
            RasterDataType::U16 => dispatch!(U16, u16),
            RasterDataType::U32 => dispatch!(U32, u32),
            RasterDataType::U64 => dispatch!(U64, u64),
            RasterDataType::I8 => dispatch!(I8, i8),
            RasterDataType::I16 => dispatch!(I16, i16),
            RasterDataType::I32 => dispatch!(I32, i32),
            RasterDataType::I64 => dispatch!(I64, i64),
            RasterDataType::F32 => dispatch!(F32, f32),
            RasterDataType::F64 => dispatch!(F64, f64),
        }
    }

    fn store_tile_data_concrete<T: Pixel + zarrs::array::Element>(
        &self,
        data: Vec<T>,
        chunk_indices: &[u64],
    ) -> Result<usize> {
        let ret = data.byte_size(); // TODO (mid): figure out if this will return the correct size.
        self.tiles
            .store_chunk(chunk_indices, data)
            .map_err(|source| Error::ZarrArrayStoreError {
                which_tile: WhichTile::Tiles,
                source,
            })?;
        Ok(ret)
    }

    /// Store a tile.
    fn store_tile_v2(
        &self,
        tile_data: Option<TypedGrid2D>,
        chunk_indices: [u64; 2],
    ) -> Result<usize> {
        match tile_data {
            None => Ok(0),
            // TODO (low): {C482F320-772B-46FE-A6E5-0D7EE22B3AB4}
            //      make sure that the 0 is correct.
            //      (we dont write any tile (we dont need to) and at read side we initialize a NoDataTile)
            Some(typed_grid) => match typed_grid {
                TypedGrid::U8(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U16(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I8(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I16(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::F32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::F64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
            },
        }
    }

    /// Store a tile.
    fn store_tile(&self, tile: &TypedRasterTile2D, chunk_indices: [u64; 2]) -> Result<usize> {
        match typed_raster_tile_2d_to_typed_grid_2d_grid_array(tile) {
            None => Ok(0),
            // TODO (low): {C482F320-772B-46FE-A6E5-0D7EE22B3AB4}
            //      make sure that the 0 is correct.
            //      (we dont write any tile (we dont need to) and at read side we initialize a NoDataTile)
            Some(typed_grid) => match typed_grid {
                TypedGrid::U8(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U16(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::U64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I8(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I16(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::I64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::F32(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
                TypedGrid::F64(grid) => self.store_tile_data_concrete(grid.data, &chunk_indices),
            },
        }
    }

    // Store the validity mask.
    fn store_validity_mask_v2(
        &self,
        validity_mask: Option<Vec<bool>>,
        chunk_indices: [u64; 2],
    ) -> Result<usize> {
        if let Some(mask) = validity_mask {
            let ret = mask.byte_size();
            let () = self
                .validity
                .store_chunk(&chunk_indices, mask)
                .map_err(|source| Error::ZarrArrayStoreError {
                    which_tile: WhichTile::Validity,
                    source,
                })?;
            return Ok(ret);
        }
        return Ok(0); // TODO (low): see C482F320-772B-46FE-A6E5-0D7EE22B3AB4 , ef998d34-209e-4eb1-a8dc-30b6cf084e55
    }

    // Store the validity mask.
    fn store_validity_mask(
        &self,
        chunk_indices: [u64; 2],
        tile: &TypedRasterTile2D,
    ) -> Result<usize> {
        if let Some(mask) = typed_raster_tile_2d_to_grid_validity_mask(tile) {
            let ret = mask.data.byte_size();
            let () = self
                .validity
                .store_chunk(&chunk_indices, mask.data)
                .map_err(|source| Error::ZarrArrayStoreError {
                    which_tile: WhichTile::Validity,
                    source,
                })?;
            return Ok(ret);
        }
        return Ok(0); // TODO (low): see C482F320-772B-46FE-A6E5-0D7EE22B3AB4 , ef998d34-209e-4eb1-a8dc-30b6cf084e55
    }

    fn store_metadata_chunk_v2(
        &self,
        metadata_chunk: ZarrsMetaDataChunk,
        chunk_indices: [u64; 2],
    ) -> Result<()> {
        self.metadata
            .store_chunk(&chunk_indices, metadata_chunk)
            .map_err(|source| Error::ZarrArrayStoreError {
                which_tile: WhichTile::Metadata,
                source,
            })
    }

    fn store_metadata_chunk(
        &self,
        chunk_indices: [u64; 2],
        data: ZarrsMetaDataChunk,
    ) -> Result<()> {
        self.metadata
            .store_chunk(&chunk_indices, data)
            .map_err(|source| Error::ZarrArrayStoreError {
                which_tile: WhichTile::Metadata,
                source,
            })
    }

    fn single_tile_shape(self: &Self, chunk_indices: &[u64]) -> GridShape<[usize; 2]> {
        let single_tile_shape = self
            .tiles
            .chunk_shape(chunk_indices)
            .expect("it to be present"); // TODO (low): if this can even fail, handle it properly.
        [
            single_tile_shape[0].get() as usize,
            single_tile_shape[1].get() as usize,
        ]
        .into()
    }

    fn read_global_geo_transform(&self) -> GeoTransform {
        let arr: &Array<TStore> = &self.tiles;
        match arr.metadata() {
            zarrs::array::ArrayMetadata::V2(_) => {
                unreachable!("Reading metadata from V2 which is not supported.")
            }
            zarrs::array::ArrayMetadata::V3(meta) => {
                let val = meta
                    .attributes
                    .get(GLOBAL_GEOTRANSFORM_KEY)
                    .expect("the key to be present");
                serde_json::from_value(val.clone()).expect("it to be deserialisable")
            }
        }
    }
}

/// The fields from [`BaseTile`] which have to be stored in the metadata array.
/// These cannot be infered from other info (i.e. the cache key) and not stored globally (unlike the `global_geo_transform`).
///
/// These chunks are serialised as json and then stored in a special zarrs array under `.../metadata`.
/// The indicies are identical to the indicies of the tile data in the tiles array.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ZarrsMetaDataChunk {
    /// Same as tile.tile_position
    pub tile_position: GridIdx2D,
    /// The RasterProperties of one tile are not necessarily unique across tiles from one raster file.
    pub properties: RasterProperties,
    /// The time interval of the tile
    pub time_interval: TimeInterval,
    /// Whether the tile is a NoDataTile or not.
    /// Necessary for reads to not construct a tile filled with filelr values.
    pub is_no_data: bool,
}

impl<'a> IntoArrayBytes<'a> for ZarrsMetaDataChunk {
    fn into_array_bytes(
        self,
        data_type: &zarrs::array::DataType,
    ) -> std::result::Result<zarrs::array::ArrayBytes<'a>, zarrs::array::ElementError> {
        let self_as_json = serde_json::to_vec(&self).expect("it works");
        // one metadata chunk is exactly one element.
        let offsets = unsafe { ArrayBytesOffsets::new_unchecked(vec![0, self_as_json.len()]) };
        Ok(ArrayBytes::new_vlen(self_as_json, offsets).expect("it works"))
    }
}

impl FromArrayBytes for ZarrsMetaDataChunk {
    fn from_array_bytes(
        bytes: ArrayBytes<'static>,
        shape: &[u64],
        data_type: &zarrs::array::DataType,
    ) -> std::result::Result<Self, zarrs::array::ArrayError> {
        let variable = bytes
            .into_variable()
            .expect("it to be a fixed array by construction in IntoArrayBytes");
        Ok(serde_json::from_slice(variable.bytes()).expect("it to be valid metadata"))
    }
}

fn open_or_create_new_zarrs_array<TStore>(
    path: &str,
    store: Arc<TStore>,
    shape: impl Into<ArrayShape>, // The shape of the array
    chunk_shape: impl Into<ArrayBuilderChunkGridMetadata>, // Usually the shape of a chunk
    data_type: zarrs::array::DataType,
    filler_value: impl Into<ArrayBuilderFillValue>,
    attributes: Option<serde_json::Map<String, serde_json::Value>>,
) -> std::result::Result<Array<TStore>, ArrayCreateError>
where
    TStore: ReadableWritableStorageTraits + 'static,
{
    match Array::open(store.clone(), path) {
        Ok(array) => Ok(array),
        Err(_) => {
            let array = {
                let mut builder = ArrayBuilder::new(shape, chunk_shape, data_type, filler_value);
                if let Some(attrs) = attributes {
                    builder.attributes(attrs);
                }
                builder.build(store.clone(), path)?
            };
            array.store_metadata()?;
            Ok(array)
        }
    }
}

/// Opens or creates a new array at `/{operator_name}/band_{band}/{time}/tiles`.
/// Stores the metadata of the array if a new one is created.
fn build_array_and_prepare_insert<TStore>(
    key @ (operator_name, band, time_interval, tile_index): &CacheKey,
    paths: &ZarrsArrayPath,
    store: Arc<TStore>,
    tile: &TypedRasterTile2D,
    result_descriptor: &RasterResultDescriptor,
) -> Result<ZarrsArrayAccess<TStore>>
where
    TStore: ReadableWritableStorageTraits + 'static,
{
    let paths = ZarrsArrayPath::from_cache_key(key);
    let attributes = geotransform_to_attributes_map(
        &call_generic_raster_tile_2d!(tile, tile => tile.global_geo_transform),
    );

    // also the chunk shape
    let tile_shape = {
        let [tile_height, tile_width] =
            call_generic_raster_tile_2d!(tile, tile => tile.grid_shape_array());
        [tile_height as u64, tile_width as u64]
    };
    let tile_bounds = raster_tile_bounds(
        result_descriptor,
        call_generic_raster_tile_2d!(tile, tile => tile.global_geo_transform),
        call_generic_raster_tile_2d!(tile, tile => tile.grid_shape_array()).into(),
    );
    let mut tile_counts = [0; 2];
    let mut array_shape = [0; 2];
    for axis in 0..2 {
        let count = tile_bounds.max_index().as_slice()[axis] as i128
            - tile_bounds.min_index().as_slice()[axis] as i128
            + 1;
        tile_counts[axis] = u64::try_from(count).map_err(|_| Error::InvalidZarrTileIndex {
            reason: "array tile count exceeds u64",
        })?;
        array_shape[axis] =
            tile_counts[axis]
                .checked_mul(tile_shape[axis])
                .ok_or(Error::InvalidZarrTileIndex {
                    reason: "array pixel extent exceeds u64",
                })?;
    }

    let data_arr = open_or_create_new_zarrs_array(
        &paths.tiles(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        zarrs_data_type(tile),
        filler_value(tile),
        Some(attributes),
    )
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    let validity_arr = open_or_create_new_zarrs_array(
        &paths.validity(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        data_type::bool(),
        zarrs::array::FillValue::from(false),
        None,
    )
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    let metadata_arr = open_or_create_new_zarrs_array(
        &paths.metadata(),
        store.clone(),
        tile_counts,
        vec![1, 1],
        zarrs::array::data_type::bytes(),
        zarrs::array::FillValue::from(Vec::<u8>::new()),
        None,
    )
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    Ok(ZarrsArrayAccess {
        tiles: data_arr,
        validity: validity_arr,
        metadata: metadata_arr,
    })
}

// TODO (mid): Are those NAN / filler values actually the right ones? Should they be made configurable?
// Zarrs requires us to specify these!
fn filler_value(tile: &TypedRasterTile2D) -> ArrayBuilderFillValue {
    match tile {
        TypedRasterTile2D::I8(_)
        | TypedRasterTile2D::I16(_)
        | TypedRasterTile2D::I32(_)
        | TypedRasterTile2D::I64(_)
        | TypedRasterTile2D::U8(_)
        | TypedRasterTile2D::U16(_)
        | TypedRasterTile2D::U32(_)
        | TypedRasterTile2D::U64(_) => 0.into(),
        TypedRasterTile2D::F32(_) => f32::NAN.into(),
        TypedRasterTile2D::F64(_) => f64::NAN.into(),
    }
}

fn zarrs_data_type(tile: &TypedRasterTile2D) -> zarrs::array::DataType {
    use zarrs::array;
    match tile {
        TypedRasterTile2D::I8(base_tile) => data_type::int8(),
        TypedRasterTile2D::I16(base_tile) => data_type::int16(),
        TypedRasterTile2D::I32(base_tile) => data_type::int32(),
        TypedRasterTile2D::I64(base_tile) => data_type::int64(),
        TypedRasterTile2D::U8(base_tile) => data_type::uint8(),
        TypedRasterTile2D::U16(base_tile) => data_type::uint16(),
        TypedRasterTile2D::U32(base_tile) => data_type::uint32(),
        TypedRasterTile2D::U64(base_tile) => data_type::uint64(),
        TypedRasterTile2D::F32(base_tile) => data_type::float32(),
        TypedRasterTile2D::F64(base_tile) => data_type::float64(),
    }
}

fn format_cache_key(root: PathBuf, paths: &ZarrsArrayPath) -> PathBuf {
    let (operator_name, band, interval) = (&paths.operator_name, paths.band, paths.time_interval);
    let time = time_interval_to_path_safe_iso(&interval);
    let op = paths.operator_name_path_safe();
    root.join(op).join(format!("band_{band}")).join(time)
}

const ZARR_CACHE: &str = "zarr_cache";

// TODO (mid): check the callsites how they handle this
/// Opens a new ZarrArrayStore at "{op}/band_{band}/{time}"
fn open_zarrs_filesystemstore(paths: &ZarrsArrayPath) -> Result<Arc<FilesystemStore>> {
    let cache_dir = cache_dir();
    let path = format_cache_key(cache_dir.join(ZARR_CACHE), &paths);
    let zarrs = zarrs::filesystem::FilesystemStore::new(path)
        // TODO (mid): check the callsites how they handle this
        .map_err(|err| Error::ZarrFilesystemError { source: err })?;
    Ok(Arc::new(zarrs))
}

/// Returns the inclusive global `[y, x]` tile-index bounds intersecting the raster.
/// Partial edge tiles are included; indices may be negative and are not zarr-chunk indices.
fn raster_tile_bounds(
    result_descriptor: &RasterResultDescriptor,
    global_geo_transform: GeoTransform,
    tile_shape: GridShape2D,
) -> GridBoundingBox2D {
    let raster_bounds = result_descriptor
        .spatial_grid_descriptor()
        .spatial_partition();
    let pixel_bounds = global_geo_transform.spatial_to_grid_bounds(&raster_bounds);
    TilingStrategy::new(tile_shape, global_geo_transform)
        .global_pixel_grid_bounds_to_tile_grid_bounds(pixel_bounds)
}

/// Maps a global tile position to local Zarr chunk indices relative to the first tile.
///
/// The formular is: `tile_position - first_tile_position`, this accounts for a different origin.
fn tile_position_to_chunk_indices(
    tile_position: GridIdx2D,
    first_tile_position: GridIdx2D,
) -> Result<[u64; 2]> {
    let mut chunk_indices = [0; 2];
    for axis in 0..2 {
        let offset =
            tile_position.as_slice()[axis] as i128 - first_tile_position.as_slice()[axis] as i128;
        chunk_indices[axis] = u64::try_from(offset).map_err(|_| Error::InvalidZarrTileIndex {
            reason: "tile precedes the array tile origin or its offset exceeds u64",
        })?;
    }
    Ok(chunk_indices)
}

/// Where the error was at
#[derive(Clone, Debug)]
#[repr(u8)]
enum CompletedZarrWrite {
    Tile = 1 << 0,
    Validity = 1 << 1,
    Metadata = 1 << 2,
}

struct ZarrChunkWriter<'a, TStore> {
    arrays: &'a ZarrsArrayAccess<TStore>,
    attempted_writes: u8, // = Combination of [`CompletedZarrWrites`]
}

impl<'a, TStore> ZarrChunkWriter<'a, TStore>
where
    TStore: ReadableWritableStorageTraits + 'static,
{
    fn new(arrays: &'a ZarrsArrayAccess<TStore>) -> Self {
        ZarrChunkWriter {
            arrays,
            attempted_writes: 0,
        }
    }

    fn mark_attempted(&mut self, write: CompletedZarrWrite) {
        self.attempted_writes |= write as u8;
    }

    fn attempted(&self) -> Vec<CompletedZarrWrite> {
        let mut attempted = Vec::new();
        if self.attempted_writes & (CompletedZarrWrite::Tile as u8) != 0 {
            attempted.push(CompletedZarrWrite::Tile);
        }
        if self.attempted_writes & (CompletedZarrWrite::Validity as u8) != 0 {
            attempted.push(CompletedZarrWrite::Validity);
        }
        if self.attempted_writes & (CompletedZarrWrite::Metadata as u8) != 0 {
            attempted.push(CompletedZarrWrite::Metadata);
        }
        attempted
    }

    fn store_tile_chunks_v2(
        &mut self,
        tile: ZarrTileData,
        chunk_indices: [u64; 2],
    ) -> Result<(usize, usize)> {
        let ZarrTileData {
            tile_grid,
            validity_mask,
            zarr_metadata_chunk,
        } = tile;
        self.mark_attempted(CompletedZarrWrite::Tile);
        let byte_size_tile_data = self.arrays.store_tile_v2(tile_grid, chunk_indices)?;

        self.mark_attempted(CompletedZarrWrite::Validity);
        let byte_size_tile_validity = self
            .arrays
            .store_validity_mask_v2(validity_mask, chunk_indices)?;

        self.mark_attempted(CompletedZarrWrite::Metadata);
        let () /* TODO (crit): this must return the correct bytes {ef998d34-209e-4eb1-a8dc-30b6cf084e55} */
                = self.arrays.store_metadata_chunk_v2(
                    zarr_metadata_chunk,
                    chunk_indices,
                )?;

        Ok((byte_size_tile_data, byte_size_tile_validity)) // TODO (crit): {ef998d34-209e-4eb1-a8dc-30b6cf084e55}
    }

    fn store_tile_chunks(
        &mut self,
        tile: &TypedRasterTile2D,
        chunk_indices: [u64; 2],
    ) -> Result<(usize, usize)> {
        self.mark_attempted(CompletedZarrWrite::Tile);
        let byte_size_tile_data = self.arrays.store_tile(tile, chunk_indices)?;

        self.mark_attempted(CompletedZarrWrite::Validity);
        let byte_size_tile_validity = self.arrays.store_validity_mask(chunk_indices, tile)?;

        self.mark_attempted(CompletedZarrWrite::Metadata);
        let () /* TODO (crit): this must return the correct bytes {ef998d34-209e-4eb1-a8dc-30b6cf084e55} */ = self.arrays.store_metadata_chunk(
                chunk_indices,
                ZarrsMetaDataChunk {
                    tile_position: call_generic_raster_tile_2d!(tile, tile => tile
                            .tile_position),
                    properties: call_generic_raster_tile_2d!(tile, tile => tile
                            .properties
                            .clone()),
                    time_interval: call_generic_raster_tile_2d!(tile, tile => tile
                            .time
                            .clone()),
                    is_no_data: call_generic_raster_tile_2d!(tile, tile => tile
                            .grid_array
                            .is_empty()),
                },
            )?;

        Ok((byte_size_tile_data, byte_size_tile_validity)) // TODO (crit): {ef998d34-209e-4eb1-a8dc-30b6cf084e55}
    }
}

// used for testing etc
// remove once everything is finalised
#[cfg(test)]
fn compare_tiles(tile: &TypedRasterTile2D, loaded_tile: &TypedRasterTile2D) {
    let b = match (tile, loaded_tile) {
        (TypedRasterTile2D::I8(t), TypedRasterTile2D::I8(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::I16(t), TypedRasterTile2D::I16(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::I32(t), TypedRasterTile2D::I32(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::I64(t), TypedRasterTile2D::I64(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::U8(t), TypedRasterTile2D::U8(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::U16(t), TypedRasterTile2D::U16(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::U32(t), TypedRasterTile2D::U32(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::U64(t), TypedRasterTile2D::U64(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::F32(t), TypedRasterTile2D::F32(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        (TypedRasterTile2D::F64(t), TypedRasterTile2D::F64(t2)) => {
            t.tiles_equal_ignoring_cache_hint(t2)
        }
        _ => unreachable!(),
    };
    let ld = loaded_tile.clone();
    assert!(
        b,
        "Actual tile {tile:?} and loaded_tile {ld:?} are different."
    );
}

#[cfg(test)]
#[path = "tile_position_tests.rs"]
mod tile_position_tests;

#[cfg(test)]
mod tests {

    use crate::cache::new_raster_cache::on_disk::set_raster_cache_dir;
    use crate::cache::new_raster_cache::on_disk::tests::test_key;
    use crate::cache::new_raster_cache::on_disk::zarrs_cache::format_cache_key;
    use crate::engine::{RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{primitives::DateTime, spatial_reference::SpatialReference};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn it_checks_metadata_as_json() {
        let key = StoreKey::new("tiles/zarr.json").unwrap();
        let expected = br#"{"shape":[512,512],"data_type":"uint8"}"#;
        let reordered = br#"{ "data_type": "uint8", "shape": [512, 512] }"#;

        check_metadata_matches(&key, reordered, expected).unwrap();

        assert!(matches!(
            check_metadata_matches(
                &key,
                br#"{"shape":[256,512],"data_type":"uint8"}"#,
                expected,
            ),
            Err(Error::ZarrCacheMetadataMismatch { key: actual_key })
                if actual_key == key.as_str()
        ));

        for (actual, expected) in [
            (b"invalid".as_slice(), expected.as_slice()),
            (expected.as_slice(), b"invalid".as_slice()),
            (b"invalid".as_slice(), b"invalid".as_slice()),
        ] {
            assert!(matches!(
                check_metadata_matches(&key, actual, expected),
                Err(Error::SerdeJson { .. })
            ));
        }
    }

    fn set_tempdir() -> TempDir {
        let directory = tempfile::tempdir().unwrap();
        set_raster_cache_dir(directory.path().to_path_buf()).unwrap();
        directory
    }

    mod indexing {

        use super::*;
        use crate::cache::new_raster_cache::on_disk::set_raster_cache_dir;
        use crate::engine::{RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor};
        use geoengine_datatypes::{raster::BoundedGrid, spatial_reference::SpatialReference};

        #[test]
        fn tile_positions_to_chunks_at_zero_origin() {
            let origin = [0, 0].into();

            assert_eq!(
                tile_position_to_chunk_indices([0, 0].into(), origin).unwrap(),
                [0, 0]
            );
            assert_eq!(
                tile_position_to_chunk_indices([0, 1].into(), origin).unwrap(),
                [0, 1]
            );
            assert_eq!(
                tile_position_to_chunk_indices([1, 0].into(), origin).unwrap(),
                [1, 0]
            );
            assert_eq!(
                tile_position_to_chunk_indices([1, 1].into(), origin).unwrap(),
                [1, 1]
            );
        }

        #[test]
        fn tile_position_to_chunk_with_offset() {
            assert_eq!(
                tile_position_to_chunk_indices([12, 23].into(), [10, 20].into()).unwrap(),
                [2, 3]
            );
        }

        #[test]
        fn negative_global_tile_positions_map_to_nonnegative_chunks() {
            assert_eq!(
                tile_position_to_chunk_indices([-1, -1].into(), [-2, -2].into()).unwrap(),
                [1, 1]
            );
        }

        #[test]
        fn tile_positions_before_array_origin_return_errors() {
            let origin = [0, 0].into();

            assert!(tile_position_to_chunk_indices([-1, 0].into(), origin).is_err());
            assert!(tile_position_to_chunk_indices([0, -1].into(), origin).is_err());
        }

        fn first_chunk_tile_origin(raster_upper_left: [isize; 2]) -> GridIdx2D {
            let geo_transform = GeoTransform::new([10., 20.].into(), 0.5, -0.5);
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    geo_transform,
                    GridBoundingBox2D::new_unchecked(
                        raster_upper_left,
                        [raster_upper_left[0] + 3, raster_upper_left[1] + 5],
                    ),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            };
            raster_tile_bounds(&descriptor, geo_transform, [2, 3].into()).min_index()
        }

        #[test]
        fn first_chunk_origin_handles_negative_pixel_positions() {
            assert_eq!(first_chunk_tile_origin([-3, -5]), [-2, -2].into());
        }

        #[test]
        fn first_chunk_origin_aligns_down_to_tile_boundary() {
            assert_eq!(first_chunk_tile_origin([1, 2]), [0, 0].into());
            assert_eq!(first_chunk_tile_origin([3, 5]), [1, 1].into());
        }

        async fn assert_store_uses_pixel_positions(
            world_origin: Coordinate2D,
            pixel_origin: [isize; 2],
        ) {
            let directory = set_tempdir();
            let geo_transform = GeoTransform::new(world_origin, 0.5, -0.5);
            let time = TimeInterval::new(0, 1).unwrap();
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: TimeDescriptor::new_irregular(Some(time)),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    geo_transform,
                    GridBoundingBox2D::new_unchecked(
                        pixel_origin,
                        [pixel_origin[0] + 3, pixel_origin[1] + 5],
                    ),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            };
            let mut stored_tiles = Vec::new();

            for (tile_position, pixel_position, value) in [
                ([0, 0], [0, 0], 1_u8),
                ([0, 1], [0, 3], 2),
                ([1, 0], [2, 0], 3),
                ([1, 1], [2, 3], 4),
            ] {
                let global_tile_position = [
                    tile_position[0] + pixel_origin[0] / 2,
                    tile_position[1] + pixel_origin[1] / 3,
                ];
                let global_pixel_position = [
                    pixel_position[0] + pixel_origin[0],
                    pixel_position[1] + pixel_origin[1],
                ];
                let tile = TypedRasterTile2D::U8(RasterTile2D::new(
                    time,
                    global_tile_position.into(),
                    0,
                    geo_transform,
                    Grid2D::new([2, 3].into(), vec![value; 6]).unwrap().into(),
                    CacheHint::max_duration(),
                ));
                let key: Arc<(CanonicOperatorName, Band, TimeInterval, TileIndex)> =
                    Arc::new((test_key(0).0.clone(), 0, time, global_pixel_position.into()));
                let stored = PreparedZarrStorageFormat::store(tile.clone(), key, &descriptor)
                    .await
                    .expect("store tile at its upper-left pixel position");

                assert_eq!(
                    stored.chunk_indices,
                    [tile_position[0] as u64, tile_position[1] as u64]
                );
                stored_tiles.push((tile, stored, value));
            }

            for (tile, stored, value) in stored_tiles {
                let arrays: ZarrsArrayAccess<FilesystemStore> = stored
                    .paths
                    .open(open_zarrs_filesystemstore(&stored.paths).unwrap())
                    .unwrap();
                let pixels: Vec<u8> = arrays.tiles.retrieve_chunk(&stored.chunk_indices).unwrap();
                assert_eq!(pixels, vec![value; 6]);
                compare_tiles(&tile, &stored.load().await.unwrap());
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn store_maps_pixel_positions_to_distinct_chunks() {
            assert_store_uses_pixel_positions([0., 0.].into(), [0, 0]).await;
        }

        #[tokio::test(flavor = "current_thread")]
        async fn store_does_not_treat_world_origin_as_pixel_offset() {
            assert_store_uses_pixel_positions([10., 20.].into(), [0, 0]).await;
        }

        #[tokio::test(flavor = "current_thread")]
        async fn store_and_load_preserve_negative_global_tile_positions() {
            assert_store_uses_pixel_positions([10., 20.].into(), [-4, -6]).await;
        }

        #[tokio::test(flavor = "current_thread")]
        async fn array_extent_includes_tiles_intersecting_unaligned_raster_bounds() {
            let directory = set_tempdir();
            let geo_transform = GeoTransform::new([10., 20.].into(), 0.5, -0.5);
            let time = TimeInterval::new(0, 1).unwrap();
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: TimeDescriptor::new_irregular(Some(time)),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    geo_transform,
                    GridBoundingBox2D::new([1, 2], [4, 7]).unwrap(),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            };
            let tile = RasterTile2D::new(
                time,
                [2, 2].into(),
                0,
                geo_transform,
                Grid2D::new([2, 3].into(), vec![1_u8; 6]).unwrap().into(),
                CacheHint::max_duration(),
            );
            let key = Arc::new((
                test_key(0).0.clone(),
                0,
                time,
                tile.tile_information().global_upper_left_pixel_idx(),
            ));
            let stored =
                PreparedZarrStorageFormat::store(TypedRasterTile2D::U8(tile), key, &descriptor)
                    .await
                    .unwrap();
            let arrays: ZarrsArrayAccess<FilesystemStore> = stored
                .paths
                .open(open_zarrs_filesystemstore(&stored.paths).unwrap())
                .unwrap();
            assert_eq!(stored.chunk_indices, [2, 2]);
            assert_eq!(arrays.tiles.shape(), &[6, 9]);
            assert_eq!(arrays.validity.shape(), &[6, 9]);
            assert_eq!(arrays.metadata.shape(), &[3, 3]);
        }
    }

    mod cache_hint {

        use super::*;

        fn set_up() -> (
            CacheHint,
            RasterTile2D<u8>,
            RasterResultDescriptor,
            Arc<CacheKey>,
        ) {
            let cache_hint = CacheHint::with_created_and_expires(
                "2024-01-01T12:34:56.123Z".parse::<DateTime>().unwrap(),
                "2024-01-02T12:34:56.789Z"
                    .parse::<DateTime>()
                    .unwrap()
                    .into(),
            );
            let time = TimeInterval::new(0, 1).unwrap();
            let geo_transform = GeoTransform::new([0., 0.].into(), 1., -1.);
            let descriptor = RasterResultDescriptor {
                data_type: RasterDataType::U8,
                spatial_reference: SpatialReference::epsg_4326().into(),
                time: TimeDescriptor::new_irregular(Some(time)),
                spatial_grid: SpatialGridDescriptor::source_from_parts(
                    geo_transform,
                    GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
                ),
                bands: RasterBandDescriptors::new_single_band(),
            };
            let tile = RasterTile2D::new(
                time,
                [0, 0].into(),
                0,
                geo_transform,
                Grid2D::new([2, 2].into(), vec![42_u8; 4]).unwrap().into(),
                cache_hint,
            );
            let key = Arc::new((test_key(0).0.clone(), 0, time, [0, 0].into()));
            (cache_hint, tile, descriptor, key)
        }

        #[tokio::test(flavor = "current_thread")]
        async fn cache_hint_is_stored_correctly() {
            let directory = set_tempdir();
            let (cache_hint, tile, descriptor, key) = set_up();
            let stored = PreparedZarrStorageFormat::store(
                TypedRasterTile2D::U8(tile.clone()),
                key,
                &descriptor,
            )
            .await
            .expect("tile should be stored with its cache hint");

            assert_eq!(stored.common.cache_hint, cache_hint);
            let TypedRasterTile2D::U8(loaded) =
                stored.load().await.expect("stored tile should load")
            else {
                panic!("expected a U8 tile");
            };
            assert_eq!(loaded.cache_hint, cache_hint);
            assert_eq!(loaded, tile);
        }

        #[tokio::test(flavor = "current_thread")]
        async fn expired_cache_hint() {
            set_tempdir();
            let (cache_hint, tile, descriptor, key) = set_up();
            assert!(cache_hint.is_expired());
            let stored = PreparedZarrStorageFormat::store(
                TypedRasterTile2D::U8(tile.clone()),
                key,
                &descriptor,
            )
            .await
            .expect("tile should be stored with its cache hint");
            assert!(stored.is_expired().await);
        }
    }

    pub(crate) fn setup() -> (
        GeoTransform,
        TypedRasterTile2D,
        Arc<CacheKey>,
        RasterResultDescriptor,
    ) {
        let geo_transform = GeoTransform::new([0., 0.].into(), 1., -1.);
        let tile = {
            TypedRasterTile2D::U8(RasterTile2D::new_with_properties(
                TimeInterval::default(),
                [0, 0].into(),
                0,
                geo_transform,
                GridOrEmpty::new_grid(
                    MaskedGrid2D::new(
                        Grid::new([2, 2].into(), vec![1_u8, 2, 3, 4]).unwrap(),
                        Grid::new([2, 2].into(), vec![true; 4]).unwrap(),
                    )
                    .unwrap(),
                ),
                RasterProperties::default(),
                CacheHint::max_duration(),
            ))
        };
        let key = test_key(0);
        let result_descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                geo_transform,
                GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };
        (geo_transform, tile, key, result_descriptor)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_test_zarr_storage_format_v2() {
        set_tempdir();
        let (geo_transform, tile, key, result_descriptor) = setup();
        let storage_format =
            PreparedZarrStorageFormat::store(tile.clone(), key, &result_descriptor)
                .await
                .expect("store should work");
        let disk_entry = storage_format.write().await.expect("write should work");
        let mut tiles = Vec::with_capacity(10);
        for _ in (0..10) {
            let loaded = disk_entry.read().await.expect("read should work");
            tiles.push(loaded);
        }
        tiles.iter().for_each(|loaded| compare_tiles(&tile, loaded));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_store_load_with_new_zarr_storage_format_v2() {
        let (geo_transform, tile, key, result_descriptor) = setup();
        let storage_format =
            PreparedZarrStorageFormat::store(tile.clone(), key, &result_descriptor)
                .await
                .expect("store should work");
        let loaded = storage_format.load().await.expect("load should work");
        compare_tiles(&tile, &loaded);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zarrs_failed_write_removes_partial_writes() {
        let directory = set_tempdir();
        let key = test_key(0);
        let geo_transform = GeoTransform::new([0., 0.].into(), 1., -1.);
        let descriptor = RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                geo_transform,
                GridBoundingBox2D::new([0, 0], [1, 1]).unwrap(),
            ),
            bands: RasterBandDescriptors::new_single_band(),
        };

        let array_path = &ZarrsArrayPath::from_cache_key(&key);
        let store = open_zarrs_filesystemstore(&array_path).unwrap();

        let _array_access = build_array_and_prepare_insert(
            &key,
            &array_path,
            store,
            &TypedRasterTile2D::U8(RasterTile2D::new(
                TimeInterval::new(0, 1).unwrap(),
                [0, 0].into(),
                0,
                geo_transform,
                Grid2D::new([2, 2].into(), vec![42_u8; 4]).unwrap().into(),
                CacheHint::max_duration(),
            )),
            &descriptor,
        )
        .unwrap();

        /*
        The cache is at:

            CACHE_DIR/zarr_cache/

        In that directory the store writes multiple arrays for one set of tiles.
        The writes are attempted in this order:
            1. tiles
            2. validity
            3. metadata
        Zarr will write the chunks at:

            CACHE_DIR/zarr_cache/.../{tiles, validity, metadata}/c/dim1_key/dim2_key
            where ... is some other stuff related to the cache key  that is not relevant here.

        Eg. if chunk_index = [1,2] for 'tiles', then:
            CACHE_DIR/zarr_cache/tiles/c/1/2

        This test will place a regular file 'c' at
            CACHE_DIR/zarr_cache/metadata/c
        to prevent the writer from creating this chunk directory, thus failing the write.
        */
        let root = directory.path().join(ZARR_CACHE);
        let tiles_path = root.join(array_path.tiles().trim_start_matches("/"));
        let validity_path = root.join(array_path.validity().trim_start_matches("/"));
        let metadata_path = root.join(array_path.metadata().trim_start_matches("/"));
        let blocker_file = metadata_path.join("c");
        let () = std::fs::write(blocker_file, "You shall not pass!").unwrap();
        assert!(metadata_path.join("c").exists());

        let tile: TypedRasterTile2D = RasterTile2D::new(
            TimeInterval::default(),
            [0, 0].into(),
            0,
            descriptor.spatial_grid.geo_transform(),
            Grid2D::new([2, 2].into(), vec![42_u8; 4]).unwrap().into(),
            CacheHint::max_duration(),
        )
        .into();
        let result = PreparedZarrStorageFormat::store(tile.clone(), key.clone(), &descriptor).await;

        assert!(matches!(
            result,
            Err(crate::error::Error::ZarrArrayStoreError {
                which_tile: WhichTile::Metadata,
                ..
            })
        ));

        // both should not exist
        assert!(!tiles_path.join("c/0/0").exists());
        assert!(!validity_path.join("c/0/0").exists());

        // zarr internal metadata is kept
        assert!(tiles_path.join("zarr.json").exists());
        assert!(validity_path.join("zarr.json").exists());
        assert!(metadata_path.join("zarr.json").exists());

        // removing the blocker, should allow the write
        std::fs::remove_file(metadata_path.join("c")).unwrap();
        let stored = PreparedZarrStorageFormat::store(tile.clone(), key, &descriptor)
            .await
            .expect("tile should be stored after removing blocker file");
        compare_tiles(
            &tile,
            &stored.load().await.expect("stored tile should load"),
        );
    }

    #[test]
    fn zarrs_array_path_operator_name_pathable() {
        let path = ZarrsArrayPath::from_cache_key(&test_key(0));
        let tiles_path = path.operator_name_path_safe();
        assert_eq!(
            "05643c4ac1135b60c1e9b65c47fc77d7ccd550b05485dc4f5597ac3d067f065d".to_string(),
            tiles_path
        );
    }

    #[test]
    fn get_tiles_path() {
        let path = ZarrsArrayPath::from_cache_key(&test_key(0));
        let tiles_path = path.tiles();
        assert_eq!("/tiles".to_string(), tiles_path);
    }

    #[test]
    fn get_metadata_path() {
        let path = ZarrsArrayPath::from_cache_key(&test_key(0));
        let metadata_path = path.metadata();
        assert_eq!("/metadata".to_string(), metadata_path);
    }

    #[test]
    fn get_validity_path() {
        let path = ZarrsArrayPath::from_cache_key(&test_key(0));
        let validity_path = path.validity();
        assert_eq!("/validity".to_string(), validity_path);
    }

    #[test]
    fn test_format_cache_key() {
        let directory = set_tempdir();

        let key = test_key(0);
        let path = ZarrsArrayPath::from_cache_key(&key);
        let pathbuf: String = format_cache_key(cache_dir().join(ZARR_CACHE), &path)
            .to_str()
            .unwrap()
            .to_string();
        let tmp = directory.path().display();
        assert_eq!(
            format!(
                "{tmp}/{ZARR_CACHE}/05643c4ac1135b60c1e9b65c47fc77d7ccd550b05485dc4f5597ac3d067f065d/band_0/-262143-01-01T00-00-00Z_to_+262142-12-31T23-59-59.999Z"
            ),
            pathbuf
        );
    }

    #[test]
    fn metadata_and_tiles_are_valid_node_paths() {
        let paths = ZarrsArrayPath::from_cache_key(&test_key(0));
        let _ =
            zarrs::node::NodePath::try_from(paths.tiles().as_str()).expect("it to be a valid path");
        let _ = zarrs::node::NodePath::try_from(paths.metadata().as_str())
            .expect("it to be a valid path");
    }
}
