#![allow(unused)]
#[cfg(test)]
use std::sync::LazyLock;
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
};

use crate::{
    cache::{self, new_raster_cache::*},
    call_generic_typed_raster_tile_2d_cache,
    engine::OperatorName,
    error::{
        Error,
        WhichTile::{Metadata, Tiles, Validity},
    },
    util::gdal::create_ndvi_meta_data,
};

const GLOBAL_GEOTRANSFORM_KEY: &'static str = "global_geo_transform";

pub struct OnDiskStore<SF: StorageFormat, ES: EvictionStrategy> {
    pub cache: RwLock<HashMap<Arc<CacheKey>, Arc<SF>>>,
    pub eviction_strategy: RwLock<ES>,
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
        self.inner
            .take()
            .expect("taking the inner value of a pending file should be valid")
    }
}

impl<SF: OnDiskStorageFormat> Deref for PendingFile<SF> {
    type Target = SF;
    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("deref the inner value of a pending file to the storage format.")
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
    async fn write(
        tile: TypedRasterTile2D,
        key: Arc<CacheKey>,
        result_descriptor: &RasterResultDescriptor,
    ) -> Result<PendingFile<Self>> {
        let store = Self::store(tile, key, result_descriptor).await?;
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

    async fn get(&self, key: &CacheKey) -> Result<Option<Arc<Self::SF>>> {
        let x = self.cache.read().await.get(key).map(Arc::clone);
        match x {
            Some(key) => Ok(Some(key)),
            None => Ok(None),
        }
    }

    // TODO (crit): this function probably not right anymore, the base has changed a lot. fix it! after rebase
    async fn insert(
        &self,
        key: Arc<CacheKey>,
        tile: TypedRasterTile2D,
        raster_result_descriptor: /* TODO (crit): must be used */ &RasterResultDescriptor,
    ) -> Result<()> {
        /*
        We need to make sure that the file is not just written to the disk
        without actually being inserted in the cache / index later on.
        Otherwise we will have orphaned files.
        */
        let stored_tile = SF::write(tile, key.clone(), raster_result_descriptor).await?;
        let required_space = stored_tile.byte_size().await?;

        let mut cache = self.cache.write().await;
        let mut eviction_strategy = self.eviction_strategy.write().await;
        let eviction_plan = eviction_strategy.plan_eviction(required_space, usize::MAX, |key| {
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
    tile: TypedRasterTile2D, // TODO (low): make this optional and implement proper ways of not having it in RAM
    /// The cached bytesize of the file ins storage
    byte_size: usize,
    /// the datatype of the TypedRasterTile2D
    data_type: RasterDataType,
}

// How it is stored:
// The grid is stored as raw bytes (gotta figure out how, exactly)
// the rest is stored as metadata in the zarr file / zarr.json
pub struct ZarrsStorageFormat {
    paths: ZarrsArrayPath,
    tile: TypedRasterTile2D, // TODO (low): make this optional and implement proper ways of not having it in RAM
    data_type: RasterDataType,
    byte_size: usize,
    chunk_indices: [u64; 2],
    // key: CacheKey,
    // operator? path? group?
}

/// using the `time_interval` in the `key` field does not work in all cases
/// there are instances where the tile has a different time_interval:
///
///     [operators/src/cache/new_raster_cache/mod.rs:702:33] tile.time == time_interval = false
///     [operators/src/cache/new_raster_cache/mod.rs:702:33] &tile.time = TimeInterval [1404172800000, 8210266876799999)
///     [operators/src/cache/new_raster_cache/mod.rs:702:33] time_interval = TimeInterval [1417392000000, 1420070400000)
///
#[derive(Debug)]
struct ZarrsArrayPath {
    // /// the path to the tiles array
    // pub tiles: String,
    // /// the path to the metadata array
    // pub metadata: String,
    key: (CanonicOperatorName, Band, TimeInterval),
}

impl ZarrsArrayPath {
    pub fn new_from_cache_key((operator_name, band, time_interval, _): &CacheKey) -> Self {
        ZarrsArrayPath {
            key: (operator_name.clone(), band.clone(), time_interval.clone()),
        }
    }

    pub fn band(&self) -> Band {
        self.key.1
    }

    pub fn operator_name(&self) -> CanonicOperatorName {
        self.key.0.clone()
    }

    fn operator_name_pathable(&self) -> String {
        self.operator_name().as_sha256()
    }

    pub fn tiles(&self) -> String {
        let (operator_name, band, interval) = &self.key;
        let time = time_interval_to_path_safe_iso(interval);
        let op = self.operator_name_pathable();
        format!("/{op}/band_{band}/{time}/tiles")
    }

    pub fn metadata(&self) -> String {
        let (operator_name, band, interval) = &self.key;
        let time = time_interval_to_path_safe_iso(interval);
        let op = self.operator_name_pathable();
        format!("/{op}/band_{band}/{time}/metadata")
    }

    pub fn validity(&self) -> String {
        let (operator_name, band, interval) = &self.key;
        let time = time_interval_to_path_safe_iso(interval);
        let op = self.operator_name_pathable();
        format!("/{op}/band_{band}/{time}/validity")
    }

    fn open(&self, store: Arc<FilesystemStore>) -> Result<ZarrsArrayAccess> {
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

type TileStore = zarrs::array::Array<FilesystemStore>;
type MetaDataStore = zarrs::array::Array<FilesystemStore>;
type ValidityMaskStore = zarrs::array::Array<FilesystemStore>;

/// This also
struct ZarrsArrayAccess {
    /// This also stores [`BaseTile::global_geo_transform`] inside the zarrs metadata
    pub tiles: TileStore,
    pub validity: ValidityMaskStore,
    pub metadata: MetaDataStore,
}

// TODO (mid): refactor the other functions to use this instead
impl ZarrsArrayAccess {
    fn store_chunk(&self) {}
    fn retreive_chunk(&self) {}

    fn tiles_shape(self: &Self) -> GridShape<[usize; 2]> {
        let shape = self.tiles.shape();
        [shape[0] as usize, shape[1] as usize].into()
    }

    fn read_global_geo_transform(&self) -> GeoTransform {
        let arr: &Array<FilesystemStore> = &self.tiles;
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

/// The fields from [`BaseTile`] which are not in the tiles array
/// and cannot be infered from other info (i.e. the cache key)
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ZarrsMetaDataChunk {
    /// this is equiv to tile.tile_position
    pub tile_position: GridIdx2D, // not sure if this is needed
    // pub global_geo_transform: GeoTransform, // can be saved in the array instead
    pub properties: RasterProperties, // properties are not necessarily unique across tiles from one raster/file
    /// The time initerval of the tile
    pub time_interval: TimeInterval,
    /// Whether the tile is a NoDataTile or not
    pub is_no_data: bool,
}

impl<'a> IntoArrayBytes<'a> for ZarrsMetaDataChunk {
    fn into_array_bytes(
        self,
        data_type: &zarrs::array::DataType,
    ) -> std::result::Result<zarrs::array::ArrayBytes<'a>, zarrs::array::ElementError> {
        let self_as_json = serde_json::to_vec(&self).expect("it works");
        // one metadata chunk is exactly one element (hopefully this is fine)
        // TODO (mid): check that this is indeed fine
        let offsets = unsafe { ArrayBytesOffsets::new_unchecked(vec![0, self_as_json.len()]) };
        Ok(ArrayBytes::new_vlen(self_as_json, offsets).expect("it works"))
    }
}

impl FromArrayBytes for ZarrsMetaDataChunk {
    fn from_array_bytes(
        bytes: ArrayBytes<'static>,
        shape: &[u64],
        data_type: &zarrs::array::DataType,
    ) -> std::prelude::v1::Result<Self, zarrs::array::ArrayError> {
        let variable = bytes
            .into_variable()
            .expect("it to be a fixed array by construction in IntoArrayBytes");
        Ok(serde_json::from_slice(variable.bytes()).expect("it to be valid metadata"))
    }
}

fn open_or_create_new_zarrs_array(
    path: &str,
    store: Arc<FilesystemStore>,
    shape: impl Into<ArrayShape>, // The shape of the array
    chunk_grid_metadata: impl Into<ArrayBuilderChunkGridMetadata>, // Usually the shape of a chunk
    data_type: zarrs::array::DataType,
    filler_value: impl Into<ArrayBuilderFillValue>,
    attributes: Option<serde_json::Map<String, serde_json::Value>>,
) -> std::result::Result<
    zarrs::array::Array<zarrs::filesystem::FilesystemStore>,
    zarrs::array::ArrayCreateError,
> {
    match Array::open(store.clone(), path) {
        Ok(array) => Ok(array),
        Err(_) => {
            let array = {
                let mut builder =
                    ArrayBuilder::new(shape, chunk_grid_metadata, data_type, filler_value);
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

/// The other direction is defined in [`ZarrsArrayAccess::read_global_geo_transform`].
fn geotransform_to_attributes_map(tile: &TypedRasterTile2D) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    let transform =
        call_generic_typed_raster_tile_2d_cache!(tile, |base_tile| base_tile.global_geo_transform);
    map.insert(
        GLOBAL_GEOTRANSFORM_KEY.to_string(),
        serde_json::to_value(transform).expect("it to be serialisable"),
    );
    map
}

/*
FLEGMON :
   - run the benchmarks (is the new version faster / as fast / faulty)
   - go over this code and the open code_tickets
   - read the next point and prioritize
   - look at the papers again and see how we can implement the dynamic storage
     also fix the additonal questions of this code.
        (right now the tiles are stored both in memory and on disc, while we always want to write
        we could decide to not read from disc, but from RAM or evict from RAM.
        There is also the issue of re-reading the tiles from the disc after reboot.
        We must also check if the the eviction from the disc is fine as is (if it actually deletes the tiles from disc).
        We must check what happens if the tiles cannot be written (does the code handle this case already?))
*/

/// Opens or creates a new array at `/{operator_name}/band_{band}/{time}/tiles`.
/// Stores the metadata of the array if a new one is created.
fn build_array_and_prepare_insert(
    key @ (operator_name, band, time_interval, tile_index): &CacheKey,
    store: Arc<FilesystemStore>,
    tile: &TypedRasterTile2D,
    result_descriptor: &RasterResultDescriptor,
) -> Result<(ZarrsArrayAccess, ZarrsArrayPath), zarrs::array::ArrayCreateError> {
    let paths = ZarrsArrayPath::new_from_cache_key(key);
    let attributes = geotransform_to_attributes_map(tile);

    let tile_shape = {
        let [tile_height, tile_width] = tile.grid_shape_array();
        vec![tile_height as u64, tile_width as u64]
    };
    // TODO (mid) : check if this is correct
    let array_shape = {
        let temp = result_descriptor.spatial_grid_descriptor().grid_shape();
        [temp.x() as u64, temp.y() as u64]
    };

    let data_arr = open_or_create_new_zarrs_array(
        &paths.tiles(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        zarrs_data_type(tile),
        filler_value(tile),
        Some(attributes),
    )?;

    let validity_arr = open_or_create_new_zarrs_array(
        &paths.validity(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        data_type::bool(),
        zarrs::array::FillValue::from(false),
        None,
    )?;

    let metadata_arr = open_or_create_new_zarrs_array(
        &paths.metadata(),
        store.clone(),
        array_shape.clone(),
        vec![1, 1],
        zarrs::array::data_type::bytes(),
        zarrs::array::FillValue::from(Vec::<u8>::new()),
        None,
    )?;

    Ok((
        ZarrsArrayAccess {
            tiles: data_arr,
            validity: validity_arr,
            metadata: metadata_arr,
        },
        paths,
    ))
}

// TODO (mid): Are those NAN / filler values actually the right ones? Should they be made configurable?
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

fn open_zarrs_filesystemstore() -> Result<Arc<FilesystemStore>> {
    let cache_dir = cache_dir();
    let zarrs = zarrs::filesystem::FilesystemStore::new(cache_dir.join("zarr_cache"))
        // TODO (mid): see if the error handling is okay like that
        .map_err(|err| Error::ZarrFilesystemError { source: err })?;
    Ok(Arc::new(zarrs))
}

fn raster_datatype_from_tile(tile: &TypedRasterTile2D) -> RasterDataType {
    match tile {
        TypedRasterTile2D::I8(_) => RasterDataType::I8,
        TypedRasterTile2D::I16(_) => RasterDataType::I16,
        TypedRasterTile2D::I32(_) => RasterDataType::I32,
        TypedRasterTile2D::I64(_) => RasterDataType::I64,
        TypedRasterTile2D::U8(_) => RasterDataType::U8,
        TypedRasterTile2D::U16(_) => RasterDataType::U16,
        TypedRasterTile2D::U32(_) => RasterDataType::U32,
        TypedRasterTile2D::U64(_) => RasterDataType::U64,
        TypedRasterTile2D::F32(_) => RasterDataType::F32,
        TypedRasterTile2D::F64(_) => RasterDataType::F64,
    }
}

impl OnDiskStorageFormat for ZarrsStorageFormat {
    fn cleanup(&mut self) {
        todo!(
            "figure out the correct semantics for this (cannot just delete the files, as they might have data from other tiles in them no?)"
        )
    }
}

#[async_trait]
impl StorageFormat for ZarrsStorageFormat {
    async fn store(
        tile: TypedRasterTile2D,
        // (operator_name, band, time_interval, tile_index)
        cache_key: Arc<CacheKey>,
        result_descriptor: /* TODO (crit): must be used! after rebase! */ &RasterResultDescriptor,
    ) -> Result<Self> {
        let store = open_zarrs_filesystemstore()?;
        // dbg!(&tile_index);

        let (arrays, paths) =
            build_array_and_prepare_insert(&cache_key, store.clone(), &tile, result_descriptor)
                .map_err(|source| Error::ZarrArrayCreateError { source })?;

        let tile_index = cache_key.3;
        let chunk_indices = tile_index
            .as_slice()
            .iter()
            .map(|x| *x as u64)
            .collect::<Vec<u64>>();
        let byte_size_tile_data =
            store_tile_data(tile.clone(), store.clone(), &arrays.tiles, &chunk_indices).map_err(
                |source| Error::ZarrArrayStoreError {
                    source,
                    storage: Tiles,
                },
            )?;
        let byte_size_tile_validity =
            store_tile_data_validity(tile.clone(), store, &arrays.validity, &chunk_indices)?;
        let () = arrays
            .metadata
            .store_chunk(
                &chunk_indices,
                ZarrsMetaDataChunk {
                    tile_position: call_generic_typed_raster_tile_2d_cache!(&tile, |tile| tile
                        .tile_position),
                    properties: call_generic_typed_raster_tile_2d_cache!(&tile, |tile| tile
                        .properties
                        .clone()),
                    time_interval: call_generic_typed_raster_tile_2d_cache!(&tile, |tile| tile
                        .time
                        .clone()),
                    is_no_data: call_generic_typed_raster_tile_2d_cache!(&tile, |tile| tile
                        .grid_array
                        .is_empty()),
                },
            )
            .map_err(|source| Error::ZarrArrayStoreError {
                source,
                storage: Metadata,
            })?;

        let data_type = raster_datatype_from_tile(&tile);
        Ok(ZarrsStorageFormat {
            paths,
            tile,
            data_type,
            byte_size: byte_size_tile_data + byte_size_tile_validity, // TODO (mid): this might not be the total size that we actually need
            chunk_indices: [chunk_indices[0], chunk_indices[1]],
        })
    }

    async fn load(&self) -> Result<TypedRasterTile2D> {
        let ZarrsStorageFormat {
            paths,
            tile,
            data_type,
            byte_size,
            chunk_indices,
        } = self;
        let store = open_zarrs_filesystemstore()?;
        let arrays = paths.open(store.clone())?;
        // dbg!("loading tile ", paths);
        let loaded_tile = load_tile(arrays, &self.paths, chunk_indices, *data_type)?;
        {
            let b = match (tile.clone(), loaded_tile.clone()) {
                (TypedRasterTile2D::I8(t), TypedRasterTile2D::I8(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::I16(t), TypedRasterTile2D::I16(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::I32(t), TypedRasterTile2D::I32(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::I64(t), TypedRasterTile2D::I64(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::U8(t), TypedRasterTile2D::U8(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::U16(t), TypedRasterTile2D::U16(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::U32(t), TypedRasterTile2D::U32(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::U64(t), TypedRasterTile2D::U64(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::F32(t), TypedRasterTile2D::F32(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                (TypedRasterTile2D::F64(t), TypedRasterTile2D::F64(t2)) => {
                    t.tiles_equal_ignoring_cache_hint(&t2)
                }
                _ => unreachable!(),
            };
            let ld = loaded_tile.clone();
            assert!(
                b,
                "Actual tile {tile:?} and loaded_tile {ld:?} are different."
            );
        }
        Ok(loaded_tile)
    }

    async fn byte_size(&self) -> Result<usize> {
        Ok(self.byte_size)
    }

    async fn is_expired(&self) -> bool {
        false // TODO (crit): figure out how to properly implement this
    }
}

fn is_no_data(tile: TypedRasterTile2D) -> bool {
    call_generic_typed_raster_tile_2d_cache!(tile, |tile| tile.grid_array.is_empty())
}

fn load_tile(
    arrays: ZarrsArrayAccess,
    paths: &ZarrsArrayPath,
    chunk_indices: &[u64],
    data_type: RasterDataType,
) -> Result<TypedRasterTile2D> {
    let metadata_chunk: ZarrsMetaDataChunk = arrays
        .metadata
        .retrieve_chunk(chunk_indices)
        .map_err(|source| Error::ZarrArrayError { source })?;
    let time = metadata_chunk.time_interval;
    let tile_position = metadata_chunk.tile_position;
    let band = paths.band();
    let global_geo_transform = arrays.read_global_geo_transform();
    // TODO (high): what should I use here? What is the proper default value for these datachunks that are retreived from the on-disk cache?
    let cache_hint = CacheHint::max_duration(); // TODO (crit): 27B344EE-600E-448B-982D-8A7D6DDA2E30

    macro_rules! empty_for_type {
        ($variant:ident) => {
            TypedRasterTile2D::$variant(RasterTile2D::new(
                time,
                tile_position,
                band,
                global_geo_transform,
                // TODO (mid): do the tiles actually all have the same shape?
                GridOrEmpty::new_empty(EmptyGrid::new(arrays.tiles_shape())),
                cache_hint,
            ))
        };
    }

    if (metadata_chunk.is_no_data) {
        return Ok(match data_type {
            RasterDataType::U8 => empty_for_type!(U8),
            RasterDataType::U16 => empty_for_type!(U16),
            RasterDataType::U32 => empty_for_type!(U32),
            RasterDataType::U64 => empty_for_type!(U64),
            RasterDataType::I8 => empty_for_type!(I8),
            RasterDataType::I16 => empty_for_type!(I16),
            RasterDataType::I32 => empty_for_type!(I32),
            RasterDataType::I64 => empty_for_type!(I64),
            RasterDataType::F32 => empty_for_type!(F32),
            RasterDataType::F64 => empty_for_type!(F64),
        });
    }

    let validity_mask: Vec<bool> = arrays
        .validity
        .retrieve_chunk(chunk_indices)
        .map_err(|source| Error::ZarrArrayError { source })?;

    // if let Err(validity_err) = arrays.validity.retrieve_chunk::<Vec<bool>>(chunk_indices) {
    //     dbg!(validity_err);
    // }

    // TODO (low): are local macros ok??
    macro_rules! load_for_type {
        ($t:ty, $variant:ident) => {{
            let raw_data: Vec<$t> = arrays
                .tiles
                .retrieve_chunk(chunk_indices)
                .map_err(|source| Error::ZarrArrayError { source })?;
            // let validity_mask: Vec<bool> = arrays
            //     .validity
            //     .retrieve_chunk(chunk_indices)
            //     .map_err(|source| Error::ZarrArrayError { source })?;
            let data = GridOrEmpty::new_grid(MaskedGrid2D::new(
                Grid::new(
                    // TODO (low): do we care about 32 bit systems at all? <- why is that even relevant?
                    arrays.tiles_shape(), /* TODO (mid): check if this is correct */
                    raw_data,
                )
                .expect("it to have the correct shape"),
                Grid::new(arrays.tiles_shape(), validity_mask).expect("it to have the correct shape"),
            ).expect("it to be a valid input"));
            Ok(TypedRasterTile2D::$variant(RasterTile2D::new(
                time,
                tile_position,
                band,
                global_geo_transform,
                data,
                cache_hint,
            )))
        }};
    }

    match data_type {
        RasterDataType::U8 => load_for_type!(u8, U8),
        RasterDataType::U16 => load_for_type!(u16, U16),
        RasterDataType::U32 => load_for_type!(u32, U32),
        RasterDataType::U64 => load_for_type!(u64, U64),
        RasterDataType::I8 => load_for_type!(i8, I8),
        RasterDataType::I16 => load_for_type!(i16, I16),
        RasterDataType::I32 => load_for_type!(i32, I32),
        RasterDataType::I64 => load_for_type!(i64, I64),
        RasterDataType::F32 => load_for_type!(f32, F32),
        RasterDataType::F64 => load_for_type!(f64, F64),
    }
}

fn get_tile_position(tile: &TypedRasterTile2D) -> GridIdx2D {
    call_generic_typed_raster_tile_2d_cache!(tile, |tile| tile.tile_position)
}

fn store_tile_data_concrete<T: Pixel + zarrs::array::Element>(
    grid: Grid<GridShape2D, T>,
    array: &Array<FilesystemStore>,
    chunk_indices: &[u64],
) -> std::result::Result<usize, zarrs::array::ArrayError> {
    let ret = grid.data.byte_size(); // TODO (mid): figure out if this will return the correct size.
    array.store_chunk(chunk_indices, grid.data)?;
    Ok(ret)
}

fn store_tile_data_validity(
    tile: TypedRasterTile2D,
    store: Arc<FilesystemStore>,
    array: &Array<FilesystemStore>,
    chunk_indices: &[u64],
) -> Result<usize> {
    if let Ok(mask) = Grid2D::<bool>::try_from(tile) {
        let ret = mask.data.byte_size();
        let () = array
            .store_chunk(chunk_indices, mask.data)
            .map_err(|source| Error::ZarrArrayStoreError {
                source,
                storage: Validity,
            })?;
        return Ok(ret);
    }
    return Ok(0); // TODO (low): see C482F320-772B-46FE-A6E5-0D7EE22B3AB4
}

/// returns the amount of bytes written
fn store_tile_data(
    tile: TypedRasterTile2D,
    store: Arc<FilesystemStore>,
    array: &Array<FilesystemStore>,
    chunk_indices: &[u64],
) -> std::result::Result<usize, zarrs::array::ArrayError> {
    match TypedGrid2D::try_from(tile) {
        Err(()) => Ok(0),
        // TODO (low): {C482F320-772B-46FE-A6E5-0D7EE22B3AB4}
        //      make sure that the 0 is correct.
        //      (we dont write any tile (we dont need to) and at read side we initialize a NoDataTile)
        Ok(typed_grid) => match typed_grid {
            TypedGrid::U8(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::U16(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::U32(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::U64(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::I8(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::I16(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::I32(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::I64(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::F32(grid) => store_tile_data_concrete(grid, array, chunk_indices),
            TypedGrid::F64(grid) => store_tile_data_concrete(grid, array, chunk_indices),
        },
    }
}

#[cfg(not(test))]
static RASTER_CACHE_DIR: OnceLock<PathBuf> = OnceLock::new();

// TODO (mid): this causes a race condition when running multiple tests at once
// either the inner thing must be held as long as the test is running (s.t. we dont write another tempdir in test_02 and then get confused in test_01)
// or by just having some kind of map that associates thread/test -> tempdir
#[cfg(test)]
static RASTER_CACHE_DIR: LazyLock<std::sync::Mutex<Option<PathBuf>>> =
    LazyLock::new(|| std::sync::Mutex::new(None));

#[cfg(not(test))]
pub fn set_raster_cache_dir(path: PathBuf) -> Result<()> {
    RASTER_CACHE_DIR
        .set(path)
        .map_err(|err| Error::MustNotHappen {
            message: "RASTER_CACHE_DIR should only be set once during startup".to_string(),
        })?;
    Ok(())
}

#[cfg(test)]
pub fn set_raster_cache_dir(path: PathBuf) -> Result<()> {
    *RASTER_CACHE_DIR.lock().expect("it to be not poisoned") = Some(path);
    Ok(())
}

/// Returns the `RASTER_CACHE_DIR` config setting if it is set,
/// if is not, then return then return the xdg-cache dir
#[cfg(not(test))]
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

/// Returns the `RASTER_CACHE_DIR` config setting if it is set,
/// if is not, then return then return the xdg-cache dir
#[cfg(test)]
fn cache_dir() -> PathBuf {
    RASTER_CACHE_DIR
        .lock()
        .as_ref()
        .expect("locking the mutex is possible")
        .as_ref()
        .cloned()
        .expect("the path to be present")
}

impl OnDiskStorageFormat for ArrowIpcStorageFormat {
    fn cleanup(&mut self) {
        remove_file(&self.path);
    }
}

fn index_to_string(idx: &TileIndex) -> String {
    let mut sb = String::new();
    let mut iter = idx.as_slice().iter();
    while let Some(value) = iter.next() {
        sb.write_str(&format!("{}-", value));
    }
    sb[0..sb.len() - 1].to_string()
}

fn time_interval_to_path_safe_iso(time: &TimeInterval) -> String {
    let fmt = &DateTimeParseFormat::custom("%Y-%m-%dT%H-%M-%S%.3fZ".to_string());
    format!(
        "{}_to_{}",
        time.start()
            .as_date_time()
            .expect("it to be a valid datetime")
            .format(fmt),
        time.end()
            .as_date_time()
            .expect("it to be a valid datetime")
            .format(fmt)
    )
}

/// returns the path to the directory (does not create) as the first element
///         and the filepath as the second element.
async fn path_for_cache_key(key: &CacheKey) -> Result<(PathBuf, PathBuf)> {
    let cache_dir = cache_dir();
    let ops_dir_name = key.0.to_string();
    let file_name = format!(
        "band_{}-timeinterval_{}-grididx_{}.raster_cache.ipc",
        key.1,
        time_interval_to_path_safe_iso(&key.2),
        index_to_string(&key.3),
    );
    tokio::fs::create_dir_all(&cache_dir).await?;
    Ok((
        cache_dir,
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

        let data_type = raster_datatype_from_tile(&tile);
        Ok(Self {
            path: cache_path,
            tile,
            byte_size,
            data_type,
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
        false // TODO (crit): Implement this properly
    }
}

fn typed_raster_tile_to_arrow_ipc(tile: TypedRasterTile2D) -> Result<Vec<u8>> {
    call_generic_typed_raster_tile_2d_cache!(tile, |tile| {
        raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(tile)
    })
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
    use geo::k_nearest_concave_hull;
    use geoengine_datatypes::hashmap;
    use geoengine_datatypes::primitives::{CacheHint, DateTimeParseFormat, TimeInterval};
    use geoengine_datatypes::raster::{
        Grid2D, RasterTile2D, TileInformation, TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::util::test::TestDefault;
    use httptest::matchers::key;
    use serde::de::Expected;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use std::str::FromStr;
    use std::sync::LazyLock;
    use tempfile::tempdir;

    use tokio::sync::{OnceCell, RwLock};

    fn test_key(idx: u32) -> Arc<CacheKey> {
        Arc::new((
            // TODO (mid): Not sure what this looks like in reality
            CanonicOperatorName::new_unchecked(&json!({ "type": "CacheOperator", "params": {} })),
            idx,
            TimeInterval::default(),
            [idx as isize, 0].into(),
        ))
    }

    fn test_tile_u8(band: u32) -> TypedRasterTile2D {
        let values: Vec<u8> = (0_u8..64).collect();
        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: TestDefault::test_default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [8, 8].into(),
            },
            band,
            Grid2D::new([8, 8].into(), values)
                .expect("it to be a valid Grid2D")
                .into(),
            CacheHint::max_duration(),
        );

        TypedRasterTile2D::U8(raster_tile)
    }

    fn initialize_test_zarrs_cache_dir() -> tempfile::TempDir {
        let temp_dir = tempdir().expect("it to work");
        let () = set_raster_cache_dir(temp_dir.path().to_path_buf()).expect("it to work");
        temp_dir
    }

    #[tokio::test]
    async fn arrow_ipc_storage_format_store_and_load() {
        initialize_test_zarrs_cache_dir();

        let band = 0;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let stored = ArrowIpcStorageFormat::store(
            tile.clone(),
            key,
            todo!("figure out where the raster result descriptor lives"),
        )
        .await
        .expect("it should be possible to store the tile.");

        let path = stored.path.clone();
        let metadata = fs::metadata(&path).expect(&format!("File should be at {path:#?}"));
        let byte_size = stored
            .byte_size()
            .await
            .expect("it should be possible to get the stored byte size.");
        assert_eq!(byte_size as u64, metadata.len());

        let loaded = stored
            .load()
            .await
            .expect(&format!("File '{path:#?}' should loadable as a RasterTile"));
        match loaded {
            TypedRasterTile2D::U8(loaded_tile) => {
                if let TypedRasterTile2D::U8(original_tile) = tile {
                    assert_eq!(
                        loaded_tile, original_tile,
                        "Tiles are not the same after on-disk cache"
                    );
                } else {
                    panic!("expected u8 tile");
                }
            }
            _ => panic!("expected u8 tile"),
        }
    }

    #[tokio::test]
    async fn on_disk_storage_insert_and_get_with_arrow_ipc() {
        initialize_test_zarrs_cache_dir();

        let store = OnDiskStore {
            cache: RwLock::new(HashMap::new()),
            eviction_strategy: RwLock::new(FifoEvictionStrategy::new(usize::MAX)),
        };

        let band = 1;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let expected_tile = tile.clone();

        store
            .insert(
                key.clone(),
                tile,
                todo!("figure out how to aquire the result descriptor"),
            )
            .await
            .expect("it should be possible to insert the tile into the OnDiskStorage.");
        let stored: Arc<ArrowIpcStorageFormat> = store
            .get(&key)
            .await
            .expect("it should be present")
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
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zarrs_storage_format_store_and_load() {
        let temp_dir = initialize_test_zarrs_cache_dir();

        let band = 0;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let stored = ZarrsStorageFormat::store(
            tile.clone(),
            key,
            todo!("figure out how to get the raster result descriptor"),
        )
        .await
        .expect("it should be possible to store the tile.");

        {
            // check if the zarrs thing is there
            let full_path = temp_dir.path().join(PathBuf::from("zarr_cache"));
            let _ = fs::metadata(&full_path).expect(&format!("File should be at {full_path:#?}"));
        }

        let loaded = stored
            .load()
            .await
            .expect(&format!("File should loadable as a TypedRasterTile"));

        match loaded {
            TypedRasterTile2D::U8(loaded_tile) => {
                if let TypedRasterTile2D::U8(original_tile) = tile {
                    assert!(
                        loaded_tile.tiles_equal_ignoring_cache_hint(&original_tile),
                        "Tiles are not the same after on-disk zarrs cache\nLoaded: {:#?}\nOrig: {:#?}",
                        loaded_tile,
                        original_tile
                    );
                } else {
                    panic!("expected u8 tile");
                }
            }
            _ => panic!("expected u8 tile"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zarrs_storage_format_insert_and_get() {
        initialize_test_zarrs_cache_dir();

        let store = OnDiskStore {
            cache: RwLock::new(HashMap::new()),
            eviction_strategy: RwLock::new(FifoEvictionStrategy::new(usize::MAX)),
        };

        let band = 1;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let expected_tile = tile.clone();

        store
            .insert(
                key.clone(),
                tile,
                todo!("figure out how to get the raster result descriptor"),
            )
            .await
            .expect("it should be possible to insert the tile into the OnDiskStorage.");
        let stored: Arc<ZarrsStorageFormat> = store
            .get(&key)
            .await
            .expect("it should be present") // TODO (crit): figure out a better way
            .expect("it should be possible to extract the tile-ref from the cache.");
        let loaded = stored
            .load()
            .await
            .expect("it should be possible to load the tile via the tile-ref.");

        match loaded {
            TypedRasterTile2D::U8(loaded_tile) => {
                if let TypedRasterTile2D::U8(original_tile) = expected_tile {
                    assert!(
                        loaded_tile.tiles_equal_ignoring_cache_hint(&original_tile),
                        "Tiles are not the same after on-disk zarrs cache\nLoaded: {:#?}\nOrig: {:#?}",
                        loaded_tile,
                        original_tile
                    );
                } else {
                    panic!("expected u8 tile");
                }
            }
            _ => panic!("expected u8 tile"),
        }
    }

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

    #[test]
    fn time_interval_to_iso_test() {
        let time = TimeInterval::new(0, 2).expect("it should be valid");
        let expected = "1970-01-01T00-00-00Z_to_1970-01-01T00-00-00.002Z";
        let actual = time_interval_to_path_safe_iso(&time);
        assert_eq!(expected, actual);
    }

    #[test]
    fn zarrs_array_path_operator_name_pathable() {
        let path = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let tiles_path = path.operator_name_pathable();
        assert_eq!(
            "05643c4ac1135b60c1e9b65c47fc77d7ccd550b05485dc4f5597ac3d067f065d".to_string(),
            tiles_path
        );
    }

    #[test]
    fn get_tiles_path() {
        let path = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let tiles_path = path.tiles();
        let time = time_interval_to_path_safe_iso(todo!("&path.time_interval()"));
        assert_eq!(
            "/05643c4ac1135b60c1e9b65c47fc77d7ccd550b05485dc4f5597ac3d067f065d/band_0/-262143-01-01T00-00-00Z_to_+262142-12-31T23-59-59.999Z/tiles"
                .to_string(),
            tiles_path
        );
    }

    #[test]
    fn get_metadata_path() {
        let path = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let metadata_path = path.metadata();
        let time = time_interval_to_path_safe_iso(todo!("&path.time_interval()"));
        assert_eq!(
            "/05643c4ac1135b60c1e9b65c47fc77d7ccd550b05485dc4f5597ac3d067f065d/band_0/-262143-01-01T00-00-00Z_to_+262142-12-31T23-59-59.999Z/metadata"
                .to_string(),
            metadata_path
        );
    }

    #[test]
    fn metadata_and_tiles_are_valid_node_paths() {
        let paths = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let _ =
            zarrs::node::NodePath::try_from(paths.tiles().as_str()).expect("it to be a valid path");
        let _ = zarrs::node::NodePath::try_from(paths.metadata().as_str())
            .expect("it to be a valid path");
    }
}
