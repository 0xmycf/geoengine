#![allow(unused)]
#[cfg(test)]
use std::sync::LazyLock;
use std::{
    fmt::Write,
    fs,
    io::{self, ErrorKind, Write as IoWrite},
    ops::Deref,
    path::PathBuf,
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
        GeoTransform, Grid, GridIdx, GridOrEmpty, GridShapeAccess, GridSize, MaskedGrid2D,
        RasterDataType, RasterProperties, TypedGrid, TypedGrid2D,
        arrow_ipc_file_to_raster_tile_2d_for_ipc_channel,
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
        Array, ArrayBuilder, ArrayBytes, ArrayBytesOffsets, ArrayCreateError, ArrayShape,
        ElementOwned, FromArrayBytes, IntoArrayBytes,
        builder::{ArrayBuilderChunkGridMetadata, ArrayBuilderFillValue},
        data_type,
    },
    filesystem::FilesystemStore,
    group::GroupBuilder,
};

use crate::{
    cache::{self, new_raster_cache::*},
    call_generic_typed_raster_tile_2d_cache,
    error::{
        Error,
        WhichTile::{Metadata, Tiles},
    },
};

const GLOBAL_GEOTRANSFORM_KEY: &'static str = "global_geo_transform";

pub struct OnDiskStore<SF: StorageFormat, ES: EvictionStrategy> {
    pub cache: RwLock<HashMap<CacheKey, Arc<SF>>>,
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
    async fn write(tile: TypedRasterTile2D, key: CacheKey) -> Result<PendingFile<Self>> {
        let store = Self::store(tile, key).await?;
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
        let stored_tile = SF::write(tile, key.clone()).await?;
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

// How it is stored:
// The grid is stored as raw bytes (gotta figure out how, exactly)
// the rest is stored as metadata in the zarr file / zarr.json
pub struct ZarrsStorageFormat {
    paths: ZarrsArrayPath,
    tile: TypedRasterTile2D,
    data_type: RasterDataType,
    byte_size: usize,
    chunk_indices: [u64; 2],
    // key: CacheKey,
    // operator? path? group?
}

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

    pub fn time_interval(&self) -> TimeInterval {
        self.key.2.clone()
    }

    pub fn band(&self) -> Band {
        self.key.1
    }

    pub fn operator_name(&self) -> CanonicOperatorName {
        self.key.0.clone()
    }

    fn operator_name_pathable(&self) -> String {
        let op = self.operator_name().to_string();
        let name: serde_json::Value = serde_json::from_str(&op).expect("it to work");
        name.get("type")
            .expect("the operator name to have a type value")
            .as_str()
            .expect("it to be a string")
            .to_string()
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

    fn open(&self, store: Arc<FilesystemStore>) -> Result<ZarrsArrayAccess> {
        let tiles_array = Array::open(store.clone(), &self.tiles())
            .map_err(|source| Error::ZarrArrayCreateError { source })?;
        let metadata_array = Array::open(store.clone(), &self.metadata())
            .map_err(|source| Error::ZarrArrayCreateError { source })?;

        Ok(ZarrsArrayAccess {
            tiles: tiles_array,
            metadata: metadata_array,
        })
    }
}

type TileStore = zarrs::array::Array<FilesystemStore>;
type MetaDataStore = zarrs::array::Array<FilesystemStore>;

/// This also
struct ZarrsArrayAccess {
    /// This also stores [`BaseTile::global_geo_transform`] inside the zarrs metadata
    pub tiles: TileStore,
    pub metadata: MetaDataStore,
}

// TODO (mid): refactor the other functions to use this instead
impl ZarrsArrayAccess {
    fn store_chunk(&self) {}
    fn retreive_chunk(&self) {}

    fn shape(self: &Self) -> GridShape<[usize; 2]> {
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
    shape: impl Into<ArrayShape>,
    chunk_grid_metadata: impl Into<ArrayBuilderChunkGridMetadata>,
    data_type: zarrs::array::DataType,
    filler_value: ArrayBuilderFillValue,
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

/// Opens or creates a new array at "/{operator_name}/band_{band}/{time}/tiles".
/// Stores the metadata of the array if a new one is created.
fn build_array_and_prepare_insert(
    key @ (operator_name, band, time_interval, tile_index): &CacheKey,
    store: Arc<FilesystemStore>,
    tile: &TypedRasterTile2D,
) -> Result<(ZarrsArrayAccess, ZarrsArrayPath), zarrs::array::ArrayCreateError> {
    let [tile_height, tile_width] = tile.grid_shape_array();
    let paths = ZarrsArrayPath::new_from_cache_key(key);

    let attributes = geotransform_to_attributes_map(tile);

    let shape = vec![tile_height as u64, tile_width as u64];

    let data_arr = open_or_create_new_zarrs_array(
        &paths.tiles(),
        store.clone(),
        shape.clone(),
        vec![tile_height as u64, tile_width as u64], // TODO (high): this seems incorrect (same as below)
        zarrs_data_type(tile),
        filler_value(tile),
        Some(attributes),
    )?;

    let metadata_arr = open_or_create_new_zarrs_array(
        &paths.metadata(),
        store.clone(),
        shape.clone(),
        vec![1, 1],
        zarrs::array::data_type::bytes(),
        Vec::<u8>::new().into(),
        None,
    )?;

    Ok((
        ZarrsArrayAccess {
            tiles: data_arr,
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

fn from_raster(tile: &TypedRasterTile2D) -> RasterDataType {
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
        (operator_name, band, time_interval, tile_index): CacheKey,
    ) -> Result<Self> {
        let store = open_zarrs_filesystemstore()?;

        let (arrays, paths) = build_array_and_prepare_insert(
            &(operator_name, band, time_interval, tile_index),
            store.clone(),
            &tile,
        )
        .map_err(|source| Error::ZarrArrayCreateError { source })?;

        let chunk_indices = tile_index
            .as_slice()
            .iter()
            .map(|x| *x as u64)
            .collect::<Vec<u64>>();
        let byte_size = store_tile_data(tile.clone(), store, &arrays.tiles, &chunk_indices)
            .map_err(|source| Error::ZarrArrayStoreError {
                source,
                storage: Tiles,
            })?;
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
                },
            )
            .map_err(|source| Error::ZarrArrayStoreError {
                source,
                storage: Metadata,
            })?;

        let data_type = from_raster(&tile);
        Ok(ZarrsStorageFormat {
            paths,
            tile,
            data_type,
            byte_size,
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
        Ok(load_tile(arrays, &self.paths, chunk_indices, *data_type)?)
    }

    async fn byte_size(&self) -> Result<usize> {
        Ok(self.byte_size)
    }
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
    let time = paths.time_interval();
    let tile_position = metadata_chunk.tile_position;
    let band = paths.band();
    let global_geo_transform = arrays.read_global_geo_transform();
    // TODO (high): what should I use here? What is the proper default value for these datachunks that are retreived from the on-disk cache?
    let cache_hint = CacheHint::default();

    // TODO (low): are local macros ok??
    macro_rules! load_for_type {
        ($t:ty, $variant:ident) => {{
            let raw_data: Vec<$t> = arrays
                .tiles
                .retrieve_chunk(chunk_indices)
                .map_err(|source| Error::ZarrArrayError { source })?;
            let data = GridOrEmpty::new_grid(MaskedGrid2D::new_with_data(
                Grid::new(
                    // TODO (low): do we care about 32 bit systems at all? <- why is that even relevant?
                    arrays.shape(), /* TODO (mid): check if this is correct */
                    raw_data,
                )
                .expect("it to have the correct shape"),
            ));
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

/// returns the amount of bytes written
fn store_tile_data(
    tile: TypedRasterTile2D,
    store: Arc<FilesystemStore>,
    array: &Array<FilesystemStore>,
    chunk_indices: &[u64],
) -> std::result::Result<usize, zarrs::array::ArrayError> {
    match TypedGrid2D::try_from(tile) {
        Err(()) => Ok(0), // TODO (high): evalulate if writing nothinig is actually what we want ∧ if returning 0 is the good return value, or if it would be better to write empty data tiles
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
    /* here */
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
        .expect("locking the mutex is possible") /* here is the other one */
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
    async fn store(tile: TypedRasterTile2D, key: CacheKey) -> Result<Self> {
        let (cache_dir, file_path) = path_for_cache_key(&key).await?;
        tokio::fs::create_dir_all(cache_dir.clone()).await?;
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

    fn test_key(idx: u32) -> CacheKey {
        (
            // TODO (mid): Not sure what this looks like in reality
            CanonicOperatorName::new_unchecked(&json!({ "type": "CacheOperator", "params": {} })),
            idx,
            TimeInterval::default(),
            [idx as isize, 0].into(),
        )
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
            CacheHint::default(),
        );

        TypedRasterTile2D::U8(raster_tile)
    }

    #[tokio::test]
    async fn arrow_ipc_storage_format_store_and_load() {
        let cache_root = cache_dir();
        fs::create_dir_all(&cache_root).expect("creating the cache dir should be possible");

        let band = 0;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let stored = ArrowIpcStorageFormat::store(tile.clone(), key)
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

        fs::remove_file(&path).expect("it should be possible to delete the file from the disk");
    }

    #[tokio::test]
    async fn on_disk_storage_insert_and_get_with_arrow_ipc() {
        let cache_root = cache_dir();
        fs::create_dir_all(&cache_root).expect("creating the dir should be possible");

        let store = OnDiskStore {
            cache: RwLock::new(HashMap::new()),
            eviction_strategy: RwLock::new(FifoEvictionStrategy::new(usize::MAX)),
        };

        let band = 1;
        let key = test_key(band);
        let tile = test_tile_u8(band);
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
        // TODO (high): use tempdir instead
        fs::remove_file(&path).expect("removing the file should be possible");
    }

    /*
    thread 'cache::new_raster_cache::anticache::tests::zarrs_storage_format_store_and_load' (229534) panicked at operators/src/cache/new_raster_cache/anticache.rs:921:14:
    it should be possible to store the tile.: ZarrArrayError
        { source: CodecError(UnexpectedChunkDecodedSize(InvalidBytesLengthError
            { len: 103, expected_len: 64 })) }
    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

    ---- cache::new_raster_cache::anticache::tests::zarrs_storage_format_insert_and_get stdout ----

    thread 'cache::new_raster_cache::anticache::tests::zarrs_storage_format_insert_and_get' (229533) panicked at operators/src/cache/new_raster_cache/anticache.rs:974:14:
    it should be possible to insert the tile into the OnDiskStorage.: ZarrArrayError
        { source: CodecError(UnexpectedChunkDecodedSize(InvalidBytesLengthError
            { len: 103, expected_len: 64 })) }
    */

    // set_raster_cache_dir requires that we only set the path once!
    #[tokio::test(flavor = "current_thread")]
    async fn zarrs_storage_format_store_and_load() {
        let temp_dir = tempdir().expect("tempdir to work in the test");
        let () = set_raster_cache_dir(temp_dir.path().to_owned())
            .expect("the cache dir setter to work fine");

        let band = 0;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let stored = ZarrsStorageFormat::store(tile.clone(), key)
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

    // set_raster_cache_dir requires that we only set the path once!
    #[tokio::test(flavor = "current_thread")]
    async fn zarrs_storage_format_insert_and_get() {
        let temp_dir = tempdir().expect("tempdir to work in the test");
        let () = set_raster_cache_dir(temp_dir.path().to_owned())
            .expect("the cache dir setter to work fine");

        let store = OnDiskStore {
            cache: RwLock::new(HashMap::new()),
            eviction_strategy: RwLock::new(FifoEvictionStrategy::new(usize::MAX)),
        };

        let band = 1;
        let key = test_key(band);
        let tile = test_tile_u8(band);
        let expected_tile = tile.clone();

        store
            .insert(key.clone(), tile)
            .await
            .expect("it should be possible to insert the tile into the OnDiskStorage.");
        let stored: Arc<ZarrsStorageFormat> = store
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
    fn set_then_read_raster_cache_dir() {
        let expected = PathBuf::from_str("/tmp/something").expect("it to be a valid pathbuf");
        if let Err(err) = set_raster_cache_dir(expected.clone()) {
            panic!("{err}");
        }
        let actual = cache_dir();
        assert_eq!(expected, actual);
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
        assert_eq!("CacheOperator".to_string(), tiles_path);
    }

    #[test]
    fn get_tiles_path() {
        let path = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let tiles_path = path.tiles();
        let time = time_interval_to_path_safe_iso(&path.time_interval());
        assert_eq!(
            "/CacheOperator/band_0/-262143-01-01T00-00-00Z_to_+262142-12-31T23-59-59.999Z/tiles"
                .to_string(),
            tiles_path
        );
    }

    #[test]
    fn get_metadata_path() {
        let path = ZarrsArrayPath::new_from_cache_key(&test_key(0));
        let metadata_path = path.metadata();
        let time = time_interval_to_path_safe_iso(&path.time_interval());
        assert_eq!(
            "/CacheOperator/band_0/-262143-01-01T00-00-00Z_to_+262142-12-31T23-59-59.999Z/metadata"
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
