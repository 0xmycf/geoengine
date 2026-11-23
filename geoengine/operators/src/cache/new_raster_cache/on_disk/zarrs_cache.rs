// TODO (low): handle all panics!
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
use tracing::instrument::WithDispatch;
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
        AsyncReadableStorageTraits, AsyncReadableWritableListableStorageTraits,
        AsyncReadableWritableStorageTraits, AsyncWritableStorageTraits, ListableStorageTraits,
        ReadableStorageTraits, ReadableWritableStorageTraits, StoreKey, WritableStorageTraits,
        storage_adapter::sync_to_async::{SyncToAsyncSpawnBlocking, SyncToAsyncStorageAdapter},
        store::MemoryStore,
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
        WhichZarrArray::{self, Metadata, Tiles, Validity},
        ZarrArray, ZarrFilesystem,
    },
    util::gdal::create_ndvi_meta_data,
};

/*
<https://docs.rs/zarrs_storage/0.4.5/zarrs_storage/storage_adapter/sync_to_async/trait.SyncToAsyncSpawnBlocking.html>
*/
struct TokioSpawnBlocking;

impl SyncToAsyncSpawnBlocking for TokioSpawnBlocking {
    async fn spawn_blocking<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        match crate::util::spawn_blocking(f).await {
            Ok(r) => r,
            Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            Err(err) => std::panic::resume_unwind(Box::new(err)),
        }
    }
}

type AsyncFileSystemStore = SyncToAsyncStorageAdapter<FilesystemStore, TokioSpawnBlocking>;

#[derive(Debug)]
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

impl ZarrDiskEntry {
    fn store_path(&self) -> PathBuf {
        format_cache_key(cache_dir().join(ZARR_CACHE), &self.paths)
    }

    fn read_sync(&self, store_path: PathBuf) -> Result<TypedRasterTile2D> {
        let store = Arc::new(open_zarrs_filesystem_store_impl(store_path)?);
        self.paths.open(store)?.retrieve_tile(
            &self.chunk_indices,
            &self.paths,
            self.data_type,
            self.cache_hint,
        )
    }
}

#[async_trait]
impl DiskEntry for ZarrDiskEntry {
    async fn read(entry: Arc<SizeTrackedEntry<Self>>) -> Result<TypedRasterTile2D> {
        let store_path = entry.store_path();
        // The job can finish after cancellation, so it owns the eviction pin.
        crate::util::spawn_blocking(move || entry.read_sync(store_path)).await?
    }

    async fn erase(&self) -> Result<()> {
        let store = async_open_zarrs_filesystem_store(&self.paths).await?;
        let arrays = self.paths.async_open(store).await?;
        arrays.async_erase_tile(self.chunk_indices).await
    }
}

#[derive(Debug, Clone)]
struct ZarrStoreEntry {
    store_key: StoreKey,
    bytes: bytes::Bytes,
}

impl ZarrStoreEntry {
    fn new((store_key, bytes): (StoreKey, bytes::Bytes)) -> Self {
        Self { store_key, bytes }
    }

    fn set_in_store<TStore>(&self, store: &TStore) -> Result<()>
    where
        TStore: WritableStorageTraits,
    {
        store
            .set(&self.store_key, self.bytes.clone())
            .map_err(|err| Error::ZarrArrayError { source: err.into() })
    }

    // TODO (high): d7cdae44-8dd2-404a-9f9d-7fb4c0abaaff
    /// Async version of [`set_in_store`]
    ///
    /// Returns the amount of bytes written.
    async fn async_set_in_store<TStore>(&self, store: &TStore) -> Result<usize>
    where
        TStore: AsyncWritableStorageTraits,
    {
        store
            .set(&self.store_key, self.bytes.clone())
            .await
            .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
        Ok(self.bytes.len())
    }
}

#[derive(Debug)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct PreparedZarrStorageFormat {
    // the zarr array definitions
    layout: [(StoreKey, bytes::Bytes); 4],
    tile_data: Option<ZarrStoreEntry>,
    validity_data: Option<ZarrStoreEntry>,
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
        let group = GroupBuilder::new() // TODO (high): {a83371d1-432d-46ff-b483-e1143a0a0fc3} look into zarr groups again
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

        let is_empty = {
            let mut writer = ZarrChunkWriter::new(&arrays);
            match writer.store_tile_chunks(&tile, chunk_indices) {
                Ok(sizes) => sizes,
                Err(err) => return Err(err),
            }
        };

        let mut layout_builder = Vec::with_capacity(4);
        let mut data: HashMap<&str, (StoreKey, bytes::Bytes)> = HashMap::with_capacity(3);
        let mut byte_size = 0_u64;
        for key in store
            .list()
            .map_err(|err| Error::ZarrArrayError { source: err.into() })?
        {
            let is_json = key.as_str().ends_with(".json");

            let bytes = store.get(&key).expect("there to be no storage error.");

            if is_json {
                // don't count the byte size for metadata, as it would be
                // duplicated by writing more chunks. The metadata is written only once.
                layout_builder.push((key, bytes.expect("the bytes to be present")));
            } else {
                byte_size += bytes.as_ref().map(|x| x.len()).unwrap_or(0) as u64;
                data.insert(
                    key_to_array(&key).ok_or(Error::ZarrsGeoengineCacheArrayMappingError {
                        key: key.to_string(),
                    })?,
                    (key, bytes.expect("the value to be present")),
                );
            }
        }
        Ok(PreparedZarrStorageFormat {
            layout: layout_builder
                .try_into()
                .expect("the layout to have 4 entries"),
            tile_data: data
                .remove(PreparedZarrStorageFormat::TILES)
                .map(ZarrStoreEntry::new),
            validity_data: data
                .remove(PreparedZarrStorageFormat::VALIDITY)
                .map(ZarrStoreEntry::new),
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

    // TODO (high): {a8ad12e5-8282-451c-b44a-275ee2702b45} currently only the size of the chunks is calculated and used. Also change the bytesize to usize, so we dont need conversions all over the place.
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
    if let Some(data) = &data.tile_data {
        data.set_in_store(store)?;
    }
    if let Some(data) = &data.validity_data {
        data.set_in_store(store)?;
    }
    data.metadata.set_in_store(store)?;
    Ok(())
}

/// Async version of [`write_to_store`]
///
/// Returns the amount of bytes written excluding keys. TODO (high): d7cdae44-8dd2-404a-9f9d-7fb4c0abaaff
async fn async_write_to_store<TStore>(
    data: &PreparedZarrStorageFormat,
    store: &TStore,
    write_json: bool,
) -> Result<usize>
where
    TStore: AsyncWritableStorageTraits,
{
    let mut bytes_written = 0_usize;
    if write_json {
        for (key, bytes) in &data.layout {
            store
                .set(&key, bytes.clone())
                .await
                .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
            bytes_written += bytes.len();
        }
    }
    if let Some(data) = &data.tile_data {
        bytes_written += data.async_set_in_store(store).await?;
    }
    if let Some(data) = &data.validity_data {
        bytes_written += data.async_set_in_store(store).await?;
    }
    bytes_written += data.metadata.async_set_in_store(store).await?;
    Ok(bytes_written)
}

// TODO (high): d7cdae44-8dd2-404a-9f9d-7fb4c0abaaff
/// Create missing layout documents and validate existing ones asynchronously.
/// Returns the amount of bytes written.
async fn async_ensure_layout<TStore>(
    store: &TStore,
    layout: &[(StoreKey, bytes::Bytes); 4],
) -> Result<usize>
where
    TStore: AsyncReadableWritableStorageTraits,
{
    let mut bytes_written = 0_usize;
    for (key, expected_bytes) in layout {
        match store
            .get(key)
            .await
            .map_err(|err| Error::ZarrArrayError { source: err.into() })?
        {
            // metadata is _not_ present -> Write it.
            None => {
                store
                    .set(key, expected_bytes.clone())
                    .await
                    .map_err(|err| Error::ZarrArrayError { source: err.into() })?;
                bytes_written += expected_bytes.len();
            }
            // metadata is present -> Check if valid
            Some(actual) => {
                check_metadata_matches(key, &actual, expected_bytes)?;
            }
        }
    }
    Ok(bytes_written)
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
        let store = async_open_zarrs_filesystem_store(&self.paths).await?;
        let _written_bytes = async_ensure_layout(&store, &self.layout).await?;

        if let Err(write_error) =
            async_write_to_store(&self, &store, false /* dont write zarr-layout jsons */).await
        {
            // Delete the keys on a failed / partial write; missing indices are fine.
            let cleanup_result = match self.paths.async_open(store).await {
                Ok(arrays) => {
                    arrays
                        .async_cleanup_parital_tile(self.chunk_indices, write_error)
                        .await
                }
                Err(err) => Err(Error::ZarrChunkCleanupOpenError {
                    chunk_indices: self.chunk_indices,
                    write_error: Box::new(write_error),
                    source: Box::new(err),
                }),
            };
            return Err(match cleanup_result {
                Ok(write_error) => write_error,
                Err(cleanup_error) => {
                    tracing::error!(
                        error = %cleanup_error,
                        chunk_indices = ?self.chunk_indices,
                        "Failed to clean up a failed Zarr write"
                    );
                    cleanup_error
                }
            });
        };
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

    /// Async version of [`Self::open`].
    ///
    /// Fails if the arrays where not created yet, or whenever [`zarrs:array::Array::open`] fails.
    async fn async_open<TStore>(&self, store: Arc<TStore>) -> Result<ZarrsArrayAccess<TStore>>
    where
        TStore: AsyncReadableStorageTraits + 'static,
    {
        let tiles_array = Array::async_open(store.clone(), &self.tiles())
            .await
            .map_err(|source| Error::ZarrArrayCreateError { source })?;
        let metadata_array = Array::async_open(store.clone(), &self.metadata())
            .await
            .map_err(|source| Error::ZarrArrayCreateError { source })?;
        let validity_array = Array::async_open(store.clone(), &self.validity())
            .await
            .map_err(|source| Error::ZarrArrayCreateError { source })?;

        Ok(ZarrsArrayAccess {
            tiles: tiles_array,
            validity: validity_array,
            metadata: metadata_array,
        })
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
    TStore: AsyncReadableWritableStorageTraits + 'static,
{
    /// Async version of [`Self::retrieve_metadata_chunk`]
    #[cfg(test)]
    async fn async_retrieve_metadata_chunk(
        &self,
        chunk_indices: &[u64],
    ) -> std::result::Result<ZarrsMetaDataChunk, zarrs::array::ArrayError> {
        self.metadata.async_retrieve_chunk(chunk_indices).await
    }

    /// Async version of [`Self::retrieve_validity_mask`]
    #[cfg(test)]
    async fn async_retrieve_validity_mask(
        &self,
        chunk_indices: &[u64],
    ) -> std::result::Result<Vec<bool>, zarrs::array::ArrayError> {
        self.validity.async_retrieve_chunk(chunk_indices).await
    }

    /// Async version of [`Self::retrieve_tile`]
    #[cfg(test)]
    async fn async_retrieve_tile(
        &self,
        chunk_indices: &[u64],
        paths: &ZarrsArrayPath,
        data_type: RasterDataType,
        cache_hint: CacheHint,
    ) -> Result<TypedRasterTile2D> {
        let metadata_chunk: ZarrsMetaDataChunk = self
            .async_retrieve_metadata_chunk(chunk_indices)
            .await
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
                    let inner_tile = self
                        .async_make_tile::<$ty>(
                            chunk_indices,
                            time,
                            tile_position,
                            band,
                            global_geo_transform,
                            cache_hint,
                            metadata_chunk.properties,
                        )
                        .await?;
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

    #[cfg(test)]
    async fn async_make_tile<T: Pixel + zarrs::array::ElementOwned>(
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
            .async_retrieve_chunk(chunk_indices)
            .await
            .map_err(|source| Error::ZarrArrayError { source })?;
        let validity_mask: Vec<bool> = self
            .async_retrieve_validity_mask(chunk_indices)
            .await
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

    async fn async_cleanup_parital_tile(
        &self,
        chunk_indices: [u64; 2],
        write_error: Error,
    ) -> Result<Error> {
        match self.async_erase_tile(chunk_indices).await {
            Ok(()) => Ok(write_error),
            Err(Error::ZarrChunkEraseError {
                chunk_indices,
                errors,
            }) => Err(Error::ZarrChunkCleanupError {
                chunk_indices,
                write_error: Box::new(write_error),
                errors,
            }),
            Err(err) => Err(err),
        }
    }

    /// Erase all three tile chunks asynchronously, collecting deletion errors.
    ///
    /// TODO (high): {801ba6b6-2134-4880-b434-12e310fd1ca0} Make deletion of parts-of-tiles atomic, and retry later if it failed and its reasonable to retry
    async fn async_erase_tile(&self, chunk_indices: [u64; 2]) -> Result<()> {
        let mut errors = Vec::new();

        // erase_chunk succeeds when the chunk_indices are not present
        for (which_array, result) in [
            (
                WhichZarrArray::Tiles,
                self.tiles.async_erase_chunk(&chunk_indices).await,
            ),
            (
                WhichZarrArray::Validity,
                self.validity.async_erase_chunk(&chunk_indices).await,
            ),
            (
                WhichZarrArray::Metadata,
                self.metadata.async_erase_chunk(&chunk_indices).await,
            ),
        ] {
            if let Err(err) = result {
                tracing::error!(error = %err, ?chunk_indices, ?which_array, "Failed to delete chunk");
                errors.push((err, which_array));
            }
        }

        if !errors.is_empty() {
            return Err(Error::ZarrChunkEraseError {
                chunk_indices,
                errors,
            });
        }

        Ok(())
    }
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
                which_array: WhichZarrArray::Tiles,
                source,
            })?;
        Ok(ret)
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
                    which_array: WhichZarrArray::Validity,
                    source,
                })?;
            return Ok(ret);
        }
        return Ok(0); // TODO (low): see C482F320-772B-46FE-A6E5-0D7EE22B3AB4 , ef998d34-209e-4eb1-a8dc-30b6cf084e55
    }

    fn store_metadata_chunk(
        &self,
        chunk_indices: [u64; 2],
        data: ZarrsMetaDataChunk,
    ) -> Result<()> {
        self.metadata
            .store_chunk(&chunk_indices, data)
            .map_err(|source| Error::ZarrArrayStoreError {
                which_array: WhichZarrArray::Metadata,
                source,
            })
    }
}

impl<TStore> ZarrsArrayAccess<TStore> {
    fn single_tile_shape(&self, chunk_indices: &[u64]) -> GridShape<[usize; 2]> {
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

/// Async version of [`open_or_create_new_zarrs_array`].
async fn async_open_or_create_new_zarrs_array<TStore>(
    path: &str,
    store: Arc<TStore>,
    shape: impl Into<ArrayShape>, // The shape of the array
    chunk_shape: impl Into<ArrayBuilderChunkGridMetadata>, // Usually the shape of a chunk
    data_type: zarrs::array::DataType,
    filler_value: impl Into<ArrayBuilderFillValue>,
    attributes: Option<serde_json::Map<String, serde_json::Value>>,
) -> std::result::Result<Array<TStore>, ArrayCreateError>
where
    TStore: AsyncReadableWritableStorageTraits + 'static,
{
    match Array::async_open(store.clone(), path).await {
        Ok(array) => Ok(array),
        Err(_) => {
            let array = {
                let mut builder = ArrayBuilder::new(shape, chunk_shape, data_type, filler_value);
                if let Some(attrs) = attributes {
                    builder.attributes(attrs);
                }
                builder.build(store.clone(), path)?
            };
            array.async_store_metadata().await?;
            Ok(array)
        }
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

/// Async version of [`open_or_create_new_zarrs_array`].
///
/// Opens or creates a new array at `/{operator_name}/band_{band}/{time}/tiles`.
/// Stores the metadata of the array if a new one is created.
async fn async_build_array_and_prepare_insert<TStore>(
    key @ (operator_name, band, time_interval, tile_index): &CacheKey,
    paths: &ZarrsArrayPath,
    store: Arc<TStore>,
    tile: &TypedRasterTile2D,
    result_descriptor: &RasterResultDescriptor,
) -> Result<ZarrsArrayAccess<TStore>>
where
    TStore: AsyncReadableWritableStorageTraits + 'static,
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

    let data_arr = async_open_or_create_new_zarrs_array(
        &paths.tiles(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        zarrs_data_type(tile),
        filler_value(tile),
        Some(attributes),
    )
    .await
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    let validity_arr = async_open_or_create_new_zarrs_array(
        &paths.validity(),
        store.clone(),
        array_shape.clone(),
        tile_shape.clone(),
        data_type::bool(),
        zarrs::array::FillValue::from(false),
        None,
    )
    .await
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    let metadata_arr = async_open_or_create_new_zarrs_array(
        &paths.metadata(),
        store.clone(),
        tile_counts,
        vec![1, 1],
        zarrs::array::data_type::bytes(),
        zarrs::array::FillValue::from(Vec::<u8>::new()),
        None,
    )
    .await
    .map_err(|source| Error::ZarrArrayCreateError { source })?;

    Ok(ZarrsArrayAccess {
        tiles: data_arr,
        validity: validity_arr,
        metadata: metadata_arr,
    })
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

/// Use [`async_open_zarrs_filesystem_store`] to resolve the cache path and wrap
/// the store while running filesystem construction on a blocking worker.
fn open_zarrs_filesystem_store_impl(path: PathBuf) -> Result<FilesystemStore> {
    let zarrs = zarrs::filesystem::FilesystemStore::new(path)
        // TODO (mid): check the callsites how they handle this
        .map_err(|err| Error::ZarrFilesystemError { source: err })?;
    Ok(zarrs)
}

// TODO (mid): check the callsites how they handle this
/// Opens a new ZarrArrayStore at "{op}/band_{band}/{time}"
async fn async_open_zarrs_filesystem_store(
    paths: &ZarrsArrayPath,
) -> Result<Arc<AsyncFileSystemStore>> {
    let cache_dir = cache_dir();
    let path = format_cache_key(cache_dir.join(ZARR_CACHE), paths);
    // let store =
    //     crate::util::spawn_blocking(move || open_zarrs_filesystem_store_impl(path)).await??;
    let store = open_zarrs_filesystem_store_impl(path)?;
    Ok(Arc::new(SyncToAsyncStorageAdapter::new(
        Arc::new(store),
        TokioSpawnBlocking,
    )))
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

    /// returns whether the file is empty (false = not empty, true = empty)
    fn store_tile_chunks(
        &mut self,
        tile: &TypedRasterTile2D,
        chunk_indices: [u64; 2],
    ) -> Result<bool> {
        // cannot return early, must write metadata.
        let is_empty = call_generic_raster_tile_2d!(&tile, tile => tile.is_empty());

        if !is_empty {
            self.mark_attempted(CompletedZarrWrite::Tile);
            let _ = self.arrays.store_tile(tile, chunk_indices)?;

            self.mark_attempted(CompletedZarrWrite::Validity);
            let _ = self.arrays.store_validity_mask(chunk_indices, tile)?;
        }

        self.mark_attempted(CompletedZarrWrite::Metadata);
        self.arrays.store_metadata_chunk(
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

        Ok(is_empty)
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

    use crate::cache::new_raster_cache::on_disk::{set_raster_cache_dir, track_written_entry};
    use crate::cache::new_raster_cache::on_disk::tests::test_key;
    use crate::cache::new_raster_cache::on_disk::zarrs_cache::format_cache_key;
    use crate::engine::{RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor};
    use geoengine_datatypes::util::test::TestDefault;
    use geoengine_datatypes::{primitives::DateTime, spatial_reference::SpatialReference};
    use opentelemetry::global::set_text_map_propagator;
    use tempfile::TempDir;
    use zarrs::storage::{AsyncReadableStorageTraits, AsyncWritableStorageTraits};

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
                let prepared = PreparedZarrStorageFormat::store(tile.clone(), key, &descriptor)
                    .await
                    .expect("store tile at its upper-left pixel position");

                let disk_entry = prepared
                    .write()
                    .await
                    .expect("write prepared entry to disk");

                assert_eq!(
                    disk_entry.chunk_indices,
                    [tile_position[0] as u64, tile_position[1] as u64]
                );
                stored_tiles.push((tile, disk_entry, value));
            }

            for (tile, disk_entry, value) in stored_tiles {
                let arrays: ZarrsArrayAccess<_> = disk_entry
                    .paths
                    .async_open(
                        async_open_zarrs_filesystem_store(&disk_entry.paths)
                            .await
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let pixels: Vec<u8> = arrays
                    .tiles
                    .async_retrieve_chunk(&disk_entry.chunk_indices)
                    .await
                    .unwrap();
                assert_eq!(pixels, vec![value; 6]);
                compare_tiles(
                    &tile,
                    &DiskEntry::read(track_written_entry(disk_entry))
                        .await
                        .unwrap(),
                );
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
            let prepared =
                PreparedZarrStorageFormat::store(TypedRasterTile2D::U8(tile), key, &descriptor)
                    .await
                    .unwrap();
            let disk_entry = prepared
                .write()
                .await
                .expect("write tile to disk should work");
            let arrays: ZarrsArrayAccess<_> = disk_entry
                .paths
                .async_open(
                    async_open_zarrs_filesystem_store(&disk_entry.paths)
                        .await
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(disk_entry.chunk_indices, [2, 2]);
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
            let _directory = set_tempdir();
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

    pub(crate) struct TestSetup {
        pub(crate) geo_transform: GeoTransform,
        pub(crate) tile: TypedRasterTile2D,
        pub(crate) key: Arc<CacheKey>,
        pub(crate) result_descriptor: RasterResultDescriptor,
    }

    pub(crate) enum TestTile {
        EmptyTile,
        NonEmptyTile,
    }

    pub(crate) fn setup(test_tile: TestTile) -> TestSetup {
        let geo_transform = GeoTransform::new([0., 0.].into(), 1., -1.);
        let tile = {
            TypedRasterTile2D::U8(RasterTile2D::new_with_properties(
                TimeInterval::default(),
                [0, 0].into(),
                0,
                geo_transform,
                match test_tile {
                    TestTile::EmptyTile => GridOrEmpty::new_empty(EmptyGrid::new([2, 2].into())),
                    TestTile::NonEmptyTile => GridOrEmpty::new_grid(
                        MaskedGrid2D::new(
                            Grid::new([2, 2].into(), vec![1_u8, 2, 3, 4]).unwrap(),
                            Grid::new([2, 2].into(), vec![true; 4]).unwrap(),
                        )
                        .unwrap(),
                    ),
                },
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
        TestSetup {
            geo_transform,
            tile,
            key,
            result_descriptor,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_test_zarr_storage_format_v2() {
        let _directory = set_tempdir();
        let TestSetup {
            tile,
            key,
            result_descriptor,
            ..
        } = setup(TestTile::NonEmptyTile);
        let storage_format =
            PreparedZarrStorageFormat::store(tile.clone(), key, &result_descriptor)
                .await
                .expect("store should work");
        let disk_entry =
            track_written_entry(storage_format.write().await.expect("write should work"));
        let mut tiles = Vec::with_capacity(10);
        for _ in (0..10) {
            let loaded = DiskEntry::read(Arc::clone(&disk_entry))
                .await
                .expect("read should work");
            tiles.push(loaded);
        }
        tiles.iter().for_each(|loaded| compare_tiles(&tile, loaded));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_store_load_with_new_zarr_storage_format_v2() {
        let TestSetup {
            tile,
            key,
            result_descriptor,
            ..
        } = setup(TestTile::NonEmptyTile);
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
        let data = setup(TestTile::NonEmptyTile);
        let zarr_array_paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&zarr_array_paths)
            .await
            .expect("it to open the store asynchronously");

        let _array_access = async_build_array_and_prepare_insert(
            &data.key,
            &zarr_array_paths.clone(),
            store,
            &TypedRasterTile2D::U8(RasterTile2D::new(
                TimeInterval::new(0, 1).unwrap(),
                [0, 0].into(),
                0,
                data.geo_transform,
                Grid2D::new([2, 2].into(), vec![42_u8; 4]).unwrap().into(),
                CacheHint::max_duration(),
            )),
            &data.result_descriptor,
        )
        .await
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
            where ... is some other stuff related to the cache key that is not relevant here.

        Eg. if chunk_index = [1,2] for 'tiles', then:
            CACHE_DIR/zarr_cache/tiles/c/1/2

        This test will place a regular file 'c' at
            CACHE_DIR/zarr_cache/metadata/c
        to prevent the writer from creating this chunk directory, thus failing the write.
        */
        let root = format_cache_key(directory.path().join(ZARR_CACHE), &zarr_array_paths);
        let tiles_path = root.join(zarr_array_paths.tiles().trim_start_matches("/"));
        let validity_path = root.join(zarr_array_paths.validity().trim_start_matches("/"));
        let metadata_path = root.join(zarr_array_paths.metadata().trim_start_matches("/"));
        let blocker_file = metadata_path.join("c");

        let () = std::fs::write(blocker_file, "You shall not pass!").unwrap();
        assert!(metadata_path.join("c").exists());

        let tile: TypedRasterTile2D = RasterTile2D::new(
            TimeInterval::default(),
            [0, 0].into(),
            0,
            data.result_descriptor.spatial_grid.geo_transform(),
            Grid2D::new([2, 2].into(), vec![42_u8; 4]).unwrap().into(),
            CacheHint::max_duration(),
        )
        .into();
        let prepared_tile = PreparedZarrStorageFormat::store(
            tile.clone(),
            data.key.clone(),
            &data.result_descriptor,
        )
        .await
        .unwrap();

        let result = prepared_tile.write().await;
        assert!(matches!(
            result,
            Err(crate::error::Error::ZarrChunkCleanupError {
                chunk_indices: [0, 0],
                write_error,
                errors,
            })
                if matches!(
                    write_error.as_ref(),
                    Error::ZarrArrayError {
                        source: zarrs::array::ArrayError::StorageError(
                            zarrs::storage::StorageError::IOError(error)
                        ),
                    } if error.kind() == std::io::ErrorKind::NotADirectory
                ) && matches!(
                    errors.as_slice(),
                    [(zarrs::storage::StorageError::IOError(error), WhichZarrArray::Metadata)]
                        if error.kind() == std::io::ErrorKind::NotADirectory
                )
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
        let prepared =
            PreparedZarrStorageFormat::store(tile.clone(), data.key, &data.result_descriptor)
                .await
                .expect("tile should be stored after removing blocker file");
        let disk_entry = prepared
            .write()
            .await
            .expect("write should work after removing blocker file");
        compare_tiles(
            &tile,
            &DiskEntry::read(track_written_entry(disk_entry))
                .await
                .expect("stored tile should load"),
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn store_prepares_empty_tile_with_metadata() {
        let _directory = set_tempdir();
        let test_setup = setup(TestTile::EmptyTile);
        let prepared = PreparedZarrStorageFormat::store(
            test_setup.tile.clone(),
            test_setup.key,
            &test_setup.result_descriptor,
        )
        .await
        .unwrap();
        assert!(prepared.tile_data.is_none());
        assert!(prepared.validity_data.is_none());
        // zarr specific metadata is always present
        assert!(!prepared.metadata.bytes.is_empty());
        let loaded = prepared.load().await.unwrap();

        compare_tiles(&test_setup.tile, &loaded);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn it_runs_zarr_storage_work_on_a_blocking_thread() {
        use zarrs::storage::storage_adapter::sync_to_async::SyncToAsyncSpawnBlocking;

        let runtime_thread = std::thread::current().id();
        let worker_thread = TokioSpawnBlocking
            .spawn_blocking(|| std::thread::current().id())
            .await;

        assert_ne!(runtime_thread, worker_thread);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn it_round_trips_bytes_through_async_storage() {
        let _directory = set_tempdir();
        let data = setup(TestTile::NonEmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths)
            .await
            .expect("it to open the store asynchronously");
        let key = StoreKey::new("store/key").unwrap();
        let expected = bytes::Bytes::from_static(b"data");

        store.set(&key, expected.clone()).await.unwrap();
        assert_eq!(store.get(&key).await.unwrap(), Some(expected));

        store.erase(&key).await.unwrap();
        assert!(store.get(&key).await.unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_ensure_layout() {
        let _directory = set_tempdir();
        let data = setup(TestTile::NonEmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths).await.unwrap();
        let prepared =
            PreparedZarrStorageFormat::store(data.tile, data.key, &data.result_descriptor)
                .await
                .unwrap();
        let result = async_ensure_layout(&store, &prepared.layout).await;
        let expected_bytes_written = prepared
            .layout
            .iter()
            .fold(0, |acc, (_, bytes)| acc + bytes.len());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_bytes_written);

        // when its checked again the written bytes should be 0
        let result = async_ensure_layout(&store, &prepared.layout).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_ensure_layout_mismatch() {
        let directory = set_tempdir();
        let data = setup(TestTile::NonEmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths).await.unwrap();
        let prepared =
            PreparedZarrStorageFormat::store(data.tile, data.key, &data.result_descriptor)
                .await
                .unwrap();

        // write once to to create the necessary directories and files
        let _ = async_ensure_layout(&store, &prepared.layout).await.unwrap();
        let tiles_path = format_cache_key(directory.path().join(ZARR_CACHE), &paths)
            .join(paths.tiles().trim_start_matches('/'));
        let layout_file = tiles_path.join("zarr.json");
        // overwrite file
        std::fs::write(&layout_file, r#"{"different": "file"}"#).unwrap();
        // check again
        let result = async_ensure_layout(&store, &prepared.layout).await;
        assert!(matches!(
            result,
            Err(Error::ZarrCacheMetadataMismatch { key: actual_key })
                if actual_key == StoreKey::new("tiles/zarr.json").unwrap().as_str()
        ),);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_write_to_store() {
        let _directory = set_tempdir();
        let data = setup(TestTile::NonEmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths).await.unwrap();
        let prepared =
            PreparedZarrStorageFormat::store(data.tile, data.key, &data.result_descriptor)
                .await
                .unwrap();
        let result = async_write_to_store(&prepared, &store, true).await;
        let expected_bytes_written_layout = prepared
            .layout
            .iter()
            .fold(0, |acc, (_, bytes)| acc + bytes.len());
        let expected_bytes_data = prepared.metadata.bytes.len()
            + prepared.validity_data.as_ref().map_or(0, |v| v.bytes.len())
            + prepared.tile_data.as_ref().map_or(0, |t| t.bytes.len());
        let expected_bytes_written = expected_bytes_written_layout + expected_bytes_data;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_bytes_written);

        // check that the bytes written do not include json zarr metadata when false is passed as last argument
        let result = async_write_to_store(&prepared, &store, false).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), expected_bytes_data);

        // reading back the data should yield the same bytes
        let data_bytes = store
            .get(&prepared.tile_data.clone().unwrap().store_key)
            .await;
        let validity_bytes = store
            .get(&prepared.validity_data.clone().unwrap().store_key)
            .await;
        let metadata_bytes = store.get(&prepared.metadata.clone().store_key).await;
        assert_eq!(
            data_bytes.unwrap().unwrap(),
            prepared.tile_data.unwrap().bytes
        );
        assert_eq!(
            validity_bytes.unwrap().unwrap(),
            prepared.validity_data.unwrap().bytes
        );
        assert_eq!(metadata_bytes.unwrap().unwrap(), prepared.metadata.bytes);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_write_to_store_empty_tile() {
        let _directory = set_tempdir();
        let data = setup(TestTile::EmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths).await.unwrap();
        let prepared =
            PreparedZarrStorageFormat::store(data.tile, data.key, &data.result_descriptor)
                .await
                .unwrap();
        let bytes_written = async_write_to_store(&prepared, &store, false)
            .await
            .expect("it to write successfuly");
        assert_eq!(
            bytes_written,
            prepared.metadata.bytes.len(),
            "metadata was not written, but should always be written on succesfull write."
        );
    }

    async fn async_retrieve_tile_impl(kind: TestTile) {
        let _directory = set_tempdir();
        let data = setup(kind);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths)
            .await
            .expect("it to open the store asynchronously");
        let prepare =
            PreparedZarrStorageFormat::store(data.tile.clone(), data.key, &data.result_descriptor)
                .await
                .expect("it to prepare the tile for storage");
        let disk_entry = prepare
            .write()
            .await
            .expect("it to write the tile to disk successfully");
        let array_access = paths.async_open(store).await.unwrap();
        let actual = array_access
            .async_retrieve_tile(
                &disk_entry.chunk_indices,
                &paths,
                disk_entry.data_type,
                disk_entry.cache_hint,
            )
            .await
            .unwrap();
        compare_tiles(&data.tile, &actual);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_retrieve_tile() {
        async_retrieve_tile_impl(TestTile::NonEmptyTile).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_retrieve_tile_empty_tile() {
        async_retrieve_tile_impl(TestTile::EmptyTile).await;
    }

    fn prepare_two_tiles(
        data: &mut TestSetup,
    ) -> ((TypedRasterTile2D, CacheKey), (TypedRasterTile2D, CacheKey)) {
        fn mk_key(i: usize, data: &TestSetup) -> CacheKey {
            let pixel_position = match i {
                0 => [0, 0],
                1 => [0, 2],
                _ => unreachable!("test contains exactly two tiles"),
            };

            (
                data.key.0.clone(),
                data.key.1,
                data.key.2,
                pixel_position.into(),
            )
        }
        data.result_descriptor.spatial_grid = SpatialGridDescriptor::source_from_parts(
            data.geo_transform,
            GridBoundingBox2D::new([0, 0], [1, 3]).unwrap(),
        );
        let tile1 = data.tile.clone();
        let tile2: TypedRasterTile2D = call_generic_raster_tile_2d!(tile1.clone(), tile => {
            let mut tile = tile;
            tile.tile_position.0[1] += 1;
            tile.into()
        });
        ((tile1, mk_key(0, data)), (tile2, mk_key(1, data)))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_erase() {
        let mut data = setup(TestTile::NonEmptyTile);
        let directory = set_tempdir();
        let ((tile1, key1), (tile2, key2)) = prepare_two_tiles(&mut data);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let store = async_open_zarrs_filesystem_store(&paths)
            .await
            .expect("it to open the store asynchronously");

        // write both
        let mut disk_entries = Vec::with_capacity(2);
        for (i, (tile, key)) in [(tile1, key1), (tile2.clone(), key2)]
            .into_iter()
            .enumerate()
        {
            let prepared =
                PreparedZarrStorageFormat::store(tile, Arc::new(key), &data.result_descriptor)
                    .await
                    .expect(&format!("it to prepare the tile{} for storage", i));
            let disk_entry = prepared
                .write()
                .await
                .expect(&format!("it to write the tile{} to disk", i));
            disk_entries.push(track_written_entry(disk_entry));
        }

        // erase the first one
        disk_entries[0]
            .erase()
            .await
            .expect("it to erase the first tile");

        // verify the metadata is still present
        let root = format_cache_key(directory.path().join(ZARR_CACHE), &paths);
        let tiles_path = root.join(paths.tiles().trim_start_matches("/"));
        let validity_path = root.join(paths.validity().trim_start_matches("/"));
        let metadata_path = root.join(paths.metadata().trim_start_matches("/"));
        assert!(tiles_path.join("zarr.json").exists());
        assert!(validity_path.join("zarr.json").exists());
        assert!(metadata_path.join("zarr.json").exists());
        // verify that the actual data is not there anymore though
        assert!(!tiles_path.join("c/0/0").exists());
        assert!(!validity_path.join("c/0/0").exists());
        assert!(!metadata_path.join("c/0/0").exists());

        // verify the second tile can be read
        let actual_tile_2 = DiskEntry::read(Arc::clone(&disk_entries[1]))
            .await
            .expect("it to read the second tile after erasing the first");
        compare_tiles(&tile2, &actual_tile_2);

        // deleting the first tile should do nothing and succeed
        disk_entries[0]
            .erase()
            .await
            .expect("it to erase the first tile");
    }

    async fn erase_and_inject_failure()
    -> (Result<()>, PathBuf, TempDir, ZarrsArrayPath, ZarrDiskEntry) {
        let directory = set_tempdir();
        let data = setup(TestTile::NonEmptyTile);
        let paths = ZarrsArrayPath::from_cache_key(&data.key);
        let prepared =
            PreparedZarrStorageFormat::store(data.tile, data.key, &data.result_descriptor)
                .await
                .expect("it to prepare the tile for storage");
        let disk_entry = prepared
            .write()
            .await
            .expect("it to write the tile to disk");

        let chunk_path = overwrite_tiles_arr_coords_0_0_and_write_blocker(&directory, &paths).await;
        let result = disk_entry.erase().await;
        (result, chunk_path, directory, paths, disk_entry)
    }

    async fn overwrite_tiles_arr_coords_0_0_and_write_blocker(
        directory: &TempDir,
        paths: &ZarrsArrayPath,
    ) -> PathBuf {
        let chunk_path =
            format_cache_key(directory.path().join(ZARR_CACHE), &paths).join("tiles/c/0/0");
        tokio::fs::remove_file(&chunk_path).await.unwrap();
        tokio::fs::create_dir(&chunk_path).await.unwrap();
        tokio::fs::write(chunk_path.join("blocker"), b"You shall not pass!")
            .await
            .unwrap();
        chunk_path
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_async_erase_failure() {
        let (result, chunk_path, directory, paths, disk_entry) = erase_and_inject_failure().await;
        assert!(matches!(
            result,
            Err(Error::ZarrChunkEraseError {
                chunk_indices: [0, 0],
                errors,
            }) if matches!(
                errors.as_slice(),
                [(zarrs::storage::StorageError::IOError(_), WhichZarrArray::Tiles)]
            )
        ));
        // verify that validity and metadata chunks are deleted
        let root = format_cache_key(directory.path().join(ZARR_CACHE), &paths);
        let tiles_path = root.join(paths.tiles().trim_start_matches("/"));
        let validity_path = root.join(paths.validity().trim_start_matches("/"));
        let metadata_path = root.join(paths.metadata().trim_start_matches("/"));
        assert!(!validity_path.join("c/0/0").exists());
        assert!(!metadata_path.join("c/0/0").exists());

        // layout should remain
        let layout = format_cache_key(directory.path().join(ZARR_CACHE), &paths)
            .join(paths.tiles().trim_start_matches("/"))
            .join("zarr.json");
        assert!(layout.exists());
    }

    mod eviction_tests {
        use super::*;
        use crate::cache::new_raster_cache::{
            NewRasterCacheEnum, test_util::test_default_fifo_zarr,
        };

        struct FailedEviction {
            _directory: TempDir,
            cache: NewRasterCacheEnum,
            key1: CacheKey,
            key2: CacheKey,
            tile2: TypedRasterTile2D,
            result_descriptor: RasterResultDescriptor,
            chunk_path: PathBuf,
            expected_bytes_after_retry: usize,
        }

        async fn setup_failed_eviction() -> FailedEviction {
            let directory = set_tempdir();
            let mut data = setup(TestTile::NonEmptyTile);
            let ((tile1, key1), (tile2, key2)) = prepare_two_tiles(&mut data);
            let (prepared1, prepared2) = {
                let x = PreparedZarrStorageFormat::store(
                    tile1.clone(),
                    Arc::new(key1.clone()),
                    &data.result_descriptor,
                )
                .await
                .expect("it to prepare the first tile for storage");
                let y = PreparedZarrStorageFormat::store(
                    tile2.clone(),
                    Arc::new(key2.clone()),
                    &data.result_descriptor,
                )
                .await
                .expect("it to prepare the second tile for storage");
                (x, y)
            };

            // inserts counts per tile: tile.size + key.size
            // thus the maximum of the tiles is enough to make eviction happen
            let max_bytes_in_cache = usize::max(
                prepared1.byte_size().await.unwrap(),
                prepared2.byte_size().await.unwrap(),
            ) + cache_key_byte_size(&key1);
            assert_eq!(cache_key_byte_size(&key1), cache_key_byte_size(&key2));
            let mut cache = test_default_fifo_zarr(Some(max_bytes_in_cache));
            let () = cache
                .insert(key1.clone(), tile1.clone(), &data.result_descriptor)
                .await
                .expect("the cache should succesfully insert tile1");

            // verify its inside, but dont hold on to the Arc, or eviction cannot trigger
            {
                let get_result = cache
                    .get(&key1)
                    .await
                    .expect("it to get the option out of the store")
                    .expect("the key1 to map to tile1");
                let tile = get_result.load().await.expect("loading should work");
                compare_tiles(&tile1, &tile);
            }

            let paths = ZarrsArrayPath::from_cache_key(&key1);
            // overwrite the data to inject an erase error
            let chunk_path =
                overwrite_tiles_arr_coords_0_0_and_write_blocker(&directory, &paths).await;

            let size_before_insert_2 = cache.byte_size();
            assert!(size_before_insert_2 > 0);
            // write the second tile, triggering the eviction
            let result = cache
                .insert(key2.clone(), tile2.clone(), &data.result_descriptor)
                .await;
            assert!(matches!(
                result,
                Err(Error::ZarrChunkEraseError {
                    chunk_indices: [0, 0],
                    errors,
                }) if matches!(
                    errors.as_slice(),
                    [(zarrs::storage::StorageError::IOError(_), WhichZarrArray::Tiles)]
                )
            ));
            assert_eq!(size_before_insert_2, cache.byte_size());

            let get_result_2 = cache
                .get(&key2)
                .await
                .expect("getting a result for key2 should work");
            assert!(get_result_2.is_none());

            // eviction should have removed this (key, tile) pair value
            let get_result_1 = cache
                .get(&key1)
                .await
                .expect("getting a result for key1 should work");
            assert!(get_result_1.is_none());

            assert_eq!(size_before_insert_2, cache.byte_size());

            let expected_bytes_after_retry =
                cache_key_byte_size(&key2) + prepared2.byte_size().await.unwrap();
            FailedEviction {
                _directory: directory,
                cache,
                key1,
                key2,
                tile2,
                result_descriptor: data.result_descriptor,
                chunk_path,
                expected_bytes_after_retry,
            }
        }

        #[tokio::test(flavor = "current_thread")]
        async fn test_it_invalidates_zarr_entry_after_partial_erase() {
            let _failed_eviction = setup_failed_eviction().await;
        }

        #[tokio::test(flavor = "current_thread")]
        async fn test_it_frees_cache_capacity_after_successful_eviction_retry() {
            let FailedEviction {
                _directory,
                mut cache,
                key1,
                key2,
                tile2,
                result_descriptor,
                chunk_path,
                expected_bytes_after_retry,
            } = setup_failed_eviction().await;

            // now remove the blocker
            tokio::fs::remove_dir_all(&chunk_path).await.unwrap();

            cache
                .insert(key2.clone(), tile2.clone(), &result_descriptor)
                .await
                .expect("it to insert tile2 again after removing the blocker");

            // tile1 is a miss, tile2 should be inside
            {
                let get_result_1 = cache
                    .get(&key1)
                    .await
                    .expect("getting a result for key1 should work");
                assert!(get_result_1.is_none());

                let get_result_2 = cache
                    .get(&key2)
                    .await
                    .expect("getting a result for key2 should work")
                    .expect("the key2 to map to tile2");
                let tile = get_result_2.load().await.expect("loading should work");
                compare_tiles(&tile2, &tile);

                // size of the cache should now be only sizeof(tile2) + sizeof(key2) , as (tile1, key1) is fully removed
                assert_eq!(expected_bytes_after_retry, cache.byte_size());
            }
        }
    }

    mod format_tests {

        use super::*;

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
    }
}
