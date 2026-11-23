// TODO (low): handle all panics!
#![allow(unused)]
use std::path::PathBuf;
#[cfg(not(test))]
use std::sync::OnceLock;
#[cfg(test)]
use std::{
    sync::{LazyLock, RwLock as StdRwLock},
    thread::ThreadId,
};

use geoengine_datatypes::{primitives::DateTimeParseFormat, raster::RasterDataType};
use tokio::sync::Mutex;

use crate::cache::new_raster_cache::*;
#[cfg(not(test))]
use crate::error::Error;

pub mod arrow_ipc_cache;
pub mod zarrs_cache;

const GLOBAL_GEOTRANSFORM_KEY: &'static str = "global_geo_transform";

pub struct OnDiskStore<SF: OnDiskStorageFormat, ES: EvictionStrategy> {
    pub entries: RwLock<DiskEntries<SF::Written>>,
    pub eviction_strategy: Arc<RwLock<ES>>,
    pub total_current_size: Arc<AtomicUsize>,
    mutations: Mutex<()>,
}

impl<SF: OnDiskStorageFormat, ES: EvictionStrategy> OnDiskStore<SF, ES> {
    pub fn new<F>(capacity: usize, mk_evicion: F) -> Self
    where
        F: FnOnce(usize) -> ES,
    {
        OnDiskStore {
            entries: RwLock::new(DiskEntries::new()),
            eviction_strategy: Arc::new(RwLock::new(mk_evicion(capacity))),
            total_current_size: Arc::new(AtomicUsize::new(0)),
            mutations: Mutex::new(()),
        }
    }

    // The caller holds the mutation lock, so this key cannot be replaced
    // between taking its handle and completing its deletion.
    async fn erase_marked(&self, key: &CacheKey) -> Result<()> {
        let entry = self.entries.read().await.get(key).cloned();
        if let Some(entry) = entry {
            entry.erase().await?;
        }
        self.entries.write().await.remove(key);
        Ok(())
    }

    async fn retry_pending_deletes(&self) {
        let keys: Vec<_> = self
            .entries
            .read()
            .await
            .pending_deletes
            .iter()
            .cloned()
            .collect();
        for key in keys {
            if let Err(err) = self.erase_marked(&key).await {
                tracing::warn!(error = ?err, ?key, "Failed to retry cache entry deletion");
            }
        }
    }
}

pub struct DiskEntries<T> {
    pub cache_index: HashMap<Arc<CacheKey>, Arc<SizeTrackedEntry<T>>>,
    pub pending_deletes: HashSet<Arc<CacheKey>>,
}

impl<T> DiskEntries<T> {
    pub fn new() -> Self {
        Self {
            cache_index: HashMap::new(),
            pending_deletes: HashSet::new(),
        }
    }

    /// The entry for key is pinned, if its [`Arc::strong_count`] is greater than 1,
    // meaning that there is at least one other reference to the entry.
    pub fn pinned(&self, key: &Arc<CacheKey>) -> bool {
        self.cache_index
            .get(key)
            .is_none_or(|entry| Arc::strong_count(entry) > 1)
    }

    /// Insert a cache entry into the index. Returns the previous entry if it existed.
    pub fn insert(
        &mut self,
        key: Arc<CacheKey>,
        entry: Arc<SizeTrackedEntry<T>>,
    ) -> Option<Arc<SizeTrackedEntry<T>>> {
        self.cache_index.insert(key, entry)
    }

    /// Get a cache entry from the index. Returns None if it does not exist.
    pub fn get(&self, key: &CacheKey) -> Option<&Arc<SizeTrackedEntry<T>>> {
        self.cache_index.get(key)
    }

    pub fn marked_for_deletion(&self, key: &CacheKey) -> bool {
        self.pending_deletes.contains(key)
    }

    /// Mark a cache entry for deletion. Returns true if the entry was not already marked for deletion.
    pub fn mark_delete(&mut self, key: Arc<CacheKey>) -> bool {
        self.pending_deletes.insert(key)
    }

    /// Remove an erased entry and its deletion marker from the index.
    pub fn remove(&mut self, key: &CacheKey) -> Option<Arc<SizeTrackedEntry<T>>> {
        self.pending_deletes.remove(key);
        self.cache_index.remove(key)
    }
}

/// A storage format that can be written to disk and read from disk
/// Eligible for in-memory caching as well.
#[async_trait]
pub trait OnDiskStorageFormat: StorageFormat {
    /// Handle to the files on disk that can be read back into memory
    type Written: DiskEntry + Expirable;

    /// Write the data to disk and return a handle to the files on disk.
    ///
    /// Assumption: Data is not overwritten, as
    ///     key1 = key2 => data1 = data2
    async fn write(self) -> Result<Self::Written>;
}

/// A handle for one or multiple files on the disk
#[async_trait]
pub trait DiskEntry: Send + Sync + Sized + 'static {
    /// Read the entry from disk while holding it against eviction.
    ///
    /// The caller passes the same Arc held by the cache index. A detached read
    /// must keep this Arc alive until it finishes.
    async fn read(entry: Arc<SizeTrackedEntry<Self>>) -> Result<TypedRasterTile2D>;

    /// Delete the tile from disk.
    async fn erase(&self) -> Result<()>;
}

/// Wrap a directly written test entry that has no cache index or size accounting.
#[cfg(test)]
fn track_written_entry<T>(entry: T) -> Arc<SizeTrackedEntry<T>> {
    Arc::new(SizeTrackedEntry::new(entry, 0, Arc::default()))
}

#[async_trait]
impl<SF, ES> CacheStore for OnDiskStore<SF, ES>
where
    SF: OnDiskStorageFormat,
    ES: EvictionStrategy,
{
    type SF = SizeTrackedEntry<SF::Written>;
    type ES = ES;

    async fn get(&self, key: &CacheKey) -> Result<Option<Arc<Self::SF>>> {
        let hit = {
            let entries = self.entries.read().await;
            if entries.marked_for_deletion(key) {
                return Ok(None);
            }
            entries.get(key).map(Arc::clone)
        };

        if let Some(entry) = &hit {
            if entry.is_expired().await {
                // Release this lookup's pin before checking for other readers.
                drop(hit);

                let _mutation = self.mutations.lock().await;
                let arc_key = Arc::new(key.clone());
                {
                    let mut entries = self.entries.write().await;
                    if entries.marked_for_deletion(key) {
                        return Ok(None);
                    }

                    let Some(current) = entries.get(key) else {
                        return Ok(None);
                    };
                    if !current.is_expired().await || entries.pinned(&arc_key) {
                        return Ok(None);
                    }

                    let mut eviction_strategy = self.eviction_strategy.write().await;
                    entries.mark_delete(Arc::clone(&arc_key));
                    eviction_strategy.record_removal(&arc_key);
                }

                if let Err(err) = self.erase_marked(key).await {
                    tracing::warn!(error = ?err, ?key, "Failed to erase expired cache entry");
                }

                return Ok(None);
            }

            ES::record_hit(self.eviction_strategy.clone(), key).await;
        }

        Ok(hit)
    }

    // TODO (high): insertion should be looked over
    async fn insert(
        &self,
        key: Arc<CacheKey>,
        tile: TypedRasterTile2D,
        result_descriptor: &RasterResultDescriptor,
    ) -> Result<()> {
        /* We need to make sure that the file is not just written to the disk
        without actually being inserted in the cache / index later on.
        Otherwise we will have orphaned files. */
        {
            let entries = self.entries.read().await;
            if entries.get(&key).is_some() && !entries.marked_for_deletion(&key) {
                ES::record_hit(self.eviction_strategy.clone(), &key).await;
                return Ok(());
            }
        }

        let cache_hint = call_generic_raster_tile_2d!(&tile, tile => tile.cache_hint);
        // OnDiskStore prepares the tile here to insert it later with write()
        // this (usually) does not write to disk, though this limitation is not enforced
        let prepared_tile = SF::store(tile, key.clone(), result_descriptor).await?;
        let value_size = prepared_tile.byte_size().await?;

        let key_size = cache_key_byte_size(&key);
        let required_space = value_size + key_size;

        // Serialize disk mutations and capacity decisions while letting
        // lookups access the index during filesystem I/O.
        let _mutation = self.mutations.lock().await;
        self.retry_pending_deletes().await;

        let (eviction_plan, capacity) = {
            let entries = self.entries.read().await;
            if entries.marked_for_deletion(&key) {
                return Err(CacheError::PendingDelete.into());
            }
            if entries.get(&key).is_some() {
                return Ok(());
            }

            let eviction_strategy = self.eviction_strategy.read().await;
            let current_size = self.total_current_size.load(Ordering::SeqCst);
            let capacity = eviction_strategy.capacity();
            let plan = eviction_strategy
                .plan_eviction(current_size, required_space, |key| entries.pinned(key))?;
            let needed = required_space.saturating_sub(capacity.saturating_sub(current_size));
            if plan.freed_bytes < needed {
                return Err(CacheError::NotEnoughSpaceInCache.into());
            }
            (plan, capacity)
        };

        for evict_key in eviction_plan.keys_to_remove {
            {
                let mut entries = self.entries.write().await;
                if entries.marked_for_deletion(&evict_key) || entries.pinned(&evict_key) {
                    continue;
                }
                let mut eviction_strategy = self.eviction_strategy.write().await;
                entries.mark_delete(Arc::clone(&evict_key));
                eviction_strategy.record_removal(&evict_key);
            }

            self.erase_marked(&evict_key).await.inspect_err(|err| {
                tracing::warn!(error = ?err, key = ?evict_key, "Failed to erase cache entry.");
            })?;
        }

        // Check that eviction freed enough space.
        // A planned candidate may have become pinned after planning eviction.
        let current_size = self.total_current_size.load(Ordering::SeqCst);
        if required_space > capacity.saturating_sub(current_size) {
            return Err(CacheError::NotEnoughSpaceInCache.into());
        }

        let written = prepared_tile.write().await?;
        let mut entries = self.entries.write().await;
        let mut eviction_strategy = self.eviction_strategy.write().await;
        eviction_strategy.record_access(&key, required_space, cache_hint);
        entries.insert(
            key,
            Arc::new(SizeTrackedEntry::new(
                written,
                required_space,
                self.total_current_size.clone(),
            )),
        );
        Ok(())
    }
}

#[cfg(not(test))]
static RASTER_CACHE_DIR: OnceLock<PathBuf> = OnceLock::new();

// TODO (mid): this caused a race condition when running multiple tests at once, it must now be properly used
#[cfg(test)]
static RASTER_CACHE_DIR: LazyLock<StdRwLock<HashMap<ThreadId, PathBuf>>> =
    LazyLock::new(|| StdRwLock::new(HashMap::new()));

#[cfg(not(test))]
pub fn set_raster_cache_dir(path: PathBuf) -> Result<()> {
    RASTER_CACHE_DIR
        .set(path)
        .map_err(|_err| Error::MustNotHappen {
            message: "RASTER_CACHE_DIR should only be set once during startup".to_string(),
        })?;
    Ok(())
}

#[cfg(test)]
pub fn set_raster_cache_dir(path: PathBuf) -> Result<()> {
    let mut x = RASTER_CACHE_DIR.write().expect("it to be not poisoned");
    x.insert(std::thread::current().id(), path);
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

/// Returns the `RASTER_CACHE_DIR` config setting if it is set for the current thread,
/// if is not, then return then return the xdg-cache dir
#[cfg(test)]
fn cache_dir() -> PathBuf {
    RASTER_CACHE_DIR
        .read()
        .expect("[cfg(test) cache_dir()] read lock should not be poisoned")
        .get(&std::thread::current().id())
        .expect("[cfg(test) cache_dir()] the thread.id() to be present")
        .clone()
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

#[derive(Debug, Clone)]
pub struct CommonOnDiskStorageData {
    /// the datatype of the TypedRasterTile2D
    pub data_type: RasterDataType,
    /// The cache_hint from the tile, only used for [`Expirable::is_expired`]
    pub cache_hint: CacheHint,
    /// the cache key
    pub cache_key: Arc<CacheKey>,
}

impl CommonOnDiskStorageData {
    pub fn new(data_type: RasterDataType, cache_hint: CacheHint, cache_key: Arc<CacheKey>) -> Self {
        Self {
            data_type,
            cache_hint,
            cache_key,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::engine::{RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor};

    use super::*;
    use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
    use geoengine_datatypes::raster::{
        BoundedGrid, GeoTransform, Grid2D, GridShape2D, RasterDataType, RasterTile2D,
        TileInformation, TilesEqualIgnoringCacheHint,
    };
    use geoengine_datatypes::spatial_reference::SpatialReference;
    use geoengine_datatypes::util::test::TestDefault;
    use serde_json::json;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::tempdir;

    use tokio::sync::RwLock;

    pub fn test_key(idx: u32) -> Arc<CacheKey> {
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

    fn test_result_descriptor() -> RasterResultDescriptor {
        RasterResultDescriptor {
            data_type: RasterDataType::U8,
            spatial_reference: SpatialReference::epsg_4326().into(),
            time: TimeDescriptor::new_irregular(Some(TimeInterval::default())),
            spatial_grid: SpatialGridDescriptor::source_from_parts(
                GeoTransform::test_default(),
                GridShape2D::from([8, 8]).bounding_box(),
            ),
            bands: RasterBandDescriptors::new_multiple_bands(2),
        }
    }

    fn initialize_test_zarrs_cache_dir() -> tempfile::TempDir {
        let temp_dir = tempdir().expect("it to work");
        let () = set_raster_cache_dir(temp_dir.path().to_path_buf()).expect("it to work");
        temp_dir
    }

    fn assert_tiles_and_cache_hints(
        loaded_tile: &RasterTile2D<u8>,
        original_tile: &RasterTile2D<u8>,
    ) {
        assert!(
            loaded_tile.tiles_equal_ignoring_cache_hint(original_tile),
            "Tiles are not the same after on-disk cache"
        );
        assert_eq!(
            loaded_tile.cache_hint.total_ttl_seconds(),
            original_tile.cache_hint.total_ttl_seconds(),
            "cache hints have different ttl seconds"
        );
        assert_eq!(
            loaded_tile
                .cache_hint
                .created()
                .to_datetime_string_with_millis(),
            original_tile
                .cache_hint
                .created()
                .to_datetime_string_with_millis(),
            "cache hints have different creation times"
        );
        assert_eq!(
            loaded_tile
                .cache_hint
                .expires()
                .datetime()
                .to_datetime_string_with_millis(),
            original_tile
                .cache_hint
                .expires()
                .datetime()
                .to_datetime_string_with_millis(),
            "cache hints have different expiration times"
        );
    }

    mod arrow_ipc {
        use super::*;

        #[tokio::test]
        async fn arrow_ipc_storage_format_store_and_load() {
            initialize_test_zarrs_cache_dir();

            let band = 0;
            let key = test_key(band);
            let tile = test_tile_u8(band);
            let stored = ArrowIpcStorageFormat::store(tile.clone(), key, &test_result_descriptor())
                .await
                .expect("it should be possible to prepare the tile.");

            let byte_size = stored
                .byte_size()
                .await
                .expect("it should be possible to get the stored byte size.");

            let written = stored
                .write()
                .await
                .expect("it should be possible to write the tile to disk.");

            let path: PathBuf = written.path.clone();
            let metadata = fs::metadata(&path).expect(&format!("File should be at {path:#?}"));
            assert_eq!(byte_size as u64, metadata.len());

            let loaded = DiskEntry::read(track_written_entry(written))
                .await
                .expect("it should be possible to read the tile from disk.");
            // .expect(&format!("File '{path:#?}' should loadable as a RasterTile"));

            match loaded {
                TypedRasterTile2D::U8(loaded_tile) => {
                    if let TypedRasterTile2D::U8(original_tile) = tile {
                        super::assert_tiles_and_cache_hints(
                            &loaded_tile.into(),
                            &original_tile.into(),
                        );
                    } else {
                        panic!("expected u8 tile");
                    }
                }
                _ => panic!("expected u8 tile"),
            }
        }
    }

    mod zarrs {
        use super::*;
        use crate::cache::new_raster_cache::on_disk::zarrs_cache::ZarrDiskEntry;

        #[tokio::test]
        async fn on_disk_storage_insert_and_get_with_zarrs() {
            initialize_test_zarrs_cache_dir();

            let store: OnDiskStore<PreparedZarrStorageFormat, FifoEvictionStrategy> =
                OnDiskStore::new(usize::MAX, FifoEvictionStrategy::new);

            let band = 1;
            let key = test_key(band);
            let tile = test_tile_u8(band);
            let expected_tile = tile.clone();

            store
                .insert(key.clone(), tile, &test_result_descriptor())
                .await
                .expect("it should be possible to insert the tile into the OnDiskStorage.");
            let stored = store
                .get(&key)
                .await
                .expect("it should be present")
                .expect("it should be possible to extract the tile-ref from the cache.");
            let loaded = DiskEntry::read(stored)
                .await
                .expect("it should be possible to load the tile via the tile-ref.");

            match loaded {
                TypedRasterTile2D::U8(loaded_tile) => {
                    if let TypedRasterTile2D::U8(original_tile) = expected_tile {
                        super::assert_tiles_and_cache_hints(
                            &loaded_tile.into(),
                            &original_tile.into(),
                        );
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
            let stored =
                PreparedZarrStorageFormat::store(tile.clone(), key, &test_result_descriptor())
                    .await
                    .expect("it should be possible to store the tile.");
            let disk_entry = stored
                .write()
                .await
                .expect("it should be possible to write the tile to disk.");

            {
                // check if the zarrs cache is there
                let full_path = temp_dir.path().join(PathBuf::from("zarr_cache"));
                let _ =
                    fs::metadata(&full_path).expect(&format!("File should be at {full_path:#?}"));
            }

            let loaded = DiskEntry::read(track_written_entry(disk_entry))
                .await
                .expect(&format!("File should readable as a TypedRasterTile"));

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

            let store: OnDiskStore<PreparedZarrStorageFormat, FifoEvictionStrategy> =
                OnDiskStore::new(usize::MAX, FifoEvictionStrategy::new);

            let band = 1;
            let key = test_key(band);
            let tile = test_tile_u8(band);
            let expected_tile = tile.clone();

            store
                .insert(key.clone(), tile, &test_result_descriptor())
                .await
                .expect("it should be possible to insert the tile into the OnDiskStorage.");
            let stored = store
                .get(&key)
                .await
                .expect("cache lookup should succeed (Result)")
                .expect("inserted tiel should exist (Option)");
            let loaded = DiskEntry::read(stored)
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
    }

    #[test]
    fn time_interval_to_iso_test() {
        let time = TimeInterval::new(0, 2).expect("it should be valid");
        let expected = "1970-01-01T00-00-00Z_to_1970-01-01T00-00-00.002Z";
        let actual = time_interval_to_path_safe_iso(&time);
        assert_eq!(expected, actual);
    }
}
