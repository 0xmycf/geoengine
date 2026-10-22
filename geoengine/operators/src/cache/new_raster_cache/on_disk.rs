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

use geoengine_datatypes::primitives::DateTimeParseFormat;

use crate::cache::new_raster_cache::*;
#[cfg(not(test))]
use crate::error::Error;

pub mod arrow_ipc_cache;
pub mod zarrs_cache;

const GLOBAL_GEOTRANSFORM_KEY: &'static str = "global_geo_transform";

pub struct OnDiskStore<SF: OnDiskStorageFormat, ES: EvictionStrategy> {
    pub cache_index: RwLock<HashMap<Arc<CacheKey>, Arc<SizeTrackedEntry<SF::Written>>>>,
    pub eviction_strategy: Arc<RwLock<ES>>,
    pub total_current_size: Arc<AtomicUsize>,
}

// TODO (high): {462286af-362d-42f0-a803-ebe807aa820b}
/// A storage format that can be written to disk and read from disk
/// Eligible for in-memory caching as well.
#[async_trait]
pub trait OnDiskStorageFormat: StorageFormat {
    /// Handle to the files on disk that can be read back into memory
    type Written: DiskEntry<Prepared = Self>;

    /// Write the data to disk and return a handle to the files on disk
    async fn write(self) -> Result<Self::Written>;
}

/// A handle for one or multiple files on the disk
#[async_trait]
pub trait DiskEntry: Send + Sync + Sized + 'static {
    // The type that may be written to disk
    // this type is also eligible for in-memory caching
    type Prepared: OnDiskStorageFormat;

    async fn read(&self) -> Result<Self::Prepared>;
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
        todo!()
        // let x = self.cache_index.read().await.get(key).map(Arc::clone);
        // match x {
        //     Some(key) => Ok(Some(key)),
        //     None => Ok(None),
        // }
    }

    // TODO (crit): this function probably not right anymore, the base has changed a lot. fix it! after rebase
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
            let cache = self.cache_index.read().await;
            if cache.contains_key(&key) {
                ES::record_hit(self.eviction_strategy.clone(), &key).await;
                return Ok(());
            }
        }

        let cache_hint = cache_hint(&tile);
        let prepared_tile = SF::store(tile, key.clone(), result_descriptor).await?;
        let value_size = prepared_tile.byte_size().await?;

        let key_size = cache_key_byte_size(&key);
        let required_space = value_size + key_size;

        let mut cache = self.cache_index.write().await;
        if cache.contains_key(&key) {
            return Ok(());
        }

        let mut eviction_strategy = self.eviction_strategy.write().await;
        let current_size = self.total_current_size.load(Ordering::SeqCst);

        let eviction_plan =
            eviction_strategy.plan_eviction(current_size, required_space, |key| {
                cache.get(key).is_none_or(|sf| Arc::strong_count(sf) > 1)
            })?;

        let needed = (current_size + required_space).saturating_sub(eviction_strategy.capacity());
        if eviction_plan.freed_bytes < needed {
            return Err(CacheError::NotEnoughSpaceInCache.into());
        }

        for evict_key in eviction_plan.keys_to_remove {
            cache.remove(&evict_key);
            eviction_strategy.record_removal(&evict_key);
        }

        eviction_strategy.record_access(&key, required_space, cache_hint);
        cache.insert(
            key,
            Arc::new(SizeTrackedEntry::new(
                prepared_tile.write().await?,
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
        .expect("read lock should not be poisoned")
        .get(&std::thread::current().id())
        .expect("the value to be present")
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

            let loaded = written
                .read()
                .await
                .expect("it should be possible to read the tile from disk.")
                .load()
                .await
                .expect(&format!("File '{path:#?}' should loadable as a RasterTile"));
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

            let store: OnDiskStore<ZarrsStorageFormat, FifoEvictionStrategy> = OnDiskStore {
                cache_index: RwLock::new(HashMap::new()),
                eviction_strategy: Arc::new(RwLock::new(FifoEvictionStrategy::new(usize::MAX))),
                total_current_size: Arc::new(AtomicUsize::new(0)),
            };

            let band = 1;
            let key = test_key(band);
            let tile = test_tile_u8(band);
            let expected_tile = tile.clone();

            store
                .insert(key.clone(), tile, &test_result_descriptor())
                .await
                .expect("it should be possible to insert the tile into the OnDiskStorage.");
            let stored: Arc<SizeTrackedEntry<ZarrDiskEntry>> = store
                .get(&key)
                .await
                .expect("it should be present")
                .expect("it should be possible to extract the tile-ref from the cache.");
            let loaded = stored
                .read()
                .await
                .unwrap()
                .load()
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
            let stored = ZarrsStorageFormat::store(tile.clone(), key, &test_result_descriptor())
                .await
                .expect("it should be possible to store the tile.");

            {
                // check if the zarrs thing is there
                let full_path = temp_dir.path().join(PathBuf::from("zarr_cache"));
                let _ =
                    fs::metadata(&full_path).expect(&format!("File should be at {full_path:#?}"));
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

            let store: OnDiskStore<ZarrsStorageFormat, FifoEvictionStrategy> = OnDiskStore {
                cache_index: RwLock::new(HashMap::new()),
                eviction_strategy: Arc::new(RwLock::new(FifoEvictionStrategy::new(usize::MAX))),
                total_current_size: Arc::new(AtomicUsize::new(0)),
            };

            let band = 1;
            let key = test_key(band);
            let tile = test_tile_u8(band);
            let expected_tile = tile.clone();

            store
                .insert(key.clone(), tile, &test_result_descriptor())
                .await
                .expect("it should be possible to insert the tile into the OnDiskStorage.");
            let stored: Arc<SizeTrackedEntry<ZarrDiskEntry>> = store
                .get(&key)
                .await
                .expect("it should be present") // TODO (crit): figure out a better way
                .expect("it should be possible to extract the tile-ref from the cache.");
            let loaded = stored
                .read()
                .await
                .unwrap()
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
    }

    #[test]
    fn time_interval_to_iso_test() {
        let time = TimeInterval::new(0, 2).expect("it should be valid");
        let expected = "1970-01-01T00-00-00Z_to_1970-01-01T00-00-00.002Z";
        let actual = time_interval_to_path_safe_iso(&time);
        assert_eq!(expected, actual);
    }
}
