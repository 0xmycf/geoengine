use futures::StreamExt;
use geoengine_datatypes::primitives::TimeInstance;
use geoengine_datatypes::{
    primitives::{BandSelection, RasterQueryRectangle, TimeInterval},
    raster::{GridBoundingBox2D, RasterTile2D, TilesEqualIgnoringCacheHint},
    util::test::TestDefault,
};
use geoengine_operators::cache::new_raster_cache::{
    CacheKind, RasterCacheOperator, StorageFormat, StorageFormatEnum, test_default_fifo_zarr,
};
use geoengine_operators::engine::{
    ChunkByteSize, InitializedRasterOperator, MockExecutionContext, MockQueryContext,
    QueryProcessor, RasterOperator, SingleRasterSource, WorkflowOperatorPath,
};
use geoengine_operators::processing::{
    AggregateFunctionParams, NeighborhoodAggregate, NeighborhoodAggregateParams, NeighborhoodParams,
};
use geoengine_operators::source::{GdalSource, GdalSourceParameters};
use geoengine_operators::util::Result;
use geoengine_operators::util::gdal::add_ndvi_dataset;
use std::str::FromStr;

fn benchmark_query() -> RasterQueryRectangle {
    RasterQueryRectangle::new(
        GridBoundingBox2D::new([-900, -1800], [899, 1799]).expect("valid benchmark bounds"),
        TimeInterval::new_unchecked(
            TimeInstance::from_str("2014-01-01T00:00:00.000Z").expect("valid start instant"),
            TimeInstance::from_str("2015-01-01T00:00:00.000Z").expect("valid end instant"),
        ),
        BandSelection::first(),
    )
}

async fn query_tiles(
    processor: &geoengine_operators::engine::BoxRasterQueryProcessor<u8>,
    query_ctx: &MockQueryContext,
) -> Vec<RasterTile2D<u8>> {
    let stream = processor
        .query(benchmark_query(), query_ctx)
        .await
        .expect("query should succeed");

    let tiles = stream.collect::<Vec<_>>().await;
    tiles
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("tile stream should be valid")
}

#[tokio::test]
async fn zarr_cache_returns_same_tiles_as_benchmark_query() {
    let () = rayon::ThreadPoolBuilder::new()
        .build_global()
        .expect("the builder to be created globally");
    let mut uncached_ctx = MockExecutionContext::test_default();
    let uncached_ndvi = add_ndvi_dataset(&mut uncached_ctx);

    let uncached_operator = NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::WeightsMatrix {
                weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource {
            raster: GdalSource {
                params: GdalSourceParameters::new(uncached_ndvi),
            }
            .boxed(),
        },
    }
    .boxed()
    .initialize(WorkflowOperatorPath::initialize_root(), &uncached_ctx)
    .await
    .expect("operator should initialize");

    let uncached_processor = uncached_operator
        .query_processor()
        .expect("processor should exist")
        .get_u8()
        .expect("u8 processor expected");
    let uncached_query_ctx = uncached_ctx.mock_query_context_with_query_extensions(
        ChunkByteSize::test_default(),
        None,
        None,
        None,
    );

    let uncached_tiles = query_tiles(&uncached_processor, &uncached_query_ctx).await;

    let mut cached_ctx = MockExecutionContext::test_default();
    let cached_ndvi = add_ndvi_dataset(&mut cached_ctx);

    let cached_source_operator = NeighborhoodAggregate {
        params: NeighborhoodAggregateParams {
            neighborhood: NeighborhoodParams::WeightsMatrix {
                weights: vec![vec![1., 2., 3.], vec![4., 5., 6.], vec![7., 8., 9.]],
            },
            aggregate_function: AggregateFunctionParams::Sum,
        },
        sources: SingleRasterSource {
            raster: GdalSource {
                params: GdalSourceParameters::new(cached_ndvi.clone()),
            }
            .boxed(),
        },
    }
    .boxed()
    .initialize(WorkflowOperatorPath::initialize_root(), &cached_ctx)
    .await
    .expect("operator should initialize");

    let cached_operator = RasterCacheOperator::wrap_operator_with_cache_kind(
        cached_source_operator,
        CacheKind::new_on_disk(StorageFormatEnum::Zarr),
    );

    let cached_query_ctx = cached_ctx.mock_query_context_with_query_extensions(
        ChunkByteSize::test_default(),
        None,
        None,
        None,
    );

    let cached_processor = cached_operator
        .query_processor()
        .expect("processor should exist")
        .get_u8()
        .expect("u8 processor expected");

    let _warmup_tiles = query_tiles(&cached_processor, &cached_query_ctx).await;
    cached_ctx.delete_meta_data(&cached_ndvi);

    let cached_tiles = query_tiles(&cached_processor, &cached_query_ctx).await;
    assert!(uncached_tiles.tiles_equal_ignoring_cache_hint(&cached_tiles));
}
