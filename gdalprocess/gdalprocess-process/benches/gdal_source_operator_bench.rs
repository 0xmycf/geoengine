use std::cell::Cell;

use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use geoengine_datatypes::{
    primitives::{
        AxisAlignedRectangle, BandSelection, RasterQueryRectangle, SpatialPartition2D,
        SpatialResolution, TimeInterval,
    },
    raster::{GridShape2D, GridSize},
};
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;
use rayon::ThreadPoolBuilder;

use geoengine_datatypes::util::test::TestDefault;
use geoengine_operators::{
    engine::{
        MockExecutionContext, MockQueryContext, RasterOperator, RasterQueryProcessor,
        WorkflowOperatorPath,
    },
    source::{GdalDatasetParameters, GdalSource, GdalSourceParameters},
    util::gdal::create_ndvi_meta_data,
};

fn random_output_bounds(
    output_shape: GridShape2D,
    params: &GdalDatasetParameters,
    rng: &mut impl Rng,
) -> SpatialPartition2D {
    let tile_x = output_shape.axis_size_x();
    let tile_y = output_shape.axis_size_y();

    let max_start_x = params.width - tile_x;
    let max_start_y = params.height - tile_y;
    let start_x = rng.random_range(0..=max_start_x);
    let start_y = rng.random_range(0..=max_start_y);

    let geo_transform = params.geo_transform;
    let origin = geo_transform.origin_coordinate;
    let x_pixel = geo_transform.x_pixel_size;
    let y_pixel = geo_transform.y_pixel_size;

    let x0 = origin.x + x_pixel * start_x as f64;
    let y0 = origin.y + y_pixel * start_y as f64;
    let x1 = x0 + x_pixel * tile_x as f64;
    let y1 = y0 + y_pixel * tile_y as f64;

    let (left_x, right_x) = if x0 <= x1 { (x0, x1) } else { (x1, x0) };
    let (upper_y, lower_y) = if y0 >= y1 { (y0, y1) } else { (y1, y0) };

    SpatialPartition2D::new_unchecked((left_x, upper_y).into(), (right_x, lower_y).into())
}

fn make_query_rectangles(
    count: usize,
    output_shape: GridShape2D,
    params: &GdalDatasetParameters,
    time_interval: TimeInterval,
    seed: u64,
) -> Vec<RasterQueryRectangle> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let output_bounds = random_output_bounds(output_shape, params, &mut rng);
            let spatial_resolution = SpatialResolution::new_unchecked(
                output_bounds.size_x() / output_shape.axis_size_x() as f64,
                output_bounds.size_y() / output_shape.axis_size_y() as f64,
            );

            RasterQueryRectangle {
                spatial_bounds: output_bounds,
                time_interval,
                spatial_resolution,
                attributes: BandSelection::first(),
            }
        })
        .collect()
}

fn gdal_source_operator_process(c: &mut Criterion) {
    let _ = ThreadPoolBuilder::new().build_global();
    let output_shape: GridShape2D = [8, 8].into();
    let meta = create_ndvi_meta_data();
    let params = meta.params.clone();
    let time_interval = meta.data_time;
    let query_rectangles = make_query_rectangles(1024, output_shape, &params, time_interval, 0);
    let query_idx = Cell::new(0);

    let mut exe_ctx = MockExecutionContext::test_default();
    let query_ctx = MockQueryContext::test_default();

    let ndvi_id = geoengine_operators::util::gdal::add_ndvi_dataset(&mut exe_ctx);

    let runtime = tokio::runtime::Runtime::new().expect("runtime creation should work");
    let processor = runtime.block_on(async {
        let op = GdalSource {
            params: GdalSourceParameters { data: ndvi_id.clone() },
        }
        .boxed();

        let initialized = op
            .initialize(WorkflowOperatorPath::initialize_root(), &exe_ctx)
            .await
            .expect("GdalSource should initialize");

        initialized
            .query_processor()
            .expect("query processor should be available")
            .get_u8()
            .expect("ndvi should be u8")
    });

    c.bench_function("gdal_source_operator_process", |b| {
        b.to_async(&runtime).iter(|| {
            let idx = query_idx.get();
            query_idx.set((idx + 1) % query_rectangles.len());
            let query = query_rectangles[idx].clone();
            async {
                let tiles = processor
                    .raster_query(query, &query_ctx)
                    .await
                    .expect("query should succeed")
                    .collect::<Vec<_>>()
                    .await;
                std::hint::black_box(tiles);
            }
        });
    });
}

criterion_group!(benches, gdal_source_operator_process);
criterion_main!(benches);
