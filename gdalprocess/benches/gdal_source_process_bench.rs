use std::{cell::Cell, rc::Rc};

use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::{
    primitives::{CacheHint, SpatialPartition2D, TimeInterval},
    raster::{
        GridShape2D, RasterPropertiesEntryType, RasterPropertiesKey, RasterTile2D, TileInformation,
        arrow_ipc_file_to_raster_tile_2d,
    },
    test_data,
};
use geoengine_operators::source::{
    FileNotFoundHandling, GdalDatasetGeoTransform, GdalDatasetParameters, GdalMetadataMapping,
    gdal_source::{
        load_tile_async,
        process::{IpcChannelMessage, JsonPayload, spawn_ipc_server_process_bytes},
    },
};
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

fn get_params() -> GdalDatasetParameters {
    GdalDatasetParameters {
        file_path: test_data!("raster/modis_ndvi/MOD13A2_M_NDVI_2014-01-01.TIFF").into(),
        rasterband_channel: 1,
        geo_transform: GdalDatasetGeoTransform {
            origin_coordinate: (-180., 90.).into(),
            x_pixel_size: 0.1,
            y_pixel_size: -0.1,
        },
        width: 3600,
        height: 1800,
        file_not_found_handling: FileNotFoundHandling::NoData,
        no_data_value: Some(0.),
        properties_mapping: Some(vec![
            GdalMetadataMapping {
                source_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
                target_type: RasterPropertiesEntryType::String,
                target_key: RasterPropertiesKey {
                    domain: None,
                    key: "AREA_OR_POINT".to_string(),
                },
            },
            GdalMetadataMapping {
                source_key: RasterPropertiesKey {
                    domain: Some("IMAGE_STRUCTURE".to_string()),
                    key: "COMPRESSION".to_string(),
                },
                target_type: RasterPropertiesEntryType::String,
                target_key: RasterPropertiesKey {
                    domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
                    key: "COMPRESSION".to_string(),
                },
            },
        ]),
        gdal_open_options: None,
        gdal_config_options: None,
        allow_alphaband_as_mask: true,
        retry: None,
    }
}

fn make_stuff_for_other_benchmark(
    params: &GdalDatasetParameters,
    output_shape: GridShape2D,
    output_bounds: SpatialPartition2D,
) -> (
    GdalDatasetParameters,
    TileInformation,
    TimeInterval,
    CacheHint,
) {
    let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
    let time_interval = TimeInterval::default();
    let cache_hint = CacheHint::default();

    (params.clone(), tile_info, time_interval, cache_hint)
}

fn make_request(
    params: &GdalDatasetParameters,
    output_shape: GridShape2D,
    output_bounds: SpatialPartition2D,
) -> IpcChannelMessage {
    let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
    let time_interval = TimeInterval::default();
    let cache_hint = CacheHint::default();
    IpcChannelMessage::RequestTileData {
        cache_hint,
        dataset_params: params.clone(),
        tile_information: tile_info,
        tile_time: time_interval,
    }
}

fn random_output_bounds(
    output_shape: GridShape2D,
    params: &GdalDatasetParameters,
    rng: &mut impl Rng,
) -> SpatialPartition2D {
    let [tile_y, tile_x] = output_shape.axis_size();
    debug_assert!(tile_x > 0 && tile_y > 0);
    debug_assert!(params.width >= tile_x && params.height >= tile_y);

    let max_start_x = params.width - tile_x;
    let max_start_y = params.height - tile_y;
    let start_x = rng.gen_range(0..=max_start_x);
    let start_y = rng.gen_range(0..=max_start_y);

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

fn make_random_bounds_sequence(
    count: usize,
    output_shape: GridShape2D,
    params: &GdalDatasetParameters,
    seed: u64,
) -> Vec<SpatialPartition2D> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..count)
        .map(|_| random_output_bounds(output_shape, params, &mut rng))
        .collect()
}

fn load_tile_data_process(
    request: &IpcChannelMessage,
    (sender, receiver): (
        ipc_channel::ipc::IpcSender<JsonPayload>,
        Rc<ipc_channel::ipc::IpcBytesReceiver>,
    ),
) -> RasterTile2D<u8> {
    sender
        .send(JsonPayload::new(request))
        .expect("Failed to send request");

    let raster: RasterTile2D<u8> = receiver
        .recv()
        .map(arrow_ipc_file_to_raster_tile_2d)
        .expect("The server should answer with the requested tile")
        .expect("The arrow IPC data should be convertible to RasterTile2D");

    debug_assert!(!raster.grid_array.is_empty());
    raster
}

fn ipc_channel_process_request_bench_without_server_start(c: &mut Criterion) {
    let output_shape: GridShape2D = [8, 8].into();
    let params = get_params();
    let bounds = make_random_bounds_sequence(1024, output_shape, &params, 0);
    let bounds_idx = Cell::new(0);

    let (mut child, sender, receiver) = spawn_ipc_server_process_bytes::<JsonPayload>();
    let receiver = Rc::new(receiver);

    c.bench_function("load_tile_data_process", |b| {
        let receiver = receiver.clone();
        b.iter(|| {
            let output_bounds = bounds[bounds_idx.get()];
            bounds_idx.set((bounds_idx.get() + 1) % bounds.len());
            let res = load_tile_data_process(
                &make_request(&params, output_shape, output_bounds),
                (sender.clone(), receiver.clone()),
            );
            std::hint::black_box(res);
        });
    });
    sender
        .send(JsonPayload::new(&IpcChannelMessage::EndConnection))
        .expect("The server should receive the end connection message");
    debug_assert!(child.kill().is_ok());
}

fn standard_reading(c: &mut Criterion) {
    let output_shape: GridShape2D = [8, 8].into();
    let params = get_params();
    let bounds = make_random_bounds_sequence(1024, output_shape, &params, 0);
    let bounds_idx = Cell::new(0);

    let runtime =
        tokio::runtime::Runtime::new().expect("It should be possible to create a runtime");
    c.bench_function("load_tile_async_without_process", |b| {
        b.to_async(&runtime).iter(|| {
            let output_bounds = bounds[bounds_idx.get()];
            bounds_idx.set((bounds_idx.get() + 1) % bounds.len());
            async move {
                let (dataset_params, tile_information, tile_time, cache_hint) =
                    make_stuff_for_other_benchmark(&params, output_shape, output_bounds);
                let tile =
                    load_tile_async::<u8>(dataset_params, tile_information, tile_time, cache_hint)
                        .await;
                std::hint::black_box(tile)
            }
        });
    });
}

criterion_group!(
    benches,
    ipc_channel_process_request_bench_without_server_start,
    standard_reading,
);
criterion_main!(benches);
