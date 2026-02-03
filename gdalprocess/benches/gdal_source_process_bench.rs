use std::rc::Rc;

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
    GdalSource,
    gdal_source::{
        load_tile_async,
        process::{IpcChannelMessage, JsonPayload, spawn_ipc_server_process_bytes},
    },
};

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

    (get_params(), tile_info, time_interval, cache_hint)
}

fn make_request(output_shape: GridShape2D, output_bounds: SpatialPartition2D) -> IpcChannelMessage {
    let tile_info = TileInformation::with_partition_and_shape(output_bounds, output_shape);
    let time_interval = TimeInterval::default();
    let cache_hint = CacheHint::default();
    IpcChannelMessage::RequestTileData {
        cache_hint,
        dataset_params: get_params(),
        tile_information: tile_info,
        tile_time: time_interval,
    }
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
    //let grid = raster.grid_array;

    debug_assert!(!raster.grid_array.is_empty());
    raster

    //
    // let grid = grid.into_materialized_masked_grid();
    //
    // debug_assert_eq!(grid.inner_grid.data.len(), 64);
    // debug_assert_eq!(
    //     grid.inner_grid.data,
    //     &[
    //         255, 255, 255, 255, 255, 255, 255, 255, 255, 75, 37, 255, 44, 34, 39, 32, 255, 86, 255,
    //         255, 255, 30, 96, 255, 255, 255, 255, 255, 90, 255, 255, 255, 255, 255, 202, 255, 193,
    //         255, 255, 255, 255, 255, 89, 255, 111, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    //         255, 255, 255, 255, 255, 255, 255, 255, 255, 255
    //     ]
    // );
    //
    // debug_assert_eq!(grid.validity_mask.data.len(), 64);
    // debug_assert_eq!(grid.validity_mask.data, &[true; 64]);
    //
    // let properties = raster.properties;
    //
    // debug_assert!((properties.scale_option()).is_none());
    // debug_assert!(properties.offset_option().is_none());
    // debug_assert_eq!(
    //     properties.get_property(&RasterPropertiesKey {
    //         domain: None,
    //         key: "AREA_OR_POINT".to_string(),
    //     }),
    //     Some(&RasterPropertiesEntry::String("Area".to_string()))
    // );
    // debug_assert_eq!(
    //     properties.get_property(&RasterPropertiesKey {
    //         domain: Some("IMAGE_STRUCTURE_INFO".to_string()),
    //         key: "COMPRESSION".to_string(),
    //     }),
    //     Some(&RasterPropertiesEntry::String("LZW".to_string()))
    // );
}

fn ipc_channel_process_request_bench_without_server_start(c: &mut Criterion) {
    let output_shape: GridShape2D = [8, 8].into();
    let output_bounds = SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

    let (mut child, sender, receiver) = spawn_ipc_server_process_bytes::<JsonPayload>();
    let receiver = Rc::new(receiver);

    c.bench_function("request_tile_from_process_no_start", |b| {
        let receiver = receiver.clone();
        b.iter(|| {
            let res = load_tile_data_process(
                &make_request(output_shape, output_bounds),
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
    let output_bounds = SpatialPartition2D::new_unchecked((-180., 90.).into(), (180., -90.).into());

    let runtime =
        tokio::runtime::Runtime::new().expect("It should be possible to create a runtime");
    c.bench_function("request_tile_from_process_no_start", |b| {
        b.to_async(&runtime).iter(async || {
            let (dataset_params, tile_information, tile_time, cache_hint) =
                make_stuff_for_other_benchmark(output_shape, output_bounds);
            let tile =
                load_tile_async::<u8>(dataset_params, tile_information, tile_time, cache_hint)
                    .await;
            std::hint::black_box(tile)
        });
    });
}

criterion_group!(
    benches,
    ipc_channel_process_request_bench_without_server_start,
    standard_reading,
    /* ipc_channel_process_request_bench_server_start */
);
criterion_main!(benches);
