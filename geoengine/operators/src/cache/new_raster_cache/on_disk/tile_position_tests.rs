use super::*;
use crate::cache::new_raster_cache::on_disk::set_raster_cache_dir;
use crate::engine::{
    CanonicOperatorName, RasterBandDescriptors, SpatialGridDescriptor, TimeDescriptor,
};
use geoengine_datatypes::spatial_reference::SpatialReference;

async fn stored_positions(first_tile_position: [isize; 2]) -> Vec<([isize; 2], [u64; 2])> {
    let directory = tempfile::tempdir().unwrap();
    set_raster_cache_dir(directory.path().to_path_buf()).unwrap();
    let geo_transform = GeoTransform::new([10., 20.].into(), 0.5, -0.5);
    let time = TimeInterval::new(0, 1).unwrap();
    let pixel_origin = [first_tile_position[0] * 2, first_tile_position[1] * 3];
    let descriptor = RasterResultDescriptor {
        data_type: RasterDataType::U8,
        spatial_reference: SpatialReference::epsg_4326().into(),
        time: TimeDescriptor::new_irregular(Some(time)),
        spatial_grid: SpatialGridDescriptor::source_from_parts(
            geo_transform,
            GridBoundingBox2D::new(pixel_origin, [pixel_origin[0] + 3, pixel_origin[1] + 5])
                .unwrap(),
        ),
        bands: RasterBandDescriptors::new_single_band(),
    };
    let operator = CanonicOperatorName::new_unchecked(&json!({"type": "TilePositionTest"}));
    let mut positions = Vec::new();

    for offset in [[0, 0], [0, 1], [1, 0], [1, 1]] {
        let tile_position = [
            first_tile_position[0] + offset[0],
            first_tile_position[1] + offset[1],
        ];
        let tile = RasterTile2D::new(
            time,
            tile_position.into(),
            0,
            geo_transform,
            Grid2D::new([2, 3].into(), vec![1_u8; 6]).unwrap().into(),
            CacheHint::max_duration(),
        );
        let key = Arc::new((
            operator.clone(),
            0,
            time,
            tile.tile_information().global_upper_left_pixel_idx(),
        ));
        struct Foo {
            chunk_indices: [u64; 2]
        }
        let stored: Foo = todo!()
            // ZarrsStorageFormat::store(TypedRasterTile2D::U8(tile), key, &descriptor)
            // .await
            // .expect("store tile")
            ;
        positions.push((tile_position, stored.chunk_indices));
    }
    positions
}

#[test]
fn tile_mapping_matches_previous_pixel_mapping() {
    for tile_size in [[2, 3], [512, 512]] {
        let strategy = TilingStrategy::new(
            tile_size.into(),
            GeoTransform::new([0., 0.].into(), 1., -1.),
        );
        for first_tile in [[0, 0], [2, 3], [-2, -3]] {
            let pixel_origin = strategy.tile_idx_to_global_pixel_idx(first_tile.into());
            for offset in [[0, 0], [0, 1], [1, 0], [1, 1]] {
                let tile_position: GridIdx2D =
                    [first_tile[0] + offset[0], first_tile[1] + offset[1]].into();
                let pixel_position = strategy.tile_idx_to_global_pixel_idx(tile_position);
                let previous_chunk_indices = [
                    ((pixel_position.as_slice()[0] - pixel_origin.as_slice()[0])
                        / tile_size[0] as isize) as u64,
                    ((pixel_position.as_slice()[1] - pixel_origin.as_slice()[1])
                        / tile_size[1] as isize) as u64,
                ];
                assert_eq!(
                    tile_position_to_chunk_indices(tile_position, first_tile.into()).unwrap(),
                    previous_chunk_indices,
                );
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn tile_positions_equal_chunk_indices_when_array_starts_at_global_tile_zero() {
    assert_eq!(
        stored_positions([0, 0]).await,
        vec![
            ([0, 0], [0, 0]),
            ([0, 1], [0, 1]),
            ([1, 0], [1, 0]),
            ([1, 1], [1, 1]),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn positive_tile_positions_differ_from_local_chunk_indices() {
    assert_eq!(
        stored_positions([2, 3]).await,
        vec![
            ([2, 3], [0, 0]),
            ([2, 4], [0, 1]),
            ([3, 3], [1, 0]),
            ([3, 4], [1, 1]),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn negative_tile_positions_differ_from_local_chunk_indices() {
    assert_eq!(
        stored_positions([-2, -3]).await,
        vec![
            ([-2, -3], [0, 0]),
            ([-2, -2], [0, 1]),
            ([-1, -3], [1, 0]),
            ([-1, -2], [1, 1]),
        ]
    );
}
