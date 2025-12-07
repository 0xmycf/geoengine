use geoengine_datatypes::{
    primitives::{CacheHint, TimeInterval},
    raster::{Grid, GridShape, MaskedGrid, Pixel, RasterTile2D, TileInformation},
    util::test::TestDefault,
};
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum IpcChannelMessage {
    RequestTileData {
        // dataset_params: geoengine_operators::source::GdalDatasetParameters,
    },
    Data(Vec<u8>),
    Error(String),
    EndConnection,
}

pub fn random_data(upper: u32) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..upper).map(|_| rng.random_range(0..=255)).collect()
}

pub fn construct_tile<T: Pixel>(data: Vec<T>, shape: GridShape<[usize; 2]>) -> RasterTile2D<T> {
    let time = TimeInterval::default();
    let tile_info = TileInformation {
        // TODO (low): copy-paste from xxxx/gdal_source/mod.rs line 2338
        tile_size_in_pixels: shape,
        global_tile_position: [0, 0].into(),
        global_geo_transform: TestDefault::test_default(),
    };
    let band = 0; // TODO 0 or 1?
    let grid = Grid::new(shape, data).expect("creating grid failed");
    let data = geoengine_datatypes::raster::GridOrEmpty::new_grid(MaskedGrid::new_with_data(grid));

    RasterTile2D::new_with_tile_info(time, tile_info, band, data, CacheHint::default())
}

pub mod ipc_service {
    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
    pub enum SimpleIpcChannelMessage {
        RequestTileData,
        Data(Vec<u8>),
    }
}

pub mod grpc_service {

    use std::sync::Arc;

    use tonic::{Request, Response, Status};

    use crate::grpc_service::proto_service::gdal_dataset_service_server::GdalDatasetService;
    use crate::grpc_service::proto_service::{RequestTileData, TileDataReply};

    pub mod proto_service {
        tonic::include_proto!("gdal_dataset_service_simple");
    }

    #[derive(Debug)]
    pub struct TileServiceImpl {
        pub grid: Arc<Vec<u8>>,
    }

    #[tonic::async_trait]
    impl GdalDatasetService for TileServiceImpl {
        async fn load_tile_data(
            &self,
            _request: Request<RequestTileData>,
        ) -> Result<Response<proto_service::TileDataReply>, Status> {
            Ok(Response::new(TileDataReply {
                data: self.grid.as_ref().clone(),
            }))
        }
    }
}
