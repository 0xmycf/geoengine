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

pub mod ipc_channel_service {
    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
    pub enum SimpleIpcChannelMessage {
        RequestTileData,
        Data(Vec<u8>),
    }

    pub enum SendType {
        IpcArrow,
        Serde,
        Bytes,
    }

    impl ToString for SendType {
        fn to_string(&self) -> String {
            match self {
                SendType::IpcArrow => "ipc".to_string(),
                SendType::Serde => "serde".to_string(),
                SendType::Bytes => "bytes".to_string(),
            }
        }
    }

    impl TryFrom<&str> for SendType {
        type Error = String;

        fn try_from(value: &str) -> Result<Self, Self::Error> {
            parse_input(value)
        }
    }

    pub fn parse_input(s: &str) -> Result<SendType, String> {
        match s {
            "ipc" => Ok(SendType::IpcArrow),
            "serde" => Ok(SendType::Serde),
            "bytes" => Ok(SendType::Bytes),
            _ => Err(format!("Unknown send type: {}", s)),
        }
    }

    mod server {}

    pub mod client {

        use ipc_channel::ipc::{self, IpcBytesReceiver, IpcBytesSender, IpcReceiver, IpcSender};

        /// Creates channels and connects to the IPC server with the given token,
        /// sending the channels to the server, so that communication can be established.
        ///
        /// Assumes that the server is already running and listening for connections.
        pub fn setup_client<S, C>(token: String) -> (IpcSender<C>, IpcReceiver<S>)
        where
            S: for<'de> serde::Deserialize<'de> + serde::Serialize,
            C: for<'de> serde::Deserialize<'de> + serde::Serialize,
        {
            let (server_sender, client_reciever) =
                ipc::channel::<S>().expect("Failed to create IPC channel");
            let (client_sender, server_reciever) =
                ipc::channel::<C>().expect("Failed to create IPC channel");

            let sender = ipc::IpcSender::<(IpcSender<S>, IpcReceiver<C>)>::connect(token)
                .expect("Failed to connect to IPC Server");

            sender
                .send((server_sender, server_reciever))
                .expect("Failed to send sender to server");

            (client_sender, client_reciever)
        }

        /// Creates channels and connects to the IPC server with the given token,
        /// sending the channels to the server, so that communication can be established.
        ///
        /// Assumes that the server is already running and listening for connections.
        pub fn setup_client_for_bytes<S>(token: String) -> (IpcBytesSender, IpcReceiver<S>)
        where
            S: for<'de> serde::Deserialize<'de> + serde::Serialize,
        {
            let (server_sender, client_reciever) =
                ipc::channel::<S>().expect("Failed to create IPC channel");
            let (client_sender, server_reciever) =
                ipc::bytes_channel().expect("Failed to create IPC byte channel");

            let sender = ipc::IpcSender::<(IpcSender<S>, IpcBytesReceiver)>::connect(token)
                .expect("Failed to connect to IPC Server");

            sender
                .send((server_sender, server_reciever))
                .expect("Failed to send sender to server");

            (client_sender, client_reciever)
        }
    }
}

pub mod grpc_service {

    use geoengine_datatypes::raster::{RasterTile2D, raster_tile_2d_to_arrow_ipc_file};
    use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
    use serde::Serialize;
    use tonic::{Request, Response, Status};

    use crate::grpc_service::proto_service::gdal_dataset_service_server::GdalDatasetService;
    use crate::grpc_service::proto_service::{RequestTileData, TileDataReply};

    pub mod proto_service {
        tonic::include_proto!("gdal_dataset_service_simple");
    }

    #[derive(Debug)]
    pub enum SerilizationType {
        IpcArrow,
        Bincode,
    }

    #[derive(Debug)]
    pub struct TileServiceImplWithSerialization {
        pub grid: RasterTile2D<u8>,
        pub t: SerilizationType,
    }

    #[tonic::async_trait]
    impl GdalDatasetService for TileServiceImplWithSerialization {
        async fn load_tile_data(
            &self,
            _request: Request<RequestTileData>,
        ) -> Result<Response<proto_service::TileDataReply>, Status> {
            let data: Vec<u8> = if matches!(self.t, SerilizationType::Bincode) {
                std::hint::black_box(
                    bincode::serde::encode_to_vec(
                        std::hint::black_box(&self.grid),
                        bincode::config::standard(),
                    )
                    .map_err(|err| {
                        Status::internal(format!(
                            "Failed to convert tile to bincode serialized data: {}",
                            err
                        ))
                    }),
                )?
                // bincode::serde::serialize(&self.grid)
            } else {
                std::hint::black_box(raster_tile_2d_to_arrow_ipc_file(
                    std::hint::black_box(self.grid.clone()),
                    SpatialReferenceOption::Unreferenced,
                ))
                .map_err(|err| {
                    Status::internal(format!("Failed to convert tile to arrow ipc: {}", err))
                })?
            };
            Ok(Response::new(TileDataReply { data }))
        }
    }

    #[derive(Debug)]
    pub struct TileServiceImpl {
        pub grid: Vec<u8>,
    }

    #[tonic::async_trait]
    impl GdalDatasetService for TileServiceImpl {
        async fn load_tile_data(
            &self,
            _request: Request<RequestTileData>,
        ) -> Result<Response<proto_service::TileDataReply>, Status> {
            Ok(Response::new(TileDataReply {
                data: self.grid.clone(),
            }))
        }
    }
}
