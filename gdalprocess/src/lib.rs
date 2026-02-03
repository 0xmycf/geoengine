use geoengine_datatypes::{
    primitives::{CacheHint, TimeInterval},
    raster::{Grid, GridShape, MaskedGrid, Pixel, RasterTile2D, TileInformation},
    util::test::TestDefault,
};
use rand::Rng;

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub enum IpcChannelMessage {
//     RequestTileData {
//         // dataset_params: geoengine_operators::source::GdalDatasetParameters,
//     },
//     Data(Vec<u8>),
//     Error(String),
//     EndConnection,
// }

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

    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
    pub enum SimpleIpcChannelMessage {
        RequestTileData,
        Data(Vec<u8>),
    }

    #[derive(Debug, Clone, Copy)]
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
            _ => Err(format!("Unknown send type: {s}")),
        }
    }

    pub mod server {}

    // the server is actually the client and vice-versa, but the naming
    // is actually only relevant for the pairing of the processes
    pub mod client {
    }
}

