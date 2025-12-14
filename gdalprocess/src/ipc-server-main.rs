// server

use geoengine_datatypes::raster::{GridShape, RasterTile2D, raster_tile_2d_to_arrow_ipc_file};
use libgdalprocess::{
    construct_tile,
    ipc_channel_service::{
        self, SendType, SimpleIpcChannelMessage, client::setup_client,
        client::setup_client_for_bytes,
    },
    random_data,
};

fn main() {
    let (send_type, token, serialize_on_iteration) = setup();

    let tile = prepare_tile(100_000, [100, 1000].into());

    match send_type {
        SendType::IpcArrow => {
            let (sender, receiver) = setup_client(token);
            let mut the_data = to_vec8(tile.clone()).expect("Failed to convert tile to IPC data");
            loop {
                let msg = receiver
                    .recv()
                    .expect("Failed to receive message from client");
                if serialize_on_iteration {
                    the_data = std::hint::black_box(
                        to_vec8(std::hint::black_box(tile.clone())).expect("Failed to convert tile to IPC data"),
                    );
                }
                match msg {
                    SimpleIpcChannelMessage::RequestTileData {} => {
                        sender
                            .send(SimpleIpcChannelMessage::Data(the_data.clone()))
                            .expect("Failed to send tile to client");
                    }
                    _ => panic!("Received unexpected message from client"),
                };
            }
        }
        SendType::Serde => {
            let (sender, receiver) = setup_client(token);
            loop {
                let tile = tile.clone();
                let msg = receiver
                    .recv()
                    .expect("Failed to receive message from client");
                match msg {
                    SimpleIpcChannelMessage::RequestTileData {} => {
                        sender.send(tile).expect("Failed to send tile to client");
                    }
                    _ => panic!("Received unexpected message from client"),
                };
            }
        }
        SendType::Bytes => {
            let (sender, receiver) = setup_client_for_bytes(token);
            let mut the_data = to_vec8(tile.clone()).expect("Failed to convert tile to IPC data");
            loop {
                let msg = receiver
                    .recv()
                    .expect("Failed to receive message from client");
                if serialize_on_iteration {
                    the_data = std::hint::black_box(
                        to_vec8(std::hint::black_box(tile.clone())).expect("Failed to convert tile to IPC data"),
                    );
                }
                match msg {
                    SimpleIpcChannelMessage::RequestTileData {} => {
                        sender
                            .send(&the_data)
                            .expect("Failed to send tile to client");
                    }
                    _ => panic!("Received unexpected message from client"),
                }
            }
        }
    }
}

type Token = String;

fn setup() -> (SendType, Token, bool) {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        panic!("Usage: gdalprocess-ipc-channel-server <token> <send_type>");
    }
    let token = &args[1];
    let send_type_str = &args[2];
    let serialize_on_iteration = if args.len() > 3 {
        args[3]
            .parse::<bool>()
            .expect("Failed to parse serialize_on_iteration")
    } else {
        false
    };
    let send_type =
        ipc_channel_service::parse_input(send_type_str).expect("Failed to parse send type");
    (send_type, token.clone(), serialize_on_iteration)
}

fn prepare_tile(upper: u32, shape: GridShape<[usize; 2]>) -> RasterTile2D<u8> {
    let data = random_data(upper);
    construct_tile(data, shape)
}

fn to_vec8(tile: RasterTile2D<u8>) -> Result<Vec<u8>, String> {
    let ipc_data = raster_tile_2d_to_arrow_ipc_file(
        tile,
        geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced,
    );
    ipc_data.map_err(|e| format!("Failed to convert tile to IPC data: {}", e))
}

#[allow(unused)]
fn log(message: &str) {
    println!("[SERVER] {}", message);
}
