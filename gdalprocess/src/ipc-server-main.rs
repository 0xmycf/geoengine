// server

use std::any::Any;

use geoengine_datatypes::raster::{GridShape, raster_tile_2d_to_arrow_ipc_file};
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use libgdalprocess::{IpcChannelMessage, construct_tile, random_data};

fn main() {
    let (server, token) = ipc::IpcOneShotServer::<(
        IpcSender<IpcChannelMessage>,
        IpcReceiver<IpcChannelMessage>,
    )>::new()
    .expect("Failed to create IPC Server");

    println!(
        "[SERVER] IPC channel created with token: \x1b[31m{}\x1b[0m",
        token
    );

    let (_rx, (sender, receiver)) = server.accept().expect("accept failed to receive message");
    log(&format!("Received channel: {:?}", sender.type_id()));

    loop {
        match receiver.recv() {
            Ok(IpcChannelMessage::RequestTileData {}) => {
                match send_random_data(10_000, [1000, 1000].into(), &sender) {
                    Ok(_) => log("Sent tile data successfully."),
                    Err(err) => log(&format!("Error sending tile data: {}", err)),
                }
            }
            Ok(IpcChannelMessage::EndConnection) => {
                log("Client requested to end connection.");
                sender
                    .send(IpcChannelMessage::EndConnection)
                    .expect("Failed to send EndConnection to client");
                break;
            }
            Ok(msg) => {
                log(&format!("Received unexpected message: {:?}", msg));
            }
            Err(err) => {
                log(&format!("Error receiving message: {}", err));
            }
        }
    }

    log("Server exiting.");
}

fn send_random_data(
    upper: u32,
    shape: GridShape<[usize; 2]>,
    sender: &IpcSender<IpcChannelMessage>,
) -> Result<(), String> {
    let data = random_data(upper);
    let tile = construct_tile(data, shape);
    let ipc_data = raster_tile_2d_to_arrow_ipc_file(
        tile,
        geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced,
    );
    let the_data = ipc_data.map_err(|e| format!("Failed to convert tile to IPC data: {}", e))?;
    sender
        .send(IpcChannelMessage::Data(the_data))
        .map_err(|e| format!("Failed to send tile: {}", e))?;

    Ok(())
}

fn log(message: &str) {
    println!("[SERVER] {}", message);
}
