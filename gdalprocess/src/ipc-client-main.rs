// client

use geoengine_datatypes::raster::arrow_ipc_file_to_raster_tile_2d;
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use libgdalprocess::IpcChannelMessage;

fn main() {
    let token = std::env::var("IPC_CHANNEL").expect("IPC_CHANNEL not set");

    println!("[CLIENT] Connecting to IPC channel with token: {}", token);

    let (server_sender, client_reciever) =
        ipc::channel::<IpcChannelMessage>().expect("Failed to create IPC channel");
    let (client_sender, server_reciever) =
        ipc::channel::<IpcChannelMessage>().expect("Failed to create IPC channel");

    let sender =
        ipc::IpcSender::<(IpcSender<IpcChannelMessage>, IpcReceiver<IpcChannelMessage>)>::connect(
            token,
        )
        .expect("Failed to connect to IPC Server");

    sender
        .send((server_sender, server_reciever))
        .expect("Failed to send sender to server");

    loop {
        let input = read_input("Request Tile? (Y/n)");
        if input.to_lowercase() == "n" {
            client_sender
                .send(IpcChannelMessage::EndConnection)
                .expect("Failed to send EndConnection to server");
        }
        client_sender
            .send(IpcChannelMessage::RequestTileData {})
            .expect("Failed to send message to server");
        let msg = client_reciever
            .recv()
            .expect("Failed to receive ack from server");
        match msg {
            IpcChannelMessage::RequestTileData {} => {
                log("Received unexpected RequestTileData message from server (makes no sense)");
            }
            IpcChannelMessage::Data(tile_data) => {
                let tile = arrow_ipc_file_to_raster_tile_2d::<u8>(tile_data, None);
                log(&format!("Received tile from server: {:?}", tile));
            }
            IpcChannelMessage::Error(err) => {
                log(&format!("Received error from server: {}", err));
            }
            IpcChannelMessage::EndConnection => {
                log("Server ended connection.");
                break;
            }
        }

        if input == "exit" {
            break;
        }
    }
}

fn log(message: &str) {
    println!("[CLIENT] {}", message);
}

fn read_input(prompt: &str) -> String {
    use std::io::{self, Write};

    print!("{}", prompt);
    io::stdout().flush().expect("Failed to flush stdout");

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read line");
    input.trim().to_string()
}
