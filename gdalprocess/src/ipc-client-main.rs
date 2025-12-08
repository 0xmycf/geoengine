// client

use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use libgdalprocess::ipc_channel_service::{SendType, SimpleIpcChannelMessage};

fn main() {
    let (sender, receiver) = spawn_ipc_server_proccess::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(SendType::IpcArrow);

    log("Requesting tile data from server...");

    (0..100).for_each(|_| {
        sender
            .send(SimpleIpcChannelMessage::RequestTileData {})
            .expect("Failed to send request to server");

        receiver
            .recv()
            .map(|msg| match msg {
                SimpleIpcChannelMessage::Data(data) => {
                    log(&format!("Received tile data of length: {}", data.len()));
                }
                _ => {
                    log("Received unexpected message from server");
                }
            })
            .expect("Failed to receive message from server");
    });
}

fn spawn_ipc_server_proccess<S, C>(t: SendType) -> (IpcSender<S>, IpcReceiver<C>) {
    assert!(
        matches!(t, SendType::IpcArrow | SendType::Serde),
        "Only IpcArrow and Serde types supported in this function"
    );
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<C>)>::new()
        .expect("Failed to create IPC Server");

    println!(
        "cargo run --bin gdalprocess-ipc-channel-server -- {} {}",
        token,
        SendType::IpcArrow.to_string(),
    );

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (
        channels.0,
        match t {
            SendType::IpcArrow | SendType::Serde => channels.1,
            SendType::Bytes => panic!("Bytes type not supported in this function"),
        },
    )
}

fn log(message: &str) {
    println!("[CLIENT] {}", message);
}
