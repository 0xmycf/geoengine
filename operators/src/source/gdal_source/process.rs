use std::process::{Child, Command};

use geoengine_datatypes::{
    primitives::{CacheHint, TimeInterval},
    raster::TileInformation,
};
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcBytesSender, IpcReceiver, IpcSender};

use crate::source::GdalDatasetParameters;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonPayload(String);

impl JsonPayload {
    pub fn new(message: &IpcChannelMessage) -> Self {
        Self(serde_json::to_string(message).expect("Failed to serialize IpcChannelMessage to JSON"))
    }

    pub fn get(self) -> IpcChannelMessage {
        let message: IpcChannelMessage = serde_json::from_str(&self.0)
            .expect("Failed to deserialize IpcChannelMessage from JSON");
        message
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum IpcChannelMessage {
    RequestTileData {
        cache_hint: CacheHint, // TODO does it even need this?
        dataset_params: GdalDatasetParameters,
        tile_information: TileInformation,
        tile_time: TimeInterval,
    },
    EndConnection, // Till death does them part
                   // Data(Vec<u8>), // this can go, as the answer is send via byteschannel
}

pub fn spawn_ipc_server_process_bytes<S>() -> (Child, IpcSender<S>, IpcBytesReceiver) {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcBytesReceiver)>::new()
        .expect("Failed to create IPC Server");
    // let path = env!("CARGO_BIN_EXE_gdalprocess-ipc-channel-server");
    // let path = std::env::var("CARGO_BIN_EXE_gdalsource-process").expect("the CARGO_BIN_EXE_gdalsource-process env var is not set");

    // let exe = std::env::current_exe().expect("failed to get current exe path");
    // let path = exe
    //     .parent()
    //     .expect("failed to get exe parent dir")
    //     .join("gdalsource-process");

    // get the users home
    let home = std::env::var("HOME").expect("failed to get HOME env var");
    let location = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let path = format!(
        "{home}/Documents/work/arbeit-geoengine/geoengine-workflow-backend/target/{location}/gdalsource-process",
    );

    // dbg!(path);

    let child = Command::new(path)
        .arg(token)
        .spawn()
        .expect("failed to spawn ipc server process");

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (child, channels.0, channels.1)
}

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
