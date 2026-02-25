use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::{Arc, LazyLock, Mutex};

use gdal::raster::GdalType;
use geoengine_datatypes::primitives::{CacheHint, TimeInterval};
use geoengine_datatypes::raster::{Pixel, RasterTile2D, TileInformation};
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcBytesSender, IpcReceiver, IpcSender};
use num::FromPrimitive;

use super::{GdalDatasetParameters, GdalReadAdvise};

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub struct IpcChannelMessagePayload {
    pub cache_hint: CacheHint,
    pub dataset_params: GdalDatasetParameters,
    pub tile_information: TileInformation,
    pub tile_time: TimeInterval,
    pub read_advise: GdalReadAdvise,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
pub enum IpcChannelMessage {
    RequestTileData(Box<IpcChannelMessagePayload>),
    EndConnection,
}

impl IpcChannelMessage {
    pub fn new_request_tile_message(data: IpcChannelMessagePayload) -> Self {
        Self::RequestTileData(Box::new(data))
    }
}

pub fn spawn_ipc_server_process_bytes<S>() -> (Child, IpcSender<S>, IpcBytesReceiver) {
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcBytesReceiver)>::new()
        .expect("Failed to create IPC Server");

    let path: PathBuf = std::env::var("GDAL_SOURCE_PROCESS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::current_exe()
                .expect("failed to get current executable path")
                .parent()
                .expect("executable has no parent directory")
                .join("gdalsource-process")
        });

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

/// Worker pool of multiple processes
/// RoundRobin by default, but if a [`RaterTile2D`] is requested that is already
/// in cache use the corresponding process instead.
///
/// The workers are only started once requested.
pub struct ProcessManager {
    rr_index: usize,
    pool: Vec<LazyLock<Arc<ProcessData>>>,
    cache: HashMap<PathBuf, Arc<ProcessData>>,
}

impl ProcessManager {
    /// Creates a new [`ProcessManager`] with `size` many workers
    pub fn new(size: usize) -> Self {
        let pool = (0..size)
            .map(|_| LazyLock::new(ProcessData::spawn as fn() -> Arc<ProcessData>))
            .collect();

        Self {
            rr_index: 0,
            pool,
            cache: HashMap::with_capacity(size),
        }
    }

    #[inline]
    pub fn new_with_arc_mutex(size: usize) -> Arc<tokio::sync::Mutex<Self>> {
        Arc::new(tokio::sync::Mutex::new(Self::new(size)))
    }

    /// Returns the next process from the pool using round-robin scheduling,
    /// advancing the index for the next call.
    fn next_rr(&mut self) -> Arc<ProcessData> {
        let process = Arc::clone(&*self.pool[self.rr_index]);
        self.rr_index = (self.rr_index + 1) % self.pool.len();
        process
    }

    /// Acquires a process for the given file `path`.
    ///
    /// If the path is already in the cache, the previously assigned process is
    /// returned so that the worker that already has the file open handles the
    /// request. Otherwise the next round-robin process is selected, recorded in
    /// the cache, and returned.
    pub fn acquire(&mut self, path: &PathBuf) -> Arc<ProcessData> {
        if let Some(process) = self.cache.get(path) {
            return Arc::clone(process);
        }

        let process = self.next_rr();
        self.cache.insert(path.clone(), Arc::clone(&process));
        process
    }
}

pub struct ProcessData {
    child: Child,
    /// stored in one mutex to gether to ensure that one call to `send`,
    /// ends up with the correct result from `recv`
    sender_receiver_pair: Mutex<(IpcSender<IpcChannelMessage>, IpcBytesReceiver)>,
}

impl ProcessData {
    fn new(child: Child, sender: IpcSender<IpcChannelMessage>, receiver: IpcBytesReceiver) -> Self {
        Self {
            child,
            sender_receiver_pair: Mutex::new((sender, receiver)),
        }
    }

    /// Spawns a new child process
    /// Is kept alive until `Self` is dropped.
    pub fn spawn() -> Arc<Self> {
        let (child, sender, receiver) = spawn_ipc_server_process_bytes::<IpcChannelMessage>();
        Arc::new(Self::new(child, sender, receiver))
    }

    /// Sends `message` to the worker and blocks until the response arrives.
    pub fn send_recv_blocking(&self, message: IpcChannelMessage) -> crate::util::Result<Vec<u8>> {
        let pair = self
            .sender_receiver_pair
            .lock()
            .expect("lock should not be poisoned");

        let sender = &pair.0;
        let receiver = &pair.1;

        sender.send(message).map_err(|e| crate::error::Error::Io {
            source: std::io::Error::other(e.to_string()),
        })?;

        let bytes = receiver.recv().map_err(|e| crate::error::Error::Io {
            source: std::io::Error::other(e.to_string()),
        })?;

        Ok(bytes)
    }
}

impl Drop for ProcessData {
    fn drop(&mut self) {
        if let Ok((sender, _receiver)) = self.sender_receiver_pair.get_mut() {
            let _ = sender.send(IpcChannelMessage::EndConnection);
        }
        let _ = self.child.kill();
    }
}

/// A simple, single-entry cache for an open GDAL dataset tile.
pub struct GdalDatasetCache<T: Pixel + GdalType + FromPrimitive> {
    parameters: Option<GdalDatasetParameters>,
    tile_information: Option<TileInformation>,
    dataset: Option<RasterTile2D<T>>,
}

impl<T: Pixel + GdalType + FromPrimitive> GdalDatasetCache<T> {
    pub fn new() -> Self {
        Self {
            parameters: None,
            tile_information: None,
            dataset: None,
        }
    }

    pub fn cache(
        &mut self,
        parameters: GdalDatasetParameters,
        tile_information: TileInformation,
        dataset: RasterTile2D<T>,
    ) {
        self.parameters = Some(parameters);
        self.tile_information = Some(tile_information);
        self.dataset = Some(dataset);
    }

    pub fn clear(&mut self) {
        self.parameters = None;
        self.tile_information = None;
        self.dataset = None;
    }

    /// Copies the dataset before returning
    pub fn get(
        &self,
        parameters: &GdalDatasetParameters,
        tile_information: &TileInformation,
    ) -> Option<RasterTile2D<T>> {
        if let (Some(ps), Some(ti)) = (self.parameters.as_ref(), self.tile_information.as_ref())
            && *ps == *parameters
            && *ti == *tile_information
        {
            return self.dataset.clone();
        }
        None
    }
}
