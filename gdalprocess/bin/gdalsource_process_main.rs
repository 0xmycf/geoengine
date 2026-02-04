use geoengine_datatypes::{
    raster::{RasterTile2D, raster_tile_2d_to_arrow_ipc_file_for_ipc_channel},
    spatial_reference::SpatialReferenceOption,
};
use geoengine_operators::source::gdal_source::{
    self,
    process::{IpcChannelMessage, JsonPayload, setup_client_for_bytes},
    GdalDatasetCache,
};
use ipc_channel::ipc::IpcError;
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let token = setup();
    dbg!(token.clone());

    let (sender, receiver) = setup_client_for_bytes::<JsonPayload>(token);

    let dataset_cache = Arc::new(Mutex::new(GdalDatasetCache::new()));

    loop {
        let message = receiver.recv().map(JsonPayload::get).map_err(|e| {
            match e {
                IpcError::Bincode(ref error_kind) => {
                    // bincode error
                    dbg!(error_kind);
                }
                IpcError::Io(_) | IpcError::Disconnected => (),
            }
            format!("Failed to receive message from client: {e}")
        })?;

        match message {
            IpcChannelMessage::RequestTileData {
                dataset_params,
                tile_information,
                tile_time,
                cache_hint,
            } => {
                // dbg!("Received request for tile data");
                // TODO: make more general for the other pixel types... how (Phantom Data?)?
                // dbg!(tile_time);
                let tile: RasterTile2D<u8> = gdal_source::load_tile_data_cached_async(
                    Arc::clone(&dataset_cache),
                    dataset_params,
                    tile_information,
                    tile_time,
                    cache_hint,
                )
                .await?;
                let ipc_data = raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(
                    tile,
                )?;

                sender.send(&ipc_data)?;
                // dbg!("Sent tile data to client");
            }
            IpcChannelMessage::EndConnection => {
                // dbg!("Received end connection message");
                return Ok(());
            }
        }
    }
}

type Token = String;

fn setup() -> Token {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Usage: gdalprocess-ipc-channel-server <token>");
    }
    args[1].clone()
}
