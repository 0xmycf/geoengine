use geoengine_datatypes::raster::{RasterTile2D, raster_tile_2d_to_arrow_ipc_file_for_ipc_channel};
use geoengine_operators::source::{
    self, GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, setup_client_for_bytes,
};
use ipc_channel::ipc::{IpcBytesSender, IpcError};

#[tokio::main]
async fn main() -> Result<()> {
    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    Ok(())
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

async fn run() -> Result<()> {
    let token = setup();
    println!("Starting GDAL process with token: {token}");

    let (sender, receiver) = setup_client_for_bytes::<IpcChannelMessage>(token);

    let mut dataset_cache = GdalDatasetCache::new();

    loop {
        let message = receiver.recv().map_err(|e| {
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
            IpcChannelMessage::RequestTileData(b) => {
                let IpcChannelMessagePayload {
                    dataset_params,
                    tile_information,
                    tile_time,
                    cache_hint,
                    read_advise,
                } = *b;

                // dbg!("Received request for tile data", &dataset_params.file_path);
                let params_for_cache = &dataset_params;

                if let Some(tile) = dataset_cache.get(&params_for_cache, &tile_information) {
                    if let Err(some_err) = send_tile(tile, &sender) {
                        panic!("Cannot send data back to engine: {some_err:#?}")
                    }
                    continue;
                }

                #[allow(deprecated)] // this is the place where it should be used!
                let tile = source::__private::load_tile_async(
                    dataset_params.clone(),
                    read_advise,
                    tile_information.clone(),
                    tile_time,
                    cache_hint,
                )
                .await?;
                dataset_cache.cache(params_for_cache.clone(), tile_information, tile.clone());
                if let Err(some_err) = send_tile(tile, &sender) {
                    panic!("Error sending data to client {some_err:#?}");
                }
            }
            IpcChannelMessage::EndConnection => {
                return Ok(());
            }
        }
    }
}

fn send_tile(tile: RasterTile2D<u8>, sender: &IpcBytesSender) -> Result<()> {
    let ipc_data = raster_tile_2d_to_arrow_ipc_file_for_ipc_channel::<u8>(tile)?;
    sender.send(&ipc_data)?;
    Ok(())
}

type Token = String;

fn setup() -> Token {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        panic!("Usage: gdalprocess-ipc-channel-server <token>");
    }
    args[1].clone()
}
