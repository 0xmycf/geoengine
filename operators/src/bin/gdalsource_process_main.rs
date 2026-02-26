
use gdal::raster::GdalType;
use geoengine_datatypes::raster::{
    Pixel, RasterTile2D, TypedRasterTile2D, raster_tile_2d_to_arrow_ipc_file_for_ipc_channel,
};
use geoengine_operators::source::{
    self, GdalDatasetCache, IpcChannelMessage, IpcChannelMessagePayload, setup_client_for_bytes,
};
use ipc_channel::ipc::{IpcBytesSender, IpcError};
use num::FromPrimitive;

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
                    dbg!(error_kind);
                }
                IpcError::Io(_) | IpcError::Disconnected => (),
            }
            format!("Failed to receive message from client: {e}")
        })?;

        match message {
            IpcChannelMessage::RequestTileData(b) => match b.data_type {
                geoengine_datatypes::raster::RasterDataType::U8 => {
                    handle::<u8>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::U16 => {
                    handle::<u16>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::U32 => {
                    handle::<u32>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::U64 => {
                    handle::<u64>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::I8 => {
                    handle::<i8>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::I16 => {
                    handle::<i16>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::I32 => {
                    handle::<i32>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::I64 => {
                    handle::<i64>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::F32 => {
                    handle::<f32>(*b, &mut dataset_cache, &sender).await?
                }
                geoengine_datatypes::raster::RasterDataType::F64 => {
                    handle::<f64>(*b, &mut dataset_cache, &sender).await?
                }
            },
            IpcChannelMessage::EndConnection => {
                return Ok(());
            }
        }
    }
}

async fn handle<P: FromPrimitive + Pixel + GdalType>(
    IpcChannelMessagePayload {
        cache_hint,
        dataset_params,
        tile_information,
        tile_time,
        read_advise,
        data_type: _,
    }: IpcChannelMessagePayload,
    dataset_cache: &mut GdalDatasetCache,
    sender: &IpcBytesSender,
) -> Result<()>
where
    RasterTile2D<P>: Into<TypedRasterTile2D>,
{
    // cache now happens in the callchain
    #[allow(deprecated)] // this is the place where it should be used!
    let tile: RasterTile2D<P> = source::__private::load_tile_async_cached(
        dataset_cache,
        dataset_params.clone(),
        read_advise,
        tile_information.clone(),
        tile_time,
        cache_hint,
    )
    .await?;

    if let Err(some_err) = send_tile(tile, sender) {
        panic!("Error sending data to client {some_err:#?}");
    }
    Ok(())
}

fn send_tile<T: Pixel + FromPrimitive + GdalType>(
    tile: RasterTile2D<T>,
    sender: &IpcBytesSender,
) -> Result<()> {
    let ipc_data = raster_tile_2d_to_arrow_ipc_file_for_ipc_channel(tile)?;
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
