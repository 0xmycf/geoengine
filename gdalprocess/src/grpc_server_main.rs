use geoengine_datatypes::raster::raster_tile_2d_to_arrow_ipc_file;
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use libgdalprocess::grpc_service::TileServiceImpl;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_server::GdalDatasetServiceServer;
use tonic::{transport::Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // println!("Server listening on [::1]:50051");
    let addr = "[::1]:50051".parse()?;
    let tile =
        libgdalprocess::construct_tile(libgdalprocess::random_data(100_000), [100, 1000].into());
    let grid = raster_tile_2d_to_arrow_ipc_file(tile, SpatialReferenceOption::Unreferenced)
        .expect("conversion to arrow ipc format failed");

    let tile_reader = TileServiceImpl { grid };

    Server::builder()
        .add_service(GdalDatasetServiceServer::new(tile_reader))
        .serve(addr)
        .await?;

    Ok(())
}
