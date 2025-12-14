use libgdalprocess::grpc_service::TileServiceImplWithSerialization;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_server::GdalDatasetServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let grid =
        libgdalprocess::construct_tile(libgdalprocess::random_data(100_000), [100, 1000].into());

    let tile_reader = TileServiceImplWithSerialization { grid };

    Server::builder()
        .add_service(GdalDatasetServiceServer::new(tile_reader))
        .serve(addr)
        .await?;

    Ok(())
}
