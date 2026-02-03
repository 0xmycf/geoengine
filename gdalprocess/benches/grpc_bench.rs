use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::raster::{RasterTile2D, raster_tile_2d_to_arrow_ipc_file};
use geoengine_datatypes::spatial_reference::SpatialReferenceOption;
use libgdalprocess::grpc_service::TileServiceImpl;
use libgdalprocess::grpc_service::proto_service::RequestTileData;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_client::GdalDatasetServiceClient;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_server::GdalDatasetServiceServer;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;

async fn start_server(svc: TileServiceImpl, addr: SocketAddr) {
    Server::builder()
        .add_service(GdalDatasetServiceServer::new(svc))
        .serve(addr)
        .await
        .expect("gRPC server failed to start");
}

async fn setup_server(tile_data: RasterTile2D<u8>) -> SocketAddr {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let data = raster_tile_2d_to_arrow_ipc_file(tile_data, SpatialReferenceOption::Unreferenced)
        .expect("conversion to arrow ipc format failed");
    tokio::spawn(async move {
        let svc = libgdalprocess::grpc_service::TileServiceImpl { grid: data };
        start_server(svc, addr).await;
    });
    addr
}

async fn setup_client(addr: SocketAddr) -> GdalDatasetServiceClient<tonic::transport::Channel> {
    GdalDatasetServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to server")
}

fn grpc_bench(c: &mut Criterion) {
    let data =
        libgdalprocess::construct_tile(libgdalprocess::random_data(100_000), [100, 1000].into());

    let rt = tokio::runtime::Runtime::new().unwrap();
    let server_addr = rt.block_on(setup_server(data));
    let client = rt.block_on(setup_client(server_addr));
    let client = Arc::new(Mutex::new(client));

    c.bench_function("client-server roundtrip of grpc", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(move || {
            let client = client.clone();
            async move {
                let mut rc = client.lock().unwrap();
                let resp = rc.load_tile_data(RequestTileData {}).await.unwrap();
                std::hint::black_box(resp);
            }
        });
    });
}

fn raster_to_ipc_data_bench(c: &mut Criterion) {
    let data =
        libgdalprocess::construct_tile(libgdalprocess::random_data(100_000), [100, 1000].into());

    c.bench_function("raster tile to arrow ipc data", |b| {
        b.iter(|| {
            let ipc_data = raster_tile_2d_to_arrow_ipc_file(
                data.clone(),
                SpatialReferenceOption::Unreferenced,
            )
            .expect("conversion to arrow ipc format failed");
            std::hint::black_box(ipc_data);
        });
    });
}

criterion_group!(benches, grpc_bench, raster_to_ipc_data_bench);
criterion_main!(benches);
