use criterion::{Criterion, criterion_group, criterion_main};
use libgdalprocess::grpc_service::proto_service::RequestTileData;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_client::GdalDatasetServiceClient;
use std::net::SocketAddr;
use std::process;
use std::sync::{Arc, Mutex};

enum ServerType {
    SerializeOnStartup,
    SerializeOnEachRequest,
}

fn start_process_server(t: ServerType) -> process::Child {
    let cmd = match t {
        ServerType::SerializeOnStartup => env!("CARGO_BIN_EXE_gdalprocess-grpc-bench-server"),
        ServerType::SerializeOnEachRequest => env!("CARGO_BIN_EXE_gdalprocess-grpc-bench-server-with-serialization")
    };
    process::Command::new(cmd)
        .spawn()
        .expect("Failed to start gdalprocess-grpc-bench-server process")
}

async fn setup_client() -> GdalDatasetServiceClient<tonic::transport::Channel> {
    let addr: SocketAddr = "[::1]:50051".parse().unwrap();
    GdalDatasetServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to server")
}

fn grpc_bench_process(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let mut child = start_process_server(ServerType::SerializeOnStartup);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Arc::new(Mutex::new(rt.block_on(async { setup_client().await })));

    c.bench_function("client-server roundtrip of grpc (process)", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(|| async {
            let client = client.clone();
            let mut rc = client.lock().unwrap();
            let resp = rc.load_tile_data(RequestTileData {}).await.unwrap();
            std::hint::black_box(resp);
        });
    });

    child.kill().expect("Failed to kill server process");
}

fn grpc_bench_process_with_serialisation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let mut child = start_process_server(ServerType::SerializeOnEachRequest);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Arc::new(Mutex::new(rt.block_on(async { setup_client().await })));

    c.bench_function("client-server roundtrip of grpc (process) (serialisation_during_iteration=true)", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(|| async {
            let client = client.clone();
            let mut rc = client.lock().unwrap();
            let resp = rc.load_tile_data(RequestTileData {}).await.unwrap();
            std::hint::black_box(resp);
        });
    });

    child.kill().expect("Failed to kill server process");
}

criterion_group!(
    benches,
    grpc_bench_process,
    grpc_bench_process_with_serialisation,
);
criterion_main!(benches);
