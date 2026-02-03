use criterion::{Criterion, PlotConfiguration, criterion_group, criterion_main};
use geoengine_datatypes::raster::RasterTile2D;
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcReceiver, IpcSender};
use libgdalprocess::grpc_service::proto_service::RequestTileData;
use libgdalprocess::grpc_service::proto_service::gdal_dataset_service_client::GdalDatasetServiceClient;
use libgdalprocess::ipc_channel_service::{SendType, SimpleIpcChannelMessage};
use std::net::SocketAddr;
use std::process;
use std::process::{Child, Command};
use std::sync::{Arc, Mutex};
use tonic::transport::Channel;

#[inline(always)]
fn ensure_params(matches: bool, msg: &str) {
    assert!(matches, "{}", msg);
}

fn spawn_ipc_server_process_bytes<S>(
    t: SendType,
    ser_per_iter: bool,
) -> (Child, IpcSender<S>, IpcBytesReceiver) {
    // {{{
    ensure_params(
        matches!(t, SendType::Bytes),
        "only Bytes type supported in this function",
    );
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcBytesReceiver)>::new()
        .expect("Failed to create IPC Server");
    let path = env!("CARGO_BIN_EXE_gdalprocess-ipc-channel-server");
    let child = Command::new(path)
        .arg(token)
        .arg(t.to_string())
        .arg(ser_per_iter.to_string())
        .spawn()
        .expect("failed to spawn ipc server process");

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (
        child,
        channels.0,
        match t {
            SendType::IpcArrow | SendType::Serde => {
                panic!("Only Bytes type supported in this function")
            }
            SendType::Bytes => channels.1,
        },
    )
} //}}}

fn spawn_ipc_server_process<S, C>(
    t: SendType,
    ser_per_iter: bool,
) -> (Child, IpcSender<S>, IpcReceiver<C>) {
    //{{{
    ensure_params(
        matches!(t, SendType::IpcArrow | SendType::Serde),
        "Only IpcArrow and Serde types supported in this function",
    );
    let (server, token) = ipc::IpcOneShotServer::<(IpcSender<S>, IpcReceiver<C>)>::new()
        .expect("Failed to create IPC Server");
    let path = env!("CARGO_BIN_EXE_gdalprocess-ipc-channel-server");

    let child = Command::new(path)
        .arg(token)
        .arg(t.to_string())
        .arg(ser_per_iter.to_string())
        .spawn()
        .expect("failed to spawn ipc server process");

    let (_rx, channels) = server.accept().expect("accept failed to receive message");
    (
        child,
        channels.0,
        match t {
            SendType::IpcArrow | SendType::Serde => channels.1,
            SendType::Bytes => panic!("Bytes type not supported in this function"),
        },
    )
} //}}}

// this one spawns a new process each benching process
fn ipc_channel_process_start_per_iter(c: &mut Criterion) {
    // {{{
    c.bench_function("ipc-channel_process_iter", |b| {
        b.iter(|| process_start_per_iter());
    });
}

fn process_start_per_iter() {
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(SendType::IpcArrow, false);

    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver.recv().unwrap();
    child.kill().expect("Failed to kill child process");
    std::hint::black_box(resp);
}
//}}}

fn bench_ipc_channel_process(c: &mut Criterion, ser_per_iter: bool, t: SendType) {
    // {{{
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(t, ser_per_iter);

    c.bench_function(
        &format!("ipc-channel_process__ser_per_iter={}", ser_per_iter),
        |b| {
            b.iter(|| process(&mut child, &sender, &receiver));
        },
    );
    child.kill().expect("Failed to kill child process");
}

fn process(
    mut child: &mut Child,
    sender: &IpcSender<SimpleIpcChannelMessage>,
    receiver: &IpcReceiver<SimpleIpcChannelMessage>,
) {
    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver.recv().unwrap();
    std::hint::black_box(resp);
}
//}}}

// this one only requests / sends data
fn ipc_channel_process(c: &mut Criterion) {
    bench_ipc_channel_process(c, false, SendType::IpcArrow);
}

fn ipc_channel_serde_process(c: &mut Criterion) {
    // {{{{
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        RasterTile2D<u8>,
    >(SendType::Serde, false);

    c.bench_function("ipc-channel_serde_process__ser_per_iter=false", |b| {
        b.iter(|| serde_process(&mut child, &sender, &receiver));
    });
    child.kill().expect("Failed to kill child process");
}

fn serde_process(
    child: &mut Child,
    sender: &IpcSender<SimpleIpcChannelMessage>,
    receiver: &IpcReceiver<RasterTile2D<u8>>,
) {
    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver
        .recv()
        .expect("Failed to receive or deserialise data");
    std::hint::black_box(resp);
}
// }}}}

fn ipc_channel_serde_process_iter(c: &mut Criterion) {
    // {{{{
    c.bench_function("ipc-channel_serde_process_iter", |b| {
        b.iter(|| serde_process_iter());
    });
}

fn serde_process_iter() {
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        RasterTile2D<u8>,
    >(SendType::Serde, false);
    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver
        .recv()
        .expect("Failed to receive or deserialise data");
    child.kill().expect("Failed to kill child process");
    std::hint::black_box(resp);
}
//}}}}

fn ipc_channel_bytes_process_per_iter(c: &mut Criterion) {
    // {{{
    c.bench_function("ipc-channel_bytes_process_iter", |b| {
        b.iter(|| bytes_process_per_iter());
    });
}

fn bytes_process_per_iter() {
    let (mut child, sender, receiver) = spawn_ipc_server_process_bytes(SendType::Bytes, false);
    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver.recv().unwrap();
    child.kill().expect("Failed to kill child process");
    std::hint::black_box(resp);
}
// }}}

fn bench_ipc_channel_process_bytes(c: &mut Criterion, ser_per_iter: bool) {
    // {{{
    let (mut child, sender, receiver) =
        spawn_ipc_server_process_bytes(SendType::Bytes, ser_per_iter);

    c.bench_function(
        &format!("ipc-channel_bytes_process__ser_per_iter={}", ser_per_iter),
        |b| {
            b.iter(|| process_bytes(&mut child, &receiver, &sender));
        },
    );
    child.kill().expect("Failed to kill child process");
}

fn process_bytes(
    child: &mut Child,
    receiver: &IpcBytesReceiver,
    sender: &IpcSender<SimpleIpcChannelMessage>,
) {
    match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
        Ok(_) => (),
        Err(err) => {
            child
                .kill()
                .expect("Failed to kill child process after send error");
            panic!("Failed to send request to server: {}", err)
        }
    }
    let resp = receiver.recv().unwrap();
    std::hint::black_box(resp);
}
//}}}

fn ipc_channel_bytes_process(c: &mut Criterion) {
    bench_ipc_channel_process_bytes(c, false);
}

fn ipc_channel_bench_process_with_serialisation(c: &mut Criterion) {
    bench_ipc_channel_process(c, true, SendType::IpcArrow);
}

fn ipc_channel_bench_process_bytes_with_serialisation(c: &mut Criterion) {
    bench_ipc_channel_process_bytes(c, true);
}

fn bench_all(c: &mut Criterion) {
    let mut g = c.benchmark_group("all_ipc_channel_benchmarks");
    g.plot_config(PlotConfiguration::default().summary_scale(criterion::AxisScale::Linear));

    let t = SendType::IpcArrow;
    let ser_per_iter = false;
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(t, ser_per_iter);
    g.bench_function(
        &format!("chan_{}_iter={}", t.to_string(), ser_per_iter),
        |b| b.iter(|| process(&mut child, &sender, &receiver)),
    );
    child.kill().expect("Failed to kill child process");

    let ser_per_iter = true;
    let (mut child, sender, receiver) = spawn_ipc_server_process::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(t, ser_per_iter);
    g.bench_function(
        &format!("chan_{}_iter={}", t.to_string(), ser_per_iter),
        |b| b.iter(|| process(&mut child, &sender, &receiver)),
    );
    child.kill().expect("Failed to kill child process");

    /*


    */

    let t = SendType::Bytes;
    let ser_per_iter = false;
    let (mut child, sender, receiver) =
        spawn_ipc_server_process_bytes::<SimpleIpcChannelMessage>(t, ser_per_iter);
    g.bench_function(
        &format!("chan_{}__iter={}", t.to_string(), ser_per_iter),
        |b| b.iter(|| process_bytes(&mut child, &receiver, &sender)),
    );
    child.kill().expect("Failed to kill child process");

    let ser_per_iter = true;
    let (mut child, sender, receiver) =
        spawn_ipc_server_process_bytes::<SimpleIpcChannelMessage>(t, ser_per_iter);
    g.bench_function(
        &format!("chan_{}_iter={}", t.to_string(), ser_per_iter),
        |b| b.iter(|| process_bytes(&mut child, &receiver, &sender)),
    );
    child.kill().expect("Failed to kill child process");

    /*


    */

    let t = SendType::Serde;
    let ser_per_iter = false;
    let (mut child, sender, receiver) =
        spawn_ipc_server_process::<SimpleIpcChannelMessage, RasterTile2D<u8>>(t, ser_per_iter);
    g.bench_function(
        &format!("chan_{}_iter={}", t.to_string(), ser_per_iter),
        |b| b.iter(|| serde_process(&mut child, &sender, &receiver)),
    );
    child.kill().expect("Failed to kill child process");

    /*


    */

    for t in [
        ServerType::SerializeOnStartup,
        ServerType::SerializeOnEachRequest,
        ServerType::SerializeOnEachRequestWithBincode,
    ] {
        let ser_per_iter: bool;
        if matches!(
            t,
            ServerType::SerializeOnEachRequestWithBincode | ServerType::SerializeOnEachRequest
        ) {
            ser_per_iter = true;
        } else {
            ser_per_iter = false;
        }
        let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
        let mut child = start_process_server(t);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let client = Arc::new(Mutex::new(rt.block_on(async { setup_client(t).await })));

        g.bench_function(
            &format!("grpc_{}_iter={}", t.to_string(), ser_per_iter),
            |b| {
                b.to_async(&rt).iter(|| grpc_process(client.clone()));
            },
        );

        child.kill().expect("Failed to kill child process");
        child.wait().expect("Failed to wait on child process");
    }

    g.finish();
}

///////
///////
///////
///////
///////

#[derive(Clone, Copy)]
enum ServerType {
    SerializeOnStartup,
    SerializeOnEachRequest,
    SerializeOnEachRequestWithBincode,
}

impl ToString for ServerType {
    fn to_string(&self) -> String {
        match self {
            ServerType::SerializeOnStartup => "SerializeOnStartup",
            ServerType::SerializeOnEachRequest => "SerializeOnEachRequest",
            ServerType::SerializeOnEachRequestWithBincode => "SerializeOnEachRequestWithBincode",
        }
        .to_string()
    }
}

fn start_process_server(t: ServerType) -> process::Child {
    let cmd = match t {
        ServerType::SerializeOnStartup => env!("CARGO_BIN_EXE_gdalprocess-grpc-bench-server"),
        ServerType::SerializeOnEachRequest => {
            env!("CARGO_BIN_EXE_gdalprocess-grpc-bench-server-with-serialization")
        }
        ServerType::SerializeOnEachRequestWithBincode => {
            env!("CARGO_BIN_EXE_gdalprocess-grpc-bench-server-with-serialization_bincode")
        }
    };
    process::Command::new(cmd)
        .spawn()
        .expect("Failed to start gdalprocess-grpc-bench-server process")
}

async fn setup_client(t: ServerType) -> GdalDatasetServiceClient<tonic::transport::Channel> {
    let addr: SocketAddr = format!("[::1]:5005{}", match t {
        ServerType::SerializeOnStartup => 1,
        ServerType::SerializeOnEachRequest => 2,
        ServerType::SerializeOnEachRequestWithBincode => 3,
    }).parse().unwrap();
    GdalDatasetServiceClient::connect(format!("http://{}", addr))
        .await
        .expect("Failed to connect to server")
}

fn grpc_bench_process(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let t = ServerType::SerializeOnStartup;
    let mut child = start_process_server(t);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Arc::new(Mutex::new(rt.block_on(async { setup_client(t).await })));

    c.bench_function("client-server roundtrip of grpc (process)", |b| {
        b.to_async(&rt).iter(|| grpc_process(client.clone()));
    });

    child.kill().expect("Failed to kill server process");
}

async fn grpc_process(client: Arc<Mutex<GdalDatasetServiceClient<Channel>>>) {
    let client = client.clone();
    let mut rc = client.lock().unwrap();
    let resp = rc.load_tile_data(RequestTileData {}).await.unwrap();
    std::hint::black_box(resp);
}

fn grpc_bench_process_with_serialisation(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let t = ServerType::SerializeOnEachRequest;
    let mut child = start_process_server(t);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Arc::new(Mutex::new(rt.block_on(async { setup_client(t).await })));

    c.bench_function("grpc_process__ser_per_iter=true", |b| {
        b.to_async(&rt).iter(|| grpc_process(client.clone()));
    });

    child.kill().expect("Failed to kill server process");
}

fn grpc_bench_process_with_serialisation_bincode(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    let t = ServerType::SerializeOnEachRequestWithBincode;
    let mut child = start_process_server(t);
    std::thread::sleep(std::time::Duration::from_secs(1));
    let client = Arc::new(Mutex::new(rt.block_on(async { setup_client(t).await })));

    c.bench_function("grpc_process__serde__process__ser_per_iter=true", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(|| grpc_process(client.clone()));
    });

    child.kill().expect("Failed to kill server process");
}

// criterion_group!(
//     name = benches;
//     config = Criterion::default().with_plots();
//     targets = ipc_channel_process,
//     ipc_channel_process_start_per_iter,
//     ipc_channel_serde_process,
//     ipc_channel_serde_process_iter,
//     ipc_channel_bytes_process,
//     ipc_channel_bytes_process_per_iter,
//     ipc_channel_bench_process_with_serialisation,
//     ipc_channel_bench_process_bytes_with_serialisation,
//     // ipc_channel_bench_process_bytes_serde_with_serialisation
// );

criterion_group!(
    name = benches_group;
    config = Criterion::default().with_plots();
    targets = bench_all,
);
criterion_main!(/* benches, */ benches_group);
