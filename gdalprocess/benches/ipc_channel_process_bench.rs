use std::process::{Child, Command};

use criterion::{Criterion, criterion_group, criterion_main};
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcReceiver, IpcSender};
use libgdalprocess::ipc_channel_service::{SendType, SimpleIpcChannelMessage};

fn ensure_params(matches: bool, msg: &str) {
    assert!(matches, "{}", msg);
}

fn spawn_ipc_server_proccess_bytes<S>(t: SendType) -> (Child, IpcSender<S>, IpcBytesReceiver) {
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
}

fn spawn_ipc_server_proccess<S, C>(t: SendType) -> (Child, IpcSender<S>, IpcReceiver<C>) {
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
}

// this one spawns a new process each benching process
fn ipc_channel_process_per_iter(c: &mut Criterion) {
    c.bench_function(
        "client-server roundtrip of ipc-channel (process|iter)",
        |b| {
            b.iter(|| {
                let (mut child, sender, receiver) = spawn_ipc_server_proccess::<
                    SimpleIpcChannelMessage,
                    SimpleIpcChannelMessage,
                >(SendType::IpcArrow);

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
            });
        },
    );
}

// this one only requests / sends data
fn ipc_channel_process(c: &mut Criterion) {
    let (mut child, sender, receiver) = spawn_ipc_server_proccess::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(SendType::IpcArrow);

    c.bench_function("client-server roundtrip of ipc-channel (process)", |b| {
        b.iter(|| {
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
        });
    });
    child.kill().expect("Failed to kill child process");
}

// fn ipc_channel_serde_process(c: &mut Criterion) {
//     let (mut child, sender, receiver) =
//         spawn_ipc_server_proccess::<SimpleIpcChannelMessage, RasterTile2D<u8>>(SendType::Serde);
//
//     c.bench_function(
//         "client-server roundtrip of ipc-channel (serde|process|iter)",
//         |b| {
//             b.iter(|| {
//                 match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
//                     Ok(_) => (),
//                     Err(err) => {
//                         child
//                             .kill()
//                             .expect("Failed to kill child process after send error");
//                         panic!("Failed to send request to server: {}", err)
//                     }
//                 }
//                 let resp = receiver
//                     .recv()
//                     .expect("Failed to receive or deserialise data");
//                 std::hint::black_box(resp);
//             });
//         },
//     );
//     child.kill().expect("Failed to kill child process");
// }

// fn ipc_channel_serde_process_iter(c: &mut Criterion) {
//     c.bench_function(
//         "client-server roundtrip of ipc-channel (serde|process)",
//         |b| {
//             b.iter(|| {
//                 let (mut child, sender, receiver) = spawn_ipc_server_proccess::<
//                     SimpleIpcChannelMessage,
//                     RasterTile2D<u8>,
//                 >(SendType::Serde);
//                 match sender.send(SimpleIpcChannelMessage::RequestTileData {}) {
//                     Ok(_) => (),
//                     Err(err) => {
//                         child
//                             .kill()
//                             .expect("Failed to kill child process after send error");
//                         panic!("Failed to send request to server: {}", err)
//                     }
//                 }
//                 let resp = receiver
//                     .recv()
//                     .expect("Failed to receive or deserialise data");
//                 child.kill().expect("Failed to kill child process");
//                 std::hint::black_box(resp);
//             });
//         },
//     );
// }

fn ipc_channel_bytes_process_per_iter(c: &mut Criterion) {
    c.bench_function(
        "client-server roundtrip of ipc-channel (bytes|process|iter)",
        |b| {
            b.iter(|| {
                let (mut child, sender, receiver) =
                    spawn_ipc_server_proccess_bytes(SendType::Bytes);
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
            });
        },
    );
}

fn ipc_channel_bytes_process(c: &mut Criterion) {
    let (mut child, sender, receiver) = spawn_ipc_server_proccess_bytes(SendType::Bytes);

    c.bench_function(
        "client-server roundtrip of ipc-channel (bytes|process)",
        |b| {
            b.iter(|| {
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
            });
        },
    );
    child.kill().expect("Failed to kill child process");
}

// #[test]
// fn it_works(){
//     assert!(false);
// }

// #[test]
// fn serialize_test() {
//     let d = libgdalprocess::random_data(100_000);
//     let tile: RasterTile2D<u8> = libgdalprocess::construct_tile(d, [100, 1000].into());
//     let (sender, receiver) = ipc::channel::<RasterTile2D<u8>>().unwrap();
//     sender.send(tile.clone()).unwrap();
//     let received_tile = receiver.recv().unwrap();
//     assert_eq!(tile, received_tile);
// }

criterion_group!(
    benches,
    ipc_channel_process,
    ipc_channel_process_per_iter,
    // ipc_channel_serde_process,
    // ipc_channel_serde_process_iter,
    ipc_channel_bytes_process,
    ipc_channel_bytes_process_per_iter
);
criterion_main!(benches);
