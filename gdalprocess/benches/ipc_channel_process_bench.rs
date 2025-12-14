use std::process::{Child, Command};

use criterion::{Criterion, criterion_group, criterion_main};
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcReceiver, IpcSender};
use libgdalprocess::ipc_channel_service::{SendType, SimpleIpcChannelMessage};

#[inline(always)]
fn ensure_params(matches: bool, msg: &str) {
    assert!(matches, "{}", msg);
}

fn spawn_ipc_server_proccess_bytes<S>(
    t: SendType,
    serialize_during_iteration: bool,
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
        .arg(serialize_during_iteration.to_string())
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
}//}}}

fn spawn_ipc_server_proccess<S, C>(
    t: SendType,
    serialize_during_iteration: bool,
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
        .arg(serialize_during_iteration.to_string())
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
}//}}}

// this one spawns a new process each benching process
fn ipc_channel_process_start_per_iter(c: &mut Criterion) {
    // {{{
    c.bench_function(
        "client-server roundtrip of ipc-channel (process|iter)",
        |b| {
            b.iter(|| {
                let (mut child, sender, receiver) = spawn_ipc_server_proccess::<
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
            });
        },
    );
} //}}}

fn bench_ipc_channel_process(c: &mut Criterion, serialize_during_iteration: bool) {
    // {{{
    let (mut child, sender, receiver) = spawn_ipc_server_proccess::<
        SimpleIpcChannelMessage,
        SimpleIpcChannelMessage,
    >(SendType::IpcArrow, serialize_during_iteration);

    c.bench_function(
        &format!(
            "client-server roundtrip of ipc-channel (process) (serialize_during_iteration={})",
            serialize_during_iteration
        ),
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
} //}}}

// this one only requests / sends data
fn ipc_channel_process(c: &mut Criterion) {
    bench_ipc_channel_process(c, false);
}

// serde {{{
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
// }}}

fn ipc_channel_bytes_process_per_iter(c: &mut Criterion) {
    // {{{
    c.bench_function(
        "client-server roundtrip of ipc-channel (bytes|process|iter)",
        |b| {
            b.iter(|| {
                let (mut child, sender, receiver) =
                    spawn_ipc_server_proccess_bytes(SendType::Bytes, false);
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
} // }}}

fn bench_ipc_channel_process_bytes(c: &mut Criterion, serialize_during_iteration: bool) {
    // {{{
    let (mut child, sender, receiver) =
        spawn_ipc_server_proccess_bytes(SendType::Bytes, serialize_during_iteration);

    c.bench_function(
        &format!(
            "client-server roundtrip of ipc-channel (bytes|process) (serialize_during_iteration={})",
            serialize_during_iteration
        ),
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
} //}}}

fn ipc_channel_bytes_process(c: &mut Criterion) {
    bench_ipc_channel_process_bytes(c, false);
}

fn ipc_channel_bench_process_with_serialisation(c: &mut Criterion) {
    bench_ipc_channel_process(c, true);
}

fn ipc_channel_bench_process_bytes_with_serialisation(c: &mut Criterion) {
    bench_ipc_channel_process_bytes(c, true);
}

// fn ipc_channel_bench_process_bytes_serde_with_serialisation(c: &mut Criterion) {
//     unimplemented!()
// }

criterion_group!(
    benches,
    ipc_channel_process,
    ipc_channel_process_start_per_iter,
    // ipc_channel_serde_process,
    // ipc_channel_serde_process_iter,
    ipc_channel_bytes_process,
    ipc_channel_bytes_process_per_iter,
    ipc_channel_bench_process_with_serialisation,
    ipc_channel_bench_process_bytes_with_serialisation,
);
criterion_main!(benches);
