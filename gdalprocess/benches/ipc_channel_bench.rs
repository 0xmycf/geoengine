use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::raster::{RasterTile2D, raster_tile_2d_to_arrow_ipc_file};
use ipc_channel::ipc::{self, IpcBytesReceiver, IpcReceiver, IpcSender};
use libgdalprocess::ipc_channel_service::SimpleIpcChannelMessage;

fn setup_ipc_server(
    data: Vec<u8>,
) -> (
    IpcSender<SimpleIpcChannelMessage>,
    IpcReceiver<SimpleIpcChannelMessage>,
) {
    let (server_sender, client_receiver) = ipc::channel().unwrap();
    let (client_sender, server_receiver) = ipc::channel().unwrap();

    let jh = std::thread::spawn(move || {
        while let Ok(_req) = server_receiver.recv() {
            server_sender
                .send(SimpleIpcChannelMessage::Data(data.clone()))
                .unwrap();
        }
    });

    if jh.is_finished() {
        panic!("server thread panicked");
    }

    (client_sender, client_receiver)
}

fn setup_ipc_serde(
    data: RasterTile2D<u8>,
) -> (
    IpcSender<SimpleIpcChannelMessage>,
    IpcReceiver<RasterTile2D<u8>>,
) {
    let (server_sender, client_receiver) = ipc::channel().unwrap();
    let (client_sender, server_receiver) = ipc::channel().unwrap();

    let jh = std::thread::spawn(move || {
        while let Ok(_req) = server_receiver.recv() {
            server_sender
                .send(data.clone()) // can I remove this clone?
                .unwrap();
        }
    });

    if jh.is_finished() {
        panic!("server thread panicked");
    }

    (client_sender, client_receiver)
}

fn setup_ipc_server_bytes(data: Vec<u8>) -> (IpcSender<SimpleIpcChannelMessage>, IpcBytesReceiver) {
    let (server_sender, client_receiver) = ipc::bytes_channel().unwrap();
    let (client_sender, server_receiver) = ipc::channel().unwrap();

    let jh = std::thread::spawn(move || {
        while let Ok(_req) = server_receiver.recv() {
            server_sender.send(&data.clone()).unwrap();
        }
    });

    if jh.is_finished() {
        panic!("server thread panicked");
    }

    (client_sender, client_receiver)
}

struct Data(RasterTile2D<u8>);

impl Data {
    fn into_bytes(self) -> Vec<u8> {
        raster_tile_2d_to_arrow_ipc_file(
            self.0,
            geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced,
        )
        .expect("conversion to ipc format failed")
    }

    fn into_inner(self) -> RasterTile2D<u8> {
        self.0
    }
}

fn setup_data() -> Data {
    let data = libgdalprocess::random_data(100_000);
    let grid = libgdalprocess::construct_tile(data, [100, 1000].into());
    Data(grid)
}

fn ipc_channel_bytes_bench(c: &mut Criterion) {
    let data = setup_data();

    let (client_sender, client_receiver) = setup_ipc_server_bytes(data.into_bytes());

    c.bench_function("client-server roundtrip of ipc-channel (bytes)", |b| {
        b.iter(|| {
            client_sender
                .send(SimpleIpcChannelMessage::RequestTileData)
                .unwrap();
            let resp = client_receiver.recv().unwrap();
            std::hint::black_box(resp);
        })
    });
}

fn ipc_channel_bench(c: &mut Criterion) {
    let data = setup_data();

    let (client_sender, client_receiver) = setup_ipc_server(data.into_bytes());

    c.bench_function("client-server roundtrip of ipc-channel", |b| {
        b.iter(|| {
            client_sender
                .send(SimpleIpcChannelMessage::RequestTileData)
                .unwrap();
            let resp = client_receiver.recv().unwrap();
            std::hint::black_box(resp);
        })
    });
}

// fn ipc_channel_serde_bench(c: &mut Criterion) {
//     let data = setup_data();
//
//     let (client_sender, client_receiver) = setup_ipc_serde(data.into_inner());
//
//     c.bench_function("client-server roundtrip of ipc-channel (serde)", |b| {
//         b.iter(|| {
//             client_sender
//                 .send(SimpleIpcChannelMessage::RequestTileData)
//                 .unwrap();
//             let resp = client_receiver
//                 .recv()
//                 .expect("Failed to receive or deserialise data");
//             std::hint::black_box(resp);
//         })
//     });
// }

criterion_group!(
    benches,
    ipc_channel_bench,
    ipc_channel_bytes_bench,
    // ipc_channel_serde_bench,
);
criterion_main!(benches);
