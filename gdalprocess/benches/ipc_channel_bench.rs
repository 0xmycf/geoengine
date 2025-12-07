use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use geoengine_datatypes::raster::raster_tile_2d_to_arrow_ipc_file;
use ipc_channel::ipc::{self, IpcReceiver, IpcSender};
use libgdalprocess::ipc_service::SimpleIpcChannelMessage;

fn setup_ipc_server(
    // grid: RasterTile2D<u8>,
    data: Vec<u8>,
) -> (
    IpcSender<SimpleIpcChannelMessage>,
    IpcReceiver<SimpleIpcChannelMessage>,
) {
    let (server_sender, client_receiver) = ipc::channel().unwrap();
    let (client_sender, server_receiver) = ipc::channel().unwrap();

    let arc = Arc::new(data);
    let jh = std::thread::spawn(move || {
        while let Ok(_req) = server_receiver.recv() {
            server_sender
                .send(SimpleIpcChannelMessage::Data(arc.as_ref().clone()))
                .unwrap();
        }
    });

    if jh.is_finished() {
        panic!("server thread panicked");
    }

    (client_sender, client_receiver)
}

fn ipc_channel_bench(c: &mut Criterion) {
    let data = libgdalprocess::random_data(100_000);
    let grid = libgdalprocess::construct_tile(data, [100, 1000].into());
    let data = raster_tile_2d_to_arrow_ipc_file(
        grid.clone(),
        geoengine_datatypes::spatial_reference::SpatialReferenceOption::Unreferenced,
    )
    .expect("conversion to arrow ipc format does not work");

    let (client_sender, client_receiver) = setup_ipc_server(data);

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

criterion_group!(benches, ipc_channel_bench);
criterion_main!(benches);
