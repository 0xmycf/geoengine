use geoengine_services::error::Error;
use geoengine_services::server;
use tokio::sync::oneshot;
use geoengine_datatypes::operations::image::{Colorizer, RgbaColor};
use std::convert::TryInto;

/*#[tokio::main]
async fn main() -> Result<(), Error> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let (server, interrupt_success) = tokio::join!(
        server::start_server(Some(shutdown_rx), None),
        server::interrupt_handler(shutdown_tx, Some(|| eprintln!("Shutting down server…"))),
    );

    server.and(interrupt_success)
}*/

fn main() {
    println!("{}", serde_json::to_string(&Colorizer::linear_gradient(
        vec![
            (1.0, RgbaColor::black()).try_into().unwrap(),
            (10.0, RgbaColor::white()).try_into().unwrap(),
        ],
        RgbaColor::transparent(),
        RgbaColor::pink(),
    ).unwrap()).unwrap());
}
