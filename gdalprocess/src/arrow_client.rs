
use arrow_flight::FlightClient;
use futures::StreamExt;
use tonic::transport::Channel;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("grpc://[::1]:50051")
        .connect()
        .await
        .expect("error connecting");

    let mut client = FlightClient::new(channel);

    // Send 'Hi' bytes as the handshake request to the server
    let response = client
        .get_flight_info(arrow_flight::FlightDescriptor::new_path(vec![ "/tmp/some_path.png".to_string() ]))
        .await?;

    println!("total_bytes={:?}", response.clone().total_bytes);
    println!("total_records={:?}", response.clone().total_records);

    let schema = response.clone().try_decode_schema()?;
    println!("schema={:?}", schema);

    println!("================================================================");
    println!();
    println!("endpoints:");

    for endpoint in response.endpoint {
        println!("[INFO] fetchinng from endpoint={:?}", endpoint);
        let ticket = endpoint
            .ticket
            .as_ref()
            .expect("endpoint should have a ticket");
        let mut stream = client.do_get(ticket.clone()).await.expect("do_get error").into_inner();
        while let Some(flight_data) = stream.next().await {
            match flight_data {
                Err(e) => {
                    println!("Error during do_get: {}", e);
                    continue;
                }
                Ok(data) => {
                    println!("[INFO] received flight data chunk with {:?} bytes", data.payload);
                }
            }
        }
    }

    Ok(())
}
