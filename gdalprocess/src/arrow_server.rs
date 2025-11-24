/* License {{{
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
}}} */

// copied from the github exampple
// see: https://arrow.apache.org/docs/format/Flight.html

use std::pin::Pin;
use std::sync::Arc;

use arrow_array::record_batch;
use arrow_flight::Location;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_schema::ArrowError;
use futures::stream::BoxStream;
use futures::{Stream, TryStreamExt};
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
};

#[derive(Clone)]
pub struct FlightServiceImpl {}

fn get_schema() -> arrow::datatypes::Schema {
    arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("path", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("uuid", arrow_schema::DataType::Utf8, true),
    ])
}

fn ticket_for(path: &str) -> Ticket {
    let uuid = uuid::Uuid::new_v4().to_string();
    println!("\t[INFO] Generated ticket UUID: {}", uuid);
    Ticket {
        ticket: bytes::Bytes::from(format!("{}:{}", uuid, path)),
    }
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let foo = request.into_inner();
        // let FlightDescriptor { r#type: _type, cmd, path } = foo;
        if foo.r#type() == DescriptorType::Cmd {
            return Err(Status::unimplemented("Implement get_flight_info for Cmd"));
        }
        let paths = foo.path;
        // for this demo / test we only support a single path
        let path = paths.get(0).unwrap(/* for this demo its fine if we just crash */);

        println!("\t[INFO] get_flight_info for path: {}", path);

        let answer = FlightInfo::new()
            .try_with_schema(&get_schema())
            .expect("schema should be valid")
            .with_endpoint(arrow_flight::FlightEndpoint {
                ticket: Some(ticket_for(path)),
                location: vec![Location {
                    uri: "grpc://localhost:50051".to_string(),
                }],
                expiration_time: None,
                app_metadata: bytes::Bytes::new(),
            })
            .with_descriptor(FlightDescriptor::new_path(vec![path.clone()]));

        Ok(Response::new(answer))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let ticket_str: &str = str::from_utf8(&ticket.ticket).expect("Invalid UTF-8 in ticket");
        let parts: Vec<&str> = ticket_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(Status::invalid_argument("Invalid ticket format"));
        }
        let _uuid = parts[0].to_string();
        let path = parts[1].to_string();

        println!("\t[INFO] do_get for path: {}", path);

        let data_stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(get_schema()))
            .build(futures::stream::once(async move {
                let batch = record_batch!(("uuid", Utf8, [_uuid]), ("path", Utf8, [path]));
                batch.map_err(|e| {
                    arrow_flight::error::FlightError::Tonic(Box::new(Status::internal(format!(
                        "Error creating record batch: {}",
                        e
                    ))))
                })
            }));

        let boxed_stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> = Box::pin(
            data_stream
                .map_err(|e| tonic::Status::internal(format!("Arrow Flight error: {}", e))),
        );
        let response = tonic::Response::new(boxed_stream);
        Ok(response)
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    println!("Starting Arrow Flight server on addr {}...", addr);

    let service = FlightServiceImpl {};

    let svc = FlightServiceServer::new(service);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
