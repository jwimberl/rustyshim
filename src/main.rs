use datafusion::prelude::*;
use rustyshim::SciDBConnection;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::env;
use std::time::Instant;
use tokio; // 0.3.5

// Configuration format //

#[derive(Serialize, Deserialize, Debug)]
struct SciDBArray {
    name: String,
    afl: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ShimConfig {
    arrays: Vec<SciDBArray>,
}

use clap::Parser;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Args {
    /// The path to the YAML config file to read
    config: std::path::PathBuf,
}

/// Flight server
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use std::pin::Pin;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
//use tonic::IntoStreamingRequest;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket
};
//use arrow_flight::encode::FlightDataEncoder;
use arrow_flight::encode::FlightDataEncoderBuilder;
//use arrow_flight::error::FlightError;

#[derive(Clone)]
pub struct FlightServiceImpl {
    ctx: SessionContext,
}


// Convert this DFSchema to Bytes, which is
// surprisingly verbose and requires picking some IpcWriteOptions
fn schema_to_bytes(dfschema: &datafusion_common::DFSchema) -> bytes::Bytes {
    use datafusion::arrow::datatypes::Schema;
    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_flight::{SchemaAsIpc, IpcMessage};
    let schema: Schema = dfschema.into();
    let iwo = IpcWriteOptions::try_new(
        64,
        false,
        arrow_ipc::gen::Schema::MetadataVersion::V5
    ).unwrap();
    let schemaipc = SchemaAsIpc::new(&schema, &iwo);
    let ipc = IpcMessage::try_from(schemaipc).unwrap();
    ipc.0
}

#[tonic::async_trait]
impl FlightService for FlightServiceImpl {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + Sync + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone();
        // Do enough DataFusion logic to get the schema of sql output
        let df = self.ctx.sql(&query).await.unwrap();
        let schema = schema_to_bytes(df.schema());
        // Return a flight info with the ticket exactly equal to the
        // query string; this is inconsistent with the Flight standard
        // and should be replaced by an opaque ticket that can be used
        // to retrieve the DataFrame df created above and execute it
        let fi = FlightInfo {
            schema: schema,
            flight_descriptor: Some(fd),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: query.into() // the ticket
                }),
                location: vec![],
            }],
            total_records: -1,
            total_bytes: -1,
        };

        let response = tonic::Response::new(fi);
        Ok(response)
    }
    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone();
        // Do enough DataFusion logic to get the schema of sql output
        let df = self.ctx.sql(&query).await.unwrap();
        let schema = schema_to_bytes(df.schema());

        let sr = SchemaResult {
            schema: schema,
        };

        let response = tonic::Response::new(sr);
        Ok(response)
    }
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = _request.into_inner();
        let query = ticket.ticket.escape_ascii().to_string();
        let results = self.ctx.sql(&query).await.unwrap();
        let batches = results.collect().await.unwrap();

        // Get an input stream of Result<RecordBatch, FlightError>
        // NOTE: would be much better to call results.execute_stream()
        // and turn this stream of RecordBatch objects into a stream
        // of Result<RecordBatch, FlightError> objects
        let record_batch =
            datafusion::arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let input_stream = futures::stream::iter(vec![Ok(record_batch)]);

        // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(input_stream)
            .map_err(|_e| Status::ok("ok"))
            .boxed();

        // Create a tonic `Response` that can be returned from a Flight server
        let response = tonic::Response::new(flight_data_stream);
        Ok(response)
    }
    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

// Main function //

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments and read YAML config
    let args = Args::parse();
    let conff = std::fs::File::open(&args.config).unwrap();
    let config: ShimConfig = serde_yaml::from_reader(conff).unwrap();

    // SciDB connection config...
    let hostname = match env::var("SCIDB_HOST") {
        Err(_) => String::from("localhost"),
        Ok(host) => host,
    };
    let username = match env::var("SCIDB_USER") {
        Err(_) => String::from("scidbadmin"),
        Ok(user) => user,
    };
    let password = match env::var("SCIDB_PASSWORD") {
        Err(_) => String::from(""),
        Ok(passwd) => passwd,
    };
    let scidbport = match env::var("SCIDB_PORT") {
        Err(_) => 1239,
        Ok(port) => port.parse::<i32>().unwrap(),
    };

    // Connect to SciDB...
    let mut conn = SciDBConnection::new(&hostname, &username, &password, scidbport);
    if let SciDBConnection::Closed(status) = conn {
        panic!("Connection to SciDB failed! status code {status}");
    }

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Run queries and register as DataFusion tables
    let db_start = Instant::now();
    for arr in config.arrays {
        let q_start = Instant::now();
        let res = conn.execute_aio_query(&arr.afl);
        match res {
            Err(error) => println!(
                "Error code {} in executing query:\n\n{}",
                error.code, error.explanation
            ),
            Ok(aio) => {
                println!(
                    "Executed SciDB query {}.{}",
                    aio.qid.coordinatorid, aio.qid.queryid
                );
                let q_duration = q_start.elapsed();
                println!("Elapsed SciDB query duration: {:?}", q_duration);
                // at this point data is still on-disk in buffer file
                let data = aio.to_batches().unwrap(); // consumes buffer file, data lives in memory
                                                      // todo: should check that array length is > 0
                let record_batch =
                    datafusion::arrow::compute::concat_batches(&data[0].schema(), &data).unwrap();
                let reg = ctx.register_batch(&arr.name, record_batch);
                if let Err(error) = reg {
                    println!(
                        "Error while loading SciDB query results into DataFusion:\n\n{}",
                        error
                    )
                }
            }
        }
    }
    let db_duration = db_start.elapsed();
    println!("Elapsed database construction duration: {:?}", db_duration);

    // Launch Flight server //

    let addr = "127.0.0.1:50051".parse()?;
    let service = FlightServiceImpl { ctx: ctx };
    let svc = FlightServiceServer::new(service);
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
