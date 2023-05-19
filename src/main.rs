use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use clap::Parser;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::{distributions::Alphanumeric, Rng};
use rustyshim::SciDBConnection;
use serde::{Deserialize, Serialize};
use serde_yaml;
use std::collections::HashMap;
use std::io::Write;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio; // 0.3.5
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

//////////////////////////
// Configuration format //
//////////////////////////

// Configuration file format

#[derive(Serialize, Deserialize, Debug)]
struct ShimConfig {
    arrays: Vec<SciDBArray>,
}

// Command line arguments

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The SciDB hostname
    #[arg(long, default_value = "localhost")]
    hostname: String,

    /// The SciDB port
    #[arg(long, default_value_t = 1239)]
    port: i32,

    /// The SciDB admin username
    #[arg(short, long)]
    username: Option<String>,

    /// The SciDB admin password
    #[arg(short, long)]
    password: Option<String>,

    /// Flag to read the SciDB admin password from TTY
    #[arg(long, action)]
    password_stdin: bool,

    /// The path to the YAML config file to read
    #[arg(short, long)]
    config: std::path::PathBuf,
}

// SQL request config

#[derive(Serialize, Deserialize, Debug)]
struct SciDBArray {
    name: String,
    afl: String,
}

///////////////////////////////////////////
// DataFusion <-> Flight interop methods //
///////////////////////////////////////////

// Map error types

fn dferr_to_flighterr(dferr: DataFusionError) -> FlightError {
    match dferr {
        DataFusionError::ArrowError(e) => FlightError::Arrow(e),
        e => FlightError::ExternalError(Box::new(e)),
    }
}

fn dferr_to_status(dferr: DataFusionError) -> Status {
    let e = dferr_to_flighterr(dferr);
    Status::new(tonic::Code::Unknown, e.to_string())
}

fn mderr_to_status(_e: tonic::metadata::errors::ToStrError) -> Status {
    Status::new(tonic::Code::Unknown, "error reading request header")
}

// Convert this DFSchema to Bytes, which is
// surprisingly verbose and requires picking some IpcWriteOptions
fn schema_to_bytes(dfschema: &datafusion_common::DFSchema) -> bytes::Bytes {
    use arrow_flight::{IpcMessage, SchemaAsIpc};
    use arrow_ipc::writer::IpcWriteOptions;
    use datafusion::arrow::datatypes::Schema;
    let schema: Schema = dfschema.into();
    let iwo =
        IpcWriteOptions::try_new(64, false, arrow_ipc::gen::Schema::MetadataVersion::V5).unwrap();
    let schemaipc = SchemaAsIpc::new(&schema, &iwo);
    let ipc = IpcMessage::try_from(schemaipc).unwrap();
    ipc.0
}

//////////////////////////////////
// FlightService implementation //
//////////////////////////////////

#[derive(Clone)]
pub struct ClientSessionInfo {
    username: String,
    start: Instant,
}

type SessionMap = Arc<Mutex<HashMap<String, ClientSessionInfo>>>;

#[derive(Clone)]
pub struct FlightServiceImpl {
    ctx: SessionContext,
    token_map: SessionMap,
    scidb_hostname: String,
    scidb_port: i32,
}

impl FlightServiceImpl {
    pub fn create_token(&self, username: &String) -> String {
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let mut db = self.token_map.lock().unwrap();
        db.entry(token.clone())
            .and_modify(|session| {
                (*session).username = username.clone();
                (*session).start = Instant::now();
            })
            .or_insert(ClientSessionInfo {
                username: username.clone(),
                start: Instant::now(),
            });
        token
    }

    pub fn validate_headers(&self, headers: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        dbg!(headers);
        let provided_token = headers
            .get("authorization")
            .ok_or(Status::unauthenticated("no session token provided"))?
            .to_str()
            .map_err(mderr_to_status)?;

        let db = self.token_map.lock().unwrap();
        let info = db
            .get(provided_token)
            .ok_or(Status::unauthenticated("invalid session token"))?;
        let token_age = info.start.elapsed();
        if token_age > Duration::from_secs(86400) {
            return Err(Status::unauthenticated("expired session token"));
        }
        Ok(())
    }
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
        // Get username and password as separate messages
        let mut rq = _request.into_inner();
        let username = if let Some(hsr) = rq.message().await? {
            hsr.payload.escape_ascii().to_string()
        } else {
            return Err(Status::invalid_argument(
                "Username not provided during handshake",
            ));
        };
        let password = if let Some(hsr) = rq.message().await? {
            hsr.payload.escape_ascii().to_string()
        } else {
            return Err(Status::invalid_argument(
                "Password not provided during handshake",
            ));
        };

        // Authenticate via SciDB
        SciDBConnection::new(&self.scidb_hostname, &username, &password, self.scidb_port)?;

        // With a successful connection, generate token and add it to token_map
        let token = self.create_token(&username);

        let response = Ok(arrow_flight::HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::from(token),
        });

        // Send response
        let total_response = futures::stream::iter(vec![response]);
        Ok(tonic::Response::new(Box::pin(total_response)))
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
        // Authorize
        self.validate_headers(_request.metadata())?;

        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone();
        // Do enough DataFusion logic to get the schema of sql output
        let df = self.ctx.sql(&query).await.map_err(dferr_to_status)?;
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
                    ticket: query.into(), // the ticket
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
        // Authorize
        self.validate_headers(_request.metadata())?;

        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone();
        // Do enough DataFusion logic to get the schema of sql output
        let df = self.ctx.sql(&query).await.map_err(dferr_to_status)?;
        let schema = schema_to_bytes(df.schema());

        let sr = SchemaResult { schema: schema };

        let response = tonic::Response::new(sr);
        Ok(response)
    }
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Authorize
        self.validate_headers(_request.metadata())?;

        // Process
        let ticket = _request.into_inner();
        let query = ticket
            .ticket
            .escape_ascii()
            .to_string()
            .replace("\\\'", "'");
        let results = self.ctx.sql(&query).await.map_err(dferr_to_status)?;
        let dfstream = results.execute_stream().await.map_err(dferr_to_status)?;

        // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(dfstream.map_err(dferr_to_flighterr))
            .map_err(|e| Status::new(tonic::Code::Unknown, e.to_string()))
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
    // Logic:
    // -- must not have both --password or --password_stdin argument
    // -- if have either, must not have username
    if args.password.is_some() && args.password_stdin {
        panic!("You may not choose both the --password and --password_stdin argument");
    }
    if !args.username.is_some() && (args.password.is_some() || args.password_stdin) {
        panic!("You may not supply a password via the arugments without a username");
    }
    // Parse/prompt for needed credentials
    let username = match args.username {
        Some(provided) => provided,
        None => {
            let mut prompted = String::new();
            print!("SciDB username: ");
            let _ = std::io::stdout().flush();
            std::io::stdin()
                .read_line(&mut prompted)
                .expect("Invalid username");
            prompted.trim().to_string()
        }
    };
    let password = if args.password_stdin {
        let mut prompted = String::new();
        std::io::stdin()
            .read_line(&mut prompted)
            .expect("Invalid password via stdin");
        prompted.trim().to_string()
    } else {
        match args.password {
            Some(provided) => provided,
            None => rpassword::prompt_password("SciDB password: ")?,
        }
    };

    let conff = std::fs::File::open(&args.config)?;
    let config: ShimConfig = serde_yaml::from_reader(conff)?;

    // Connect to SciDB...
    let mut conn = SciDBConnection::new(&args.hostname, &username, &password, args.port)?;

    // Create a DataFusion context
    let ctx = SessionContext::new();

    // Run queries and register as DataFusion tables
    let db_start = Instant::now();
    for arr in config.arrays {
        let q_start = Instant::now();
        let aio = conn.execute_aio_query(&arr.afl)?;
        println!(
            "Executed SciDB query {}.{}",
            aio.qid.coordinatorid, aio.qid.queryid
        );
        let q_duration = q_start.elapsed();
        println!("Elapsed SciDB query duration: {:?}", q_duration);
        // at this point data is still on-disk in buffer file
        let data = aio.to_batches()?; // consumes buffer file, data lives in memory
                                      // todo: should check that array length is > 0
        let record_batch = datafusion::arrow::compute::concat_batches(&data[0].schema(), &data)?;
        let reg = ctx.register_batch(&arr.name, record_batch);
        if let Err(error) = reg {
            println!(
                "Error while loading SciDB query results into DataFusion:\n\n{}",
                error
            )
        }
    }
    let db_duration = db_start.elapsed();
    println!("Elapsed database construction duration: {:?}", db_duration);

    // Launch Flight server //

    let addr = "127.0.0.1:50051".parse()?;
    let service = FlightServiceImpl {
        ctx: ctx,
        token_map: Arc::new(Mutex::new(HashMap::<String, ClientSessionInfo>::new())),
        scidb_hostname: args.hostname,
        scidb_port: args.port,
    };
    let svc = FlightServiceServer::new(service);
    Server::builder().add_service(svc).serve(addr).await?;
    Ok(())
}
