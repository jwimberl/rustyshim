use crate::scidb::SciDBConnection;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status, Streaming};

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
pub struct FusionFlightService {
    ctx: SessionContext,
    token_map: SessionMap,
    hostname: String,
    port: i32,
}

impl FusionFlightService {
    pub fn new(ctx: SessionContext, hostname: String, port: i32) -> Self {
        FusionFlightService {
            ctx: ctx,
            token_map: Arc::new(Mutex::new(HashMap::<String, ClientSessionInfo>::new())),
            hostname: hostname,
            port: port,
        }
    }

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
impl FlightService for FusionFlightService {
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
        SciDBConnection::new(&self.hostname, &username, &password, self.port)?;

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
