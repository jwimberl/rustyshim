use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use rand::{distributions::Alphanumeric, Rng};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
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
fn schema_to_bytes(schema: &Schema) -> bytes::Bytes {
    use arrow_flight::{IpcMessage, SchemaAsIpc};
    use arrow_ipc::writer::IpcWriteOptions;
    let iwo =
        IpcWriteOptions::try_new(64, false, arrow_ipc::gen::Schema::MetadataVersion::V5).unwrap();
    let schemaipc = SchemaAsIpc::new(&schema, &iwo);
    let ipc = IpcMessage::try_from(schemaipc).unwrap();
    ipc.0
}

//////////////////////////////////
// FlightService implementation //
//////////////////////////////////

const ITEM_EXPIRATION_AGE: Duration = Duration::from_secs(86400);

#[derive(Clone, Copy, PartialEq)]
pub enum SessionType {
    Admin,
    Regular,
    Unauthenticated,
}

#[derive(Clone)]
pub struct ClientSessionInfo {
    username: String,
    start: Instant,
    session_type: SessionType,
}

// TODO:
// - implement timeout based on creation time
// - store token to add per-session protection
#[derive(Clone)]
pub struct TicketInfo {
    start: Instant,
    dataframe: DataFrame,
}

type SessionMap = Arc<RwLock<HashMap<String, ClientSessionInfo>>>;
type TicketMap = Arc<RwLock<HashMap<String, TicketInfo>>>;

pub trait FusionFlightAdministrator {
    // Authentication and authorization
    fn authenticate(
        &self,
        username: &String,
        password: &String,
        request_admin: bool,
    ) -> SessionType;

    // (Re)create datafusion SessionContext
    fn refresh_context(&self) -> Result<SessionContext, Box<dyn std::error::Error>>;
}

pub struct FusionFlightService {
    ctx: Arc<RwLock<SessionContext>>,
    token_map: SessionMap,
    ticket_map: TicketMap,
    flight_info: Arc<RwLock<Vec<Result<FlightInfo, Status>>>>,
    administrator: Box<dyn FusionFlightAdministrator + Send + Sync + 'static>,
}

impl FusionFlightService {
    pub async fn new(
        ctx: SessionContext,
        administrator: Box<dyn FusionFlightAdministrator + Send + Sync + 'static>,
    ) -> Self {
        // Create default flights (for each table))
        let schema_provider = ctx
            .catalog("datafusion")
            .expect("catalog 'datafusion' must exist")
            .schema("public")
            .expect("schema 'public' must exist");

        let tables = schema_provider.table_names();

        // Turn into stream of FlightInfo
        let flight_info = tables.iter().map(|table| async {
            let tcopy = table.clone();
            let table_data = schema_provider
                .table(&tcopy[..])
                .await
                .ok_or(Status::unknown("table schema not found"))?;
            let schema = table_data.schema();
            Ok::<FlightInfo, Status>(FlightInfo {
                schema: schema_to_bytes(&schema),
                flight_descriptor: Some(FlightDescriptor::new_path(vec![tcopy])),
                endpoint: vec![],
                total_records: -1,
                total_bytes: -1,
            })
        });

        let collected_flight_info = futures::future::join_all(flight_info).await;

        // Create and return service object
        FusionFlightService {
            ctx: Arc::new(RwLock::new(ctx)),
            token_map: Arc::new(RwLock::new(HashMap::<String, ClientSessionInfo>::new())),
            ticket_map: Arc::new(RwLock::new(HashMap::<String, TicketInfo>::new())),
            flight_info: Arc::new(RwLock::new(collected_flight_info)),
            administrator: administrator,
        }
    }

    pub async fn create_token(&self, username: &String, session_type: SessionType) -> String {
        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let mut db = self.token_map.write().await;
        (*db)
            .entry(token.clone())
            .and_modify(|session| {
                (*session).username = username.clone();
                (*session).start = Instant::now();
                (*session).session_type = session_type
            })
            .or_insert(ClientSessionInfo {
                username: username.clone(),
                start: Instant::now(),
                session_type: session_type,
            });
        token
    }

    pub async fn validate_headers(
        &self,
        headers: &tonic::metadata::MetadataMap,
    ) -> Result<SessionType, Status> {
        dbg!(headers);
        let provided_token = headers
            .get("authorization")
            .ok_or(Status::unauthenticated("no session token provided"))?
            .to_str()
            .map_err(mderr_to_status)?;

        let db = self.token_map.read().await;
        let info = (*db)
            .get(provided_token)
            .ok_or(Status::unauthenticated("invalid session token"))?;
        let token_age = info.start.elapsed();
        if token_age > ITEM_EXPIRATION_AGE {
            return Err(Status::unauthenticated("expired session token"));
        }
        Ok(info.session_type)
    }

    pub async fn create_ticket(&self, dataframe: DataFrame) -> String {
        let ticket: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let mut tdb = self.ticket_map.write().await;
        (*tdb).insert(
            ticket.clone(),
            TicketInfo {
                start: Instant::now(),
                dataframe: dataframe,
            },
        );
        ticket
    }

    pub async fn get_ticket(&self, ticket: String) -> Option<TicketInfo> {
        let mut tdb = self.ticket_map.write().await;
        (*tdb).remove(&ticket)
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
            Ok(hsr.payload.escape_ascii().to_string())
        } else {
            Err(Status::invalid_argument(
                "username not provided during handshake",
            ))
        }?;
        let password = if let Some(hsr) = rq.message().await? {
            Ok(hsr.payload.escape_ascii().to_string())
        } else {
            Err(Status::invalid_argument(
                "password not provided during handshake",
            ))
        }?;
        let request_admin = if let Some(hsr) = rq.message().await? {
            match &hsr.payload[..] {
                b"0" => Ok(false),
                b"1" => Ok(true),
                _ => Err(Status::invalid_argument(
                    "request_admin flag has invalid value",
                )),
            }
        } else {
            Err(Status::invalid_argument(
                "request_admin flag not provided during handshake",
            ))
        }?;

        // Authenticate
        let st = self
            .administrator
            .authenticate(&username, &password, request_admin);
        if st == SessionType::Unauthenticated {
            return Err(Status::unauthenticated("authentication failed"));
        }

        // With a successful connection, generate token and add it to token_map
        let token = self.create_token(&username, st).await;

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
        // Authorize
        self.validate_headers(_request.metadata()).await?;

        // Send response
        let flight_info = self.flight_info.read().await;
        let flight_info_clone = (*flight_info).clone();
        let tablestream = futures::stream::iter(flight_info_clone);
        Ok(tonic::Response::new(Box::pin(tablestream)))
    }
    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Authorize
        self.validate_headers(_request.metadata()).await?;

        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone().replace("\\\'", "'");
        // Do enough DataFusion logic to get the schema of sql output
        let rctx = self.ctx.read().await;
        let df = (*rctx).sql(&query).await.map_err(dferr_to_status)?;
        let schema: Schema = df.schema().into();

        // Store this in the TicketMap
        let ticket = self.create_ticket(df).await;

        // Return a flight info with the ticket exactly equal to the
        // query string; this is inconsistent with the Flight standard
        // and should be replaced by an opaque ticket that can be used
        // to retrieve the DataFrame df created above and execute it
        let fi = FlightInfo {
            schema: schema_to_bytes(&schema),
            flight_descriptor: Some(fd),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket.into(),
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
        self.validate_headers(_request.metadata()).await?;

        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = _request.into_inner();
        let query = fd.path[0].clone();

        // Do enough DataFusion logic to get the schema of sql output
        let rctx = self.ctx.read().await;
        let df = (*rctx).sql(&query).await.map_err(dferr_to_status)?;
        let schema: Schema = df.schema().into();
        let sr = SchemaResult {
            schema: schema_to_bytes(&schema),
        };

        let response = tonic::Response::new(sr);
        Ok(response)
    }
    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Authorize
        self.validate_headers(_request.metadata()).await?;

        // Process
        let ticket = _request.into_inner().ticket.escape_ascii().to_string();
        let df = self
            .get_ticket(ticket)
            .await
            .ok_or(Status::not_found("ticket not found"))?;
        let dfstream = df
            .dataframe
            .execute_stream()
            .await
            .map_err(dferr_to_status)?;

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
        Err(Status::unauthenticated(
            "PUT not authorized for this database",
        ))
    }
    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Authorize
        let auth = self.validate_headers(_request.metadata()).await?;
        if auth != SessionType::Admin {
            return Err(Status::permission_denied(
                "permission to perform admin action denied",
            ));
        }

        // Perform action
        let action = _request.into_inner();
        let actiontype = action.r#type;
        match actiontype.as_str() {
            "REFRESH_CONTEXT" => {
                let mut wctx = self.ctx.write().await;
                let new_ctx = self
                    .administrator
                    .refresh_context()
                    .map_err(|_e| Status::internal("internal error refreshing context"))?;
                *wctx = new_ctx;
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("SUCCESS"),
                };
                let response = futures::stream::iter(vec![Ok(result)]);
                Ok(tonic::Response::new(Box::pin(response)))
            }
            "CLEAR_EXPIRED_ITEMS" => {
                let mut tokdb = self.token_map.write().await;
                let mut tikdb = self.ticket_map.write().await;
                let tokcount = tokdb.len();
                let tikcount = tikdb.len();
                (*tokdb).retain(|_, v| v.start.elapsed() < ITEM_EXPIRATION_AGE);
                (*tikdb).retain(|_, v| v.start.elapsed() < ITEM_EXPIRATION_AGE);
                let result = arrow_flight::Result {
                    body: bytes::Bytes::from("SUCCESS"),
                };
                let tokdiff = tokcount - tokdb.len();
                let tokprune = arrow_flight::Result {
                    body: bytes::Bytes::from(format!("REMOVED {tokdiff} EXPIRED SESSION TOKENS")),
                };
                let tikdiff = tikcount - tikdb.len();
                let tikprune = arrow_flight::Result {
                    body: bytes::Bytes::from(format!("REMOVED {tikdiff} EXPIRED TICKETS")),
                };
                let response = futures::stream::iter(vec![Ok(result), Ok(tokprune), Ok(tikprune)]);
                Ok(tonic::Response::new(Box::pin(response)))
            }
            _ => Err(Status::invalid_argument("invalid action")),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        // Authorize
        let auth = self.validate_headers(_request.metadata()).await?;
        if auth != SessionType::Admin {
            return Err(Status::permission_denied(
                "permission to perform admin action denied",
            ));
        }

        // Return list of actions
        let refresh_context = arrow_flight::ActionType {
            r#type: String::from("REFRESH_CONTEXT"),
            description: String::from("Re-generate the tables by querying SciDB"),
        };
        let clear_expired_items = arrow_flight::ActionType {
            r#type: String::from("CLEAR_EXPIRED_ITEMS"),
            description: format!(
                "Clear all sessions and tokens greater than {ITEM_EXPIRATION_AGE:?} secs old",
            ),
        };

        let actions = vec![Ok(refresh_context), Ok(clear_expired_items)];
        let response = futures::stream::iter(actions);
        Ok(tonic::Response::new(Box::pin(response)))
    }
    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}
