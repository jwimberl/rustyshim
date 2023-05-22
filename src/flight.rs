use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::{
    flight_service_server::FlightService, sql::server::FlightSqlService,
    sql::ActionClosePreparedStatementRequest, sql::ActionCreatePreparedStatementRequest,
    sql::ActionCreatePreparedStatementResult, sql::CommandGetCatalogs,
    sql::CommandGetCrossReference, sql::CommandGetDbSchemas, sql::CommandGetExportedKeys,
    sql::CommandGetImportedKeys, sql::CommandGetPrimaryKeys, sql::CommandGetSqlInfo,
    sql::CommandGetTableTypes, sql::CommandGetTables, sql::CommandPreparedStatementQuery,
    sql::CommandPreparedStatementUpdate, sql::CommandStatementQuery, sql::CommandStatementUpdate,
    sql::SqlInfo, sql::TicketStatementQuery, Action, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, Ticket,
};
use datafusion::arrow::array::{StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
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

#[derive(Clone)]
pub struct ClientSessionInfo {
    username: String,
    start: Instant,
}

// TODO:
// - implement timeout based on creation time
// - store token to add per-session protection
#[derive(Clone)]
pub struct TicketInfo {
    dataframe: DataFrame,
}

type SessionMap = Arc<Mutex<HashMap<String, ClientSessionInfo>>>;
type TicketMap = Arc<Mutex<HashMap<String, TicketInfo>>>;

pub trait FusionFlightAuthenticator {
    fn authenticate(&self, username: &String, password: &String) -> bool;
}

pub struct FusionFlightService {
    ctx: SessionContext,
    token_map: SessionMap,
    ticket_map: TicketMap,
    flight_info: Arc<Mutex<Vec<Result<FlightInfo, Status>>>>,
    authenticator: Box<dyn FusionFlightAuthenticator + Send + Sync + 'static>,
}

impl FusionFlightService {
    pub async fn new(
        ctx: SessionContext,
        authenticator: Box<dyn FusionFlightAuthenticator + Send + Sync + 'static>,
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
            ctx: ctx,
            token_map: Arc::new(Mutex::new(HashMap::<String, ClientSessionInfo>::new())),
            ticket_map: Arc::new(Mutex::new(HashMap::<String, TicketInfo>::new())),
            flight_info: Arc::new(Mutex::new(collected_flight_info)),
            authenticator: authenticator,
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

    pub fn create_ticket(&self, dataframe: DataFrame) -> String {
        let ticket: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();
        let mut tdb = self.ticket_map.lock().unwrap();
        tdb.insert(
            ticket.clone(),
            TicketInfo {
                dataframe: dataframe,
            },
        );
        ticket
    }

    pub fn get_ticket(&self, ticket: String) -> Option<TicketInfo> {
        let mut tdb = self.ticket_map.lock().unwrap();
        tdb.remove(&ticket)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FusionFlightService {
    type FlightService = FusionFlightService;

    /// Accept authentication and return a token
    /// <https://arrow.apache.org/docs/format/Flight.html#authentication>
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
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

        // Authenticate
        if !self.authenticator.authenticate(&username, &password) {
            return Err(Status::unauthenticated("authentication failed"));
        }

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

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Authorize
        self.validate_headers(request.metadata())?;

        // Note: abusing a FlightDescriptor of type PATH
        // and effectively treating it as a flight descriptor
        // of type CMD; to adhere strictly to the experimental
        // FlightSQL protocol the sql query should be encoded
        // in a specific command format
        let fd = request.into_inner();
        let query = fd.path[0].clone().replace("\\\'", "'");
        // Do enough DataFusion logic to get the schema of sql output
        let df = self.ctx.sql(&query).await.map_err(dferr_to_status)?;
        let schema: Schema = df.schema().into();

        // Store this in the TicketMap
        let ticket = self.create_ticket(df);

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

    /// Get a FlightInfo for executing an already created prepared statement.
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo for listing schemas.
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo for listing tables.
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo to extract information about the table types.
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo for retrieving other information (See SqlInfo).
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo to extract information about primary and foreign keys.
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo to extract information about exported keys.
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo to extract information about imported keys.
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightInfo to extract information about cross reference.
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    // do_get

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Authorize
        self.validate_headers(request.metadata())?;

        // Process
        let ticket = request.into_inner().ticket.escape_ascii().to_string();
        let df = self
            .get_ticket(ticket)
            .ok_or(Status::not_found("ticket not found"))?;
        let dfstream = df
            .dataframe
            .execute_stream()
            .await
            .map_err(dferr_to_status)?;

        // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
        let catalogs_stream = FlightDataEncoderBuilder::new()
            .build(dfstream.map_err(dferr_to_flighterr))
            .map_err(|e| Status::new(tonic::Code::Unknown, e.to_string()))
            .boxed();

        // Create a tonic `Response` that can be returned from a Flight server
        let response = tonic::Response::new(catalogs_stream);
        Ok(response)
    }

    /// Get a FlightDataStream containing the prepared statement query results.
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the list of catalogs.
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Authorize
        self.validate_headers(request.metadata())?;

        // Process
        let catalogs_schema = Schema::new(vec![Field::new("CATALOG_NAME", DataType::Utf8, false)]);
        let catalogs = StringArray::from(self.ctx.catalog_names());
        let catalogs_batch =
            RecordBatch::try_new(Arc::new(catalogs_schema), vec![Arc::new(catalogs)]);

        // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
        let catalogs_stream = futures::stream::iter(vec![catalogs_batch]);
        let catalogs_stream = FlightDataEncoderBuilder::new()
            .build(catalogs_stream.map_err(|e| FlightError::Arrow(e)))
            .map_err(|e| Status::new(tonic::Code::Unknown, e.to_string()))
            .boxed();

        // Create a tonic `Response` that can be returned from a Flight server
        let response = tonic::Response::new(catalogs_stream);
        Ok(response)
    }

    /// Get a FlightDataStream containing the list of schemas.
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the list of tables.
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Authorize
        self.validate_headers(request.metadata())?;

        // Send response
        /*
        let flight_info = self.flight_info.lock().unwrap().clone();
        let tablestream = futures::stream::iter(flight_info);
        Ok(tonic::Response::new(Box::pin(tablestream)))
        */
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the data related to the table types.
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the list of SqlInfo results.
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the data related to the primary and foreign keys.
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the data related to the exported keys.
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the data related to the imported keys.
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Get a FlightDataStream containing the data related to the cross reference.
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    // do_put

    /// Execute an update SQL statement.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Bind parameters to given prepared statement.
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Execute an update SQL prepared statement.
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    // do_action

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Close a prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) {
        // noop
    }

    /// Register a new SqlInfo result, making it available when calling GetSqlInfo.
    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        // noop
    }
}
