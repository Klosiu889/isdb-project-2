use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::consts::{AUTHOR, INTERFACE_VERSION, MAX_QUERY_WORKERS, SERVER_VERSION};
use crate::metastore::{self, Metastore, MetastoreError, SharedMetastore};
use crate::query::QueryEngine;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper_util::rt::TokioIo;
use log::{info, warn};
use openapi_client::models::{
    ExecuteQueryRequest, MultipleProblemsError, MultipleProblemsErrorProblemsInner, QueryResult,
    SystemInformation, TableSchema,
};
use openapi_client::server::MakeService;
use openapi_client::{
    Api, CreateTableResponse, DeleteTableResponse, GetQueriesResponse, GetQueryByIdResponse,
    GetQueryErrorResponse, GetQueryResultResponse, GetSystemInfoResponse, GetTableByIdResponse,
    GetTablesResponse, SubmitQueryResponse, models,
};
use std::net::SocketAddr;
use std::sync::Arc;
use swagger::auth::MakeAllowAllAuthenticator;
use swagger::{ApiError, EmptyContext, Has, OneOf3, XSpanIdString};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
use openssl::ssl::{Ssl, SslAcceptor, SslFiletype, SslMethod};

pub async fn create(addr: &str, https: bool, metastore: SharedMetastore) {
    let addr: SocketAddr = addr.parse().expect("Failed to parse bind address");
    let listener = TcpListener::bind(&addr).await.unwrap();

    let (sender, receiver) = mpsc::channel(100);

    let engine = Arc::new(QueryEngine::new(metastore.clone(), MAX_QUERY_WORKERS));

    tokio::spawn(async move {
        engine.run(receiver).await;
    });

    let server = Server::new(metastore, sender);

    let service = MakeService::new(server);
    let service = MakeAllowAllAuthenticator::new(service, "cosmo");

    #[allow(unused_mut)]
    let mut service =
        openapi_client::server::context::MakeAddContext::<_, EmptyContext>::new(service);

    if https {
        #[cfg(any(target_os = "macos", target_os = "windows", target_os = "ios"))]
        {
            unimplemented!("SSL is not implemented for the examples on MacOS, Windows or iOS");
        }

        #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
        {
            let mut ssl = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())
                .expect("Failed to create SSL Acceptor");

            // Server authentication
            ssl.set_private_key_file("examples/server-key.pem", SslFiletype::PEM)
                .expect("Failed to set private key");
            ssl.set_certificate_chain_file("examples/server-chain.pem")
                .expect("Failed to set certificate chain");
            ssl.check_private_key()
                .expect("Failed to check private key");

            let tls_acceptor = ssl.build();

            info!("Starting a server (with https)");
            loop {
                if let Ok((tcp, addr)) = listener.accept().await {
                    let ssl = Ssl::new(tls_acceptor.context()).unwrap();
                    let service = service.call(addr);

                    tokio::spawn(async move {
                        let tls = tokio_openssl::SslStream::new(ssl, tcp).map_err(|_| ())?;
                        let service = service.await.map_err(|_| ())?;

                        http1::Builder::new()
                            .serve_connection(TokioIo::new(tls), service)
                            .await
                            .map_err(|_| ())
                    });
                }
            }
        }
    } else {
        info!("Starting a server (over http, so no TLS)");
        println!("Listening on http://{}", addr);

        loop {
            // When an incoming TCP connection is received grab a TCP stream for
            // client<->server communication.
            //
            // Note, this is a .await point, this loop will loop forever but is not a busy loop. The
            // .await point allows the Tokio runtime to pull the task off of the thread until the task
            // has work to do. In this case, a connection arrives on the port we are listening on and
            // the task is woken up, at which point the task is then put back on a thread, and is
            // driven forward by the runtime, eventually yielding a TCP stream.
            let (tcp_stream, addr) = listener
                .accept()
                .await
                .expect("Failed to accept connection");

            let service = service.call(addr).await.unwrap();
            let io = TokioIo::new(tcp_stream);
            // Spin up a new task in Tokio so we can continue to listen for new TCP connection on the
            // current task without waiting for the processing of the HTTP1 connection we just received
            // to finish
            tokio::task::spawn(async move {
                // Handle the connection from the client using HTTP1 and pass any
                // HTTP requests received on that connection to the `hello` function
                let result = http1::Builder::new().serve_connection(io, service).await;
                if let Err(err) = result {
                    println!("Error serving connection: {err:?}");
                }
            });
        }
    }
}

#[derive(Clone)]
pub struct Server {
    version: String,
    interface_version: String,
    author: String,
    start_time: Instant,
    metastore: Arc<RwLock<Metastore>>,
    query_queue: mpsc::Sender<String>,
}

impl Server {
    pub fn new(metastore: SharedMetastore, query_queue: mpsc::Sender<String>) -> Self {
        Server {
            version: SERVER_VERSION.to_string(),
            interface_version: INTERFACE_VERSION.to_string(),
            author: AUTHOR.to_string(),
            start_time: Instant::now(),
            metastore,
            query_queue,
        }
    }
}

impl From<metastore::Error> for models::Error {
    fn from(value: metastore::Error) -> Self {
        Self {
            message: value.message,
        }
    }
}

#[async_trait]
impl<C> Api<C> for Server
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint.
    async fn get_tables(&self, _: &C) -> Result<GetTablesResponse, ApiError> {
        info!("API: get_tables | Starting processing");

        let shallow_tables = self.metastore.read().await.get_shallow_tables();
        info!("API: get_tables | Success | Tables: {:?}", shallow_tables);
        Ok(GetTablesResponse::ArrayOfTablesInDatabase(shallow_tables))
    }

    /// Get detailed description of selected table
    async fn get_table_by_id(
        &self,
        table_id: String,
        _: &C,
    ) -> Result<GetTableByIdResponse, ApiError> {
        info!("API: get_table_by_id | Starting processing");

        let table = self.metastore.read().await.get_table(&table_id);
        match table {
            Ok(table) => {
                info!("API: get_table_by_id | Success | TableID: {}", table_id);
                Ok(GetTableByIdResponse::DetailedTableDescription(table))
            }
            Err(MetastoreError::TableAccessError(error)) => {
                warn!(
                    "API: get_table_by_id | Failed | TableID: {} | Error: {:?}",
                    table_id, error
                );
                Ok(GetTableByIdResponse::GenericError(error.into()))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Delete selected table from database
    async fn delete_table(&self, table_id: String, _: &C) -> Result<DeleteTableResponse, ApiError> {
        info!("API: delete_table | Starting processing");

        match self.metastore.write().await.delete_table(&table_id) {
            Ok(_) => {
                info!("API: delete_table | Success | TableID: {}", table_id);
                Ok(DeleteTableResponse::TableHasBeenDeletedSuccessfully)
            }
            Err(MetastoreError::TableDeletionError(error)) => {
                warn!(
                    "API: delete_table | Failed | TableID: {} | Error: {:?}",
                    table_id, error
                );
                Ok(DeleteTableResponse::GenericError(error.into()))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Create new table in database
    async fn create_table(
        &self,
        table_schema: TableSchema,
        _: &C,
    ) -> Result<CreateTableResponse, ApiError> {
        info!("API: create_table | Starting processing");

        match self.metastore.write().await.create_table(table_schema) {
            Ok(id) => {
                info!("API: create_table | Success | TableID: {}", id);
                Ok(CreateTableResponse::TableCreatedSuccessfully(id))
            }
            Err(MetastoreError::TableCreationError(errors)) => {
                let problems = errors
                    .iter()
                    .map(|error| MultipleProblemsErrorProblemsInner {
                        error: error.message.clone(),
                        context: error.context.clone(),
                    })
                    .collect();
                let e = MultipleProblemsError { problems };
                warn!("API: create_table | Failed | Error: {:?}", e);
                Ok(CreateTableResponse::ResponseUsedWhenMoreProblemsCanOccurInTheSystemWhenProcessingRequest(e))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.
    async fn get_queries(&self, _: &C) -> Result<GetQueriesResponse, ApiError> {
        info!("API: get_queries | Starting processing");

        info!("API: get_queries  | Success");
        Ok(GetQueriesResponse::ArrayOfQueriesSubmittedToTheSystem(
            self.metastore.read().await.get_queries(),
        ))
    }

    /// Get detailed status of selected query
    async fn get_query_by_id(
        &self,
        query_id: String,
        _: &C,
    ) -> Result<GetQueryByIdResponse, ApiError> {
        info!("API: get_query_by_id | Starting processing");

        match self.metastore.read().await.get_query(&query_id) {
            Ok(query) => {
                info!("API: get_query_by_id | Success | QueryID {:}", query_id);
                Ok(GetQueryByIdResponse::DetailedQueryDescription(query))
            }
            Err(MetastoreError::QueryAccessError(error)) => {
                warn!("API: get_query_by_id | Failed | Error: {:?}", error);
                Ok(GetQueryByIdResponse::GenericError(error.into()))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Submit new query for execution
    async fn submit_query(
        &self,
        execute_query_request: ExecuteQueryRequest,
        _: &C,
    ) -> Result<SubmitQueryResponse, ApiError> {
        info!("API: submit_query | Starting processing");

        let mut metastore_guard = self.metastore.write().await;
        let query_def = execute_query_request.query_definition;
        let result = match &*query_def {
            OneOf3::A(select_all) => metastore_guard.create_select_all_query(select_all),
            OneOf3::B(select) => metastore_guard.create_select_query(select),
            OneOf3::C(copy) => metastore_guard.create_copy_query(copy),
        };

        match result {
            Ok(id) => {
                let _ = self.query_queue.send(id.clone()).await;
                info!("API: submit_query | Success | QueryID: {}", id);
                Ok(SubmitQueryResponse::QueryHasBeenCreatedSuccessfully(id))
            }
            Err(MetastoreError::QueryCreationError(errors)) => {
                let problems = errors
                    .iter()
                    .map(|error| MultipleProblemsErrorProblemsInner {
                        error: error.message.clone(),
                        context: error.context.clone(),
                    })
                    .collect();
                let e = MultipleProblemsError { problems };
                warn!("API: submit_query | Failed | Error: {:?}", e);
                Ok(SubmitQueryResponse::ResponseUsedWhenMoreProblemsCanOccurInTheSystemWhenProcessingRequest(e))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Get result of selected query (will be available only for SELECT queries after they are completed)
    async fn get_query_result(
        &self,
        query_id: String,
        get_query_result_request: Option<models::GetQueryResultRequest>,
        _: &C,
    ) -> Result<GetQueryResultResponse, ApiError> {
        info!("API: get_query_result | Starting processing");

        let row_limit = match get_query_result_request.as_ref() {
            Some(r) => r.row_limit,
            None => None,
        };
        let flush_result = match get_query_result_request {
            Some(r) => r.flush_result.unwrap_or(false),
            None => false,
        };

        let result = if flush_result {
            self.metastore
                .write()
                .await
                .get_query_result_flush(&query_id, row_limit)
        } else {
            self.metastore
                .read()
                .await
                .get_query_result(&query_id, row_limit)
        };

        match result {
            Ok(res) => {
                info!("API: get_query_result | Success | QueryID: {}", query_id);
                Ok(GetQueryResultResponse::ResultOfSelectedQuery(
                    QueryResult::from(res),
                ))
            }
            Err(MetastoreError::QueryAccessError(error)) => {
                warn!("API: get_query_result | Failed | Error: {:?}", error);
                Ok(GetQueryResultResponse::GenericError(error.into()))
            }
            Err(MetastoreError::QueryResultAccessError(error)) => {
                warn!("API: get_query_result | Failed | Error: {:?}", error);
                Ok(GetQueryResultResponse::GenericError_2(error.into()))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Get error of selected query (will be available only for queries in FAILED state)
    async fn get_query_error(
        &self,
        query_id: String,
        _: &C,
    ) -> Result<GetQueryErrorResponse, ApiError> {
        info!("API: get_query_error | Starting processing");

        match self.metastore.read().await.get_query_error(&query_id) {
            Ok(errors) => {
                let problems = errors
                    .iter()
                    .map(|error| MultipleProblemsErrorProblemsInner {
                        error: error.message.clone(),
                        context: error.context.clone(),
                    })
                    .collect();
                let e = MultipleProblemsError { problems };

                info!("API: get_query_error | Success | QueryID: {}", query_id);
                Ok(GetQueryErrorResponse::ResponseUsedWhenMoreProblemsCanOccurInTheSystemWhenProcessingRequest(e))
            }
            Err(MetastoreError::QueryAccessError(error)) => {
                warn!("API: get_query_error | Failed | Error: {:?}", error);
                Ok(GetQueryErrorResponse::GenericError(error.into()))
            }
            Err(MetastoreError::QueryErrorAccessError(error)) => {
                warn!("API: get_query_error | Success | Error: {:?}", error);
                Ok(GetQueryErrorResponse::GenericError_2(error.into()))
            }
            _ => Err(ApiError("Internal server error".to_string())),
        }
    }

    /// Get basic information about the system (e.g. version, uptime, etc.)
    async fn get_system_info(&self, _: &C) -> Result<GetSystemInfoResponse, ApiError> {
        info!("API: get_system_info | Starting processing");

        info!("API: get_system_info | Success");
        Ok(GetSystemInfoResponse::BasicInformationAboutTheSystem(
            SystemInformation {
                interface_version: Some(self.interface_version.clone()),
                version: self.version.clone(),
                author: self.author.clone(),
                uptime: Some(self.start_time.elapsed().as_secs() as i64),
            },
        ))
    }
}
