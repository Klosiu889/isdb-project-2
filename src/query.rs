use log::info;
use openapi_client::models;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{executor::Executor, metastore::SharedMetastore, planner::Planner};

#[derive(Clone, Serialize, Deserialize)]
pub struct SelectQuery {
    pub table_id: String,
    pub table_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CopyQuery {
    pub table_id: String,
    pub table_name: String,
    pub source_filepath: String,
    pub destination_columns: Option<Vec<String>>,
    pub does_csv_contain_header: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum QueryStatus {
    Created,
    Planning,
    Running,
    Completed,
    Failed,
}

impl From<QueryStatus> for models::QueryStatus {
    fn from(value: QueryStatus) -> Self {
        match value {
            QueryStatus::Created => models::QueryStatus::Created,
            QueryStatus::Planning => models::QueryStatus::Planning,
            QueryStatus::Running => models::QueryStatus::Running,
            QueryStatus::Completed => models::QueryStatus::Completed,
            QueryStatus::Failed => models::QueryStatus::Failed,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum QueryDefinition {
    Select(SelectQuery),
    Copy(CopyQuery),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryError {
    pub message: String,
    pub context: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub(crate) table_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Query {
    pub(crate) status: QueryStatus,
    pub(crate) definition: QueryDefinition,
    pub(crate) result: Option<Vec<QueryResult>>,
    pub(crate) errors: Option<Vec<QueryError>>,
}

impl Query {
    pub fn new(status: QueryStatus, definition: QueryDefinition) -> Self {
        Self {
            status,
            definition,
            result: None,
            errors: None,
        }
    }
}

pub struct QueryEngine {
    planner: Planner,
    executor: Executor,
    metastore: SharedMetastore,
}

impl QueryEngine {
    pub fn new(metastore: SharedMetastore) -> Self {
        Self {
            planner: Planner::new(),
            executor: Executor::new(),
            metastore,
        }
    }

    pub async fn run(self, mut receiver: mpsc::Receiver<String>) {
        info!("Query Engine started and waiting for jobs...");

        while let Some(query_id) = receiver.recv().await {
            info!("Engine received query: {}", query_id);
            self.process_query(&query_id).await;
        }

        info!("Query Engine channel closed. Shutting down worker.");
    }

    async fn process_query(&self, query_id: &String) {
        let plan = self.planner.plan(query_id, &self.metastore).await;
        self.executor.execute(query_id, plan, &self.metastore).await;
    }
}
