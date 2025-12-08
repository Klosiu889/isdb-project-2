use openapi_client::models::QueryStatus;
use serde::{Deserialize, Serialize};

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
pub enum QueryDefinition {
    SELECT(SelectQuery),
    COPY(CopyQuery),
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
    status: QueryStatus,
    description: QueryDefinition,
    result: Option<Vec<QueryResult>>,
    errors: Option<Vec<QueryError>>,
}

impl Query {
    pub fn new(status: QueryStatus, description: QueryDefinition) -> Self {
        Self {
            status,
            description,
            result: None,
            errors: None,
        }
    }

    pub fn get_status(&self) -> &QueryStatus {
        &self.status
    }

    pub fn get_definition(&self) -> &QueryDefinition {
        &self.description
    }

    pub fn get_result(&self) -> &Option<Vec<QueryResult>> {
        &self.result
    }

    pub fn get_errors(&self) -> &Option<Vec<QueryError>> {
        &self.errors
    }

    pub fn set_status(&mut self, status: QueryStatus) {
        self.status = status;
    }

    pub fn set_result(&mut self, result: Vec<QueryResult>) {
        self.result = Some(result);
    }
}
