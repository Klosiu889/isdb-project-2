use log::info;
use openapi_client::models;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{executor::Executor, metastore::SharedMetastore, planner::Planner};

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnReferenceExpression {
    pub table_name: String,
    pub column_name: String,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum Literal {
    I64(i64),
    String(String),
    Bool(bool),
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum FunctionName {
    Strlen,
    Concat,
    Upper,
    Lower,
}

impl From<models::FunctionFunctionName> for FunctionName {
    fn from(value: models::FunctionFunctionName) -> Self {
        match value {
            models::FunctionFunctionName::Strlen => Self::Strlen,
            models::FunctionFunctionName::Concat => Self::Concat,
            models::FunctionFunctionName::Upper => Self::Upper,
            models::FunctionFunctionName::Lower => Self::Lower,
        }
    }
}

impl FunctionName {
    pub fn num_arguments(&self) -> usize {
        match self {
            Self::Strlen | Self::Upper | Self::Lower => 1,
            Self::Concat => 2,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct Function {
    pub name: FunctionName,
    pub arguments: Vec<ColumnExpression>,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnarBinaryOperation {
    pub left_operand: Box<ColumnExpression>,
    pub right_operand: Box<ColumnExpression>,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnarUnaryOperation {
    pub operand: Box<ColumnExpression>,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum ColumnExpression {
    Ref(ColumnReferenceExpression),
    Literal(Literal),
    Function(Function),
    Binary(ColumnarBinaryOperation),
    Unary(ColumnarUnaryOperation),
}

impl ColumnExpression {
    pub fn get_column_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        self.collect_column_names(&mut names);
        names
    }

    fn collect_column_names(&self, acc: &mut Vec<String>) {
        match self {
            ColumnExpression::Ref(reference) => acc.push(reference.column_name.clone()),
            ColumnExpression::Literal(_) => {}
            ColumnExpression::Function(function) => {
                for arg in &function.arguments {
                    arg.collect_column_names(acc);
                }
            }
            ColumnExpression::Binary(binary) => {
                binary.left_operand.collect_column_names(acc);
                binary.right_operand.collect_column_names(acc);
            }
            ColumnExpression::Unary(unary) => unary.operand.collect_column_names(acc),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct OrderByExpression {
    pub column_index: usize,
    pub asscending: bool,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SelectQuery {
    pub table_id: String,
    pub column_clauses: Vec<ColumnExpression>,
    pub where_clause: Option<ColumnExpression>,
    pub order_by_clause: Vec<OrderByExpression>,
    pub limit: Option<i32>,
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
        if let Some(successfull_plan) = plan {
            self.executor
                .execute(query_id, successfull_plan, &self.metastore)
                .await;
        }
    }
}
