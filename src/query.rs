use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use log::info;
use openapi_client::models;
use serde::{Deserialize, Serialize};
use swagger::{OneOf3, OneOf5};
use tokio::sync::{Semaphore, mpsc};

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

impl From<models::LiteralValue> for Literal {
    fn from(value: models::LiteralValue) -> Self {
        match value.into() {
            OneOf3::A(val) => Self::I64(val),
            OneOf3::B(val) => Self::String(val),
            OneOf3::C(val) => Self::Bool(val),
        }
    }
}

impl From<Literal> for models::LiteralValue {
    fn from(value: Literal) -> Self {
        match value {
            Literal::I64(val) => Self::from(OneOf3::A(val)),
            Literal::String(val) => Self::from(OneOf3::B(val)),
            Literal::Bool(val) => Self::from(OneOf3::C(val)),
        }
    }
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

    pub fn arguments_types(&self) -> Vec<ExpressionType> {
        match self {
            Self::Strlen | Self::Upper | Self::Lower => vec![ExpressionType::String],
            Self::Concat => vec![ExpressionType::String, ExpressionType::String],
        }
    }

    pub fn get_type(&self) -> ExpressionType {
        match self {
            Self::Upper | Self::Lower | Self::Concat => ExpressionType::String,
            Self::Strlen => ExpressionType::I64,
        }
    }
}

impl From<FunctionName> for models::FunctionFunctionName {
    fn from(value: FunctionName) -> Self {
        match value {
            FunctionName::Strlen => Self::Strlen,
            FunctionName::Concat => Self::Concat,
            FunctionName::Upper => Self::Upper,
            FunctionName::Lower => Self::Lower,
        }
    }
}

impl Display for FunctionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strlen => write!(f, "STRLEN"),
            Self::Concat => write!(f, "CONCAT"),
            Self::Upper => write!(f, "UPPER"),
            Self::Lower => write!(f, "LOWER"),
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
    pub operator: BinOperator,
    pub left_operand: Box<ColumnExpression>,
    pub right_operand: Box<ColumnExpression>,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ColumnarUnaryOperation {
    pub operator: Operator,
    pub operand: Box<ColumnExpression>,
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum BinOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
}

impl BinOperator {
    pub fn get_type(&self) -> ExpressionType {
        match self {
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide => ExpressionType::I64,
            _ => ExpressionType::Bool,
        }
    }

    pub fn get_args_types(&self) -> Option<(ExpressionType, ExpressionType)> {
        match self {
            Self::Add | Self::Subtract | Self::Multiply | Self::Divide => {
                Some((ExpressionType::I64, ExpressionType::I64))
            }
            Self::And | Self::Or => Some((ExpressionType::Bool, ExpressionType::Bool)),
            _ => None,
        }
    }

    pub fn is_commutative(&self) -> bool {
        match self {
            Self::Add | Self::Multiply | Self::And | Self::Or | Self::Equal | Self::NotEqual => {
                true
            }
            _ => false,
        }
    }
}

impl From<models::ColumnarBinaryOperationOperator> for BinOperator {
    fn from(value: models::ColumnarBinaryOperationOperator) -> Self {
        match value {
            models::ColumnarBinaryOperationOperator::Add => Self::Add,
            models::ColumnarBinaryOperationOperator::Subtract => Self::Subtract,
            models::ColumnarBinaryOperationOperator::Multiply => Self::Multiply,
            models::ColumnarBinaryOperationOperator::Divide => Self::Divide,
            models::ColumnarBinaryOperationOperator::And => Self::And,
            models::ColumnarBinaryOperationOperator::Or => Self::Or,
            models::ColumnarBinaryOperationOperator::Equal => Self::Equal,
            models::ColumnarBinaryOperationOperator::NotEqual => Self::NotEqual,
            models::ColumnarBinaryOperationOperator::LessThan => Self::LessThan,
            models::ColumnarBinaryOperationOperator::LessEqual => Self::LessEqual,
            models::ColumnarBinaryOperationOperator::GreaterThan => Self::GreaterThan,
            models::ColumnarBinaryOperationOperator::GreaterEqual => Self::GreaterEqual,
        }
    }
}

impl From<BinOperator> for models::ColumnarBinaryOperationOperator {
    fn from(value: BinOperator) -> Self {
        match value {
            BinOperator::Add => Self::Add,
            BinOperator::Subtract => Self::Subtract,
            BinOperator::Multiply => Self::Multiply,
            BinOperator::Divide => Self::Divide,
            BinOperator::And => Self::And,
            BinOperator::Or => Self::Or,
            BinOperator::Equal => Self::Equal,
            BinOperator::NotEqual => Self::NotEqual,
            BinOperator::LessThan => Self::LessThan,
            BinOperator::LessEqual => Self::LessEqual,
            BinOperator::GreaterThan => Self::GreaterThan,
            BinOperator::GreaterEqual => Self::GreaterEqual,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum Operator {
    Not,
    Minus,
}

impl Operator {
    pub fn get_type(&self) -> ExpressionType {
        match self {
            Self::Not => ExpressionType::Bool,
            Self::Minus => ExpressionType::I64,
        }
    }
    pub fn get_argument_type(&self) -> ExpressionType {
        match self {
            Self::Not => ExpressionType::Bool,
            Self::Minus => ExpressionType::I64,
        }
    }
}

impl From<models::ColumnarUnaryOperationOperator> for Operator {
    fn from(value: models::ColumnarUnaryOperationOperator) -> Self {
        match value {
            models::ColumnarUnaryOperationOperator::Not => Self::Not,
            models::ColumnarUnaryOperationOperator::Minus => Self::Minus,
        }
    }
}

impl From<Operator> for models::ColumnarUnaryOperationOperator {
    fn from(value: Operator) -> Self {
        match value {
            Operator::Not => Self::Not,
            Operator::Minus => Self::Minus,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum ExpressionType {
    I64,
    String,
    Bool,
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
    pub fn get_tables_names(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        self.collect_tables_names(&mut names);
        names
    }

    fn collect_tables_names(&self, acc: &mut HashSet<String>) {
        match self {
            ColumnExpression::Ref(reference) => {
                acc.insert(reference.table_name.clone());
            }
            ColumnExpression::Literal(_) => {}
            ColumnExpression::Function(function) => {
                for arg in &function.arguments {
                    arg.collect_tables_names(acc);
                }
            }
            ColumnExpression::Binary(binary) => {
                binary.left_operand.collect_tables_names(acc);
                binary.right_operand.collect_tables_names(acc);
            }
            ColumnExpression::Unary(unary) => unary.operand.collect_tables_names(acc),
        }
    }

    pub fn get_columns_names(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        self.collect_columns_names(&mut names);
        names
    }

    fn collect_columns_names(&self, acc: &mut HashSet<String>) {
        match self {
            ColumnExpression::Ref(reference) => {
                acc.insert(reference.column_name.clone());
            }
            ColumnExpression::Literal(_) => {}
            ColumnExpression::Function(function) => {
                for arg in &function.arguments {
                    arg.collect_columns_names(acc);
                }
            }
            ColumnExpression::Binary(binary) => {
                binary.left_operand.collect_columns_names(acc);
                binary.right_operand.collect_columns_names(acc);
            }
            ColumnExpression::Unary(unary) => unary.operand.collect_columns_names(acc),
        }
    }

    pub fn get_type(
        &self,
        table_schema: &HashMap<String, ExpressionType>,
    ) -> Result<ExpressionType, String> {
        match self {
            ColumnExpression::Ref(reference) => table_schema
                .get(&reference.column_name)
                .cloned()
                .ok_or("Column not found in schema".to_string()),
            ColumnExpression::Literal(literal) => match literal {
                Literal::I64(_) => Ok(ExpressionType::I64),
                Literal::String(_) => Ok(ExpressionType::String),
                Literal::Bool(_) => Ok(ExpressionType::Bool),
            },
            ColumnExpression::Function(function) => {
                let arg_types = function
                    .arguments
                    .iter()
                    .map(|arg| arg.get_type(table_schema))
                    .collect::<Result<Vec<_>, _>>()?;
                let expected_argument_types = function.name.arguments_types();
                if arg_types.len() != expected_argument_types.len() {
                    return Err(format!(
                        "Wrong number of arguments in function '{}'",
                        function.name
                    ));
                }
                for (i, arg) in arg_types.iter().enumerate() {
                    if *arg != expected_argument_types[i] {
                        return Err(format!(
                            "Wrong type of arguments in function '{}'",
                            function.name
                        ));
                    }
                }
                Ok(function.name.get_type())
            }
            ColumnExpression::Binary(binary) => {
                let left_type = binary.left_operand.get_type(table_schema)?;
                let right_type = binary.right_operand.get_type(table_schema)?;
                let operator_types = binary.operator.get_args_types();
                match operator_types {
                    Some((left, right)) => {
                        if left_type != left || right_type != right {
                            return Err("Wrong types of arguments in binary oparation".to_string());
                        }
                    }
                    None => {
                        if left_type != right_type {
                            return Err("Wrong types of arguments in binary oparation".to_string());
                        }
                    }
                }
                Ok(binary.operator.get_type())
            }
            ColumnExpression::Unary(unary) => {
                let operand_type = unary.operand.get_type(table_schema)?;
                let operator_type = unary.operator.get_argument_type();

                if operand_type != operator_type {
                    return Err("Wrong argument type in unary operation".to_string());
                }

                Ok(unary.operator.get_type())
            }
        }
    }
}

impl TryFrom<models::ColumnExpression> for ColumnExpression {
    type Error = String;

    fn try_from(value: models::ColumnExpression) -> Result<Self, Self::Error> {
        match value.into() {
            OneOf5::A(reference) => Ok(ColumnExpression::Ref(ColumnReferenceExpression {
                table_name: reference.table_name,
                column_name: reference.column_name,
            })),
            OneOf5::B(literal) => Ok(ColumnExpression::Literal(literal.value.into())),
            OneOf5::C(function) => Ok(ColumnExpression::Function(Function {
                name: function.function_name.into(),
                arguments: function
                    .arguments
                    .unwrap_or_default()
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()?,
            })),
            OneOf5::D(binary) => Ok(ColumnExpression::Binary(ColumnarBinaryOperation {
                operator: binary.operator.into(),
                left_operand: Box::new((*binary.left_operand).try_into()?),
                right_operand: Box::new((*binary.right_operand).try_into()?),
            })),
            OneOf5::E(unary) => Ok(ColumnExpression::Unary(ColumnarUnaryOperation {
                operator: unary.operator.into(),
                operand: Box::new((*unary.operand).try_into()?),
            })),
        }
    }
}

impl From<ColumnExpression> for models::ColumnExpression {
    fn from(value: ColumnExpression) -> Self {
        let one_of_value = match value {
            ColumnExpression::Ref(reference) => OneOf5::A(models::ColumnReferenceExpression {
                table_name: reference.table_name,
                column_name: reference.column_name,
            }),
            ColumnExpression::Literal(literal) => OneOf5::B(models::Literal {
                value: literal.into(),
            }),
            ColumnExpression::Function(function) => OneOf5::C(models::Function {
                function_name: function.name.into(),
                arguments: if function.arguments.len() == 0 {
                    None
                } else {
                    Some(function.arguments.into_iter().map(Into::into).collect())
                },
            }),
            ColumnExpression::Binary(binary) => OneOf5::D(models::ColumnarBinaryOperation {
                operator: binary.operator.into(),
                left_operand: Box::new((*binary.left_operand).into()),
                right_operand: Box::new((*binary.right_operand).into()),
            }),
            ColumnExpression::Unary(unary) => OneOf5::E(models::ColumnarUnaryOperation {
                operator: unary.operator.into(),
                operand: Box::new((*unary.operand).into()),
            }),
        };

        Self::from(one_of_value)
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct OrderByExpression {
    pub column_index: usize,
    pub asscending: bool,
}

impl TryFrom<models::OrderByExpression> for OrderByExpression {
    type Error = String;

    fn try_from(value: models::OrderByExpression) -> Result<Self, Self::Error> {
        Ok(Self {
            column_index: value.column_index as usize,
            asscending: value.ascending.unwrap_or(false),
        })
    }
}

impl From<OrderByExpression> for models::OrderByExpression {
    fn from(value: OrderByExpression) -> Self {
        Self {
            column_index: value.column_index as i32,
            ascending: Some(value.asscending),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SelectAllQuery {
    pub table_id: String,
    pub table_name: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct SelectQuery {
    pub table_id: Option<String>,
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
            QueryStatus::Created => Self::Created,
            QueryStatus::Planning => Self::Planning,
            QueryStatus::Running => Self::Running,
            QueryStatus::Completed => Self::Completed,
            QueryStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum QueryDefinition {
    SelectAll(SelectAllQuery),
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
    planner: Arc<Planner>,
    executor: Arc<Executor>,
    metastore: SharedMetastore,
    semaphore: Arc<Semaphore>,
}

impl QueryEngine {
    pub fn new(metastore: SharedMetastore, max_concurrent: usize) -> Self {
        Self {
            planner: Arc::new(Planner::new()),
            executor: Arc::new(Executor::new()),
            metastore,
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub async fn run(self: Arc<Self>, mut receiver: mpsc::Receiver<String>) {
        info!("Query Engine started and waiting for jobs...");

        while let Some(query_id) = receiver.recv().await {
            let engine = Arc::clone(&self);
            let permit = engine.semaphore.clone().acquire_owned().await.unwrap();
            tokio::spawn(async move {
                info!("Processing query {}", query_id);
                let _permit = permit;
                engine.process_query(&query_id).await;
            });
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
