use std::{
    collections::{HashMap, HashSet},
    mem::swap,
};

use log::error;
use serde::{Deserialize, Serialize};

use crate::{metastore, query};

#[derive(Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum FlatExpression {
    Ref(String),
    Literal(query::Literal),
    Function(query::FunctionName, Vec<usize>),
    Binary(usize, query::BinOperator, usize),
    Unary(query::Operator, usize),
}

pub struct SelectAllPlan {
    pub table_id: String,
}

pub struct SelectPlan {
    pub table_id: Option<String>,
    pub column_indexes_map: HashMap<String, usize>,
    pub expressions_map: Vec<FlatExpression>,
    pub column_expressions: Vec<usize>,
    pub filter_expression: Option<usize>,
    pub sorts: Vec<query::OrderByExpression>,
    pub limit: Option<usize>,
}

pub struct CopyFromCsvPlan {
    pub table_id: String,
    pub table_name: String,
    pub file_path: String,
    pub mapping: Option<Vec<String>>,
    pub has_headers: bool,
}

pub enum PhysicalPlan {
    SelectAll(SelectAllPlan),
    Select(SelectPlan),
    CopyFromCsv(CopyFromCsvPlan),
}

#[derive(Clone)]
pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn plan(
        &self,
        query_id: &String,
        metastore: &metastore::SharedMetastore,
    ) -> Option<PhysicalPlan> {
        let (query_def, status_update_result) = {
            let mut guard = metastore.write().await;
            match guard.get_query_internal_mut(query_id) {
                Some(query) => {
                    query.status = query::QueryStatus::Planning;
                    (query.definition.clone(), Ok(()))
                }
                None => (
                    query::QueryDefinition::Select(query::SelectQuery::default()),
                    Err("Query was deleted before planning".to_string()),
                ),
            }
        };

        if let Err(e) = status_update_result {
            self.fail_query(query_id, e, metastore).await;
            return None;
        }

        let result = match query_def {
            query::QueryDefinition::SelectAll(select_all) => {
                self.select_all(select_all, metastore).await
            }
            query::QueryDefinition::Select(select) => self.select(select, metastore).await,
            query::QueryDefinition::Copy(copy) => self.copy_from_csv(copy, metastore).await,
        };

        match result {
            Ok(plan) => Some(plan),
            Err(e) => {
                self.fail_query(query_id, e, metastore).await;
                None
            }
        }
    }

    async fn select_all(
        &self,
        select_all: query::SelectAllQuery,
        _: &metastore::SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        Ok(PhysicalPlan::SelectAll(SelectAllPlan {
            table_id: select_all.table_id.clone(),
        }))
    }

    async fn select(
        &self,
        select: query::SelectQuery,
        metastore: &metastore::SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        let column_names = select
            .column_clauses
            .iter()
            .chain(select.where_clause.iter())
            .flat_map(|expr| expr.get_columns_names())
            .collect::<HashSet<_>>();
        let (column_indexes_map, column_types_map) = if let Some(table_id) = &select.table_id {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(table_id)
                .ok_or("Table was deleted before planning query".to_string())?;

            let mut indexes = HashMap::new();
            let mut types = HashMap::new();
            for (i, col) in table.columns.iter().enumerate() {
                indexes.insert(col.name.clone(), i);
                let type_ = match col.data {
                    lib::ColumnData::STR(_) => query::ExpressionType::String,
                    lib::ColumnData::INT64(_) => query::ExpressionType::I64,
                };
                types.insert(col.name.clone(), type_);
            }

            (indexes, types)
        } else {
            (HashMap::new(), HashMap::new())
        };
        for name in column_names {
            if !column_indexes_map.contains_key(&name) {
                return Err(format!("Column '{}' not found", name));
            }
        }

        for expr in select
            .column_clauses
            .iter()
            .chain(select.where_clause.iter())
        {
            let _ = expr.get_type(&column_types_map)?;
        }
        if let Some(clause) = &select.where_clause {
            let type_ = clause.get_type(&column_types_map)?;
            if type_ != query::ExpressionType::Bool {
                return Err("Filter expression must be of type Boolean".to_string());
            }
        }

        let mut seen_expression = HashMap::new();
        let mut flat_expressions = Vec::new();

        let column_expressions = select
            .column_clauses
            .iter()
            .map(|expr| self.flatten_expression(expr, &mut flat_expressions, &mut seen_expression))
            .collect();

        let filter_expression = select.where_clause.map(|expr| {
            self.flatten_expression(&expr, &mut flat_expressions, &mut seen_expression)
        });

        for clause in &select.order_by_clause {
            if clause.column_index >= select.column_clauses.len() {
                return Err(format!(
                    "Column index '{}' in order by clause out of bounds",
                    clause.column_index
                ));
            }
        }

        Ok(PhysicalPlan::Select(SelectPlan {
            table_id: select.table_id,
            column_indexes_map: column_indexes_map,
            expressions_map: flat_expressions,
            column_expressions: column_expressions,
            filter_expression: filter_expression,
            sorts: select.order_by_clause,
            limit: select.limit.map(|limit| limit as usize),
        }))
    }

    fn flatten_expression(
        &self,
        expr: &query::ColumnExpression,
        flat_expressions: &mut Vec<FlatExpression>,
        seen: &mut HashMap<FlatExpression, usize>,
    ) -> usize {
        let flat_node = match expr {
            query::ColumnExpression::Ref(reference) => {
                FlatExpression::Ref(reference.column_name.clone())
            }
            query::ColumnExpression::Literal(literal) => FlatExpression::Literal(literal.clone()),
            query::ColumnExpression::Function(function) => {
                let arg_ids = function
                    .arguments
                    .iter()
                    .map(|arg| self.flatten_expression(arg, flat_expressions, seen))
                    .collect();
                FlatExpression::Function(function.name.clone(), arg_ids)
            }
            query::ColumnExpression::Binary(binary) => {
                let mut left_id =
                    self.flatten_expression(&binary.left_operand, flat_expressions, seen);
                let mut right_id =
                    self.flatten_expression(&binary.right_operand, flat_expressions, seen);

                // Canonizing binary expressions so they will hash to the same value no matter the
                // order
                if binary.operator.is_commutative() && left_id < right_id {
                    swap(&mut left_id, &mut right_id);
                }

                FlatExpression::Binary(left_id, binary.operator.clone(), right_id)
            }
            query::ColumnExpression::Unary(unary) => {
                let child_id = self.flatten_expression(&unary.operand, flat_expressions, seen);
                FlatExpression::Unary(unary.operator.clone(), child_id)
            }
        };

        if let Some(&id) = seen.get(&flat_node) {
            return id;
        }

        let id = flat_expressions.len();
        flat_expressions.push(flat_node.clone());
        seen.insert(flat_node, id);

        id
    }

    async fn copy_from_csv(
        &self,
        copy: query::CopyQuery,
        metastore: &metastore::SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(&copy.table_id)
                .ok_or("Table was deleted before planning query".to_string())?;
            if let Some(m) = copy.destination_columns.as_ref()
                && table.get_num_cols() != m.len()
            {
                return Err(
                    "Mapping have different number of rows then destination table".to_string(),
                );
            }
        }

        Ok(PhysicalPlan::CopyFromCsv(CopyFromCsvPlan {
            table_id: copy.table_id,
            table_name: copy.table_name,
            file_path: copy.source_filepath,
            mapping: copy.destination_columns,
            has_headers: copy.does_csv_contain_header,
        }))
    }

    async fn fail_query(
        &self,
        query_id: &String,
        error_msg: String,
        metastore: &metastore::SharedMetastore,
    ) {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = query::QueryStatus::Failed;
            q.errors = Some(vec![query::QueryError {
                message: error_msg.clone(),
                context: None,
            }]);
            error!("Query {} failed: {}", query_id, error_msg);
        }
    }
}
