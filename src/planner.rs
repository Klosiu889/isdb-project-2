use std::collections::{HashMap, HashSet};

use log::error;

use crate::{metastore, query};

pub enum PhysicalPlan {
    Select {
        table_id: String,
        column_indexes_map: HashMap<String, usize>,
        expressions_map: HashMap<query::ColumnExpression, usize>,
        column_expressions: Vec<usize>,
        filter_expression: Option<usize>,
        sorts: Vec<query::OrderByExpression>,
        limit: Option<i32>,
    },
    CopyFromCsv {
        table_id: String,
        table_name: String,
        file_path: String,
        mapping: Option<Vec<String>>,
        have_headers: bool,
    },
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
            query::QueryDefinition::Select(select) => self.select_all(select, metastore).await,
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
        select: query::SelectQuery,
        metastore: &metastore::SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        let column_names = select
            .column_clauses
            .iter()
            .chain(select.where_clause.iter())
            .flat_map(|expr| expr.get_columns_names())
            .collect::<HashSet<_>>();
        let column_indexes_map = {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(&select.table_id)
                .ok_or("Table was deleted before planning query".to_string())?;
            table
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| (col.name.clone(), i))
                .collect::<HashMap<_, _>>()
        };
        for name in column_names {
            if !column_indexes_map.contains_key(&name) {
                return Err(format!("Column '{}' not found", name));
            }
        }

        let mut expression_index_counter = 0usize;
        let mut expression_map = HashMap::new();
        let all_expressions = select
            .column_clauses
            .iter()
            .chain(select.where_clause.iter());
        for expr in all_expressions {
            self.add_expression_to_map(&mut expression_map, &mut expression_index_counter, expr)
        }

        let column_expressions = select
            .column_clauses
            .iter()
            .map(|expr| {
                expression_map
                    .get(expr)
                    .cloned()
                    .ok_or("Column expression was not mapped correctly")
            })
            .collect::<Result<Vec<_>, _>>()?;

        let filter_expression = select
            .where_clause
            .map(|expr| {
                expression_map
                    .get(&expr)
                    .copied()
                    .ok_or("Filter expression was not mapped correctly")
            })
            .transpose()?;

        for clause in &select.order_by_clause {
            if clause.column_index >= select.column_clauses.len() {
                return Err(format!(
                    "Column index '{}' in order by clause out of bounds",
                    clause.column_index
                ));
            }
        }
        Ok(PhysicalPlan::Select {
            table_id: select.table_id,
            column_indexes_map: column_indexes_map,
            expressions_map: expression_map,
            column_expressions: column_expressions,
            filter_expression: filter_expression,
            sorts: select.order_by_clause,
            limit: select.limit,
        })
    }

    fn add_expression_to_map(
        &self,
        map: &mut HashMap<query::ColumnExpression, usize>,
        counter: &mut usize,
        expr: &query::ColumnExpression,
    ) {
        match expr {
            query::ColumnExpression::Unary(unary) => {
                self.add_expression_to_map(map, counter, &unary.operand);
            }
            query::ColumnExpression::Binary(binary) => {
                self.add_expression_to_map(map, counter, &binary.left_operand);
                self.add_expression_to_map(map, counter, &binary.right_operand);
            }
            query::ColumnExpression::Function(function) => {
                for arg in &function.arguments {
                    self.add_expression_to_map(map, counter, arg);
                }
            }
            _ => {}
        }

        if !map.contains_key(&expr) {
            map.insert(expr.clone(), *counter);
            *counter += 1;
        }
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

        Ok(PhysicalPlan::CopyFromCsv {
            table_id: copy.table_id,
            table_name: copy.table_name,
            file_path: copy.source_filepath,
            mapping: copy.destination_columns,
            have_headers: copy.does_csv_contain_header,
        })
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
