use std::fs::File;

use csv::ReaderBuilder;
use lib::Column;

use crate::{
    metastore::SharedMetastore,
    planner::PhysicalPlan,
    query::{QueryResult, QueryStatus},
};

#[derive(Clone)]
pub struct Executor {}

impl Executor {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn execute(
        &self,
        query_id: &String,
        plan: PhysicalPlan,
        metastore: &SharedMetastore,
    ) {
        match plan {
            PhysicalPlan::SelectAll { table_id } => {
                self.select_all(query_id, table_id, metastore).await
            }
            PhysicalPlan::CopyFromCsv {
                table_id,
                table_name,
                file_path,
                mapping,
                have_headers,
            } => {
                self.copy_from_csv(
                    query_id,
                    table_id,
                    table_name,
                    file_path,
                    mapping,
                    have_headers,
                    metastore,
                )
                .await
            }
        }
    }

    async fn select_all(&self, query_id: &String, table_id: String, metastore: &SharedMetastore) {
        {
            let mut metastore_guard = metastore.write().await;
            let query = metastore_guard.get_query_internal_mut(query_id).unwrap();
            query.status = QueryStatus::Running;
        }

        {
            let mut metastore_guard = metastore.write().await;
            let query = metastore_guard.get_query_internal_mut(query_id).unwrap();
            query.status = QueryStatus::Completed;
            query.result = Some(vec![QueryResult { table_id }])
        }
    }

    async fn copy_from_csv(
        &self,
        query_id: &String,
        table_id: String,
        _: String,
        file_path: String,
        mapping: Option<Vec<String>>,
        has_headers: bool,
        metastore: &SharedMetastore,
    ) {
        let file = File::open(file_path).unwrap();
        let mut metastore_guard = metastore.write().await;
        let query = metastore_guard.get_query_internal_mut(query_id).unwrap();
        query.status = QueryStatus::Running;
        let table = metastore_guard.get_table_internal_mut(&table_id).unwrap();
        let mut rdr = ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(file);

        let records = rdr
            .records()
            .map(|r| {
                r.unwrap()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<Vec<String>>>();

        let num_cols = table.columns.len();
        let num_rows = records.len() as u64;
        table.num_rows = num_rows;

        let names = match mapping {
            Some(val) => val.clone(),
            None => table.iter_columns().map(|col| col.name.clone()).collect(),
        };
        let mut columns = Vec::<Column>::new();
        for col_idx in 0..num_cols {
            let name = names[col_idx].clone();
            let mut as_int = Vec::new();
            let mut as_str = Vec::new();
            let mut all_int = true;

            for row in &records {
                let value = &row[col_idx];
                if value.trim().is_empty() {
                    as_int.push(0);
                    as_str.push(value.clone());
                } else if let Ok(v) = value.parse::<i64>() {
                    as_int.push(v);
                    as_str.push(value.clone());
                } else {
                    all_int = false;
                    as_str.push(value.clone());
                }
            }

            if all_int {
                columns.push(Column::new_int_col(name, as_int));
            } else {
                columns.push(Column::new_str_col(name, as_str));
            }
        }

        table.columns = columns;
    }
}
