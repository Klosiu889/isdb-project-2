use log::{error, info};
use std::{collections::HashMap, fs::File};

use csv::ReaderBuilder;

use crate::{
    metastore::{SharedMetastore, TableMetaData},
    planner::PhysicalPlan,
    query::{QueryDefinition, QueryError, QueryResult, QueryStatus},
    utils::convert_to_table_file_table,
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
        if let Err(e) = self
            .set_status(query_id, QueryStatus::Running, metastore)
            .await
        {
            error!("Failed to start query {}: {:?}", query_id, e);
            self.fail_query(
                query_id,
                "Query was deleted before execution".to_string(),
                metastore,
            )
            .await;
            return;
        }

        let result = match plan {
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
                let res = self
                    .copy_from_csv(
                        query_id,
                        table_id.clone(),
                        table_name,
                        file_path,
                        mapping,
                        have_headers,
                        metastore,
                    )
                    .await;
                if let Some(access_set) = metastore.write().await.table_accesses.get_mut(&table_id)
                {
                    access_set.remove(query_id);
                }
                res
            }
        };

        match result {
            Ok(query_result) => {
                self.complete_query(query_id, query_result, metastore).await;
            }
            Err(e) => {
                self.fail_query(query_id, e, metastore).await;
            }
        };
    }

    async fn select_all(
        &self,
        _: &String,
        table_id: String,
        _: &SharedMetastore,
    ) -> Result<Option<Vec<QueryResult>>, String> {
        Ok(Some(vec![QueryResult { table_id }]))
    }

    async fn copy_from_csv(
        &self,
        query_id: &String,
        table_id: String,
        table_name: String,
        file_path: String,
        mapping: Option<Vec<String>>,
        has_headers: bool,
        metastore: &SharedMetastore,
    ) -> Result<Option<Vec<QueryResult>>, String> {
        let file = File::open(&file_path)
            .map_err(|e| format!("Failed to open file '{}': {}", file_path, e))?;
        let mut rdr = ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(file);
        let records = rdr
            .records()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("CSV Parse Error: {}", e))?
            .into_iter()
            .map(|r| r.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let (mut shadow_columns, original_column_names) = {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard
                .get_table_internal(&table_id)
                .ok_or_else(|| format!("Table {} not found during execution", table_id))?;

            (
                table
                    .iter_columns()
                    .map(|column| match column.data {
                        lib::ColumnData::STR(_) => (
                            column.name.clone(),
                            lib::ColumnData::STR(Vec::with_capacity(records.len())),
                        ),
                        lib::ColumnData::INT64(_) => (
                            column.name.clone(),
                            lib::ColumnData::INT64(Vec::with_capacity(records.len())),
                        ),
                    })
                    .collect::<HashMap<_, _>>(),
                table
                    .iter_columns()
                    .map(|column| column.name.clone())
                    .collect(),
            )
        };

        let csv_width = records[0].len();
        let num_rows = records.len() as u64;

        let csv_to_table_map: Vec<String> = match mapping {
            Some(map_names) => {
                if map_names.len() != shadow_columns.len() {
                    return Err(format!(
                        "Invalid Mapping: You provided {} columns, but target table has {}. Mapping must describe every column in the target table.",
                        map_names.len(),
                        shadow_columns.len()
                    ));
                }
                if csv_width < map_names.len() {
                    return Err(format!(
                        "CSV too narrow: Mapping requires {} columns, but CSV only has {}.",
                        map_names.len(),
                        csv_width
                    ));
                }

                for name in &map_names {
                    if !shadow_columns.contains_key(name) {
                        return Err(format!(
                            "Mapping references column '{}', which does not exist in table",
                            name
                        ));
                    }
                }
                map_names
            }
            None => {
                if csv_width != shadow_columns.len() {
                    return Err(format!(
                        "Mismatch: Table has {} columns, but CSV has {}. Without mapping, counts must match exactly.",
                        shadow_columns.len(),
                        csv_width
                    ));
                }

                original_column_names
            }
        };

        for (row_idx, record) in records.iter().enumerate() {
            if record.len() != csv_width {
                return Err(format!("Row {} length mismatch", row_idx + 1));
            }

            for (i, col_name) in csv_to_table_map.iter().enumerate() {
                let raw_val = &record[i];

                // We use unwrap() safely because we validated keys exist above
                let column_data = shadow_columns.get_mut(col_name).unwrap();

                match column_data {
                    lib::ColumnData::INT64(vec) => {
                        let val = raw_val.trim().parse::<i64>().map_err(|_| {
                            format!(
                                "Type Error at Row {}, Column '{}': Expected INT64, got '{}'",
                                row_idx + 1,
                                col_name,
                                raw_val
                            )
                        })?;
                        vec.push(val);
                    }
                    lib::ColumnData::STR(vec) => {
                        vec.push(raw_val.clone());
                    }
                }
            }
        }

        {
            let mut metastore_guard = metastore.write().await;
            let active_readers: Vec<String> =
                if let Some(readers) = metastore_guard.table_accesses.get(&table_id) {
                    readers
                        .iter()
                        .filter(|&id| *id != *query_id)
                        .cloned()
                        .collect()
                } else {
                    Vec::new()
                };

            if !active_readers.is_empty() {
                info!(
                    "COPY: Table {} has {} active readers. Creating snapshot.",
                    table_id,
                    active_readers.len()
                );

                let current_table = metastore_guard
                    .get_table_internal(&table_id)
                    .ok_or_else(|| format!("Table {} not found", table_id))?;

                let snapshot_id = uuid::Uuid::new_v4().to_string();
                let snapshot_metadata = TableMetaData {
                    name: table_name,
                    table: current_table.clone(),
                    table_file: convert_to_table_file_table(&snapshot_id),
                };

                metastore_guard
                    .tables
                    .insert(snapshot_id.clone(), snapshot_metadata);

                for reader_query_id in active_readers {
                    if let Some(query) = metastore_guard.queries.get_mut(&reader_query_id) {
                        if let Some(results) = &mut query.result {
                            for res in results {
                                if res.table_id == table_id {
                                    res.table_id = snapshot_id.clone();
                                }
                            }
                        }

                        if let QueryDefinition::Select(select) = &mut query.definition {
                            if select.table_id == table_id {
                                select.table_id = snapshot_id.clone();
                            }
                        }

                        if let QueryDefinition::Copy(copy) = &mut query.definition {
                            if copy.table_id == table_id {
                                copy.table_id = snapshot_id.clone();
                            }
                        }
                    }

                    metastore_guard
                        .table_accesses
                        .entry(snapshot_id.clone())
                        .or_default()
                        .insert(reader_query_id);
                }

                metastore_guard.table_accesses.remove(&table_id);
                metastore_guard
                    .scheduled_for_deletion
                    .insert(snapshot_id.clone());
            }
        }

        {
            let mut metastore_guard = metastore.write().await;
            let table = metastore_guard
                .get_table_internal_mut(&table_id)
                .ok_or_else(|| format!("Table {} deleted during copy", table_id))?;

            for col in &mut table.columns {
                let new_data = shadow_columns
                    .remove(&col.name)
                    .unwrap_or_else(|| match col.data {
                        lib::ColumnData::INT64(_) => {
                            let mut vec = Vec::new();
                            vec.resize(num_rows as usize, 0i64);
                            lib::ColumnData::INT64(vec)
                        }
                        lib::ColumnData::STR(_) => {
                            let mut vec = Vec::new();
                            vec.resize(num_rows as usize, "".to_string());
                            lib::ColumnData::STR(vec)
                        }
                    });

                col.data = new_data;
            }

            table.num_rows = num_rows;
        }

        Ok(None)
    }

    async fn set_status(
        &self,
        query_id: &String,
        status: QueryStatus,
        metastore: &SharedMetastore,
    ) -> Result<(), ()> {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = status;
            Ok(())
        } else {
            Err(())
        }
    }

    async fn complete_query(
        &self,
        query_id: &String,
        result: Option<Vec<QueryResult>>,
        metastore: &SharedMetastore,
    ) {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = QueryStatus::Completed;
            q.result = result;
            info!("Query {} completed successfully", query_id);
        }
    }

    async fn fail_query(&self, query_id: &String, error_msg: String, metastore: &SharedMetastore) {
        let mut metastore_guard = metastore.write().await;
        if let Some(q) = metastore_guard.get_query_internal_mut(query_id) {
            q.status = QueryStatus::Failed;
            q.errors = Some(vec![QueryError {
                message: error_msg.clone(),
                context: None,
            }]);
            error!("Query {} failed: {}", query_id, error_msg);
        }
    }
}
