use log::error;

use crate::{
    metastore::SharedMetastore,
    query::{QueryDefinition, QueryError, QueryStatus, SelectQuery},
};

pub enum PhysicalPlan {
    SelectAll {
        table_id: String,
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
        metastore: &SharedMetastore,
    ) -> Option<PhysicalPlan> {
        let (query_def, status_update_result) = {
            let mut guard = metastore.write().await;
            match guard.get_query_internal_mut(query_id) {
                Some(query) => {
                    query.status = QueryStatus::Planning;
                    (query.definition.clone(), Ok(()))
                }
                None => (
                    QueryDefinition::Select(SelectQuery::default()),
                    Err("Query was deleted before planning".to_string()),
                ),
            }
        };

        if let Err(e) = status_update_result {
            self.fail_query(query_id, e, metastore).await;
            return None;
        }

        let result = match query_def {
            QueryDefinition::Select(select) => self.select_all(select.table_id, metastore).await,
            QueryDefinition::Copy(copy) => {
                self.copy_from_csv(
                    copy.table_id,
                    copy.table_name,
                    copy.source_filepath,
                    copy.destination_columns,
                    copy.does_csv_contain_header,
                    metastore,
                )
                .await
            }
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
        table_id: String,
        _: &SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        Ok(PhysicalPlan::SelectAll { table_id })
    }

    async fn copy_from_csv(
        &self,
        table_id: String,
        table_name: String,
        file_path: String,
        mapping: Option<Vec<String>>,
        have_headers: bool,
        metastore: &SharedMetastore,
    ) -> Result<PhysicalPlan, String> {
        {
            let metastore_guard = metastore.read().await;
            let table = metastore_guard.get_table_internal(&table_id);
            match table {
                Some(t) => {
                    if let Some(m) = mapping.as_ref()
                        && t.get_num_cols() != m.len()
                    {
                        return Err(
                            "Mapping have different number of rows then destination table"
                                .to_string(),
                        );
                    }
                }
                None => return Err("Table was deleted before planning query".to_string()),
            }
        }

        Ok(PhysicalPlan::CopyFromCsv {
            table_id,
            table_name,
            file_path,
            mapping,
            have_headers,
        })
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
