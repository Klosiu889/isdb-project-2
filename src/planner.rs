use crate::{
    metastore::SharedMetastore,
    query::{QueryDefinition, QueryStatus},
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

    pub async fn plan(&self, query_id: &String, metastore: &SharedMetastore) -> PhysicalPlan {
        let mut metastore_guard = metastore.write().await;
        let query = metastore_guard.get_query_internal_mut(query_id).unwrap();
        query.status = QueryStatus::Planning;

        match &query.definition {
            QueryDefinition::Select(select) => PhysicalPlan::SelectAll {
                table_id: select.table_id.clone(),
            },
            QueryDefinition::Copy(copy) => PhysicalPlan::CopyFromCsv {
                table_id: copy.table_id.clone(),
                table_name: copy.table_name.clone(),
                file_path: copy.source_filepath.clone(),
                mapping: copy.destination_columns.clone(),
                have_headers: copy.does_csv_contain_header,
            },
        }
    }
}
