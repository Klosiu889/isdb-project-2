use std::{
    cmp::min,
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
};
use uuid::Uuid;

use lib::{Column, ColumnData, Serializer as TableSerializer, Table};
use openapi_client::models::{
    Column as OpenapiColumn, CopyQuery, LogicalColumnType, Query as OpenapiQuery,
    QueryQueryDefinition, QueryResultInner, QueryResultInnerColumnsInner, SelectQuery,
    ShallowQuery, TableSchema,
};
use serde::{Deserialize, Serialize};
use swagger::OneOf2;
use tokio::sync::RwLock;

use crate::query::{self, Query, QueryDefinition, QueryError, QueryStatus};

const TABLES_DIR: &str = "tables";
const FILE_EXTENSION: &str = "isdb";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TableMetaData {
    name: String,
    #[serde(skip)]
    table: Table,
    table_file: String,
}

pub struct ShallowTable {
    pub(crate) id: String,
    pub(crate) name: String,
}

#[derive(Debug)]
pub struct Error {
    pub(crate) message: String,
    pub(crate) context: Option<String>,
}

impl Error {
    pub fn new(message: &str) -> Self {
        Self {
            message: message.to_string(),
            context: None,
        }
    }

    pub fn with_context(message: &str, context: String) -> Self {
        Self {
            message: message.to_string(),
            context: Some(context),
        }
    }
}

#[derive(Debug)]
pub enum MetastoreError {
    TableAccessError(Error),
    TableCreationError(Vec<Error>),
    TableDeletionError(Error),
    QueryAccessError(Error),
    QueryCreationError(Vec<Error>),
    QueryResultAccessError(Error),
    QueryErrorAccessError(Error),
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Metastore {
    scheduled_for_deletion: HashSet<String>,
    tables: HashMap<String, TableMetaData>,
    tables_name_id: HashMap<String, String>,
    table_accesses: HashMap<String, HashSet<String>>,
    queries: HashMap<String, Query>,
    results: HashMap<String, String>,
}

impl Metastore {
    pub fn new() -> Self {
        Self {
            scheduled_for_deletion: HashSet::new(),
            tables: HashMap::new(),
            tables_name_id: HashMap::new(),
            table_accesses: HashMap::new(),
            queries: HashMap::new(),
            results: HashMap::new(),
        }
    }

    pub fn get_shallow_tables(&self) -> Vec<ShallowTable> {
        self.tables
            .iter()
            .filter(|(id, _)| !self.scheduled_for_deletion.contains(*id))
            .map(|(id, metadata)| ShallowTable {
                id: id.clone(),
                name: metadata.name.clone(),
            })
            .collect()
    }

    pub fn get_table(&self, id: String) -> Result<TableSchema, MetastoreError> {
        if self.scheduled_for_deletion.contains(&id) {
            return Err(MetastoreError::TableAccessError(Error::new(
                "Couldn't find a table of given ID",
            )));
        }

        let table = self.tables.get(&id).map(|metadata| TableSchema {
            name: metadata.name.clone(),
            columns: metadata
                .table
                .iter_columns()
                .map(|column| OpenapiColumn {
                    name: column.name.clone(),
                    r#type: match column.data {
                        ColumnData::INT64(_) => LogicalColumnType::Int64,
                        ColumnData::STR(_) => LogicalColumnType::Varchar,
                    },
                })
                .collect(),
        });

        match table {
            Some(existing_table) => Ok(existing_table),
            None => Err(MetastoreError::TableAccessError(Error::new(
                "Couldn't find a table of given ID",
            ))),
        }
    }

    pub fn delete_table(&mut self, id: String) -> Result<(), MetastoreError> {
        if self.scheduled_for_deletion.contains(&id) {
            return Err(MetastoreError::TableDeletionError(Error::new(
                "Couldn't find a table of given ID",
            )));
        }

        if self.tables.contains_key(&id) {
            self.scheduled_for_deletion.insert(id);
            return Ok(());
        }

        Err(MetastoreError::TableDeletionError(Error::new(
            "Couldn't find a table of given ID",
        )))
    }

    pub fn create_table(&mut self, table_schema: TableSchema) -> Result<String, MetastoreError> {
        let mut errors = vec![];
        let existing_table_id = self.tables_name_id.get(&table_schema.name);

        if let Some(id) = existing_table_id
            && !self.scheduled_for_deletion.contains(id)
        {
            errors.push(Error::new("Table with given name already exists"));
        }

        let mut columns_names_counts = HashMap::new();
        for column in table_schema.columns.iter() {
            let counter = columns_names_counts.entry(column.name.clone()).or_insert(0);
            *counter += 1;
            if *counter > 1 {
                errors.push(Error::with_context(
                    "Two columns have identical names",
                    column.name.to_string(),
                ));
            }
        }

        if !errors.is_empty() {
            return Err(MetastoreError::TableCreationError(errors));
        }

        let columns = table_schema
            .columns
            .iter()
            .map(|column| match column.r#type {
                LogicalColumnType::Int64 => Column::new_int_col(column.name.clone(), vec![]),
                LogicalColumnType::Varchar => Column::new_str_col(column.name.clone(), vec![]),
            })
            .collect();
        let table = Table::new(0, columns);
        let table_id = Uuid::new_v4().to_string();
        let metadata = TableMetaData {
            name: table_schema.name.clone(),
            table,
            table_file: format!("{}/{}.{}", TABLES_DIR, table_id, FILE_EXTENSION),
        };
        self.tables.insert(table_id.clone(), metadata);
        self.tables_name_id
            .insert(table_schema.name, table_id.clone());

        Ok(table_id)
    }

    pub fn get_queries(&self) -> Vec<ShallowQuery> {
        self.queries
            .iter()
            .map(|(id, query)| ShallowQuery {
                query_id: id.clone(),
                status: query.status.clone().into(),
            })
            .collect()
    }

    pub fn get_query(&self, id: String) -> Result<OpenapiQuery, MetastoreError> {
        let query = self.queries.get(&id).map(|query| OpenapiQuery {
            query_id: id.clone(),
            status: query.status.clone().into(),
            is_result_available: Some(self.results.contains_key(&id)),
            query_definition: match &query.definition {
                QueryDefinition::Select(val) => {
                    Some(QueryQueryDefinition::from(OneOf2::A(SelectQuery {
                        table_name: Some(val.table_name.clone()),
                    })))
                }
                QueryDefinition::Copy(val) => {
                    Some(QueryQueryDefinition::from(OneOf2::B(CopyQuery {
                        source_filepath: val.source_filepath.clone(),
                        destination_table_name: val.table_name.clone(),
                        destination_columns: val.destination_columns.clone(),
                        does_csv_contain_header: Some(val.does_csv_contain_header),
                    })))
                }
            },
        });

        match query {
            Some(existing_query) => Ok(existing_query),
            None => Err(MetastoreError::QueryAccessError(Error::new(
                "Couldn't find a query of given ID",
            ))),
        }
    }

    pub fn create_select_query(&mut self, query: &SelectQuery) -> Result<String, MetastoreError> {
        let table_name = query
            .table_name
            .as_ref()
            .ok_or(MetastoreError::QueryCreationError(vec![Error::new(
                "Missing table name",
            )]))?;

        let table_id =
            self.tables_name_id
                .get(table_name)
                .ok_or(MetastoreError::QueryCreationError(vec![
                    Error::with_context("There is no table with that name", table_name.to_string()),
                ]))?;

        let query_id = Uuid::new_v4().to_string();
        self.table_accesses
            .entry(table_id.clone())
            .or_insert_with(HashSet::new)
            .insert(query_id.clone());
        self.queries.insert(
            query_id.clone(),
            Query::new(
                QueryStatus::Created,
                QueryDefinition::Select(query::SelectQuery {
                    table_id: table_id.clone(),
                    table_name: table_name.clone(),
                }),
            ),
        );

        Ok(query_id)
    }

    pub fn create_copy_query(&mut self, query: &CopyQuery) -> Result<String, MetastoreError> {
        let path = Path::new(&query.source_filepath);
        if !path.exists() {
            return Err(MetastoreError::QueryCreationError(vec![
                Error::with_context("File does not exist", query.source_filepath.clone()),
            ]));
        }

        let table_id = self
            .tables_name_id
            .get(&query.destination_table_name)
            .ok_or(MetastoreError::QueryCreationError(vec![
                Error::with_context(
                    "There is no table with that name",
                    query.destination_table_name.clone(),
                ),
            ]))?;

        let query_id = Uuid::new_v4().to_string();
        self.table_accesses
            .entry(table_id.clone())
            .or_insert_with(HashSet::new)
            .insert(query_id.clone());
        self.queries.insert(
            query_id.clone(),
            Query::new(
                QueryStatus::Created,
                QueryDefinition::Copy(query::CopyQuery {
                    table_id: table_id.clone(),
                    table_name: query.destination_table_name.clone(),
                    source_filepath: query.source_filepath.clone(),
                    destination_columns: query.destination_columns.clone(),
                    does_csv_contain_header: query.does_csv_contain_header.unwrap_or(false),
                }),
            ),
        );

        Ok(query_id)
    }

    pub fn get_query_result(
        &self,
        query_id: String,
        row_limit: Option<i32>,
    ) -> Result<Vec<QueryResultInner>, MetastoreError> {
        let query = self
            .queries
            .get(&query_id)
            .ok_or(MetastoreError::QueryAccessError(Error::new(
                "Couldn't find a query of given ID",
            )))?;

        let result = query
            .result
            .as_ref()
            .ok_or(MetastoreError::QueryResultAccessError(Error::new(
                "Result for this query is not available",
            )))?;

        let table_ids = result
            .iter()
            .map(|res| res.table_id.clone())
            .collect::<Vec<_>>();

        let r = table_ids
            .iter()
            .map(|id| {
                let table = &self.tables.get(id).unwrap().table;
                let row_count = match row_limit {
                    Some(limit) => min(table.get_num_rows() as i32, limit),
                    None => table.get_num_rows() as i32,
                };
                QueryResultInner {
                    row_count: Some(row_count),
                    columns: Some(
                        table
                            .iter_columns()
                            .map(|column| match &column.data {
                                ColumnData::INT64(raw) => {
                                    QueryResultInnerColumnsInner::from(OneOf2::A(
                                        raw.iter().take(row_count as usize).cloned().collect(),
                                    ))
                                }
                                ColumnData::STR(raw) => {
                                    QueryResultInnerColumnsInner::from(OneOf2::B(
                                        raw.iter().take(row_count as usize).cloned().collect(),
                                    ))
                                }
                            })
                            .collect(),
                    ),
                }
            })
            .collect();

        Ok(r)
    }

    pub fn get_query_result_flush(
        &mut self,
        query_id: String,
        row_limit: Option<i32>,
    ) -> Result<Vec<QueryResultInner>, MetastoreError> {
        let query = self
            .queries
            .get(&query_id)
            .ok_or(MetastoreError::QueryAccessError(Error::new(
                "Couldn't find a query of given ID",
            )))?;

        let result = query
            .result
            .as_ref()
            .ok_or(MetastoreError::QueryResultAccessError(Error::new(
                "Result for this query is not available",
            )))?;

        let table_ids = result
            .iter()
            .map(|res| res.table_id.clone())
            .collect::<Vec<_>>();

        let r = table_ids
            .iter()
            .map(|id| {
                let table = &self.tables.get(id).unwrap().table;
                let row_count = match row_limit {
                    Some(limit) => min(table.get_num_rows() as i32, limit),
                    None => table.get_num_rows() as i32,
                };
                QueryResultInner {
                    row_count: Some(row_count),
                    columns: Some(
                        table
                            .iter_columns()
                            .map(|column| match &column.data {
                                ColumnData::INT64(raw) => {
                                    QueryResultInnerColumnsInner::from(OneOf2::A(
                                        raw.iter().take(row_count as usize).cloned().collect(),
                                    ))
                                }
                                ColumnData::STR(raw) => {
                                    QueryResultInnerColumnsInner::from(OneOf2::B(
                                        raw.iter().take(row_count as usize).cloned().collect(),
                                    ))
                                }
                            })
                            .collect(),
                    ),
                }
            })
            .collect();

        table_ids.iter().for_each(|id| {
            if let Some(set) = self.table_accesses.get_mut(id) {
                set.remove(&query_id);
            }
        });

        Ok(r)
    }

    pub fn get_query_error(&self, id: String) -> Result<Vec<QueryError>, MetastoreError> {
        let query = self.queries.get(&id);

        match query {
            Some(existing_query) => match &existing_query.errors {
                Some(errors) => Ok(errors.clone()),
                None => Err(MetastoreError::QueryErrorAccessError(Error::new(
                    "Error for this query is not available",
                ))),
            },
            None => Err(MetastoreError::QueryAccessError(Error::new(
                "Couldn't find a query of given ID",
            ))),
        }
    }

    pub fn get_query_internal_mut(&mut self, id: &String) -> Option<&mut Query> {
        self.queries.get_mut(id)
    }

    pub fn get_table_internal_mut(&mut self, table_id: &String) -> Option<&mut Table> {
        self.tables
            .get_mut(table_id)
            .map(|metadata| &mut metadata.table)
    }
}

pub type SharedMetastore = Arc<RwLock<Metastore>>;

pub async fn load_metastore(file_path: &str, serializer: &TableSerializer) -> SharedMetastore {
    let mut metastore = if let Ok(data) = fs::read_to_string(file_path) {
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        Metastore::new()
    };

    for metadata in metastore.tables.values_mut() {
        let path = Path::new(&metadata.table_file);
        let table = serializer.deserialize(path).unwrap();
        metadata.table = table;
    }

    Arc::new(RwLock::new(metastore))
}

pub async fn save_metastore(
    metastore: SharedMetastore,
    file_path: &str,
    serializer: &TableSerializer,
) {
    let metastore_guard = metastore.read().await;

    for metadata in metastore_guard.tables.values() {
        let path = Path::new(&metadata.table_file);
        serializer.serialize(path, &metadata.table).unwrap();
    }

    let json = serde_json::to_string_pretty(&*metastore_guard).unwrap();
    fs::write(file_path, json).expect("Failed to write metastore file");
}
