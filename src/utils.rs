use crate::consts::{FILE_EXTENSION, TABLES_DIR};

pub fn convert_to_table_file_table(table_id: &String) -> String {
    format!("{}/{}.{}", TABLES_DIR, table_id, FILE_EXTENSION)
}
