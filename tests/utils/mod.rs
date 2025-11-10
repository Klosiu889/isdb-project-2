use std::{
    fs::{self, File},
    io::Result,
    path::Path,
};

use csv::ReaderBuilder;
use proj2::{Column, Table};
use rand::{Rng, rngs::StdRng};

const STRING_SIZE_RANGE: std::ops::Range<usize> = 3..10;
const INT64_SIZE_RANGE: std::ops::Range<i64> = -100..100;
const CHARS_RANGE: std::ops::RangeInclusive<char> = 'a'..='z';
const TABLE_ROWS_RANGE: std::ops::Range<usize> = 5..10;
const TABLE_COLS_RANGE: std::ops::Range<usize> = 5..10;

pub const TESTS_DIRECTORY: &'static str = "tests";

pub fn generate_random_int_vec(rng: &mut StdRng, size: usize) -> Vec<i64> {
    (0..size)
        .map(|_| rng.random_range(INT64_SIZE_RANGE))
        .collect()
}

pub fn generate_random_string_vec(rng: &mut StdRng, size: usize) -> Vec<String> {
    (0..size)
        .map(|_| {
            let size = rng.random_range(STRING_SIZE_RANGE);
            generate_random_string(rng, size)
        })
        .collect()
}

pub fn generate_random_string(rng: &mut StdRng, size: usize) -> String {
    (0..size).map(|_| rng.random_range(CHARS_RANGE)).collect()
}

pub fn generate_random_table(rng: &mut StdRng) -> Table {
    let num_rows = rng.random_range(TABLE_ROWS_RANGE) as usize;
    let num_cols = rng.random_range(TABLE_COLS_RANGE) as usize;

    Table::new(
        num_rows as u64,
        (0..num_cols)
            .map(|_| {
                let coin_flip = rng.random_bool(0.5);
                let name_size = rng.random_range(3..10) as usize;
                if coin_flip {
                    Column::new_int_col(
                        generate_random_string(rng, name_size),
                        generate_random_int_vec(rng, num_rows),
                    )
                } else {
                    Column::new_str_col(
                        generate_random_string(rng, name_size),
                        generate_random_string_vec(rng, num_rows),
                    )
                }
            })
            .collect(),
    )
}

pub fn get_file_size(path: &Path) -> u64 {
    fs::metadata(path).expect("Error reading file size").len()
}

pub fn get_table_from_csv(path: &Path) -> Result<Table> {
    let file = File::open(path)?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    let headers = rdr.headers()?.clone();
    let records = rdr
        .records()
        .map(|r| {
            r.unwrap()
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        })
        .collect::<Vec<Vec<String>>>();

    let num_cols = headers.len();
    let num_rows = records.len() as u64;

    let mut columns = Vec::<Column>::new();
    for col_idx in 0..num_cols {
        let name = headers[col_idx].to_string();
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

    Ok(Table::new(num_rows, columns))
}
