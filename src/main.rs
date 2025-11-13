use std::{collections::HashMap, path::Path};

use proj2::{ColumnData, Serializer, SerializerError};

fn main() -> Result<(), SerializerError> {
    let path = Path::new("data/bmw_sales_data_2010_2014.isdb");

    let serializer = Serializer::new();
    let data = serializer.deserialize(path)?;

    let mut averages = HashMap::new();
    let mut ascii_counts = HashMap::new();

    for column in data.iter_columns() {
        match &column.data {
            ColumnData::INT64(col_data) => {
                let sum: i64 = col_data.iter().sum();
                let count = col_data.len();
                averages.insert(column.name.clone(), sum as f64 / count as f64);
            }
            ColumnData::STR(col_data) => {
                let mut characters_counts = [0u64; 256];
                println!("{:?}", col_data);
                col_data.iter().for_each(|value| {
                    value
                        .as_bytes()
                        .iter()
                        .for_each(|b| characters_counts[*b as usize] += 1u64)
                });

                ascii_counts.insert(column.name.clone(), characters_counts);
            }
        }
    }

    println!("Averages:");
    averages
        .iter()
        .for_each(|(col_name, average)| println!("{} -> {}", col_name, average));
    println!("");

    println!("Ascii characters counts:");
    ascii_counts.iter().for_each(|(col_name, counts)| {
        println!(
            "{} -> {}",
            col_name,
            counts
                .iter()
                .enumerate()
                .map(|(i, c)| format!("{}: {}", i, c))
                .collect::<Vec<_>>()
                .join(", ")
        )
    });

    Ok(())
}
