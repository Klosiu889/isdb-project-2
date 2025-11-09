use std::path::Path;

use proj2::{Column, Serializer, Table};

/*
* [HEADER]
* 4 bytes for magic: b"ISBD"
* 1 byte for version number
* 2 bytes for number of columns u16
* 8 bytes for number of rows u64
* For every colum its data:
*   1 byte for name length
*   name bytes
*   1 byte for type (0 - INT64, 1 - STRING)
*   8 bytes for data offset
*   8 bytes for data length
*   8 bytes for lengths data length (for STRING only)
*
* [DATA SECTION] columns data at each offset
*
* [FOOTER]
* 4 bytes for magic: b"ENDC"
*/

fn main() {
    let table = Table::new(
        5,
        vec![
            Column::new_int_col("Temperature".to_string(), vec![6, 10, -1, 20, 6]),
            Column::new_int_col("Pressure".to_string(), vec![1000, 1001, 998, 999, 1012]),
            Column::new_str_col(
                "City name".to_string(),
                vec![
                    "Warszawa".to_string(),
                    "Radom".to_string(),
                    "Lublin".to_string(),
                    "Wrocław".to_string(),
                    "Kraków".to_string(),
                ],
            ),
            Column::new_int_col("Wind speed".to_string(), vec![10, 12, 8, 15, 13]),
        ],
    );
    let serializer = Serializer::new();
    let serializer_no_compression = Serializer::no_compression();

    let path = Path::new("data.isdb");
    serializer.serialize(path, &table).unwrap();
    let deserialized_table = serializer.deserialize(path).unwrap();

    println!("{:?}", deserialized_table);
    assert_eq!(table, deserialized_table);

    let path_no_comporession = Path::new("data_no_compression.isdb");
    serializer_no_compression
        .serialize(path_no_comporession, &table)
        .unwrap();
    let deserialized_table_no_compresion = serializer_no_compression
        .deserialize(path_no_comporession)
        .unwrap();

    println!("{:?}", deserialized_table_no_compresion);
    assert_eq!(table, deserialized_table_no_compresion);
}
