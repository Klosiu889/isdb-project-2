use std::{fs::remove_file, path::Path};

use proj2::{
    Serializer,
    compress::{
        IntCompressors, LZ4StringCompressor, NoIntCompressor, NoStringCompressor,
        StringCompressors, VleDeltaIntCompressor,
    },
};

use crate::utils::{generate_random_table, get_file_size, get_table_from_csv};

mod utils;

#[test]
fn test_compression_correctness() {
    let table = generate_random_table();
    let serializer = Serializer::new();

    let path = Path::new("tests/test_data.isdb");
    serializer.serialize(path, &table).unwrap();
    let deserialized_table = serializer.deserialize(path).unwrap();

    let _ = remove_file(path);

    assert_eq!(table, deserialized_table);
}

#[test]
fn test_lz4_compression_size_csv() {
    let path = Path::new("tests/data/titanic.csv");
    let table = get_table_from_csv(path).expect("Error reading file");
    let serializer_compression = Serializer::with_compressors(
        IntCompressors::None(NoIntCompressor),
        StringCompressors::Lz4(LZ4StringCompressor),
    );
    let serializer_no_compression = Serializer::no_compression();

    let path_compression = Path::new("tests/test_compressed_data.isdb");
    let path_no_compression = Path::new("tests/test_uncompressed_data.isdb");

    serializer_compression
        .serialize(path_compression, &table)
        .unwrap();
    serializer_no_compression
        .serialize(path_no_compression, &table)
        .unwrap();

    let size_compressed = get_file_size(path_compression);
    let size_no_compressed = get_file_size(path_no_compression);

    let _ = remove_file(path_compression);
    let _ = remove_file(path_no_compression);

    assert!(size_compressed < size_no_compressed);
}

#[test]
fn test_vle_delta_compression_size_csv() {
    let path = Path::new("tests/data/titanic.csv");
    let table = get_table_from_csv(path).expect("Error reading file");
    let serializer_compression = Serializer::with_compressors(
        IntCompressors::VleDelta(VleDeltaIntCompressor),
        StringCompressors::None(NoStringCompressor),
    );
    let serializer_no_compression = Serializer::no_compression();

    let path_compression = Path::new("tests/test_compressed_data.isdb");
    let path_no_compression = Path::new("tests/test_uncompressed_data.isdb");

    serializer_compression
        .serialize(path_compression, &table)
        .unwrap();
    serializer_no_compression
        .serialize(path_no_compression, &table)
        .unwrap();

    let size_compressed = get_file_size(path_compression);
    let size_no_compressed = get_file_size(path_no_compression);

    let _ = remove_file(path_compression);
    let _ = remove_file(path_no_compression);

    assert!(size_compressed < size_no_compressed);
}

#[test]
fn test_lz4_and_vle_delta_compression_size_csv() {
    let path = Path::new("tests/data/titanic.csv");
    let table = get_table_from_csv(path).expect("Error reading file");
    let serializer_compression = Serializer::new();
    let serializer_no_compression = Serializer::no_compression();

    let path_compression = Path::new("tests/test_compressed_data.isdb");
    let path_no_compression = Path::new("tests/test_uncompressed_data.isdb");

    serializer_compression
        .serialize(path_compression, &table)
        .unwrap();
    serializer_no_compression
        .serialize(path_no_compression, &table)
        .unwrap();

    let size_compressed = get_file_size(path_compression);
    let size_no_compressed = get_file_size(path_no_compression);

    let _ = remove_file(path_compression);
    let _ = remove_file(path_no_compression);

    assert!(size_compressed < size_no_compressed);
}
