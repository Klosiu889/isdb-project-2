use std::{
    fmt::Debug,
    fs::File,
    io::{Error, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::compress::{
    CompressedStringColumn, CompressorError, IntCompressors, LZ4StringCompressor, NoIntCompressor,
    NoStringCompressor, StringCompressors, VleDeltaIntCompressor,
};

pub mod compress;

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
*   8 bytes for lengths data offset (for STRING only)
*   8 bytes for lengths data length (for STRING only)
*
* [DATA SECTION] columns data at each offset
*
* [FOOTER]
* 4 bytes for magic: b"ENDC"
*/

const MAGIC: &[u8; 4] = b"ISBD";
const FOOTER: &[u8; 4] = b"ENDC";
const VERSION: u8 = 1;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnData {
    INT64(Vec<i64>),
    STR(Vec<String>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Column {
    pub name: String,
    pub data: ColumnData,
}

impl Column {
    pub fn new_int_col(name: String, int_data: Vec<i64>) -> Self {
        Self {
            name,
            data: ColumnData::INT64(int_data),
        }
    }

    pub fn new_str_col(name: String, str_data: Vec<String>) -> Self {
        Self {
            name,
            data: ColumnData::STR(str_data),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Table {
    pub num_rows: u64,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(num_rows: u64, columns: Vec<Column>) -> Self {
        Self { num_rows, columns }
    }

    pub fn iter_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter()
    }

    pub fn iter_columns_mut(&mut self) -> impl Iterator<Item = &mut Column> {
        self.columns.iter_mut()
    }

    pub fn get_num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn get_num_cols(&self) -> usize {
        self.columns.len()
    }
}

#[derive(Debug)]
pub enum SerializerError {
    Compressor(CompressorError),
    IO(Error),
    InvalidFileFormat(String),
}

impl From<CompressorError> for SerializerError {
    fn from(value: CompressorError) -> Self {
        Self::Compressor(value)
    }
}

impl From<Error> for SerializerError {
    fn from(value: Error) -> Self {
        Self::IO(value)
    }
}

#[derive(Debug)]
pub struct Serializer {
    int_compressor: IntCompressors,
    string_compressor: StringCompressors,
}

impl Serializer {
    pub fn new() -> Self {
        Self {
            int_compressor: IntCompressors::VleDelta(VleDeltaIntCompressor),
            string_compressor: StringCompressors::Lz4(LZ4StringCompressor),
        }
    }

    pub fn no_compression() -> Self {
        Self {
            int_compressor: IntCompressors::None(NoIntCompressor),
            string_compressor: StringCompressors::None(NoStringCompressor),
        }
    }

    pub fn with_compressors(
        int_compressor: IntCompressors,
        string_compressor: StringCompressors,
    ) -> Self {
        Self {
            int_compressor,
            string_compressor,
        }
    }

    pub fn serialize(&self, path: &Path, table: &Table) -> Result<(), SerializerError> {
        let mut f = File::create(path)?;

        f.write_all(MAGIC)?;
        f.write_all(&[VERSION])?;
        f.write_all(&(table.columns.len() as u16).to_le_bytes())?;
        f.write_all(&table.num_rows.to_le_bytes())?;

        let mut placeholders_offsets = Vec::<u64>::new();

        for column in &table.columns {
            f.write_all(&(column.name.len() as u8).to_le_bytes())?;
            f.write_all(column.name.as_bytes())?;

            let type_byte = match column.data {
                ColumnData::INT64(_) => 0u8,
                ColumnData::STR(_) => 1u8,
            };
            f.write_all(&[type_byte])?;

            placeholders_offsets.push(f.stream_position()? as u64);

            f.write_all(&0u64.to_le_bytes())?; // placeholder
            f.write_all(&0u64.to_le_bytes())?; // placeholder

            if matches!(column.data, ColumnData::STR(_)) {
                f.write_all(&0u64.to_le_bytes())?; // placeholder
            }
        }

        #[derive(Debug)]
        enum Location {
            INT {
                offset: u64,
                length: u64,
            },
            STR {
                offset: u64,
                length: u64,
                length2: u64,
            },
        }

        let mut offsets_and_lengths = Vec::<Location>::new();
        for column in &table.columns {
            match &column.data {
                ColumnData::INT64(data) => {
                    let compressed_data = self.int_compressor.compress(data.as_slice())?;
                    let offset = f.stream_position()?;
                    let length = compressed_data.len() as u64;
                    offsets_and_lengths.push(Location::INT { offset, length });
                    f.write_all(&compressed_data)?;
                }
                ColumnData::STR(data) => {
                    let compressed_data = self.string_compressor.compress(data.as_slice())?;
                    let compressed_str_data = compressed_data.data;
                    let lengths = compressed_data.lengths;

                    let offset = f.stream_position()?;
                    let length = compressed_str_data.len() as u64;
                    f.write_all(&compressed_str_data)?;

                    let compressed_int_data = self.int_compressor.compress(lengths.as_slice())?;
                    let length2 = compressed_int_data.len() as u64;
                    f.write_all(&compressed_int_data)?;

                    offsets_and_lengths.push(Location::STR {
                        offset,
                        length,
                        length2,
                    });
                }
            }
        }

        f.write_all(FOOTER)?;

        for (i, &offset) in placeholders_offsets.iter().enumerate() {
            f.seek(SeekFrom::Start(offset))?;
            match offsets_and_lengths[i] {
                Location::INT { offset, length } => {
                    f.write_all(&offset.to_le_bytes())?;
                    f.write_all(&length.to_le_bytes())?;
                }
                Location::STR {
                    offset,
                    length,
                    length2,
                } => {
                    f.write_all(&offset.to_le_bytes())?;
                    f.write_all(&length.to_le_bytes())?;
                    f.write_all(&length2.to_le_bytes())?;
                }
            }
        }

        Ok(())
    }

    pub fn deserialize(&self, path: &Path) -> Result<Table, SerializerError> {
        let mut f = File::open(path)?;

        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(SerializerError::InvalidFileFormat(
                "Invalid file indicator".to_string(),
            ));
        }

        let mut v = [0u8; 1];
        f.read_exact(&mut v)?;
        let _version = v[0];

        let mut tmp2 = [0u8; 2];
        f.read_exact(&mut tmp2)?;
        let num_cols = u16::from_le_bytes(tmp2) as usize;

        let mut tmp8 = [0u8; 8];
        f.read_exact(&mut tmp8)?;
        let num_rows = u64::from_le_bytes(tmp8);

        #[derive(Debug)]
        struct ColumnDescription {
            name: String,
            data: ColumnData,
            offset: u64,
            length: u64,
            length2: u64,
        }
        let mut descriptions = Vec::<ColumnDescription>::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let mut nl = [0u8; 1];
            f.read_exact(&mut nl)?;
            let name_length = nl[0];

            let mut name_bytes = vec![0u8; name_length as usize];
            f.read_exact(&mut name_bytes)?;
            let name = String::from_utf8_lossy(&name_bytes).into_owned();

            let mut t = [0u8; 1];
            f.read_exact(&mut t)?;
            let data = match t[0] {
                0u8 => ColumnData::INT64(Vec::new()),
                1u8 => ColumnData::STR(Vec::new()),
                _ => {
                    return Err(SerializerError::InvalidFileFormat(format!(
                        "Invalid column type at column: {}",
                        col_idx
                    )));
                }
            };

            let mut off = [0u8; 8];
            f.read_exact(&mut off)?;
            let offset = u64::from_le_bytes(off);

            let mut len = [0u8; 8];
            f.read_exact(&mut len)?;
            let length = u64::from_le_bytes(len);

            let description = match data {
                ColumnData::INT64(_) => ColumnDescription {
                    name,
                    data,
                    offset,
                    length,
                    length2: 0u64,
                },
                ColumnData::STR(_) => {
                    let mut len2 = [0u8; 8];
                    f.read_exact(&mut len2)?;
                    let length2 = u64::from_le_bytes(len2);
                    ColumnDescription {
                        name,
                        data,
                        offset,
                        length,
                        length2,
                    }
                }
            };

            descriptions.push(description);
        }

        let mut columns = Vec::<Column>::with_capacity(num_cols);
        for desc in descriptions {
            f.seek(SeekFrom::Start(desc.offset))?;
            let mut buf = vec![0u8; desc.length as usize];
            f.read_exact(&mut buf)?;

            match desc.data {
                ColumnData::INT64(_) => {
                    let mut int_data = self.int_compressor.decompress(&buf)?;
                    int_data.resize(num_rows as usize, 0i64);
                    columns.push(Column::new_int_col(desc.name, int_data));
                }
                ColumnData::STR(_) => {
                    let mut buf2 = vec![0u8; desc.length2 as usize];
                    f.read_exact(&mut buf2)?;

                    let lengths_data = self.int_compressor.decompress(&buf2)?;
                    let mut str_data =
                        self.string_compressor.decompress(&CompressedStringColumn {
                            data: buf,
                            lengths: lengths_data,
                        })?;
                    str_data.resize(num_rows as usize, "".to_string());
                    columns.push(Column::new_str_col(desc.name, str_data));
                }
            }
        }

        let mut footer = [0u8; 4];
        f.read_exact(&mut footer)?;
        if &footer != FOOTER {
            return Err(SerializerError::InvalidFileFormat(
                "Invalid file footer".to_string(),
            ));
        }

        Ok(Table { num_rows, columns })
    }
}
