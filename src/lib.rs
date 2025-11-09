use std::{
    fmt::Debug,
    fs::File,
    io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write},
    path::Path,
};

use crate::compress::{
    CompressedStringColumn, IntCompressor, IntCompressors, LZ4StringCompressor, NoIntCompressor,
    NoStringCompressor, StringCompressors,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    INT64,
    STR,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Column {
    name: String,
    col_type: ColumnType,
    int_data: Option<Vec<i64>>,
    str_data: Option<Vec<String>>,
}

impl Column {
    pub fn new_int_col(name: String, int_data: Vec<i64>) -> Self {
        Self {
            name,
            col_type: ColumnType::INT64,
            int_data: Some(int_data),
            str_data: None,
        }
    }

    pub fn new_str_col(name: String, str_data: Vec<String>) -> Self {
        Self {
            name,
            col_type: ColumnType::STR,
            int_data: None,
            str_data: Some(str_data),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Table {
    num_rows: u64,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(num_rows: u64, columns: Vec<Column>) -> Self {
        Self { num_rows, columns }
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
            int_compressor: IntCompressors::VLE(IntCompressor),
            string_compressor: StringCompressors::LZ4(LZ4StringCompressor),
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
impl StringCompressorEnum {
    pub fn compress(&self, data: &[String]) -> CompressedStringColumn {
        match self {
            StringCompressorEnum::Lz4(c) => c.compress(data),
            StringCompressorEnum::None(c) => c.compress(data),
        }
    }

    pub fn decompress(&self, data: &CompressedStringColumn) -> Vec<String> {
        match self {
            StringCompressorEnum::Lz4(c) => c.decompress(data),
            StringCompressorEnum::None(c) => c.decompress(data),
        }
    }
}
    }

    pub fn serialize(&self, path: &Path, table: &Table) -> Result<()> {
        let mut f = File::create(path)?;

        f.write_all(MAGIC)?;
        f.write_all(&[VERSION])?;
        f.write_all(&(table.columns.len() as u16).to_le_bytes())?;
        f.write_all(&table.num_rows.to_le_bytes())?;

        let mut placeholders_offsets = Vec::<u64>::new();

        for column in &table.columns {
            f.write_all(&(column.name.len() as u8).to_le_bytes())?;
            f.write_all(column.name.as_bytes())?;

            let type_byte = match column.col_type {
                ColumnType::INT64 => 0u8,
                ColumnType::STR => 1u8,
            };
            f.write_all(&[type_byte])?;

            placeholders_offsets.push(f.stream_position()? as u64);

            f.write_all(&0u64.to_le_bytes())?; // placeholder
            f.write_all(&0u64.to_le_bytes())?; // placeholder

            if column.col_type == ColumnType::STR {
                f.write_all(&0u64.to_le_bytes())?; // placeholder
            }
        }

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
            match column.col_type {
                ColumnType::INT64 => {
                    let compressed_data = self.int_compressor.compress(
                        column
                            .int_data
                            .as_ref()
                            .map(|d| d.as_slice())
                            .unwrap_or(&[]),
                    );
                    let offset = f.stream_position()?;
                    let length = compressed_data.len() as u64;
                    offsets_and_lengths.push(Location::INT { offset, length });
                    f.write_all(&compressed_data)?;
                }
                ColumnType::STR => {
                    let compressed_data = self.string_compressor.compress(
                        column
                            .str_data
                            .as_ref()
                            .map(|d| d.as_slice())
                            .unwrap_or(&[]),
                    );
                    let compressed_str_data = compressed_data.data;
                    let lengths = compressed_data.lengths;

                    let offset = f.stream_position()?;
                    let length = compressed_str_data.len() as u64;
                    f.write_all(&compressed_str_data)?;

                    let compressed_int_data = self.int_compressor.compress(lengths.as_slice());
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

    pub fn deserialize(&self, path: &Path) -> Result<Table> {
        let mut f = File::open(path)?;

        let mut magic = [0u8; 4];
        f.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Incorrect file indicator",
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
            col_type: ColumnType,
            offset: u64,
            length: u64,
            length2: u64,
        }
        let mut descriptions = Vec::<ColumnDescription>::with_capacity(num_cols);
        for _ in 0..num_cols {
            let mut nl = [0u8; 1];
            f.read_exact(&mut nl)?;
            let name_length = nl[0];

            let mut name_bytes = vec![0u8; name_length as usize];
            f.read_exact(&mut name_bytes)?;
            let name = String::from_utf8_lossy(&name_bytes).into_owned();

            let mut t = [0u8; 1];
            f.read_exact(&mut t)?;
            let col_type = match t[0] {
                0u8 => ColumnType::INT64,
                1u8 => ColumnType::STR,
                _ => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Incorrect file indicator",
                    ));
                }
            };

            let mut off = [0u8; 8];
            f.read_exact(&mut off)?;
            let offset = u64::from_le_bytes(off);

            let mut len = [0u8; 8];
            f.read_exact(&mut len)?;
            let length = u64::from_le_bytes(len);

            let description = match col_type {
                ColumnType::INT64 => ColumnDescription {
                    name,
                    col_type,
                    offset,
                    length,
                    length2: 0u64,
                },
                ColumnType::STR => {
                    let mut len2 = [0u8; 8];
                    f.read_exact(&mut len2)?;
                    let length2 = u64::from_le_bytes(len2);
                    ColumnDescription {
                        name,
                        col_type,
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

            match desc.col_type {
                ColumnType::INT64 => {
                    let mut int_data = self.int_compressor.decompress(&buf);
                    int_data.resize(num_rows as usize, 0i64);
                    columns.push(Column {
                        name: desc.name,
                        col_type: desc.col_type,
                        int_data: Some(int_data),
                        str_data: None,
                    })
                }
                ColumnType::STR => {
                    let mut buf2 = vec![0u8; desc.length2 as usize];
                    f.read_exact(&mut buf2)?;

                    let lengths_data = self.int_compressor.decompress(&buf2);
                    let mut str_data = self.string_compressor.decompress(&CompressedStringColumn {
                        data: buf,
                        lengths: lengths_data,
                    });
                    str_data.resize(num_rows as usize, "".to_string());
                    columns.push(Column {
                        name: desc.name,
                        col_type: desc.col_type,
                        int_data: None,
                        str_data: Some(str_data),
                    })
                }
            }
        }

        Ok(Table { num_rows, columns })
    }
}
