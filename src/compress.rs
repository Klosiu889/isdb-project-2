use std::string::FromUtf8Error;

use integer_encoding::VarInt;
use lz4_flex::block::{DecompressError, compress_prepend_size, decompress_size_prepended};

#[derive(Debug)]
pub enum StringCompressors {
    Lz4(LZ4StringCompressor),
    None(NoStringCompressor),
}

impl StringCompressors {
    pub fn compress(&self, data: &[String]) -> Result<CompressedStringColumn, CompressorError> {
        match self {
            StringCompressors::Lz4(c) => c.compress(data),
            StringCompressors::None(c) => c.compress(data),
        }
    }

    pub fn decompress(
        &self,
        data: &CompressedStringColumn,
    ) -> Result<Vec<String>, CompressorError> {
        match self {
            StringCompressors::Lz4(c) => c.decompress(data),
            StringCompressors::None(c) => c.decompress(data),
        }
    }
}

#[derive(Debug)]
pub enum IntCompressors {
    VleDelta(VleDeltaIntCompressor),
    None(NoIntCompressor),
}

impl IntCompressors {
    pub fn compress(&self, data: &[i64]) -> Result<Vec<u8>, CompressorError> {
        match self {
            IntCompressors::VleDelta(c) => c.compress(data),
            IntCompressors::None(c) => c.compress(data),
        }
    }

    pub fn decompress(&self, data: &Vec<u8>) -> Result<Vec<i64>, CompressorError> {
        match self {
            IntCompressors::VleDelta(c) => c.decompress(data),
            IntCompressors::None(c) => c.decompress(data),
        }
    }
}

#[derive(Debug)]
pub struct CompressedStringColumn {
    pub data: Vec<u8>,
    pub lengths: Vec<i64>,
}

#[derive(Debug)]
pub enum CompressorError {
    Lz4Decompression(DecompressError),
    Utf8Decoding(FromUtf8Error),
    VleDecoding(String),
    WrongDataLength(String),
    NegativeStringLength(String),
}

impl From<DecompressError> for CompressorError {
    fn from(value: DecompressError) -> Self {
        Self::Lz4Decompression(value)
    }
}

impl From<FromUtf8Error> for CompressorError {
    fn from(value: FromUtf8Error) -> Self {
        Self::Utf8Decoding(value)
    }
}

pub trait Compressor<T> {
    type Compressed;

    fn compress(&self, data: &[T]) -> Result<Self::Compressed, CompressorError>;
    fn decompress(&self, compressed: &Self::Compressed) -> Result<Vec<T>, CompressorError>;
}

#[derive(Debug)]
pub struct VleDeltaIntCompressor;

impl Compressor<i64> for VleDeltaIntCompressor {
    type Compressed = Vec<u8>;

    fn compress(&self, data: &[i64]) -> Result<Self::Compressed, CompressorError> {
        let mut deltas = Vec::<i64>::with_capacity(data.len());
        let mut last = 0i64;

        for &d in data {
            deltas.push(d - last);
            last = d;
        }

        Ok(deltas.iter().flat_map(|d| d.encode_var_vec()).collect())
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Result<Vec<i64>, CompressorError> {
        let mut cursor = &compressed[..];
        let mut deltas = Vec::<i64>::new();
        while !cursor.is_empty() {
            let (d, n) = i64::decode_var(&cursor).ok_or(CompressorError::VleDecoding(
                "Decoder stopped before going through all data".to_string(),
            ))?;
            deltas.push(d);
            cursor = &cursor[n..];
        }

        let mut data = Vec::<i64>::with_capacity(deltas.len());
        let mut last = 0i64;

        for delta in deltas {
            data.push(delta + last);
            last += delta;
        }

        Ok(data)
    }
}

#[derive(Debug)]
pub struct LZ4StringCompressor;

impl Compressor<String> for LZ4StringCompressor {
    type Compressed = CompressedStringColumn;

    fn compress(&self, data: &[String]) -> Result<Self::Compressed, CompressorError> {
        let raw = data
            .iter()
            .flat_map(|d| d.as_bytes())
            .copied()
            .collect::<Vec<u8>>();
        let lengths = data.iter().map(|d| d.len() as i64).collect::<Vec<i64>>();

        let compressed_data = compress_prepend_size(&raw);

        Ok(Self::Compressed {
            data: compressed_data,
            lengths,
        })
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Result<Vec<String>, CompressorError> {
        let raw = decompress_size_prepended(&compressed.data)?;
        let mut res = Vec::with_capacity(compressed.lengths.len());
        let mut offset = 0;

        for &len in &compressed.lengths {
            if len < 0 {
                return Err(CompressorError::NegativeStringLength(
                    "Negative string length was passed".to_string(),
                ));
            }

            let slice =
                raw.get(offset..offset + len as usize)
                    .ok_or(CompressorError::WrongDataLength(
                        "Data length is shorter then declared strings lengths".to_string(),
                    ))?;
            res.push(String::from_utf8(slice.to_vec())?);
            offset += len as usize;
        }

        Ok(res)
    }
}

#[derive(Debug)]
pub struct NoIntCompressor;

impl Compressor<i64> for NoIntCompressor {
    type Compressed = Vec<u8>;

    fn compress(&self, data: &[i64]) -> Result<Self::Compressed, CompressorError> {
        Ok(data.iter().flat_map(|d| d.to_le_bytes()).collect())
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Result<Vec<i64>, CompressorError> {
        if compressed.len() % 8 != 0 {
            return Err(CompressorError::WrongDataLength(
                "Data length must be divisable by 8".to_string(),
            ));
        }

        Ok(compressed
            .chunks_exact(8)
            .map(|c| {
                i64::from_le_bytes(
                    c.try_into()
                        .expect("8 bytes chunks guaranteed byt chunks_exact"),
                )
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct NoStringCompressor;

impl Compressor<String> for NoStringCompressor {
    type Compressed = CompressedStringColumn;

    fn compress(&self, data: &[String]) -> Result<Self::Compressed, CompressorError> {
        let raw = data
            .iter()
            .flat_map(|d| d.as_bytes())
            .copied()
            .collect::<Vec<u8>>();
        let lengths = data.iter().map(|d| d.len() as i64).collect::<Vec<i64>>();

        Ok(Self::Compressed { data: raw, lengths })
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Result<Vec<String>, CompressorError> {
        let mut res = Vec::with_capacity(compressed.lengths.len());
        let mut offset = 0;

        for &len in &compressed.lengths {
            if len < 0 {
                return Err(CompressorError::NegativeStringLength(
                    "Negative string length was passed".to_string(),
                ));
            }

            let slice = compressed.data.get(offset..offset + len as usize).ok_or(
                CompressorError::WrongDataLength(
                    "Data length is shorter then declared strings lengths".to_string(),
                ),
            )?;
            res.push(String::from_utf8(slice.to_vec())?);
            offset += len as usize;
        }

        Ok(res)
    }
}
