use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

#[derive(Debug)]
pub enum StringCompressors {
    LZ4(LZ4StringCompressor),
    None(NoStringCompressor),
}

impl StringCompressors {
    pub fn compress(&self, data: &[String]) -> CompressedStringColumn {
        match self {
            StringCompressors::LZ4(c) => c.compress(data),
            StringCompressors::None(c) => c.compress(data),
        }
    }

    pub fn decompress(&self, data: &CompressedStringColumn) -> Vec<String> {
        match self {
            StringCompressors::LZ4(c) => c.decompress(data),
            StringCompressors::None(c) => c.decompress(data),
        }
    }
}

#[derive(Debug)]
pub enum IntCompressors {
    VLE(IntCompressor),
    None(NoIntCompressor),
}

impl IntCompressors {
    pub fn compress(&self, data: &[i64]) -> Vec<u8> {
        match self {
            IntCompressors::VLE(c) => c.compress(data),
            IntCompressors::None(c) => c.compress(data),
        }
    }

    pub fn decompress(&self, data: &Vec<u8>) -> Vec<i64> {
        match self {
            IntCompressors::VLE(c) => c.decompress(data),
            IntCompressors::None(c) => c.decompress(data),
        }
    }
}

#[derive(Debug)]
pub struct CompressedStringColumn {
    pub data: Vec<u8>,
    pub lengths: Vec<i64>,
}

pub trait Compressor<T> {
    type Compressed;

    fn compress(&self, data: &[T]) -> Self::Compressed;
    fn decompress(&self, compressed: &Self::Compressed) -> Vec<T>;
}

#[derive(Debug)]
pub struct IntCompressor;

impl Compressor<i64> for IntCompressor {
    type Compressed = Vec<u8>;

    fn compress(&self, data: &[i64]) -> Self::Compressed {
        data.iter().flat_map(|d| d.to_le_bytes()).collect()
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Vec<i64> {
        assert!(
            compressed.len() % 8 == 0,
            "Data length must be divisable by 8"
        );

        compressed
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }
}

#[derive(Debug)]
pub struct LZ4StringCompressor;

impl Compressor<String> for LZ4StringCompressor {
    type Compressed = CompressedStringColumn;

    fn compress(&self, data: &[String]) -> Self::Compressed {
        let raw = data
            .iter()
            .flat_map(|d| d.as_bytes())
            .copied()
            .collect::<Vec<u8>>();
        let lengths = data.iter().map(|d| d.len() as i64).collect::<Vec<i64>>();

        let compressed_data = compress_prepend_size(&raw);

        Self::Compressed {
            data: compressed_data,
            lengths,
        }
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Vec<String> {
        let raw = decompress_size_prepended(&compressed.data).expect("LZ4 decompression failed");
        let mut res = Vec::with_capacity(compressed.lengths.len());
        let mut offset = 0;

        for &len in &compressed.lengths {
            let end = offset + len as usize;
            let slice = &raw[offset..end];
            res.push(String::from_utf8(slice.to_vec()).expect("Invalid utf-8"));
            offset = end;
        }

        res
    }
}

#[derive(Debug)]
pub struct NoIntCompressor;

impl Compressor<i64> for NoIntCompressor {
    type Compressed = Vec<u8>;

    fn compress(&self, data: &[i64]) -> Self::Compressed {
        data.iter().flat_map(|d| d.to_le_bytes()).collect()
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Vec<i64> {
        assert!(
            compressed.len() % 8 == 0,
            "Data length must be divisable by 8"
        );

        compressed
            .chunks_exact(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }
}

#[derive(Debug)]
pub struct NoStringCompressor;

impl Compressor<String> for NoStringCompressor {
    type Compressed = CompressedStringColumn;

    fn compress(&self, data: &[String]) -> Self::Compressed {
        let raw = data
            .iter()
            .flat_map(|d| d.as_bytes())
            .copied()
            .collect::<Vec<u8>>();
        let lengths = data.iter().map(|d| d.len() as i64).collect::<Vec<i64>>();

        Self::Compressed { data: raw, lengths }
    }

    fn decompress(&self, compressed: &Self::Compressed) -> Vec<String> {
        let mut res = Vec::with_capacity(compressed.lengths.len());
        let mut offset = 0;

        for &len in &compressed.lengths {
            let end = offset + len as usize;
            let slice = &compressed.data[offset..end];
            res.push(String::from_utf8(slice.to_vec()).expect("Invalid utf-8"));
            offset = end;
        }

        res
    }
}
