use std::io;

use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::file::properties::WriterProperties;

use crate::compression::gzip::GzipLevel;
use crate::compression::zstd::ZstdLevel;

pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip(GzipLevel),
    Lzo,
    Lz4,
    Zstd(ZstdLevel),
    Lz4Raw,
}

pub const DEFAULT_COMPRESSION: Compression = Compression::Uncompressed;

impl Default for Compression {
    fn default() -> Self {
        DEFAULT_COMPRESSION
    }
}

impl TryFrom<Compression> for parquet::basic::Compression {
    type Error = io::Error;

    fn try_from(c: Compression) -> Result<Self, Self::Error> {
        match c {
            Compression::Uncompressed => Ok(parquet::basic::Compression::UNCOMPRESSED),
            Compression::Snappy => Ok(parquet::basic::Compression::SNAPPY),
            Compression::Gzip(g) => Ok(parquet::basic::Compression::GZIP(g.try_into()?)),
            Compression::Lzo => Ok(parquet::basic::Compression::LZO),
            Compression::Lz4 => Ok(parquet::basic::Compression::LZ4),
            Compression::Zstd(z) => Ok(parquet::basic::Compression::ZSTD(z.try_into()?)),
            Compression::Lz4Raw => Ok(parquet::basic::Compression::LZ4_RAW),
        }
    }
}

impl TryFrom<Compression> for WriterProperties {
    type Error = io::Error;

    fn try_from(c: Compression) -> Result<Self, Self::Error> {
        let pc: parquet::basic::Compression = c.try_into()?;
        Ok(WriterProperties::builder().set_compression(pc).build())
    }
}

impl TryFrom<Compression> for ArrowWriterOptions {
    type Error = io::Error;

    fn try_from(c: Compression) -> Result<Self, Self::Error> {
        let props: WriterProperties = c.try_into()?;
        Ok(Self::new().with_properties(props))
    }
}
