use std::io;

pub struct ZstdLevel {
    level: u8,
}

pub const ZSTD_LEVEL_DEFAULT: ZstdLevel = ZstdLevel { level: 3 };

impl Default for ZstdLevel {
    fn default() -> Self {
        ZSTD_LEVEL_DEFAULT
    }
}

impl TryFrom<u8> for ZstdLevel {
    type Error = io::Error;

    fn try_from(u: u8) -> Result<Self, Self::Error> {
        let level: u8 = match u {
            0 => Ok(3),
            1..=22 => Ok(u),
            _ => Err(io::Error::other("invalid zstd level")),
        }?;
        Ok(Self { level })
    }
}

impl TryFrom<ZstdLevel> for parquet::basic::ZstdLevel {
    type Error = io::Error;

    fn try_from(l: ZstdLevel) -> Result<Self, Self::Error> {
        let level: u8 = l.level;
        Self::try_new(level.into()).map_err(io::Error::other)
    }
}
