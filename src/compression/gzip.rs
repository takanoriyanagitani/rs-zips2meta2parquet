use std::io;

#[repr(u8)]
pub enum GzipLevel {
    Fast = 1,
    Best = 9,

    Level0 = 0,
    Level2 = 2,
    Level3 = 3,
    Level4 = 4,
    Level5 = 5,
    Level6 = 6,
    Level7 = 7,
    Level8 = 8,
}

pub const GZIP_LEVEL_DEFAULT: GzipLevel = GzipLevel::Level6;

impl Default for GzipLevel {
    fn default() -> Self {
        GZIP_LEVEL_DEFAULT
    }
}

impl TryFrom<GzipLevel> for parquet::basic::GzipLevel {
    type Error = io::Error;

    fn try_from(g: GzipLevel) -> Result<Self, Self::Error> {
        let u: u8 = g as u8;
        Self::try_new(u.into()).map_err(io::Error::other)
    }
}

impl TryFrom<u8> for GzipLevel {
    type Error = io::Error;

    fn try_from(u: u8) -> Result<Self, Self::Error> {
        match u {
            0 => Ok(GzipLevel::Level0),
            1 => Ok(GzipLevel::Fast),
            2 => Ok(GzipLevel::Level2),
            3 => Ok(GzipLevel::Level3),
            4 => Ok(GzipLevel::Level4),
            5 => Ok(GzipLevel::Level5),
            6 => Ok(GzipLevel::Level6),
            7 => Ok(GzipLevel::Level7),
            8 => Ok(GzipLevel::Level8),
            9 => Ok(GzipLevel::Best),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Unsupported GZIP level",
            )),
        }
    }
}
