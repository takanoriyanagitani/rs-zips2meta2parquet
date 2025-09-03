use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use rs_zips2meta2parquet::futures;
use rs_zips2meta2parquet::parquet;
use rs_zips2meta2parquet::rs_zips2meta2rbat2stream;

use futures::Stream;

use parquet::arrow::arrow_writer::ArrowWriterOptions;

use arrow::record_batch::RecordBatch;
use rs_zips2meta2rbat2stream::arrow;

use rs_zips2meta2parquet::compression::parse::Compression;

use rs_zips2meta2parquet::core::batch2parquet_file;

use rs_zips2meta2parquet::compression::gzip::GzipLevel;
use rs_zips2meta2parquet::compression::zstd::ZstdLevel;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "zips2parquet", about = "Converts zip files to a parquet file")]
struct RawArgs {
    /// The path to the zip files.
    #[arg(short, long)]
    zips_dir: PathBuf,

    /// Output filename(parquet file).
    #[arg(short, long)]
    output_parquet_filename: PathBuf,

    /// Compression to use: `none`, `snappy`, `lzo`, `lz4`, `lz4raw`, `gzip`, or `zstd`
    #[arg(short, long)]
    compression: Option<String>,

    /// Compression level for gzip/std.
    #[arg(short, long)]
    compression_level: Option<u8>,

    /// Flush the file to disk before returning
    #[arg(short, long)]
    fsync: bool,
}

struct Args {
    zips_dir: PathBuf,
    compression: Option<Compression>,
    output_parquet_filename: PathBuf,
    fsync: bool,
}

fn raw2args(r: RawArgs) -> Result<Args, io::Error> {
    if !r.zips_dir.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("zips directory not found: {}", r.zips_dir.display()),
        ));
    }

    let comp: Option<Compression> = match r.compression {
        Some(ref s) => {
            let s_lower = s.to_lowercase();
            match s_lower.as_str() {
                "none" => Some(Compression::Uncompressed),
                "snappy" => Some(Compression::Snappy),
                "lzo" => Some(Compression::Lzo),
                "lz4" => Some(Compression::Lz4),
                "lz4raw" => Some(Compression::Lz4Raw),
                "gzip" => {
                    let gz_level = match r.compression_level {
                        None => GzipLevel::default(),
                        Some(lvl) => GzipLevel::try_from(lvl)?,
                    };
                    Some(Compression::Gzip(gz_level))
                }
                "zstd" => {
                    let zstd_level = match r.compression_level {
                        None => ZstdLevel::default(),
                        Some(lvl) => ZstdLevel::try_from(lvl)?,
                    };
                    Some(Compression::Zstd(zstd_level))
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unsupported compression: {}", s),
                    ));
                }
            }
        }
        None => None,
    };

    Ok(Args {
        zips_dir: r.zips_dir,
        output_parquet_filename: r.output_parquet_filename,
        compression: comp,
        fsync: r.fsync,
    })
}

fn dirname2zips2stream(
    dirname: PathBuf,
) -> Result<impl Stream<Item = Result<RecordBatch, io::Error>>, io::Error> {
    rs_zips2meta2rbat2stream::dir2zips2stream(dirname)
}

async fn write_all<S, P>(
    b: S,
    compression: Option<Compression>,
    filename: P,
    fsync: bool,
) -> Result<(), io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
    P: AsRef<Path>,
{
    let c: Compression = compression.unwrap_or_default();
    let opts: ArrowWriterOptions = c.try_into()?;
    batch2parquet_file(b, Some(opts), filename, fsync).await
}

async fn zips2parquet<P>(
    zips_dir: PathBuf,
    compression: Option<Compression>,
    parquet_filename: P,
    fsync: bool,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
{
    let b = dirname2zips2stream(zips_dir)?;
    write_all(b, compression, parquet_filename, fsync).await
}

async fn sub() -> Result<(), io::Error> {
    let rargs: RawArgs = RawArgs::parse();
    let args: Args = raw2args(rargs)?;
    zips2parquet(
        args.zips_dir,
        args.compression,
        args.output_parquet_filename,
        args.fsync,
    )
    .await
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
