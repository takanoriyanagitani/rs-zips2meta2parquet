use std::io;
use std::path::Path;

use futures_util::pin_mut;

use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;

use rs_zips2meta2rbat2stream::arrow;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use futures::Stream;
use futures::StreamExt;

use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_writer::AsyncFileWriter;

pub async fn write_batch_all<S, W>(b: S, wtr: &mut AsyncArrowWriter<W>) -> Result<(), io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
    W: AsyncFileWriter,
{
    pin_mut!(b);
    while let Some(rslt) = b.next().await {
        let rb: RecordBatch = rslt?;
        wtr.write(&rb).await?;
    }
    wtr.finish().await?;
    Ok(())
}

async fn _batch2parquet<S, W>(
    b1st: RecordBatch,
    b: S,
    opts: Option<ArrowWriterOptions>,
    wtr: W,
) -> Result<W, io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
    W: AsyncFileWriter,
{
    let s: SchemaRef = b1st.schema();
    let mut aw: AsyncArrowWriter<_> = match opts {
        None => AsyncArrowWriter::try_new(wtr, s, None),
        Some(o) => AsyncArrowWriter::try_new_with_options(wtr, s, o),
    }?;
    aw.write(&b1st).await?;
    write_batch_all(b, &mut aw).await?;
    Ok(aw.into_inner())
}

pub async fn batch2parquet<S, W>(
    b: S,
    opts: Option<ArrowWriterOptions>,
    wtr: W,
) -> Result<W, io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
    W: AsyncFileWriter,
{
    pin_mut!(b);
    let b1st: Option<Result<RecordBatch, _>> = b.next().await;
    match b1st {
        None => Ok(wtr),
        Some(b1) => _batch2parquet(b1?, b, opts, wtr).await,
    }
}

pub async fn batch2parquet_file<S, P>(
    b: S,
    opts: Option<ArrowWriterOptions>,
    filename: P,
    fsync: bool,
) -> Result<(), io::Error>
where
    S: Stream<Item = Result<RecordBatch, io::Error>>,
    P: AsRef<Path>,
{
    let mut f: tokio::fs::File = tokio::fs::File::create(filename).await?;
    {
        let bw = BufWriter::new(&mut f);
        let mut bw = batch2parquet(b, opts, bw).await?;
        bw.flush().await?;
    }
    f.flush().await?;
    if fsync {
        f.sync_data().await?
    }
    Ok(())
}
