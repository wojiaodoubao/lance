use arrow_array::{
    BinaryArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    UInt32Array,
};
use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};

use futures::StreamExt;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::{arrow::FixedSizeListArrayExt, dataset::ProjectionRequest};
use lance_core::cache::LanceCache;
use lance_encoding::decoder::{DecoderPlugins, FilterExpression};
use lance_file::v2::reader::{FileReader, FileReaderOptions};
use lance_file::v2::LanceEncodingsIo;
use lance_file::version::LanceFileVersion;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_io::ReadBatchParams;
use object_store::path::Path;
#[cfg(target_os = "linux")]
use rand::Rng;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::time::Duration;
use std::time::Instant;
use datafusion::execution::context::DataFilePaths;

const BATCH_SIZE: u64 = 1024;
lazy_static::lazy_static! {
    pub static ref DATASET_BASE_PATH : String = std::env::var("DATASET_BASE_PATH")
                .unwrap_or("memory://base".to_string());
}
lazy_static::lazy_static! {
    pub static ref SAMPLE_NUMBER : usize = std::env::var("SAMPLE_NUMBER")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10);
}

fn dataset_path() -> String {
    let uuid = uuid::Uuid::new_v4().to_string();
    let base: &str = &DATASET_BASE_PATH;
    base.to_string()
}

fn gen_ranges(num_rows: u64, file_size: u64, n: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let mut ranges = Vec::with_capacity(n);
    for i in 0..n {
        ranges.push(rng.gen_range(1..num_rows));
        ranges[i] = ((ranges[i] / file_size) << 32) | (ranges[i] % file_size);
    }

    ranges
}

async fn file_reader_take(
    file_size: u64,
    num_batches: u64,
    rows_gen: Box<dyn Fn(u64, u64, usize) -> Vec<Vec<u32>>>,
) -> Result<(), lance_core::Error> {
    // Make sure there is only one fragment.
    let uri = dataset_path();
    let dataset = Dataset::open(&uri).await?;
    assert_eq!(dataset.get_fragments().len(), 1);
    let fragments = dataset.get_fragments();
    let fragment = fragments.first().unwrap();
    assert_eq!(fragment.num_data_files(), 1);
    let file = fragment.metadata().files.first().unwrap();
    let file_path = dataset.data_dir().child(file.path.as_str());


    // Bench random take.
    for num_rows in [1000] {
        let start = Instant::now();

        let sample_number: usize = *SAMPLE_NUMBER;
        for _ in 0..sample_number {
            let file_reader = create_file_reader(&dataset, &file_path).await;

            let rows_list = rows_gen(num_batches, file_size, num_rows);
            for rows in rows_list {
                let rows = ReadBatchParams::Indices(UInt32Array::from(rows));
                let stream = file_reader
                    .read_stream(
                        rows,
                        1024,
                        16,
                        FilterExpression::no_filter(),
                    )
                    .unwrap();
                stream.fold(Vec::new(), |mut acc, item| async move {
                    acc.push(item);
                    acc
                }).await;
            }
        }

        let time_cost = start.elapsed().as_millis();
        println!("File Reader Take time costs: {:?}", time_cost);
    }

    Ok(())
}

async fn create_file_reader(dataset: &Dataset, file_path: &Path) -> FileReader {
    // Create file reader v2.
    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig {
            io_buffer_size_bytes: 2 * 1024 * 1024 * 1024,
        },
    );
    let file = scheduler
        .open_file(file_path, &CachedFileSize::unknown())
        .await
        .unwrap();
    let file_metadata = FileReader::read_all_metadata(&file).await.unwrap();

    FileReader::try_open_with_file_metadata(
        Arc::new(LanceEncodingsIo(file.clone())),
        file_path.clone(),
        None,
        Arc::<DecoderPlugins>::default(),
        Arc::new(file_metadata),
        &LanceCache::no_cache(),
        FileReaderOptions::default(),
    )
        .await
        .unwrap()
}

async fn bench_random_single_take_with_file_reader() -> Result<(), lance_core::Error> {
    let num_batches: u64 = 1024;
    let file_size: u64 = num_batches * BATCH_SIZE + 1;
    let rows_gen = Box::new(|num_batches, file_size, num_rows| {
        let rows = gen_ranges(num_batches * BATCH_SIZE, file_size, num_rows);
        let mut rows_list: Vec<Vec<u32>> = Vec::with_capacity(rows.len());
        for row in rows {
            rows_list.push(vec![row as u32]);
        }
        rows_list
    });

    file_reader_take(
        file_size,
        num_batches,
        rows_gen.clone(),
    ).await
}

#[tokio::main]
async fn main() -> lance_core::Result<()> {
    bench_random_single_take_with_file_reader().await
}