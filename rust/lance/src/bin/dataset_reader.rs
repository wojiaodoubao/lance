use lance::dataset::Dataset;
#[cfg(target_os = "linux")]
use rand::Rng;
use std::sync::LazyLock;
#[cfg(target_os = "linux")]
use std::time::Duration;
use std::time::Instant;

const BATCH_SIZE: u64 = 1024;
static DATASET_BASE_PATH: LazyLock<String> = LazyLock::new(||std::env::var("DATASET_BASE_PATH")
    .unwrap_or("memory://base".to_string()));
static TAKE_NUMBER: LazyLock<usize> = LazyLock::new(|| std::env::var("SAMPLE_NUMBER")
    .ok()
    .and_then(|s| s.parse().ok())
    .unwrap_or(10));

fn dataset_path() -> String {
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

async fn dataset_take(
    file_size: u64,
    num_batches: u64,
    rows_gen: Box<dyn Fn(u64, u64, usize) -> Vec<Vec<u64>>>,
) -> Result<(), lance_core::Error> {
    // Make sure there is only one fragment.
    let uri = dataset_path();
    let dataset = Dataset::open(&uri).await?;

    // Bench random take.
        let start = Instant::now();

    let reader = dataset.open_reader(dataset.schema().clone()).await?;

    let rows_list = rows_gen(num_batches, file_size, *TAKE_NUMBER);
    for rows in rows_list {
        let _ = reader.take(&rows).await?;
    }

    let time_cost = start.elapsed().as_millis();
    println!("Dataset Reader Take time costs: {:?}", time_cost);

    Ok(())
}

async fn bench_random_single_take_with_dataset() -> Result<(), lance_core::Error> {
    let num_batches: u64 = 1024;
    let file_size: u64 = num_batches * BATCH_SIZE + 1;
    let rows_gen = Box::new(|num_batches, file_size, num_rows| {
        let rows = gen_ranges(num_batches * BATCH_SIZE, file_size, num_rows);
        let mut rows_list: Vec<Vec<u64>> = Vec::with_capacity(rows.len());
        for row in rows {
            rows_list.push(vec![row]);
        }
        rows_list
    });

    dataset_take(
        file_size,
        num_batches,
        rows_gen.clone(),
    ).await
}

#[tokio::main]
async fn main() -> lance_core::Result<()> {
    bench_random_single_take_with_dataset().await
}