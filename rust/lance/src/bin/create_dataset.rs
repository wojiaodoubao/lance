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

const BATCH_SIZE: u64 = 1024;
lazy_static::lazy_static! {
    pub static ref DATASET_BASE_PATH : String = std::env::var("DATASET_BASE_PATH")
                .unwrap_or("memory://base".to_string());
}

async fn create_dataset(
    path: &str,
    data_storage_version: LanceFileVersion,
    num_batches: i32,
    file_size: i32,
) -> Dataset {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("i", DataType::Int32, false),
        Field::new("f", DataType::Float32, false),
        Field::new("s", DataType::Binary, false),
        Field::new(
            "fsl",
            DataType::FixedSizeList(
                FieldRef::new(Field::new("item", DataType::Float32, true)),
                2,
            ),
            false,
        ),
        Field::new("blob", DataType::Binary, false),
    ]));
    let batch_size = BATCH_SIZE as i32;
    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(
                        i * batch_size..(i + 1) * batch_size,
                    )),
                    Arc::new(Float32Array::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| x as f32)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(BinaryArray::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| format!("blob-{}", x).into_bytes()),
                    )),
                    Arc::new(
                        FixedSizeListArray::try_new_from_values(
                            Float32Array::from_iter_values(
                                (i * batch_size..(i + 2) * batch_size)
                                    .map(|x| (batch_size + (x - batch_size) / 2) as f32),
                            ),
                            2,
                        )
                            .unwrap(),
                    ),
                    Arc::new(BinaryArray::from_iter_values(
                        (i * batch_size..(i + 1) * batch_size)
                            .map(|x| format!("blob-{}", x).into_bytes()),
                    )),
                ],
            )
                .unwrap()
        })
        .collect();

    let write_params = WriteParams {
        max_rows_per_file: file_size as usize,
        max_rows_per_group: batch_size as usize,
        mode: WriteMode::Create,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(reader, path, Some(write_params))
        .await
        .unwrap()
}

fn dataset_path() -> String {
    let uuid = uuid::Uuid::new_v4().to_string();
    let base: &str = &DATASET_BASE_PATH;
    base.to_string()
}

#[tokio::main]
async fn main() -> lance_core::Result<()> {
    let num_batches: u64 = 1024;
    let file_size: u64 = num_batches * BATCH_SIZE + 1;
    let path = dataset_path();

    let _ = create_dataset(
        &path,
        LanceFileVersion::V2_0,
        num_batches as i32,
        file_size as i32,
    ).await;
    Ok(())
}