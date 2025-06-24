#[cfg(test)]
mod tests {
    use std::vec;

    use crate::arrow::FixedSizeListArrayExt;
    use arrow_array::{BinaryArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_array::{
        FixedSizeListArray,
    };
    use arrow_schema::{DataType, Field, FieldRef, Schema as ArrowSchema};
    use lance_file::version::LanceFileVersion;
    use std::sync::Arc;
    use crate::Dataset;
    use crate::dataset::{ProjectionRequest, WriteMode, WriteParams};

    #[tokio::test]
    async fn test_take_rows_2_1() {
        let ds = create_dataset(
            "memory://test.lance",
            LanceFileVersion::V2_1,
            1024,
            1024 * 1024,
        ).await;
        let schema = Arc::new(ds.schema().clone());

        let rows = [0, 5, 10, 12];
        ds.take_rows(rows.as_slice(), ProjectionRequest::Schema(schema.clone())).await.unwrap();
    }

    #[tokio::test]
    async fn test_take_rows_2_0() {
        let ds = create_dataset(
            "memory://test.lance",
            LanceFileVersion::V2_0,
            1024,
            1024 * 1024,
        ).await;
        let schema = Arc::new(ds.schema().clone());

        let rows = [0, 5, 10, 12];
        ds.take_rows(rows.as_slice(), ProjectionRequest::Schema(schema.clone())).await.unwrap();
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
        let batch_size = 1024 as i32;
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
}