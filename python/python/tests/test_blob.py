# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import os
import uuid

import lance
import pyarrow as pa
import pytest
import requests
from lance import BlobColumn


def test_blob_read_from_binary():
    values = [b"foo", b"bar", b"baz"]
    data = pa.table(
        {
            "bin": pa.array(values, type=pa.binary()),
            "largebin": pa.array(values, type=pa.large_binary()),
        }
    )

    for col_name in ["bin", "largebin"]:
        blobs = BlobColumn(data.column(col_name))
        for i, f in enumerate(blobs):
            assert f.read() in values[i]


def test_blob_reject_invalid_col():
    values = pa.array([1, 2, 3])
    with pytest.raises(ValueError, match="Expected a binary array"):
        BlobColumn(values)


def test_blob_descriptions(tmp_path):
    values = pa.array([b"foo", b"bar", b"baz"], pa.large_binary())
    table = pa.table(
        [values],
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                )
            ]
        ),
    )
    ds = lance.write_dataset(table, tmp_path / "test_ds")
    # These positions may be surprising but lance pads buffers to 64-byte boundaries
    expected_positions = pa.array([0, 64, 128], pa.uint64())
    expected_sizes = pa.array([3, 3, 3], pa.uint64())
    descriptions = ds.to_table().column("blobs").chunk(0)

    assert descriptions.field(0) == expected_positions
    assert descriptions.field(1) == expected_sizes


def test_scan_blob_as_binary(tmp_path):
    values = [b"foo", b"bar", b"baz"]
    arr = pa.array(values, pa.large_binary())
    table = pa.table(
        [arr],
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                )
            ]
        ),
    )
    ds = lance.write_dataset(table, tmp_path / "test_ds")

    tbl = ds.scanner(columns=["blobs"], blob_handling="all_binary").to_table()
    assert tbl.column("blobs").to_pylist() == values


def test_fragment_scan_blob_as_binary(tmp_path):
    values = [b"foo", b"bar", b"baz"]
    arr = pa.array(values, pa.large_binary())
    table = pa.table(
        [arr],
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                )
            ]
        ),
    )
    ds = lance.write_dataset(table, tmp_path / "test_ds")

    fragment = ds.get_fragments()[0]

    tbl = fragment.scanner(columns=["blobs"], blob_handling="all_binary").to_table()
    assert tbl.column("blobs").to_pylist() == values

    tbl = fragment.to_table(columns=["blobs"], blob_handling="all_binary")
    assert tbl.column("blobs").to_pylist() == values


@pytest.fixture
def dataset_with_blobs(tmp_path):
    values = pa.array([b"foo", b"bar", b"baz"], pa.large_binary())
    idx = pa.array([0, 1, 2], pa.uint64())
    table = pa.table(
        [values, idx],
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                ),
                pa.field("idx", pa.uint64()),
            ]
        ),
    )
    ds = lance.write_dataset(table, tmp_path / "test_ds")

    values = pa.array([b"qux", b"quux", b"corge"], pa.large_binary())
    idx = pa.array([3, 4, 5], pa.uint64())
    table = pa.table(
        [values, idx],
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                ),
                pa.field("idx", pa.uint64()),
            ]
        ),
    )
    ds.insert(table)
    return ds


def test_blob_files(dataset_with_blobs):
    row_ids = (
        dataset_with_blobs.to_table(columns=[], with_row_id=True)
        .column("_rowid")
        .to_pylist()
    )
    blobs = dataset_with_blobs.take_blobs("blobs", ids=row_ids)

    for expected in [b"foo", b"bar", b"baz"]:
        with blobs.pop(0) as f:
            assert f.read() == expected


def test_blob_files_by_address(dataset_with_blobs):
    addresses = (
        dataset_with_blobs.to_table(columns=[], with_row_address=True)
        .column("_rowaddr")
        .to_pylist()
    )
    blobs = dataset_with_blobs.take_blobs("blobs", addresses=addresses)

    for expected in [b"foo", b"bar", b"baz"]:
        with blobs.pop(0) as f:
            assert f.read() == expected


def test_blob_files_by_address_with_stable_row_ids(tmp_path):
    table = pa.table(
        {
            "blobs": pa.array([b"foo"], pa.large_binary()),
            "idx": pa.array([0], pa.uint64()),
        },
        schema=pa.schema(
            [
                pa.field(
                    "blobs", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                ),
                pa.field("idx", pa.uint64()),
            ]
        ),
    )
    ds = lance.write_dataset(
        table,
        tmp_path / "test_ds",
        enable_stable_row_ids=True,
    )

    ds.insert(
        pa.table(
            {
                "blobs": pa.array([b"bar"], pa.large_binary()),
                "idx": pa.array([1], pa.uint64()),
            },
            schema=table.schema,
        )
    )

    t = ds.to_table(columns=["idx"], with_row_address=True)
    row_idx = t.column("idx").to_pylist().index(1)
    addr = t.column("_rowaddr").to_pylist()[row_idx]

    blobs = ds.take_blobs("blobs", addresses=[addr])
    assert len(blobs) == 1
    with blobs[0] as f:
        assert f.read() == b"bar"


def test_blob_by_indices(tmp_path, dataset_with_blobs):
    indices = [0, 4]
    blobs = dataset_with_blobs.take_blobs("blobs", indices=indices)

    blobs2 = dataset_with_blobs.take_blobs("blobs", ids=[0, (1 << 32) + 1])
    assert len(blobs) == len(blobs2)
    for b1, b2 in zip(blobs, blobs2):
        with b1 as f1, b2 as f2:
            assert f1.read() == f2.read()


def test_blob_file_seek(tmp_path, dataset_with_blobs):
    row_ids = (
        dataset_with_blobs.to_table(columns=[], with_row_id=True)
        .column("_rowid")
        .to_pylist()
    )
    blobs = dataset_with_blobs.take_blobs("blobs", ids=row_ids)
    with blobs[1] as f:
        assert f.seek(1) == 1
        assert f.read(1) == b"a"


def test_null_blobs(tmp_path):
    table = pa.table(
        {
            "id": range(100),
            "blob": pa.array([None] * 100, pa.large_binary()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.uint64()),
                pa.field(
                    "blob", pa.large_binary(), metadata={"lance-encoding:blob": "true"}
                ),
            ]
        ),
    )
    ds = lance.write_dataset(table, tmp_path / "test_ds")

    blobs = ds.take_blobs("blob", ids=range(100))
    for blob in blobs:
        assert blob.size() == 0

    ds.insert(pa.table({"id": pa.array(range(100, 200), pa.uint64())}))

    ds.add_columns(
        pa.field(
            "more_blob",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        )
    )

    for blob_col in ["blob", "more_blob"]:
        blobs = ds.take_blobs(blob_col, indices=range(100, 200))
        for blob in blobs:
            assert blob.size() == 0

        blobs = ds.to_table(columns=[blob_col])
        for blob in blobs.column(blob_col):
            py_blob = blob.as_py()
            # When we write blobs to a file we store the position as 1 and size as 0
            # to avoid needing a validity buffer.
            assert py_blob is None or py_blob == {
                "position": 1,
                "size": 0,
            }


def test_blob_file_read_middle(tmp_path, dataset_with_blobs):
    # This regresses an issue where we were not setting the cursor
    # correctly after a call to `read` when the blob was not the
    # first thing in the file.
    row_ids = (
        dataset_with_blobs.to_table(columns=[], with_row_id=True)
        .column("_rowid")
        .to_pylist()
    )
    blobs = dataset_with_blobs.take_blobs("blobs", ids=row_ids)
    with blobs[1] as f:
        assert f.read(1) == b"b"
        assert f.read(1) == b"a"
        assert f.read(1) == b"r"


def test_take_deleted_blob(tmp_path, dataset_with_blobs):
    row_ids = (
        dataset_with_blobs.to_table(columns=[], with_row_id=True)
        .column("_rowid")
        .to_pylist()
    )
    dataset_with_blobs.delete("idx = 1")

    with pytest.raises(
        NotImplementedError,
        match="A take operation that includes row addresses must not target deleted",
    ):
        dataset_with_blobs.take_blobs("blobs", ids=row_ids)


def test_scan_blob(tmp_path, dataset_with_blobs):
    ds = dataset_with_blobs.scanner(filter="idx = 2").to_table()
    assert ds.num_rows == 1


def test_blob_extension_write_inline(tmp_path):
    table = pa.table({"blob": lance.blob_array([b"foo", b"bar"])})
    ds = lance.write_dataset(
        table,
        tmp_path / "test_ds_v2",
        data_storage_version="2.2",
    )

    desc = ds.to_table(columns=["blob"]).column("blob").chunk(0)
    assert pa.types.is_struct(desc.type)

    blobs = ds.take_blobs("blob", indices=[0, 1])
    with blobs[0] as f:
        assert f.read() == b"foo"


def test_blob_extension_write_external(tmp_path):
    blob_path = tmp_path / "external_blob.bin"
    blob_path.write_bytes(b"hello")
    uri = blob_path.as_uri()

    table = pa.table({"blob": lance.blob_array([uri])})
    ds = lance.write_dataset(
        table,
        tmp_path / "test_ds_v2_external",
        data_storage_version="2.2",
    )

    blob = ds.take_blobs("blob", indices=[0])[0]
    assert blob.size() == 5
    with blob as f:
        assert f.read() == b"hello"


def test_blob_https_url_and_range_s3_roundtrip() -> None:
    """End-to-end test for blob https_url_and_range on S3.
    The test writes a single blob v2 row to an S3-backed dataset, obtains a
    presigned URL and byte range via the inner LanceBlobFile, and then issues
    a ranged HTTP GET to verify the returned bytes match the original blob
    content.
    """
    endpoint = os.environ.get("LANCE_S3_ENDPOINT")
    bucket = os.environ.get("LANCE_S3_BUCKET")
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if not endpoint or not bucket:
        pytest.skip(
            "LANCE_S3_ENDPOINT and LANCE_S3_BUCKET must be set for S3 blob URL test"
        )

    # Build storage options
    storage_options: dict[str, str] = {
        "aws_endpoint": endpoint,
    }
    if region:
        storage_options["aws_region"] = region
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    if access_key and secret_key:
        storage_options["aws_access_key_id"] = access_key
        storage_options["aws_secret_access_key"] = secret_key
        if session_token:
            storage_options["aws_session_token"] = session_token
    storage_options["virtual_hosted_style_request"] = "true"

    uri = f"s3://{bucket}/blobv2-url-test/{uuid.uuid4().hex}"
    data = b"hello-blob-v2-s3"
    table = pa.table({"blob": lance.blob_array([data])})

    ds = lance.write_dataset(
        table,
        uri,
        data_storage_version="2.2",
        storage_options=storage_options,
    )
    blobs = ds.take_blobs("blob", indices=[0])
    assert len(blobs) == 1

    # Use the inner LanceBlobFile to access https_url_and_range directly.
    inner = blobs[0].inner
    url, blob_range = inner.https_url_and_range(expires_in_seconds=3600)

    assert isinstance(url, str)
    assert url
    assert blob_range is not None
    offset, length = blob_range
    assert length == len(data)
    assert offset >= 0
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    assert resp.content == data
