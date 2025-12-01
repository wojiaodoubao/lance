// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{array::TryFromSliceError, ops::Range, str};

use arrow_schema::ArrowError;

use crate::variant::Buffer;
use arrow_array::{ArrayRef, ListArray};
use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use num_traits::{AsPrimitive, ToPrimitive};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::slice::SliceIndex;
use uuid::Uuid;

/// Helper for reporting integer overflow errors in a consistent way.
pub(crate) fn overflow_error(msg: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Integer overflow computing {msg}"))
}

#[inline]
pub(crate) fn slice_from_slice(bytes: &Buffer, index: Range<usize>) -> Result<Buffer, ArrowError> {
    if index.end > bytes.len() || index.start > bytes.len() {
        Err(ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {index:?} from {}-byte buffer",
            bytes.len(),
        )))
    } else {
        Ok(bytes.slice(index))
    }
}

/// Helper to safely slice bytes with offset calculations.
///
/// Equivalent to `slice_from_slice(bytes, (base_offset + range.start)..(base_offset + range.end))`
/// but using checked addition to prevent integer overflow panics on 32-bit systems.
#[inline]
pub(crate) fn slice_from_slice_at_offset(
    bytes: &Buffer,
    base_offset: usize,
    range: Range<usize>,
) -> Result<Buffer, ArrowError> {
    let start_byte = base_offset
        .checked_add(range.start)
        .ok_or_else(|| overflow_error("slice start"))?;
    let end_byte = base_offset
        .checked_add(range.end)
        .ok_or_else(|| overflow_error("slice end"))?;
    slice_from_slice(bytes, start_byte..end_byte)
}

pub(crate) fn array_from_slice<const N: usize>(
    bytes: &Buffer,
    offset: usize,
) -> Result<[u8; N], ArrowError> {
    let buffer = slice_from_slice_at_offset(bytes, offset, 0..N)?;
    let bytes: &[u8] = buffer.as_ref();
    bytes
        .try_into()
        .map_err(|e: TryFromSliceError| ArrowError::InvalidArgumentError(e.to_string()))
}

pub(crate) fn first_byte_from_slice(slice: &[u8]) -> Result<u8, ArrowError> {
    slice
        .first()
        .copied()
        .ok_or_else(|| ArrowError::InvalidArgumentError("Received empty bytes".to_string()))
}

/// Helper to get a &str from a slice at the given offset and range, or an error if it contains invalid UTF-8 data.
#[inline]
pub(crate) fn string_from_slice(
    slice: &[u8],
    offset: usize,
    range: Range<usize>,
) -> Result<&str, ArrowError> {
    let start_byte = offset
        .checked_add(range.start)
        .ok_or_else(|| overflow_error("slice start"))?;
    let end_byte = offset
        .checked_add(range.end)
        .ok_or_else(|| overflow_error("slice end"))?;

    let value = slice.get(start_byte..end_byte).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {}..{} from {}-byte buffer",
            start_byte,
            end_byte,
            slice.len(),
        ))
    })?;

    str::from_utf8(value)
        .map_err(|_| ArrowError::InvalidArgumentError("invalid UTF-8 string".to_string()))
}

/// Performs a binary search over a range using a fallible key extraction function; a failed key
/// extraction immediately terminats the search.
///
/// This is similar to the standard library's `binary_search_by`, but generalized to ranges instead
/// of slices.
///
/// # Arguments
/// * `range` - The range to search in
/// * `target` - The target value to search for
/// * `key_extractor` - A function that extracts a comparable key from slice elements.
///   This function can fail and return None.
///
/// # Returns
/// * `Some(Ok(index))` - Element found at the given index
/// * `Some(Err(index))` - Element not found, but would be inserted at the given index
/// * `None` - Key extraction failed
pub(crate) fn try_binary_search_range_by<F>(
    range: Range<usize>,
    cmp: F,
) -> Option<Result<usize, usize>>
where
    F: Fn(usize) -> Option<Ordering>,
{
    let Range { mut start, mut end } = range;
    while start < end {
        let mid = start + (end - start) / 2;
        match cmp(mid)? {
            Ordering::Equal => return Some(Ok(mid)),
            Ordering::Greater => end = mid,
            Ordering::Less => start = mid + 1,
        }
    }

    Some(Err(start))
}

/// Verifies the expected size of type T, for a type that should only grow if absolutely necessary.
#[allow(unused)]
pub(crate) const fn expect_size_of<T>(expected: usize) {
    let size = std::mem::size_of::<T>();
    if size != expected {
        let _ = [""; 0][size];
    }
}

pub(crate) fn fits_precision<const N: u32>(n: impl Into<i64>) -> bool {
    n.into().unsigned_abs().leading_zeros() >= (i64::BITS - N)
}

/// Decode variant primitive value
pub(crate) fn decode_int8(data: &Buffer) -> Result<i8, ArrowError> {
    Ok(i8::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int16(data: &Buffer) -> Result<i16, ArrowError> {
    Ok(i16::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int32(data: &Buffer) -> Result<i32, ArrowError> {
    Ok(i32::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int64(data: &Buffer) -> Result<i64, ArrowError> {
    Ok(i64::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_decimal4(data: &Buffer) -> Result<(i32, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i32::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_decimal8(data: &Buffer) -> Result<(i64, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i64::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_decimal16(data: &Buffer) -> Result<(i128, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i128::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_float(data: &Buffer) -> Result<f32, ArrowError> {
    Ok(f32::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_double(data: &Buffer) -> Result<f64, ArrowError> {
    Ok(f64::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_date(data: &Buffer) -> Result<NaiveDate, ArrowError> {
    let days_since_epoch = i32::from_le_bytes(array_from_slice(data, 0)?);
    let value = DateTime::UNIX_EPOCH + Duration::days(i64::from(days_since_epoch));
    Ok(value.date_naive())
}

pub(crate) fn decode_timestamp_micros(data: &Buffer) -> Result<DateTime<Utc>, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch).ok_or_else(|| {
        ArrowError::CastError(format!(
            "Could not cast `{micros_since_epoch}` microseconds into a DateTime<Utc>"
        ))
    })
}

pub(crate) fn decode_timestampntz_micros(data: &Buffer) -> Result<NaiveDateTime, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch)
        .ok_or_else(|| {
            ArrowError::CastError(format!(
                "Could not cast `{micros_since_epoch}` microseconds into a NaiveDateTime"
            ))
        })
        .map(|v| v.naive_utc())
}

pub(crate) fn decode_time_ntz(data: &Buffer) -> Result<NaiveTime, ArrowError> {
    let micros_since_epoch = u64::from_le_bytes(array_from_slice(data, 0)?);

    let case_error = ArrowError::CastError(format!(
        "Could not cast {micros_since_epoch} microseconds into a NaiveTime"
    ));

    if micros_since_epoch >= 86_400_000_000 {
        return Err(case_error);
    }

    let nanos_since_midnight = micros_since_epoch * 1_000;
    NaiveTime::from_num_seconds_from_midnight_opt(
        (nanos_since_midnight / 1_000_000_000) as u32,
        (nanos_since_midnight % 1_000_000_000) as u32,
    )
    .ok_or(case_error)
}

pub(crate) fn decode_timestamp_nanos(data: &Buffer) -> Result<DateTime<Utc>, ArrowError> {
    let nanos_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);

    // DateTime::from_timestamp_nanos would never fail
    Ok(DateTime::from_timestamp_nanos(nanos_since_epoch))
}

pub(crate) fn decode_timestampntz_nanos(data: &Buffer) -> Result<NaiveDateTime, ArrowError> {
    decode_timestamp_nanos(data).map(|v| v.naive_utc())
}

pub(crate) fn decode_uuid(data: &Buffer) -> Result<Uuid, ArrowError> {
    Uuid::from_slice(data.slice(0..16).as_ref()).map_err(|_| {
        ArrowError::CastError(format!(
            "Cant decode uuid from {:?}",
            data.slice(0..16).as_ref()
        ))
    })
}

pub(crate) fn decode_binary(data: &Buffer) -> Result<Buffer, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    slice_from_slice_at_offset(data, 4, 0..len)
}

pub(crate) fn decode_string(data: &Buffer) -> Result<Buffer, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    slice_from_slice_at_offset(data, 4, 0..len)
}

pub(crate) fn int_size(v: usize) -> u8 {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x10000..=0xFFFFFF => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod test {}
