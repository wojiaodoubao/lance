// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{array::TryFromSliceError, ops::Range, str};

use arrow_schema::ArrowError;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::slice::SliceIndex;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use uuid::Uuid;

/// Helper for reporting integer overflow errors in a consistent way.
pub(crate) fn overflow_error(msg: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Integer overflow computing {msg}"))
}

#[inline]
pub(crate) fn slice_from_slice<I: SliceIndex<[u8]> + Clone + Debug>(
    bytes: &[u8],
    index: I,
) -> Result<&I::Output, ArrowError> {
    bytes.get(index.clone()).ok_or_else(|| {
        ArrowError::InvalidArgumentError(format!(
            "Tried to extract byte(s) {index:?} from {}-byte buffer",
            bytes.len(),
        ))
    })
}

/// Helper to safely slice bytes with offset calculations.
///
/// Equivalent to `slice_from_slice(bytes, (base_offset + range.start)..(base_offset + range.end))`
/// but using checked addition to prevent integer overflow panics on 32-bit systems.
#[inline]
pub(crate) fn slice_from_slice_at_offset(
    bytes: &[u8],
    base_offset: usize,
    range: Range<usize>,
) -> Result<&[u8], ArrowError> {
    let start_byte = base_offset
        .checked_add(range.start)
        .ok_or_else(|| overflow_error("slice start"))?;
    let end_byte = base_offset
        .checked_add(range.end)
        .ok_or_else(|| overflow_error("slice end"))?;
    slice_from_slice(bytes, start_byte..end_byte)
}

pub(crate) fn array_from_slice<const N: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<[u8; N], ArrowError> {
    slice_from_slice_at_offset(bytes, offset, 0..N)?
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
    let offset_buffer = slice_from_slice_at_offset(slice, offset, range)?;

    //Use simdutf8 by default
    #[cfg(feature = "simdutf8")]
    {
        simdutf8::basic::from_utf8(offset_buffer).map_err(|_| {
            // Use simdutf8::compat to return details about the decoding error
            let e = simdutf8::compat::from_utf8(offset_buffer).unwrap_err();
            ArrowError::InvalidArgumentError(format!("encountered non UTF-8 data: {e}"))
        })
    }

    //Use std::str if simdutf8 is not enabled
    #[cfg(not(feature = "simdutf8"))]
    str::from_utf8(offset_buffer)
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
pub(crate) fn decode_int8(data: &[u8]) -> Result<i8, ArrowError> {
    Ok(i8::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int16(data: &[u8]) -> Result<i16, ArrowError> {
    Ok(i16::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int32(data: &[u8]) -> Result<i32, ArrowError> {
    Ok(i32::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_int64(data: &[u8]) -> Result<i64, ArrowError> {
    Ok(i64::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_decimal4(data: &[u8]) -> Result<(i32, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i32::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_decimal8(data: &[u8]) -> Result<(i64, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i64::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_decimal16(data: &[u8]) -> Result<(i128, u8), ArrowError> {
    let scale = u8::from_le_bytes(array_from_slice(data, 0)?);
    let integer = i128::from_le_bytes(array_from_slice(data, 1)?);
    Ok((integer, scale))
}

pub(crate) fn decode_float(data: &[u8]) -> Result<f32, ArrowError> {
    Ok(f32::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_double(data: &[u8]) -> Result<f64, ArrowError> {
    Ok(f64::from_le_bytes(array_from_slice(data, 0)?))
}

pub(crate) fn decode_date(data: &[u8]) -> Result<NaiveDate, ArrowError> {
    let days_since_epoch = i32::from_le_bytes(array_from_slice(data, 0)?);
    let value = DateTime::UNIX_EPOCH + Duration::days(i64::from(days_since_epoch));
    Ok(value.date_naive())
}

pub(crate) fn decode_timestamp_micros(data: &[u8]) -> Result<DateTime<Utc>, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch).ok_or_else(|| {
        ArrowError::CastError(format!(
            "Could not cast `{micros_since_epoch}` microseconds into a DateTime<Utc>"
        ))
    })
}

pub(crate) fn decode_timestampntz_micros(data: &[u8]) -> Result<NaiveDateTime, ArrowError> {
    let micros_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);
    DateTime::from_timestamp_micros(micros_since_epoch)
        .ok_or_else(|| {
            ArrowError::CastError(format!(
                "Could not cast `{micros_since_epoch}` microseconds into a NaiveDateTime"
            ))
        })
        .map(|v| v.naive_utc())
}

pub(crate) fn decode_time_ntz(data: &[u8]) -> Result<NaiveTime, ArrowError> {
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

pub(crate) fn decode_timestamp_nanos(data: &[u8]) -> Result<DateTime<Utc>, ArrowError> {
    let nanos_since_epoch = i64::from_le_bytes(array_from_slice(data, 0)?);

    // DateTime::from_timestamp_nanos would never fail
    Ok(DateTime::from_timestamp_nanos(nanos_since_epoch))
}

pub(crate) fn decode_timestampntz_nanos(data: &[u8]) -> Result<NaiveDateTime, ArrowError> {
    decode_timestamp_nanos(data).map(|v| v.naive_utc())
}

pub(crate) fn decode_uuid(data: &[u8]) -> Result<Uuid, ArrowError> {
    Uuid::from_slice(&data[0..16])
        .map_err(|_| ArrowError::CastError(format!("Cant decode uuid from {:?}", &data[0..16])))
}

pub(crate) fn decode_binary(data: &[u8]) -> Result<&[u8], ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    slice_from_slice_at_offset(data, 4, 0..len)
}

pub(crate) fn decode_long_string(data: &[u8]) -> Result<&str, ArrowError> {
    let len = u32::from_le_bytes(array_from_slice(data, 0)?) as usize;
    string_from_slice(data, 4, 0..len)
}

#[cfg(test)]
mod test {
    use super::*;
    use paste::paste;

    #[test]
    fn test_fits_precision() {
        assert!(fits_precision::<10>(1023));
        assert!(!fits_precision::<10>(1024));
        assert!(fits_precision::<10>(-1023));
        assert!(!fits_precision::<10>(-1024));
    }

    macro_rules! test_decoder_bounds {
        ($test_name:ident, $data:expr, $decode_fn:ident, $expected:expr) => {
            paste! {
                #[test]
                fn [<$test_name _exact_length>]() {
                    let result = $decode_fn(&$data).unwrap();
                    assert_eq!(result, $expected);
                }

                #[test]
                fn [<$test_name _truncated_length>]() {
                    // Remove the last byte of data so that there is not enough to decode
                    let truncated_data = &$data[.. $data.len() - 1];
                    let result = $decode_fn(truncated_data);
                    assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
                }
            }
        };
    }

    mod integer {
        use super::*;

        test_decoder_bounds!(test_i8, [0x2a], decode_int8, 42);
        test_decoder_bounds!(test_i16, [0xd2, 0x04], decode_int16, 1234);
        test_decoder_bounds!(test_i32, [0x40, 0xe2, 0x01, 0x00], decode_int32, 123456);
        test_decoder_bounds!(
            test_i64,
            [0x15, 0x81, 0xe9, 0x7d, 0xf4, 0x10, 0x22, 0x11],
            decode_int64,
            1234567890123456789
        );
    }

    mod decimal {
        use super::*;

        test_decoder_bounds!(
            test_decimal4,
            [
                0x02, // Scale
                0xd2, 0x04, 0x00, 0x00, // Unscaled Value
            ],
            decode_decimal4,
            (1234, 2)
        );

        test_decoder_bounds!(
            test_decimal8,
            [
                0x02, // Scale
                0xd2, 0x02, 0x96, 0x49, 0x00, 0x00, 0x00, 0x00, // Unscaled Value
            ],
            decode_decimal8,
            (1234567890, 2)
        );

        test_decoder_bounds!(
            test_decimal16,
            [
                0x02, // Scale
                0xd2, 0xb6, 0x23, 0xc0, 0xf4, 0x10, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, // Unscaled Value
            ],
            decode_decimal16,
            (1234567891234567890, 2)
        );
    }

    mod float {
        use super::*;

        test_decoder_bounds!(
            test_float,
            [0x06, 0x2c, 0x93, 0x4e],
            decode_float,
            1234567890.1234
        );

        test_decoder_bounds!(
            test_double,
            [0xc9, 0xe5, 0x87, 0xb4, 0x80, 0x65, 0xd2, 0x41],
            decode_double,
            1234567890.1234
        );
    }

    mod datetime {
        use super::*;

        test_decoder_bounds!(
            test_date,
            [0xe2, 0x4e, 0x0, 0x0],
            decode_date,
            NaiveDate::from_ymd_opt(2025, 4, 16).unwrap()
        );

        test_decoder_bounds!(
            test_timestamp_micros,
            [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
            decode_timestamp_micros,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestampntz_micros,
            [0xe0, 0x52, 0x97, 0xdd, 0xe7, 0x32, 0x06, 0x00],
            decode_timestampntz_micros,
            NaiveDate::from_ymd_opt(2025, 4, 16)
                .unwrap()
                .and_hms_milli_opt(16, 34, 56, 780)
                .unwrap()
        );

        test_decoder_bounds!(
            test_timestamp_nanos,
            [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
            decode_timestamp_nanos,
            NaiveDate::from_ymd_opt(2025, 8, 14)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestamp_nanos_before_epoch,
            [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
            decode_timestamp_nanos,
            NaiveDate::from_ymd_opt(1957, 11, 7)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
                .and_utc()
        );

        test_decoder_bounds!(
            test_timestampntz_nanos,
            [0x15, 0x41, 0xa2, 0x5a, 0x36, 0xa2, 0x5b, 0x18],
            decode_timestampntz_nanos,
            NaiveDate::from_ymd_opt(2025, 8, 14)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
        );

        test_decoder_bounds!(
            test_timestampntz_nanos_before_epoch,
            [0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa],
            decode_timestampntz_nanos,
            NaiveDate::from_ymd_opt(1957, 11, 7)
                .unwrap()
                .and_hms_nano_opt(12, 33, 54, 123456789)
                .unwrap()
        );
    }

    #[test]
    fn test_uuid() {
        let data = [
            0xf2, 0x4f, 0x9b, 0x64, 0x81, 0xfa, 0x49, 0xd1, 0xb7, 0x4e, 0x8c, 0x09, 0xa6, 0xe3,
            0x1c, 0x56,
        ];
        let result = decode_uuid(&data).unwrap();
        assert_eq!(
            Uuid::parse_str("f24f9b64-81fa-49d1-b74e-8c09a6e31c56").unwrap(),
            result
        );
    }

    mod time {
        use super::*;

        test_decoder_bounds!(
            test_timentz,
            [0x53, 0x1f, 0x8e, 0xdf, 0x2, 0, 0, 0],
            decode_time_ntz,
            NaiveTime::from_num_seconds_from_midnight_opt(12340, 567_891_000).unwrap()
        );

        #[test]
        fn test_decode_time_ntz_invalid() {
            let invalid_second = u64::MAX;
            let data = invalid_second.to_le_bytes();
            let result = decode_time_ntz(&data);
            assert!(matches!(result, Err(ArrowError::CastError(_))));
        }
    }

    #[test]
    fn test_binary_exact_length() {
        let data = [
            0x09, 0, 0, 0, // Length of binary data, 4-byte little-endian
            0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe,
        ];
        let result = decode_binary(&data).unwrap();
        assert_eq!(
            result,
            [0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe]
        );
    }

    #[test]
    fn test_binary_truncated_length() {
        let data = [
            0x09, 0, 0, 0, // Length of binary data, 4-byte little-endian
            0x03, 0x13, 0x37, 0xde, 0xad, 0xbe, 0xef, 0xca,
        ];
        let result = decode_binary(&data);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }

    #[test]
    fn test_string_exact_length() {
        let data = [
            0x05, 0, 0, 0, // Length of string, 4-byte little-endian
            b'H', b'e', b'l', b'l', b'o', b'o',
        ];
        let result = decode_long_string(&data).unwrap();
        assert_eq!(result, "Hello");
    }

    #[test]
    fn test_string_truncated_length() {
        let data = [
            0x05, 0, 0, 0, // Length of string, 4-byte little-endian
            b'H', b'e', b'l',
        ];
        let result = decode_long_string(&data);
        assert!(matches!(result, Err(ArrowError::InvalidArgumentError(_))));
    }
}
