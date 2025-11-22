// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Represents a 4-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 32-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is limited to 9 digits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal4 {
    integer: i32,
    scale: u8,
}

/// Represents an 8-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 64-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 10 and 18 digits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal8 {
    integer: i64,
    scale: u8,
}

/// Represents an 16-byte decimal value in the Variant format.
///
/// This struct stores a decimal number using a 128-bit signed integer for the coefficient
/// and an 8-bit unsigned integer for the scale (number of decimal places). Its precision is between 19 and 38 digits.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VariantDecimal16 {
    integer: i128,
    scale: u8,
}