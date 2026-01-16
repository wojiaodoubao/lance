// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
// NOTE: Keep this module warning-clean; avoid `#![allow(unused)]`.

use crate::partition::PartitionField;
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use arrow_schema::{DataType, FieldRef, Fields};
use lance::deps::datafusion::common::ScalarValue;
use lance::deps::datafusion::logical_expr::{Expr, Operator};
use lance_arrow::{LANCE_FIELD_ID_META_KEY, SchemaExt};
use lance_core::Error;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Truthiness {
    AlwaysTrue,
    AlwaysFalse,
    Unknown,
}

impl Truthiness {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::AlwaysTrue, Self::AlwaysTrue) => Self::AlwaysTrue,
            (_, _) => Self::AlwaysFalse,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::Unknown, _) | (_, Self::Unknown) => Self::Unknown,
            (Self::AlwaysFalse, Self::AlwaysFalse) => Self::AlwaysFalse,
            (_, _) => Self::AlwaysTrue,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::AlwaysTrue => Self::AlwaysFalse,
            Self::AlwaysFalse => Self::AlwaysTrue,
            Self::Unknown => Self::Unknown,
        }
    }
}

/// Return `True` if the input cdf is always false.
pub fn is_cdf_always_false(schema: &ArrowSchema, cdf: &Vec<Vec<Expr>>) -> bool {
    for clause in cdf {
        // If any clause is not provably false, then the whole CDF is not const-false.
        // (We are conservative: UNKNOWN => not const-false.)
        let mut clause_truth = Truthiness::AlwaysTrue;
        for atom in clause {
            let atom_t = expr_truthiness(schema, atom);
            clause_truth = match (clause_truth, atom_t) {
                (Truthiness::AlwaysFalse, _) | (_, Truthiness::AlwaysFalse) => {
                    Truthiness::AlwaysFalse
                }
                (Truthiness::AlwaysTrue, Truthiness::AlwaysTrue) => Truthiness::AlwaysTrue,
                _ => Truthiness::Unknown,
            };
            if clause_truth == Truthiness::AlwaysFalse {
                break;
            }
        }

        if clause_truth != Truthiness::AlwaysFalse {
            return false;
        }
    }
    true
}

/// Static analyze of expr. The result is always_true, always_false or unknown.
/// If the column doesn't exist in schema, it is treated as value null.
pub fn expr_truthiness(schema: &ArrowSchema, expr: &Expr) -> Truthiness {
    match expr {
        Expr::Literal(v, _) => match v {
            ScalarValue::Boolean(Some(true)) => Truthiness::AlwaysTrue,
            ScalarValue::Boolean(Some(false)) => Truthiness::AlwaysFalse,
            _ if v.is_null() => Truthiness::AlwaysFalse,
            _ => Truthiness::Unknown,
        },
        Expr::Column(_) if expr_is_always_null(schema, expr) => Truthiness::AlwaysFalse,
        Expr::IsNull(e) if expr_is_always_null(schema, e.as_ref()) => Truthiness::AlwaysTrue,
        Expr::IsNotNull(e) if expr_is_always_null(schema, e.as_ref()) => Truthiness::AlwaysFalse,
        Expr::Not(e) => expr_truthiness(schema, e).not(),
        Expr::BinaryExpr(b) if b.op == Operator::And => {
            expr_truthiness(schema, &b.left).and(expr_truthiness(schema, &b.right))
        }
        Expr::BinaryExpr(b) if b.op == Operator::Or => {
            expr_truthiness(schema, &b.left).or(expr_truthiness(schema, &b.right))
        }
        Expr::BinaryExpr(b)
            if is_comparison_op(b.op)
                && (expr_is_always_null(schema, &b.left)
                    || expr_is_always_null(schema, &b.right)) =>
        {
            Truthiness::AlwaysFalse
        }
        Expr::Between(b) if expr_is_always_null(schema, &b.expr) => Truthiness::AlwaysFalse,
        Expr::InList(inlist) if expr_is_always_null(schema, &inlist.expr) => {
            Truthiness::AlwaysFalse
        }
        _ => Truthiness::Unknown,
    }
}

fn expr_is_always_null(schema: &ArrowSchema, expr: &Expr) -> bool {
    match expr {
        Expr::Literal(v, _) => v.is_null(),
        Expr::Column(c) => !schema.fields().iter().any(|f| f.name() == &c.name),
        Expr::Cast(c) => expr_is_always_null(schema, &c.expr),
        Expr::TryCast(c) => expr_is_always_null(schema, &c.expr),
        _ => false,
    }
}

pub fn is_comparison_op(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
    )
}

/// Convert boolean expression into a conservative CDF (OR-of-ANDs) form.
///
/// The returned structure is `Vec<Vec<Expr>>`, where outer `Vec` is OR, and
/// inner `Vec` is AND of atomic predicates.
pub fn expr_to_cdf(expr: &Expr) -> Vec<Vec<Expr>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let left = expr_to_cdf(&binary.left);
            let right = expr_to_cdf(&binary.right);
            let mut out = Vec::new();
            for l in left {
                for r in &right {
                    let mut clause = Vec::with_capacity(l.len() + r.len());
                    clause.extend(l.iter().cloned());
                    clause.extend(r.iter().cloned());
                    out.push(clause);
                }
            }
            out
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let mut left = expr_to_cdf(&binary.left);
            let mut right = expr_to_cdf(&binary.right);
            left.append(&mut right);
            left
        }
        _ => vec![vec![expr.clone()]],
    }
}

/// Sanity check of table spec.
///
/// 1. Schema field must have id.
/// 2. Partition field source_ids must not be empty.
/// 3. Partition field source id must exist in schema.
/// 4. Exactly one of transform or expression must be set.
/// 5. Partition field signature should be unique.
/// 6. Partition field id should be unique.
pub fn check_table_spec_consistency(
    schema: &ArrowSchema,
    partition_spec: &Vec<PartitionField>,
) -> lance_core::Result<()> {
    // Error if any field doesn't have id.
    schema.max_id(false)?;

    // Sanity check
    let mut existed_fields = HashSet::new();
    let mut existed_ids = HashSet::new();
    for f in partition_spec {
        if f.source_ids.is_empty() {
            return Err(Error::invalid_input(
                "partition field source_ids must not be empty",
            ));
        }
        for id in &f.source_ids {
            if *id < 0 || schema.path_and_field_by_id(*id)?.is_none() {
                return Err(Error::invalid_input(format!(
                    "partition source id {} not found in schema",
                    *id
                )));
            }
        }

        let has_transform = f.transform.is_some();
        let has_expression = f
            .expression
            .as_ref()
            .map(|e| !e.trim().is_empty())
            .unwrap_or(false);
        if has_transform == has_expression {
            return Err(Error::invalid_input(
                "Exactly one of transform or expression must be set",
            ));
        }
        if !existed_fields.insert(f.signature()) || !existed_ids.insert(f.field_id.clone()) {
            return Err(Error::invalid_input(
                "Partition fields signature and field_id should be unique.",
            ));
        }
    }

    Ok(())
}

/// Ensure all fields have id by setting id to field if it doesn't have one.
pub fn ensure_all_schema_field_have_id(schema: ArrowSchema) -> lance_core::Result<ArrowSchema> {
    // Find next id.
    let max_id = schema.max_id(true)?.unwrap_or(-1);
    let mut next_id = max_id + 1;

    // Rebuild schema and set id
    let mut out: Vec<ArrowField> = Vec::with_capacity(schema.fields().len());
    for f in schema.fields().iter() {
        out.push(set_field_id(f.as_ref().clone(), &mut next_id)?);
    }
    Ok(ArrowSchema::new_with_metadata(
        out,
        schema.metadata().clone(),
    ))
}

/// Set field if for input arrow field.
fn set_field_id(field: ArrowField, next_id: &mut i32) -> lance_core::Result<ArrowField> {
    // Set id to field
    let mut field = if field.metadata().get(LANCE_FIELD_ID_META_KEY).is_some() {
        field
    } else {
        let id = *next_id;
        *next_id += 1;

        let mut md = field.metadata().clone();
        md.insert(LANCE_FIELD_ID_META_KEY.to_string(), id.to_string());

        field.with_metadata(md)
    };

    // Set id to children
    let new_dt = match field.data_type() {
        DataType::Struct(fields) => {
            let mut out: Vec<FieldRef> = Vec::with_capacity(fields.len());
            for child in fields.iter() {
                let child = set_field_id(child.as_ref().clone(), next_id)?;
                out.push(Arc::new(child));
            }
            DataType::Struct(Fields::from(out))
        }
        DataType::List(child) => {
            let child = set_field_id(child.as_ref().clone(), next_id)?;
            DataType::List(Arc::new(child))
        }
        DataType::LargeList(child) => {
            let child = set_field_id(child.as_ref().clone(), next_id)?;
            DataType::LargeList(Arc::new(child))
        }
        DataType::FixedSizeList(child, n) => {
            let child = set_field_id(child.as_ref().clone(), next_id)?;
            DataType::FixedSizeList(Arc::new(child), *n)
        }
        DataType::Map(child, sorted) => {
            let child = set_field_id(child.as_ref().clone(), next_id)?;
            DataType::Map(Arc::new(child), *sorted)
        }
        other => other.clone(),
    };
    field = field.with_data_type(new_dt);

    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
    use arrow_schema::DataType;
    use lance::deps::datafusion::prelude::{col, lit};

    #[test]
    fn test_expr_truthiness() {
        let schema_empty = ArrowSchema::new(Vec::<ArrowField>::new());
        let schema_with_a = ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, true)]);

        // Expr::Literal
        assert_eq!(
            expr_truthiness(&schema_empty, &lit(true)),
            Truthiness::AlwaysTrue
        );
        assert_eq!(
            expr_truthiness(&schema_empty, &lit(false)),
            Truthiness::AlwaysFalse
        );
        assert_eq!(
            expr_truthiness(
                &schema_empty,
                &Expr::Literal(ScalarValue::Int32(None), None)
            ),
            Truthiness::AlwaysFalse
        );
        assert_eq!(
            expr_truthiness(
                &schema_empty,
                &Expr::Literal(ScalarValue::Int32(Some(1)), None)
            ),
            Truthiness::Unknown
        );

        // Expr::Column (missing in schema)
        assert_eq!(
            expr_truthiness(&schema_with_a, &col("missing")),
            Truthiness::AlwaysFalse
        );

        // Expr::IsNull / Expr::IsNotNull
        assert_eq!(
            expr_truthiness(&schema_with_a, &Expr::IsNull(Box::new(col("missing")))),
            Truthiness::AlwaysTrue
        );
        assert_eq!(
            expr_truthiness(&schema_with_a, &Expr::IsNotNull(Box::new(col("missing")))),
            Truthiness::AlwaysFalse
        );

        // Expr::Not
        assert_eq!(
            expr_truthiness(&schema_empty, &Expr::Not(Box::new(lit(true)))),
            Truthiness::AlwaysFalse
        );

        // Expr::BinaryExpr
        assert_eq!(
            expr_truthiness(&schema_empty, &lit(true).and(lit(false))),
            Truthiness::AlwaysFalse
        );
        assert_eq!(
            expr_truthiness(&schema_empty, &lit(false).or(lit(false))),
            Truthiness::AlwaysFalse
        );

        // Expr::BinaryExpr
        assert_eq!(
            expr_truthiness(&schema_with_a, &col("missing").eq(lit(1))),
            Truthiness::AlwaysFalse
        );

        // Expr::Between
        assert_eq!(
            expr_truthiness(&schema_with_a, &col("missing").between(lit(1), lit(2))),
            Truthiness::AlwaysFalse
        );

        // Expr::InList
        assert_eq!(
            expr_truthiness(
                &schema_with_a,
                &col("missing").in_list(vec![lit(1), lit(2)], false)
            ),
            Truthiness::AlwaysFalse
        );

        // Default case: unknown
        assert_eq!(
            expr_truthiness(&schema_with_a, &col("a")),
            Truthiness::Unknown
        );
    }

    #[test]
    fn test_is_cdf_always_false() {
        let schema_with_a = ArrowSchema::new(vec![ArrowField::new("a", DataType::Int32, true)]);

        // Always false: every clause is provably false.
        let cdf_always_false = vec![vec![lit(false)], vec![col("missing")]];
        assert!(is_cdf_always_false(&schema_with_a, &cdf_always_false));

        // Not always false: at least one clause is not provably false.
        let cdf_not_always_false = vec![vec![lit(false)], vec![lit(true)]];
        assert!(!is_cdf_always_false(&schema_with_a, &cdf_not_always_false));
    }
}
