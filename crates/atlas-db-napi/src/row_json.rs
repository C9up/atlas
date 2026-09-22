//! Rendering rows as the JSON array of objects this boundary returns.
//!
//! The shape is `[{"id":1,"name":"a"},…]` and is not negotiable: it is what
//! every caller on the TypeScript side parses. What is negotiable is how it
//! gets written, and that turned out to be most of the cost of a read.
//!
//! The obvious way — collect each row into a `serde_json::Map` and serialise
//! the resulting `Vec<Value>` — allocates a `String` for the column name and
//! clones the value **for every cell**, then a map per row, to build a second
//! copy of data that is already in hand and about to be thrown away. Measured
//! on a 200 000 row read, alternating passes, median of seven: serialising cost
//! 79.9 ms that way and 10.5 ms this way, taking the whole call from 184.0 ms
//! to 114.6 ms. The remaining 104 ms is the decode, which is a different
//! problem in a different crate.
//!
//! Byte-identical to the copy it replaces in what it escapes and how it writes
//! values, and deliberately NOT identical in one thing: column order.
//!
//! `serde_json::Map` is a `BTreeMap` here, so the copy emitted columns **sorted
//! by name**. Nothing chose that — `DbRow.columns` arrives in the order the
//! statement selected, and the map threw that away on the way out. Postgres,
//! MySQL and every client above them hand back fields in select order, so the
//! sort was a deviation nobody asked for and nobody could have relied on
//! deliberately. This writes the columns in the order they were selected.
//!
//! A name that appears twice, as `SELECT a.id, b.id` produces, keeps its first
//! position and takes its last value — which is what assigning twice to one
//! key does in JavaScript, and therefore what every client built on that does.
//!
//! The ordering is worked out once for the result set rather than once per row:
//! every row of one statement carries the same columns in the same order, which
//! is what makes doing it once correct.

use atlas_db::DbRow;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

/// Rows as a JSON array of objects, written straight off the decoded rows.
pub struct RowsAsObjects<'a>(pub &'a [DbRow]);

/// Which cells to write, in which order, for every row of this result set.
///
/// The order the statement selected, with a repeated name reduced to one cell:
/// the position of its first appearance, the value of its last.
fn cell_order(first: &DbRow) -> Vec<usize> {
    let mut order: Vec<usize> = Vec::with_capacity(first.columns.len());
    for (index, (name, _)) in first.columns.iter().enumerate() {
        match order
            .iter()
            .position(|placed| first.columns[*placed].0 == *name)
        {
            Some(slot) => order[slot] = index,
            None => order.push(index),
        }
    }
    order
}

struct RowAsObject<'a> {
    row: &'a DbRow,
    order: &'a [usize],
}

impl Serialize for RowAsObject<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.order.len()))?;
        for index in self.order {
            let Some((name, value)) = self.row.columns.get(*index) else {
                continue;
            };
            map.serialize_entry(&**name, value)?;
        }
        map.end()
    }
}

impl Serialize for RowsAsObjects<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let order = self.0.first().map(cell_order).unwrap_or_default();
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            seq.serialize_element(&RowAsObject { row, order: &order })?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::Arc;

    fn row(columns: &[(&str, Value)]) -> DbRow {
        DbRow {
            columns: columns
                .iter()
                .map(|(name, value)| (Arc::from(*name), value.clone()))
                .collect(),
        }
    }

    /// What the boundary must produce, built independently of the writer under
    /// test: cells in first-seen order, a repeated name taking its last value,
    /// each key and value escaped the way `serde_json` escapes them.
    fn expected(rows: &[DbRow]) -> String {
        let mut out = String::from("[");
        for (position, row) in rows.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push('{');
            let mut names: Vec<&str> = Vec::new();
            for (name, _) in &row.columns {
                if !names.contains(&&**name) {
                    names.push(&**name);
                }
            }
            for (cell, name) in names.iter().enumerate() {
                if cell > 0 {
                    out.push(',');
                }
                let value = row
                    .columns
                    .iter()
                    .rfind(|(candidate, _)| &**candidate == *name)
                    .map(|(_, value)| value)
                    .expect("the name came from these columns");
                out.push_str(
                    &serde_json::to_string(&Value::String((*name).to_string()))
                        .expect("serialises"),
                );
                out.push(':');
                out.push_str(&serde_json::to_string(value).expect("serialises"));
            }
            out.push('}');
        }
        out.push(']');
        out
    }

    fn assert_writes(rows: &[DbRow]) {
        let want = expected(rows);
        assert_eq!(
            serde_json::to_string(&RowsAsObjects(rows)).expect("serialises"),
            want
        );
        assert_eq!(
            sonic_rs::to_string(&RowsAsObjects(rows)).expect("serialises"),
            want
        );
    }

    #[test]
    fn an_ordinary_result_set_is_written_row_by_row() {
        assert_writes(&[
            row(&[("id", json!(1)), ("name", json!("ada"))]),
            row(&[("id", json!(2)), ("name", json!("grace"))]),
        ]);
    }

    /// The regression this module was introduced with: a `BTreeMap` sorted the
    /// columns by name, so a statement selecting them in any other order came
    /// back rearranged.
    #[test]
    fn columns_keep_the_order_the_statement_selected_them_in() {
        let rows = [row(&[
            ("id", json!(1)),
            ("label", json!("a")),
            ("amount", json!("12.34")),
            ("cents", json!(1234)),
        ])];
        assert_eq!(
            serde_json::to_string(&RowsAsObjects(&rows)).expect("serialises"),
            r#"[{"id":1,"label":"a","amount":"12.34","cents":1234}]"#
        );
    }

    /// `SELECT a.id, b.id` — one key, and the value JavaScript would have been
    /// left holding after assigning to it twice.
    #[test]
    fn a_repeated_name_keeps_its_first_position_and_its_last_value() {
        let rows = [row(&[
            ("id", json!(1)),
            ("label", json!("a")),
            ("id", json!(2)),
        ])];
        assert_eq!(
            serde_json::to_string(&RowsAsObjects(&rows)).expect("serialises"),
            r#"[{"id":2,"label":"a"}]"#
        );
        assert_writes(&rows);
    }

    #[test]
    fn every_value_kind_this_boundary_carries_is_written_the_same() {
        assert_writes(&[row(&[
            ("null", Value::Null),
            ("bool", json!(true)),
            ("int", json!(-42)),
            ("big", json!(9_007_199_254_740_991i64)),
            ("float", json!(1.5)),
            ("decimal_as_text", json!("12.34")),
            ("json", json!({"nested": [1, 2, {"deep": true}]})),
        ])]);
    }

    /// The characters a hand-written map writer is most likely to get wrong.
    #[test]
    fn escapes_and_unicode_are_written_the_same() {
        assert_writes(&[row(&[
            ("quote\"and\\slash", json!("tab\there\nnewline")),
            ("unicode", json!("émoji 🦀 and \u{7f}")),
            ("empty", json!("")),
        ])]);
    }

    #[test]
    fn an_empty_result_set_is_an_empty_array() {
        assert_eq!(
            serde_json::to_string(&RowsAsObjects(&[])).expect("serialises"),
            "[]"
        );
    }
}
