//! DML compilers: INSERT, UPDATE, DELETE.
//!
//! Emits `$N` placeholders (consistent with the existing SELECT compiler).

use crate::builder::{
    compile_query_with_dialect, remap_placeholders, wrap_dml_with_ctes, CompileResult,
    CteDefinition, QueryDescription,
};
use crate::dialect::Dialect;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InsertSpec {
    pub table: String,
    /// Single-row form — column → value pairs. Insertion order preserved.
    /// Kept for backward compat with the original story 30.3 call sites.
    #[serde(default)]
    pub values: Vec<(String, Value)>,
    /// Multi-row form — each row is its own `Vec<(column, value)>`. Every row
    /// MUST have the same column set (validated on the TS side). Introduced by
    /// Story 30.1 for `multiInsert`.
    #[serde(default)]
    pub rows: Vec<Vec<(String, Value)>>,
    /// Optional `RETURNING` suffix (postgres + sqlite). Mysql callers must
    /// fall back to `LAST_INSERT_ID()` at the driver level.
    #[serde(default)]
    pub returning: Vec<String>,
    /// Per-column Postgres cast hints (column → logical type, e.g. `"timestamp"`,
    /// `"uuid"`). Applied as `$N::<pgtype>` ONLY on Postgres: sqlx binds JS
    /// strings as `text`, which Postgres won't implicitly coerce to
    /// timestamp/uuid/date. No-op on SQLite/MySQL.
    #[serde(default)]
    pub casts: HashMap<String, String>,
    /// Leading CTEs — Lucid `with().insert()`. Rendered as `WITH … INSERT …`.
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
}

/// A SET expression in an UPDATE statement.
///
/// The default form is a plain value binding (`SET col = ?`). The alternative
/// `ColumnPlusValue` form emits `SET col = col + ?`, used for atomic increments
/// and decrements — never read-modify-write, no race condition. The JSON
/// encoding is untagged for backward compat with plain value arrays.
///
/// `Expression` MUST come first: `serde(untagged)` tries variants in order, and
/// `Value(serde_json::Value)` greedily matches ANY JSON — including the
/// `{ op, value }` object — so with `Value` first the `Expression` arm was
/// unreachable and every `increment`/`decrement` silently stored the raw JSON
/// object instead of emitting `col = col + ?`. A plain scalar/array/other object
/// fails the `{ op, value }` struct shape and falls through to `Value`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SetValue {
    /// `SET col = col + ?` (or `- ?`). Emitted for atomic increments/decrements.
    Expression {
        /// One of `"increment"` | `"decrement"`.
        op: String,
        /// The amount to add or subtract.
        value: Value,
    },
    /// `SET col = ?` — literal value binding.
    Value(Value),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSpec {
    pub table: String,
    /// Column → set-value pairs. Accepts plain values or column expressions.
    pub set: Vec<(String, SetValue)>,
    #[serde(default)]
    pub wheres: Vec<Value>,
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
    /// Per-column Postgres cast hints — see `InsertSpec::casts`.
    #[serde(default)]
    pub casts: HashMap<String, String>,
    /// Leading CTEs — Lucid `with().update()`. Rendered as `WITH … UPDATE …`.
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteSpec {
    pub table: String,
    #[serde(default)]
    pub wheres: Vec<Value>,
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
    /// Per-column Postgres cast hints applied to WHERE params (e.g. a `uuid` PK
    /// in `DELETE … WHERE id = $1::uuid`). Mirrors `InsertSpec::casts`.
    #[serde(default)]
    pub casts: HashMap<String, String>,
    /// Leading CTEs — Lucid `with().delete()`. Rendered as `WITH … DELETE …`.
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
}

/// UPSERT spec — dialect-dispatched `INSERT ... ON CONFLICT DO UPDATE / DO NOTHING`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertSpec {
    pub table: String,
    /// One or more rows to upsert (all rows must share the same column set).
    pub rows: Vec<Vec<(String, Value)>>,
    /// Conflict target — columns or constraint name to match on.
    pub conflict_columns: Vec<String>,
    /// Columns to update when a conflict fires. Empty = `DO NOTHING`.
    pub update_columns: Vec<String>,
    /// Custom update assignments — Lucid `merge({ col: value | db.raw(...) })`.
    /// Takes precedence over `update_columns`: each item is `col = <bound value>`
    /// or `col = <raw sql>`.
    #[serde(default)]
    pub update_set: Vec<UpdateSetItem>,
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
    /// Per-column Postgres cast hints — see `InsertSpec::casts`.
    #[serde(default)]
    pub casts: HashMap<String, String>,
    /// Leading CTEs — Lucid `with().insert().onConflict(...)`.
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
}

/// One `col = value | raw` assignment in a custom merge (`merge({...})`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSetItem {
    pub column: String,
    /// A bound value (mutually exclusive with `raw`).
    #[serde(default)]
    pub value: Option<Value>,
    /// A verbatim SQL expression, e.g. `users.login_count + 1` (from `db.raw`).
    #[serde(default)]
    pub raw: Option<String>,
    /// Bindings for `?` placeholders inside `raw` — `db.raw('count + ?', [1])`.
    #[serde(default)]
    pub raw_params: Vec<Value>,
}

pub fn compile_insert(spec: &InsertSpec, dialect: Dialect) -> Result<CompileResult, String> {
    // Collect the rows: either the explicit `rows` field (multi-insert) or
    // fall back to wrapping `values` as a single row for backward compat.
    let rows_src: Vec<&Vec<(String, Value)>> = if !spec.rows.is_empty() {
        spec.rows.iter().collect()
    } else if !spec.values.is_empty() {
        vec![&spec.values]
    } else {
        return Err("INSERT requires at least one row".into());
    };

    // Validate all rows share the same column set (order matters — we assume
    // the TS side emits rows in a canonical order).
    let first_cols: Vec<&String> = rows_src[0].iter().map(|(c, _)| c).collect();
    for (ri, row) in rows_src.iter().enumerate().skip(1) {
        let cols: Vec<&String> = row.iter().map(|(c, _)| c).collect();
        if cols != first_cols {
            return Err(format!(
                "multi-row INSERT requires all rows to share the same columns — row {} differs",
                ri
            ));
        }
    }

    let table = dialect.quote_ident(&spec.table)?;
    let col_sql: Result<Vec<String>, String> = first_cols.iter()
        .map(|c| dialect.quote_ident(c))
        .collect();

    let mut params: Vec<Value> = Vec::with_capacity(rows_src.len() * first_cols.len());
    let mut idx = 1u32;
    let mut value_groups: Vec<String> = Vec::with_capacity(rows_src.len());
    for row in &rows_src {
        let placeholders: Vec<String> = row.iter().map(|(col, v)| {
            params.push(v.clone());
            let p = dialect.placeholder(idx);
            idx += 1;
            match spec.casts.get(col).and_then(|t| dialect.cast_for(t)) {
                Some(cast) => format!("{p}::{cast}"),
                None => p,
            }
        }).collect();
        value_groups.push(format!("({})", placeholders.join(", ")));
    }

    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES {}",
        table,
        col_sql?.join(", "),
        value_groups.join(", ")
    );

    // RETURNING suffix — postgres + sqlite support it; mysql rejects.
    if !spec.returning.is_empty() {
        match dialect {
            Dialect::Mysql => return Err("MySQL does not support RETURNING — use lastInsertRowid".into()),
            _ => {
                let returning_cols: Result<Vec<String>, String> = spec.returning.iter()
                    .map(|c| dialect.quote_ident(c))
                    .collect();
                sql.push_str(&format!(" RETURNING {}", returning_cols?.join(", ")));
            }
        }
    }

    wrap_dml_with_ctes(CompileResult { sql, params }, &spec.ctes, dialect)
}

/// Compile an UPSERT statement — dialect-dispatched.
/// - postgres + sqlite: `INSERT ... ON CONFLICT (cols) DO UPDATE SET ... = EXCLUDED....`
/// - mysql: `INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)...`
pub fn compile_upsert(spec: &UpsertSpec, dialect: Dialect) -> Result<CompileResult, String> {
    if spec.rows.is_empty() {
        return Err("UPSERT requires at least one row".into());
    }
    // Reuse insert compilation as the base.
    let insert_spec = InsertSpec {
        table: spec.table.clone(),
        rows: spec.rows.clone(),
        casts: spec.casts.clone(),
        ..Default::default()
    };
    let base = compile_insert(&insert_spec, dialect)?;
    let mut sql = base.sql;
    let mut params = base.params;

    // Build the DO-UPDATE SET body. Custom merge values (Lucid
    // `merge({ col: value | db.raw(...) })`) take precedence over the plain
    // EXCLUDED/VALUES column list; their bound values continue the placeholder
    // sequence after the INSERT values.
    let set_body: Option<String> = if !spec.update_set.is_empty() {
        let mut idx = params.len() as u32 + 1;
        let parts: Result<Vec<String>, String> = spec
            .update_set
            .iter()
            .map(|item| {
                let c = dialect.quote_ident(&item.column)?;
                if let Some(raw) = &item.raw {
                    if item.raw_params.is_empty() {
                        return Ok(format!("{} = {}", c, raw));
                    }
                    // Rewrite `?` in the raw expression to dialect placeholders,
                    // appending its bindings in order (`db.raw('count + ?', [1])`).
                    let mut rewritten = String::with_capacity(raw.len());
                    let mut binding_iter = item.raw_params.iter();
                    for ch in raw.chars() {
                        if ch == '?' {
                            let v = binding_iter.next().ok_or_else(|| {
                                "merge raw expression has more '?' placeholders than bindings"
                                    .to_string()
                            })?;
                            params.push(v.clone());
                            rewritten.push_str(&dialect.placeholder(idx));
                            idx += 1;
                        } else {
                            rewritten.push(ch);
                        }
                    }
                    Ok(format!("{} = {}", c, rewritten))
                } else {
                    let ph = dialect.placeholder(idx);
                    idx += 1;
                    params.push(item.value.clone().unwrap_or(Value::Null));
                    Ok(format!("{} = {}", c, ph))
                }
            })
            .collect();
        Some(parts?.join(", "))
    } else if !spec.update_columns.is_empty() {
        let parts: Result<Vec<String>, String> = spec
            .update_columns
            .iter()
            .map(|col| {
                let c = dialect.quote_ident(col)?;
                match dialect {
                    Dialect::Mysql => Ok(format!("{} = VALUES({})", c, c)),
                    _ => Ok(format!("{} = EXCLUDED.{}", c, c)),
                }
            })
            .collect();
        Some(parts?.join(", "))
    } else {
        None
    };

    match dialect {
        Dialect::Mysql => match &set_body {
            // MySQL has no DO NOTHING. A self-assign would double-count
            // affected_rows; `INSERT IGNORE` gives the correct no-op semantic.
            None => {
                if sql.starts_with("INSERT INTO") {
                    sql.replace_range(.."INSERT INTO".len(), "INSERT IGNORE INTO");
                }
            }
            Some(set) => sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", set)),
        },
        _ => {
            // postgres + sqlite
            let conflict_cols: Result<Vec<String>, String> = spec
                .conflict_columns
                .iter()
                .map(|c| dialect.quote_ident(c))
                .collect();
            sql.push_str(&format!(" ON CONFLICT ({})", conflict_cols?.join(", ")));
            match &set_body {
                None => sql.push_str(" DO NOTHING"),
                Some(set) => sql.push_str(&format!(" DO UPDATE SET {}", set)),
            }
        }
    }

    if !spec.returning.is_empty() {
        if matches!(dialect, Dialect::Mysql) {
            return Err("MySQL does not support RETURNING on upsert".into());
        }
        let returning_cols: Result<Vec<String>, String> = spec.returning.iter()
            .map(|c| dialect.quote_ident(c))
            .collect();
        sql.push_str(&format!(" RETURNING {}", returning_cols?.join(", ")));
    }

    wrap_dml_with_ctes(CompileResult { sql, params }, &spec.ctes, dialect)
}

pub fn compile_update(spec: &UpdateSpec, dialect: Dialect) -> Result<CompileResult, String> {
    if spec.set.is_empty() {
        return Err("UPDATE requires at least one SET assignment".into());
    }
    let table = dialect.quote_ident(&spec.table)?;
    let mut params: Vec<Value> = Vec::new();
    let mut idx = 1u32;

    let set_parts: Result<Vec<String>, String> = spec.set.iter().map(|(col, set_value)| {
        let c = dialect.quote_ident(col)?;
        match set_value {
            SetValue::Value(v) => {
                params.push(v.clone());
                let ph = dialect.placeholder(idx);
                idx += 1;
                let ph = match spec.casts.get(col).and_then(|t| dialect.cast_for(t)) {
                    Some(cast) => format!("{ph}::{cast}"),
                    None => ph,
                };
                Ok(format!("{} = {}", c, ph))
            }
            SetValue::Expression { op, value } => {
                // Atomic increment/decrement: `SET col = col + ?` — no read-modify-write,
                // no race condition. The column reference is the already-quoted `c`.
                let sql_op = match op.as_str() {
                    "increment" => "+",
                    "decrement" => "-",
                    other => return Err(format!("Unknown SET expression op: '{}'. Expected 'increment' or 'decrement'.", other)),
                };
                params.push(value.clone());
                let s = format!("{} = {} {} {}", c, c, sql_op, dialect.placeholder(idx));
                idx += 1;
                Ok(s)
            }
        }
    }).collect();

    let mut sql = format!("UPDATE {} SET {}", table, set_parts?.join(", "));
    append_wheres(&mut sql, &mut params, &mut idx, &spec.table, &spec.wheres, dialect, &spec.casts)?;
    append_returning(&mut sql, &spec.returning, dialect)?;
    wrap_dml_with_ctes(CompileResult { sql, params }, &spec.ctes, dialect)
}

pub fn compile_delete(spec: &DeleteSpec, dialect: Dialect) -> Result<CompileResult, String> {
    let table = dialect.quote_ident(&spec.table)?;
    let mut params: Vec<Value> = Vec::new();
    let mut idx = 1u32;
    let mut sql = format!("DELETE FROM {}", table);
    append_wheres(&mut sql, &mut params, &mut idx, &spec.table, &spec.wheres, dialect, &spec.casts)?;
    append_returning(&mut sql, &spec.returning, dialect)?;
    wrap_dml_with_ctes(CompileResult { sql, params }, &spec.ctes, dialect)
}

fn append_returning(sql: &mut String, returning: &[String], dialect: Dialect) -> Result<(), String> {
    if returning.is_empty() { return Ok(()); }
    if matches!(dialect, Dialect::Mysql) {
        return Err("MySQL does not support RETURNING".into());
    }
    let cols: Result<Vec<String>, String> = returning.iter()
        .map(|c| dialect.quote_ident(c))
        .collect();
    sql.push_str(&format!(" RETURNING {}", cols?.join(", ")));
    Ok(())
}

/// Compile a DML WHERE clause by REUSING the SELECT compiler's full WHERE
/// grammar (standard / raw / exists / group / inSub). We compile a throwaway
/// `SELECT * FROM <table> WHERE <wheres>` and splice its WHERE body in, with
/// placeholders remapped to continue from `idx`. This gives update/delete the
/// same predicates as reads — correlated EXISTS and sub-queries included — with
/// ONE source of truth for WHERE lowering (per-column casts, operators, raw).
fn append_wheres(
    sql: &mut String,
    params: &mut Vec<Value>,
    idx: &mut u32,
    table: &str,
    wheres: &[Value],
    dialect: Dialect,
    casts: &HashMap<String, String>,
) -> Result<(), String> {
    if wheres.is_empty() {
        return Ok(());
    }
    let fake = QueryDescription {
        table: table.to_string(),
        from_subquery: None,
        select: vec!["*".to_string()],
        wheres: wheres.to_vec(),
        order_by: vec![],
        group_by: vec![],
        having: vec![],
        limit: None,
        offset: None,
        distinct: false,
        distinct_on: vec![],
        ctes: vec![],
        unions: vec![],
        select_subqueries: vec![],
        select_raw: vec![],
        joins: vec![],
        lock_mode: None,
        casts: casts.clone(),
    };
    let sub = compile_query_with_dialect(&fake, dialect)?;
    // The fake compiles to `SELECT * FROM "table" WHERE <body>` — splice the body.
    // If every clause was a no-op (e.g. an empty group), there's no WHERE at all.
    let body = match sub.sql.split_once(" WHERE ") {
        Some((_, rest)) => rest,
        None => return Ok(()),
    };
    let remapped = remap_placeholders(body, &sub.params, params, idx);
    sql.push_str(" WHERE ");
    sql.push_str(&remapped);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn insert_basic() {
        let spec = InsertSpec {
            table: "users".into(),
            values: vec![
                ("name".into(), json!("Alice")),
                ("age".into(), json!(30)),
            ],
            ..Default::default()
        };
        let r = compile_insert(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "INSERT INTO \"users\" (\"name\", \"age\") VALUES (?, ?)");
        assert_eq!(r.params, vec![json!("Alice"), json!(30)]);
    }

    #[test]
    fn insert_mysql_backticks() {
        let spec = InsertSpec {
            table: "users".into(),
            values: vec![("name".into(), json!("Bob"))],
            ..Default::default()
        };
        let r = compile_insert(&spec, Dialect::Mysql).unwrap();
        assert_eq!(r.sql, "INSERT INTO `users` (`name`) VALUES (?)");
    }

    #[test]
    fn insert_rejects_empty() {
        let spec = InsertSpec { table: "users".into(), values: vec![],
            ..Default::default()
        };
        assert!(compile_insert(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn update_with_where() {
        let spec = UpdateSpec {
            table: "users".into(),
            set: vec![("name".into(), SetValue::Value(json!("Carol")))],
            wheres: vec![json!({ "column": "id", "operator": "=", "value": 5, "type": "and" })],
            ..Default::default()
        };
        let r = compile_update(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "UPDATE \"users\" SET \"name\" = ? WHERE \"id\" = ?");
        assert_eq!(r.params, vec![json!("Carol"), json!(5)]);
    }

    #[test]
    fn delete_with_in() {
        let spec = DeleteSpec {
            table: "users".into(),
            wheres: vec![
                json!({ "column": "status", "operator": "IN", "value": ["banned", "deleted"], "type": "and" }),
            ],
            ..Default::default()
        };
        let r = compile_delete(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "DELETE FROM \"users\" WHERE \"status\" IN (?, ?)");
        assert_eq!(r.params.len(), 2);
    }

    #[test]
    fn delete_no_where() {
        let spec = DeleteSpec { table: "logs".into(), wheres: vec![],
            ..Default::default()
        };
        let r = compile_delete(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "DELETE FROM \"logs\"");
    }

    #[test]
    fn update_with_increment_expression() {
        let spec = UpdateSpec {
            table: "accounts".into(),
            set: vec![("balance".into(), SetValue::Expression { op: "increment".into(), value: json!(10) })],
            wheres: vec![json!({ "column": "id", "operator": "=", "value": 1, "type": "and" })],
            ..Default::default()
        };
        let r = compile_update(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "UPDATE \"accounts\" SET \"balance\" = \"balance\" + ? WHERE \"id\" = ?");
        assert_eq!(r.params, vec![json!(10), json!(1)]);
    }

    #[test]
    fn update_with_decrement_expression() {
        let spec = UpdateSpec {
            table: "accounts".into(),
            set: vec![("credit".into(), SetValue::Expression { op: "decrement".into(), value: json!(5) })],
            wheres: vec![],
            ..Default::default()
        };
        let r = compile_update(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "UPDATE \"accounts\" SET \"credit\" = \"credit\" - ?");
    }

    #[test]
    fn set_value_json_disambiguates_expression_from_value() {
        // Regression: with `Value` first in the untagged enum, `{op, value}`
        // deserialized as a plain JSON Value and `increment` stored the raw
        // object instead of emitting `col = col + ?`. Deserialize from JSON
        // (the real NAPI boundary) — the Expression arm must win.
        let spec: UpdateSpec = serde_json::from_value(json!({
            "table": "accounts",
            "set": [["balance", { "op": "increment", "value": 10 }]],
            "wheres": [],
        }))
        .unwrap();
        let r = compile_update(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "UPDATE \"accounts\" SET \"balance\" = \"balance\" + ?");
        assert_eq!(r.params, vec![json!(10)]);

        // A plain scalar SET value still deserializes as `Value` — `col = ?`.
        let scalar: UpdateSpec = serde_json::from_value(json!({
            "table": "accounts",
            "set": [["name", "Ada"]],
            "wheres": [],
        }))
        .unwrap();
        let rs = compile_update(&scalar, Dialect::Sqlite).unwrap();
        assert_eq!(rs.sql, "UPDATE \"accounts\" SET \"name\" = ?");
        assert_eq!(rs.params, vec![json!("Ada")]);
    }

    #[test]
    fn update_rejects_unknown_set_expression_op() {
        let spec = UpdateSpec {
            table: "accounts".into(),
            set: vec![("balance".into(), SetValue::Expression { op: "nuke".into(), value: json!(10) })],
            wheres: vec![],
            ..Default::default()
        };
        assert!(compile_update(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn multi_insert_one_statement() {
        let spec = InsertSpec {
            table: "users".into(),
            rows: vec![
                vec![("name".into(), json!("A")), ("age".into(), json!(1))],
                vec![("name".into(), json!("B")), ("age".into(), json!(2))],
            ],
            ..Default::default()
        };
        let r = compile_insert(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.sql, "INSERT INTO \"users\" (\"name\", \"age\") VALUES (?, ?), (?, ?)");
        assert_eq!(r.params.len(), 4);
    }

    #[test]
    fn multi_insert_rejects_mismatched_columns() {
        let spec = InsertSpec {
            table: "users".into(),
            rows: vec![
                vec![("name".into(), json!("A"))],
                vec![("age".into(), json!(1))],
            ],
            ..Default::default()
        };
        assert!(compile_insert(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn insert_with_returning_postgres() {
        let spec = InsertSpec {
            table: "users".into(),
            values: vec![("name".into(), json!("A"))],
            returning: vec!["id".into(), "created_at".into()],
            ..Default::default()
        };
        let r = compile_insert(&spec, Dialect::Postgres).unwrap();
        assert!(r.sql.contains("RETURNING \"id\", \"created_at\""));
    }

    #[test]
    fn insert_returning_rejected_on_mysql() {
        let spec = InsertSpec {
            table: "users".into(),
            values: vec![("name".into(), json!("A"))],
            returning: vec!["id".into()],
            ..Default::default()
        };
        assert!(compile_insert(&spec, Dialect::Mysql).is_err());
    }

    #[test]
    fn update_with_returning_sqlite() {
        let spec = UpdateSpec {
            table: "users".into(),
            set: vec![("name".into(), SetValue::Value(json!("X")))],
            wheres: vec![],
            returning: vec!["id".into()],
            casts: Default::default(),
            ctes: vec![],
        };
        let r = compile_update(&spec, Dialect::Sqlite).unwrap();
        assert!(r.sql.ends_with("RETURNING \"id\""));
    }

    #[test]
    fn delete_with_returning_postgres() {
        let spec = DeleteSpec {
            table: "users".into(),
            wheres: vec![],
            returning: vec!["id".into()],
            ..Default::default()
        };
        let r = compile_delete(&spec, Dialect::Postgres).unwrap();
        assert!(r.sql.ends_with("RETURNING \"id\""));
    }

    #[test]
    fn upsert_postgres_do_update() {
        let spec = UpsertSpec {
            table: "users".into(),
            rows: vec![vec![("email".into(), json!("a@b")), ("name".into(), json!("A"))]],
            conflict_columns: vec!["email".into()],
            update_columns: vec!["name".into()],
            update_set: vec![],
            returning: vec![],
            casts: Default::default(),
            ctes: vec![],
        };
        let r = compile_upsert(&spec, Dialect::Postgres).unwrap();
        assert!(r.sql.contains("ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""));
    }

    #[test]
    fn upsert_postgres_do_nothing_when_update_empty() {
        let spec = UpsertSpec {
            table: "users".into(),
            rows: vec![vec![("email".into(), json!("a@b"))]],
            conflict_columns: vec!["email".into()],
            update_columns: vec![],
            update_set: vec![],
            returning: vec![],
            casts: Default::default(),
            ctes: vec![],
        };
        let r = compile_upsert(&spec, Dialect::Postgres).unwrap();
        assert!(r.sql.contains("DO NOTHING"));
    }

    #[test]
    fn upsert_mysql_on_duplicate_key() {
        let spec = UpsertSpec {
            table: "users".into(),
            rows: vec![vec![("email".into(), json!("a@b")), ("name".into(), json!("A"))]],
            conflict_columns: vec!["email".into()],
            update_columns: vec!["name".into()],
            update_set: vec![],
            returning: vec![],
            casts: Default::default(),
            ctes: vec![],
        };
        let r = compile_upsert(&spec, Dialect::Mysql).unwrap();
        assert!(r.sql.contains("ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"));
    }

    #[test]
    fn upsert_mysql_do_nothing_uses_insert_ignore() {
        // With an empty update_columns, MySQL needs `INSERT IGNORE` to avoid
        // the affected_rows=2 quirk of a `col=col` self-assign.
        let spec = UpsertSpec {
            table: "users".into(),
            rows: vec![vec![("email".into(), json!("a@b"))]],
            conflict_columns: vec!["email".into()],
            update_columns: vec![],
            update_set: vec![],
            returning: vec![],
            casts: Default::default(),
            ctes: vec![],
        };
        let r = compile_upsert(&spec, Dialect::Mysql).unwrap();
        assert!(r.sql.starts_with("INSERT IGNORE INTO"));
        assert!(!r.sql.contains("ON DUPLICATE KEY"));
    }

    #[test]
    fn update_rejects_injection_in_column() {
        let spec = UpdateSpec {
            table: "users".into(),
            set: vec![("name; DROP TABLE".into(), SetValue::Value(json!("x")))],
            wheres: vec![],
            ..Default::default()
        };
        assert!(compile_update(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn insert_emits_postgres_casts_only_on_typed_columns() {
        let mut casts = HashMap::new();
        casts.insert("id".to_string(), "uuid".to_string());
        casts.insert("created_at".to_string(), "timestamp".to_string());
        let spec = InsertSpec {
            table: "users".into(),
            rows: vec![vec![
                ("id".into(), json!("0191-uuid")),
                ("created_at".into(), json!("2026-06-09T00:00:00Z")),
                ("name".into(), json!("A")),
            ]],
            casts,
            ..Default::default()
        };
        // Postgres: typed params get `::cast`, the untyped `name` stays plain.
        let pg = compile_insert(&spec, Dialect::Postgres).unwrap();
        assert!(pg.sql.contains("$1::uuid"), "{}", pg.sql);
        assert!(pg.sql.contains("$2::timestamp"), "{}", pg.sql);
        assert!(pg.sql.contains("$3") && !pg.sql.contains("$3::"), "{}", pg.sql);
        // SQLite never casts — the driver coerces.
        let sqlite = compile_insert(&spec, Dialect::Sqlite).unwrap();
        assert!(!sqlite.sql.contains("::"), "{}", sqlite.sql);
    }

    #[test]
    fn update_emits_postgres_cast_for_typed_set() {
        let mut casts = HashMap::new();
        casts.insert("updated_at".to_string(), "timestamp".to_string());
        let spec = UpdateSpec {
            table: "users".into(),
            set: vec![
                ("updated_at".into(), SetValue::Value(json!("2026-06-09T00:00:00Z"))),
                ("name".into(), SetValue::Value(json!("A"))),
            ],
            wheres: vec![],
            returning: vec![],
            casts,
            ctes: vec![],
        };
        let pg = compile_update(&spec, Dialect::Postgres).unwrap();
        assert!(pg.sql.contains("\"updated_at\" = $1::timestamp"), "{}", pg.sql);
        assert!(pg.sql.contains("\"name\" = $2") && !pg.sql.contains("$2::"), "{}", pg.sql);
    }
}
