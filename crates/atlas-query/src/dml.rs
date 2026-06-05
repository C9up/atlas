//! DML compilers: INSERT, UPDATE, DELETE.
//!
//! Emits `$N` placeholders (consistent with the existing SELECT compiler).

use crate::builder::CompileResult;
use crate::dialect::Dialect;
use crate::identifier::validate_operator;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
}

/// A SET expression in an UPDATE statement.
///
/// The default form is a plain value binding (`SET col = ?`). The alternative
/// `ColumnPlusValue` form emits `SET col = col + ?`, used for atomic increments
/// and decrements — never read-modify-write, no race condition. The JSON
/// encoding is untagged for backward compat with plain value arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SetValue {
    /// `SET col = ?` — literal value binding.
    Value(Value),
    /// `SET col = col + ?` (or `- ?`). Emitted for atomic increments/decrements.
    Expression {
        /// One of `"increment"` | `"decrement"`.
        op: String,
        /// The amount to add or subtract.
        value: Value,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSpec {
    pub table: String,
    /// Column → set-value pairs. Accepts plain values or column expressions.
    pub set: Vec<(String, SetValue)>,
    #[serde(default)]
    pub wheres: Vec<WhereClauseDml>,
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteSpec {
    pub table: String,
    #[serde(default)]
    pub wheres: Vec<WhereClauseDml>,
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
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
    /// Optional `RETURNING` suffix (postgres + sqlite).
    #[serde(default)]
    pub returning: Vec<String>,
}

/// DML-side WHERE clause — either a standard `col op value` predicate or a raw
/// SQL fragment with `?` bindings (Story 29.7 + 30.2). The untagged enum
/// matches `{column, operator, value, type}` first and `{kind: "raw", sql, bindings, type}`
/// second, so existing call sites are backward-compatible.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WhereClauseDml {
    Standard {
        column: String,
        operator: String,
        value: Value,
        #[serde(rename = "type", default = "default_and")]
        clause_type: String,
    },
    Raw {
        /// Discriminator — always `"raw"`. Lets serde disambiguate from Standard.
        kind: String,
        sql: String,
        #[serde(default)]
        bindings: Vec<Value>,
        #[serde(rename = "type", default = "default_and")]
        clause_type: String,
    },
}

impl WhereClauseDml {
    fn clause_type(&self) -> &str {
        match self {
            Self::Standard { clause_type, .. } | Self::Raw { clause_type, .. } => clause_type,
        }
    }
}

fn default_and() -> String { "and".to_string() }

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
        let placeholders: Vec<String> = row.iter().map(|(_, v)| {
            params.push(v.clone());
            let p = dialect.placeholder(idx);
            idx += 1;
            p
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

    Ok(CompileResult { sql, params })
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
        ..Default::default()
    };
    let base = compile_insert(&insert_spec, dialect)?;
    let mut sql = base.sql;
    let params = base.params;

    match dialect {
        Dialect::Mysql => {
            if spec.update_columns.is_empty() {
                // MySQL has no DO NOTHING. A self-assign (`col = col`) would
                // still increment `affected_rows` to 2 per matched row (MySQL
                // counts matched-and-"updated" rows twice), breaking callers
                // who rely on `changes === 0` to detect no-ops. `INSERT IGNORE`
                // gives the correct no-op semantic: conflicting rows are
                // skipped, affected_rows stays 0 for them. We rewrite the
                // previously-built `INSERT INTO ...` prefix in place.
                if sql.starts_with("INSERT INTO") {
                    sql.replace_range(.."INSERT INTO".len(), "INSERT IGNORE INTO");
                }
            } else {
                let parts: Result<Vec<String>, String> = spec.update_columns.iter().map(|col| {
                    let c = dialect.quote_ident(col)?;
                    Ok(format!("{} = VALUES({})", c, c))
                }).collect();
                sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", parts?.join(", ")));
            }
        }
        _ => {
            // postgres + sqlite
            let conflict_cols: Result<Vec<String>, String> = spec.conflict_columns.iter()
                .map(|c| dialect.quote_ident(c))
                .collect();
            sql.push_str(&format!(" ON CONFLICT ({})", conflict_cols?.join(", ")));
            if spec.update_columns.is_empty() {
                sql.push_str(" DO NOTHING");
            } else {
                let parts: Result<Vec<String>, String> = spec.update_columns.iter().map(|col| {
                    let c = dialect.quote_ident(col)?;
                    Ok(format!("{} = EXCLUDED.{}", c, c))
                }).collect();
                sql.push_str(&format!(" DO UPDATE SET {}", parts?.join(", ")));
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

    Ok(CompileResult { sql, params })
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
                let s = format!("{} = {}", c, dialect.placeholder(idx));
                idx += 1;
                Ok(s)
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
    append_wheres(&mut sql, &mut params, &mut idx, &spec.wheres, dialect)?;
    append_returning(&mut sql, &spec.returning, dialect)?;
    Ok(CompileResult { sql, params })
}

pub fn compile_delete(spec: &DeleteSpec, dialect: Dialect) -> Result<CompileResult, String> {
    let table = dialect.quote_ident(&spec.table)?;
    let mut params: Vec<Value> = Vec::new();
    let mut idx = 1u32;
    let mut sql = format!("DELETE FROM {}", table);
    append_wheres(&mut sql, &mut params, &mut idx, &spec.wheres, dialect)?;
    append_returning(&mut sql, &spec.returning, dialect)?;
    Ok(CompileResult { sql, params })
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

fn append_wheres(
    sql: &mut String,
    params: &mut Vec<Value>,
    idx: &mut u32,
    wheres: &[WhereClauseDml],
    dialect: Dialect,
) -> Result<(), String> {
    if wheres.is_empty() {
        return Ok(());
    }
    sql.push_str(" WHERE ");
    for (i, w) in wheres.iter().enumerate() {
        if i > 0 {
            sql.push(' ');
            sql.push_str(if w.clause_type() == "or" { "OR" } else { "AND" });
            sql.push(' ');
        }
        match w {
            WhereClauseDml::Raw { sql: fragment, bindings, .. } => {
                // Rewrite `?` placeholders with dialect-correct ones, pushing
                // bindings to the shared param vector. Same logic as the SELECT
                // compiler's raw path.
                let mut rewritten = String::with_capacity(fragment.len());
                let mut binding_iter = bindings.iter();
                for ch in fragment.chars() {
                    if ch == '?' {
                        let value = binding_iter.next()
                            .ok_or_else(|| "whereRaw fragment has more '?' placeholders than bindings".to_string())?;
                        params.push(value.clone());
                        rewritten.push_str(&dialect.placeholder(*idx));
                        *idx += 1;
                    } else {
                        rewritten.push(ch);
                    }
                }
                if binding_iter.next().is_some() {
                    return Err("whereRaw has more bindings than '?' placeholders".to_string());
                }
                sql.push_str(&format!("({})", rewritten));
            }
            WhereClauseDml::Standard { column, operator, value, .. } => {
                let col = dialect.quote_ident(column)?;
                let op = validate_operator(operator)?;
                match op {
                    "IS NULL" | "IS NOT NULL" => {
                        sql.push_str(&format!("{} {}", col, op));
                    }
                    "IN" | "NOT IN" => {
                        let arr = value.as_array()
                            .ok_or_else(|| format!("{} operator requires an array value", op))?;
                        if arr.is_empty() {
                            sql.push_str(if op == "IN" { "1 = 0" } else { "1 = 1" });
                        } else {
                            let ph: Vec<String> = arr.iter().map(|v| {
                                params.push(v.clone());
                                let s = dialect.placeholder(*idx);
                                *idx += 1;
                                s
                            }).collect();
                            sql.push_str(&format!("{} {} ({})", col, op, ph.join(", ")));
                        }
                    }
                    _ => {
                        params.push(value.clone());
                        sql.push_str(&format!("{} {} {}", col, op, dialect.placeholder(*idx)));
                        *idx += 1;
                    }
                }
            }
        }
    }
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
            wheres: vec![WhereClauseDml::Standard {
                column: "id".into(),
                operator: "=".into(),
                value: json!(5),
                clause_type: "and".into(),
            }],
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
            wheres: vec![WhereClauseDml::Standard {
                column: "status".into(),
                operator: "IN".into(),
                value: json!(["banned", "deleted"]),
                clause_type: "and".into(),
            }],
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
            wheres: vec![WhereClauseDml::Standard {
                column: "id".into(),
                operator: "=".into(),
                value: json!(1),
                clause_type: "and".into(),
            }],
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
            returning: vec![],
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
            returning: vec![],
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
            returning: vec![],
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
            returning: vec![],
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
}
