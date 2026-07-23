//! Query builder — compiles a query description into parameterized SQL.

use crate::dialect::Dialect;
use crate::identifier::{quote_select_expr, quote_having_expr, validate_operator, validate_direction};
use serde::{Deserialize, Serialize};

// Structured WHERE/HAVING clauses are deserialized from `serde_json::Value` at
// the point of use (the compile loops read fields dynamically to also accept the
// raw/exists/group/inSub tagged variants), so this typed shape is retained only
// as documentation of the canonical structured form.
#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: serde_json::Value,
    #[serde(rename = "type")]
    pub clause_type: String, // "and" or "or"
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExistsClause {
    pub kind: String, // "exists"
    pub subquery: Box<QueryDescription>,
    #[serde(rename = "type")]
    pub clause_type: String,
}

/// A correlated subquery projection appended to the SELECT list, e.g.
/// `(SELECT COUNT(*) FROM comments WHERE ...) AS comments_count`.
/// Used by withCount / withAggregate (Story 29.2).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubqueryProjection {
    pub alias: String,
    pub subquery: Box<QueryDescription>,
}

/// A derived-table FROM source — `FROM (<sql>) AS <alias>` (Lucid/Knex
/// `from(subquery)`). The subquery is already compiled on the TS side; its
/// placeholders are remapped into the outer parameter list at the FROM position.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FromSubquery {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
    pub alias: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderByClause {
    #[serde(default)]
    pub column: String,
    #[serde(default)]
    pub direction: String,
    /// A verbatim ORDER BY fragment (`orderByRaw`). When set, `column` and
    /// `direction` are ignored. Kept on the same clause rather than in a
    /// separate list so a raw term keeps its place among the plain ones:
    /// `orderBy('a').orderByRaw('b DESC').orderBy('c')` must stay a, b, c.
    #[serde(default)]
    pub raw: Option<String>,
}

/// One GROUP BY term: a plain column (quoted) or a verbatim fragment
/// (`groupByRaw`). Untagged so the existing `["a", "b"]` wire format keeps
/// deserialising unchanged, while `{ "raw": "…" }` selects the raw arm — and
/// both keep their position in the list.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GroupByItem {
    Column(String),
    Raw { raw: String },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CteDefinition {
    pub name: String,
    pub sql: String,
    pub params: Vec<serde_json::Value>,
    /// Marks this CTE as self-referencing. RECURSIVE is a property of the WITH
    /// clause, not of one CTE, so a single recursive entry makes the whole
    /// `WITH` recursive — which is exactly what Postgres/SQLite/MySQL require.
    #[serde(default)]
    pub recursive: bool,
    /// `Some(true)` → `AS MATERIALIZED (…)`, `Some(false)` → `AS NOT
    /// MATERIALIZED (…)`, `None` → let the planner decide. Postgres 12+ and
    /// SQLite 3.35+ only; MySQL has no such hint.
    #[serde(default)]
    pub materialized: Option<bool>,
}

/// A JOIN fragment plus its own `?`-style bound values (e.g. `onVal`). Column-to-
/// column joins carry an empty `params`. Rendered before WHERE, so its params take
/// the lower placeholder indices.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinClause {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// A compound (set) operation appended to the query: UNION, INTERSECT or
/// EXCEPT. The wire field is still called `unions` — it predates the other two
/// operators and renaming it would break the contract for no gain.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnionDefinition {
    pub sql: String,
    pub params: Vec<serde_json::Value>,
    /// Keep duplicate rows (`… ALL`).
    pub all: bool,
    /// `union` (default, so the pre-existing wire format is unchanged),
    /// `intersect` or `except`.
    #[serde(default)]
    pub op: Option<String>,
}

/// Resolve a set operation to its SQL keyword, rejecting the combinations a
/// dialect genuinely cannot parse rather than emitting SQL that fails later.
fn set_operator(op: Option<&str>, all: bool, dialect: Dialect) -> Result<String, String> {
    let base = match op.unwrap_or("union") {
        "union" => "UNION",
        "intersect" => "INTERSECT",
        "except" => "EXCEPT",
        other => {
            return Err(format!(
                "E_UNSAFE_SQL: unknown set operation '{other}' (use union, intersect or except)"
            ))
        }
    };
    if !all {
        return Ok(base.to_string());
    }
    // SQLite's compound operators are UNION, UNION ALL, INTERSECT and EXCEPT —
    // there is no INTERSECT ALL / EXCEPT ALL to emit.
    if dialect == Dialect::Sqlite && base != "UNION" {
        return Err(format!(
            "E_UNSUPPORTED: SQLite has no {base} ALL — drop the `all` flag ({} keeps no duplicates there)",
            base.to_lowercase()
        ));
    }
    Ok(format!("{base} ALL"))
}

/// Full query description — sent from TypeScript to Rust.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryDescription {
    pub table: String,
    /// Derived-table FROM source (`from(subquery)`). When set, it replaces the
    /// quoted `table` name with `(<subquery-sql>) AS <alias>`.
    #[serde(default)]
    pub from_subquery: Option<FromSubquery>,
    #[serde(default = "default_select")]
    pub select: Vec<String>,
    #[serde(default)]
    pub wheres: Vec<serde_json::Value>, // WhereClause or ExistsClause
    #[serde(default)]
    pub order_by: Vec<OrderByClause>,
    #[serde(default)]
    pub group_by: Vec<GroupByItem>,
    /// HAVING clauses — structured `{column, operator, value, type}` or raw
    /// `{kind:"raw", sql, bindings, type}`, mirroring `wheres`.
    #[serde(default)]
    pub having: Vec<serde_json::Value>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    #[serde(default)]
    pub distinct: bool,
    /// `DISTINCT ON (cols)` (Postgres). Takes precedence over `distinct`.
    #[serde(default)]
    pub distinct_on: Vec<String>,
    #[serde(default)]
    pub ctes: Vec<CteDefinition>,
    #[serde(default)]
    pub unions: Vec<UnionDefinition>,
    /// Correlated subqueries appended as projections to the SELECT list.
    #[serde(default)]
    pub select_subqueries: Vec<SubqueryProjection>,
    /// Raw JOIN clauses rendered verbatim between FROM and WHERE. Each entry is
    /// already dialect-quoted on the TS side — the Rust compiler just inlines
    /// them in order. Used by Story 29.4 (innerJoin / leftJoin / joinRaw).
    #[serde(default)]
    pub joins: Vec<JoinClause>,
    /// Optional row lock suffix (FOR UPDATE / FOR SHARE). Silently dropped on sqlite.
    /// Used by Story 30.8.
    #[serde(default)]
    pub lock_mode: Option<String>,
    /// Per-column Postgres cast hints (snake column → logical type), mirroring
    /// `InsertSpec::casts`. Applied to WHERE-clause params so a `uuid` / `timestamp`
    /// column compared against a string bind gets `… = $1::uuid` instead of failing
    /// with `operator does not exist: uuid = text`. Postgres-only; empty elsewhere.
    #[serde(default)]
    pub casts: std::collections::HashMap<String, String>,
}

fn default_select() -> Vec<String> {
    vec!["*".to_string()]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompileResult {
    pub sql: String,
    pub params: Vec<serde_json::Value>,
}

/// Compile a QueryDescription into parameterized SQL.
/// Backward-compat: compile with default (sqlite/postgres-style) dialect.
pub fn compile_query(desc: &QueryDescription) -> Result<CompileResult, String> {
    compile_query_with_dialect(desc, Dialect::Sqlite)
}

/// Dialect-aware compilation — identifier quoting follows the dialect
/// (double-quotes for sqlite/postgres, backticks for mysql).
/// Rewrite a sub-query's `$N` placeholders so they continue from `*idx` in the
/// outer statement, appending its params. Boundary-aware ($1 is not matched
/// inside $10). For `?`-placeholder dialects there are no `$N` to rewrite, so it
/// just appends the params in order (positional correctness is preserved).
pub(crate) fn remap_placeholders(
    sub_sql: &str,
    sub_params: &[serde_json::Value],
    params: &mut Vec<serde_json::Value>,
    idx: &mut u32,
) -> String {
    let mut remapped = sub_sql.to_string();
    for i in (1..=sub_params.len()).rev() {
        let old = format!("${}", i);
        let new_val = format!("${}", *idx + i as u32 - 1);
        let mut result = String::new();
        let mut rest = remapped.as_str();
        while let Some(pos) = rest.find(&old) {
            result.push_str(&rest[..pos]);
            let after = pos + old.len();
            let next_is_digit = rest
                .get(after..after + 1)
                .and_then(|s| s.chars().next())
                .map(|c| c.is_ascii_digit())
                .unwrap_or(false);
            if next_is_digit {
                result.push_str(&old);
            } else {
                result.push_str(&new_val);
            }
            rest = &rest[after..];
        }
        result.push_str(rest);
        remapped = result;
    }
    params.extend(sub_params.iter().cloned());
    *idx += sub_params.len() as u32;
    remapped
}

pub fn compile_query_with_dialect(desc: &QueryDescription, dialect: Dialect) -> Result<CompileResult, String> {
    let quote = |name: &str| dialect.quote_ident(name);
    let mut params: Vec<serde_json::Value> = Vec::new();
    let mut param_index = 1u32;
    let mut sql = String::new();

    // Remap $N params — boundary-aware (does not replace $1 inside $10). Shared
    // with the DML compiler (update/delete WHERE reuse), hence a free fn.
    let remap_params = |sub_sql: &str, sub_params: &[serde_json::Value], params: &mut Vec<serde_json::Value>, idx: &mut u32| -> String {
        remap_placeholders(sub_sql, sub_params, params, idx)
    };

    // CTEs
    if !desc.ctes.is_empty() {
        let cte_parts: Result<Vec<String>, String> = desc.ctes.iter().map(|cte| {
            let name = quote(&cte.name)?;
            let remapped = remap_params(&cte.sql, &cte.params, &mut params, &mut param_index);
            let hint = match cte.materialized {
                None => "",
                Some(_) if dialect == Dialect::Mysql => {
                    return Err(
                        "E_UNSUPPORTED: MySQL has no MATERIALIZED / NOT MATERIALIZED CTE hint".into(),
                    )
                }
                Some(true) => "MATERIALIZED ",
                Some(false) => "NOT MATERIALIZED ",
            };
            Ok(format!("{} AS {}({})", name, hint, remapped))
        }).collect();
        // RECURSIVE belongs to the WITH clause, so one recursive CTE makes the
        // whole clause recursive — all three dialects spell it this way.
        let recursive = if desc.ctes.iter().any(|c| c.recursive) { "RECURSIVE " } else { "" };
        sql += &format!("WITH {}{} ", recursive, cte_parts?.join(", "));
    }

    // SELECT — regular columns first, then correlated subquery projections.
    let mut select_cols: Vec<String> = Vec::with_capacity(desc.select.len() + desc.select_subqueries.len());
    for c in &desc.select {
        select_cols.push(quote_select_expr(c)?);
    }
    for proj in &desc.select_subqueries {
        let sub_result = compile_query_with_dialect(&proj.subquery, dialect)?;
        let remapped = remap_params(&sub_result.sql, &sub_result.params, &mut params, &mut param_index);
        let alias = quote(&proj.alias)?;
        select_cols.push(format!("({}) AS {}", remapped, alias));
    }
    // DISTINCT / DISTINCT ON — the latter is a Postgres extension. Emitting it
    // elsewhere would parse as `DISTINCT` applied to a parenthesised column
    // list and silently return different rows, so refuse instead.
    let distinct = if !desc.distinct_on.is_empty() {
        if dialect != Dialect::Postgres {
            return Err(
                "E_UNSUPPORTED: DISTINCT ON is Postgres-only — use GROUP BY, or a window function, elsewhere".into(),
            );
        }
        let cols: Result<Vec<String>, String> = desc.distinct_on.iter().map(|c| quote(c)).collect();
        format!("DISTINCT ON ({}) ", cols?.join(", "))
    } else if desc.distinct {
        "DISTINCT ".to_string()
    } else {
        String::new()
    };
    // FROM — a derived table `(<subquery>) AS <alias>` when `from_subquery` is
    // set, else the quoted table name. The subquery's params are remapped into
    // the outer list here (at the FROM position); the alias is identifier-quoted.
    let table = match &desc.from_subquery {
        Some(sub) => {
            let remapped =
                remap_params(&sub.sql, &sub.params, &mut params, &mut param_index);
            format!("({}) AS {}", remapped, quote(&sub.alias)?)
        }
        None => quote(&desc.table)?,
    };
    sql += &format!("SELECT {}{} FROM {}", distinct, select_cols.join(", "), table);

    // JOINS — raw fragments, trusted to be already identifier-quoted by TS side.
    // Rust still screens for obviously dangerous tokens to keep a defence-in-depth.
    for j in &desc.joins {
        let lower = j.sql.to_lowercase();
        // Stacked statements, comments, and control characters (NUL, raw
        // newlines used to smuggle a payload past a naive screen) are never part
        // of a legitimate JOIN — a derived-table join `JOIN (SELECT …)` has none
        // of these, so this hardens without rejecting valid joins. `union`/
        // `select` are intentionally NOT blocked (they appear in derived joins).
        if lower.contains("--")
            || lower.contains(';')
            || lower.contains("/*")
            || lower.contains("*/")
            || j.sql.chars().any(|c| c.is_control())
        {
            return Err(format!(
                "E_UNSAFE_SQL: JOIN fragment contains a forbidden token (`;`, comment, or control character): {}",
                j.sql
            ));
        }
        // Rewrite `?` placeholders (e.g. from onVal) to dialect placeholders,
        // binding the join's own params BEFORE the WHERE clause consumes indices.
        let mut rewritten = String::with_capacity(j.sql.len());
        let mut binding_iter = j.params.iter().cloned();
        for ch in j.sql.chars() {
            if ch == '?' {
                let value = binding_iter.next().ok_or_else(|| {
                    "JOIN fragment has more '?' placeholders than bound params".to_string()
                })?;
                params.push(value);
                rewritten.push_str(&dialect.placeholder(param_index));
                param_index += 1;
            } else {
                rewritten.push(ch);
            }
        }
        if binding_iter.next().is_some() {
            return Err("JOIN fragment has more bound params than '?' placeholders".to_string());
        }
        sql.push(' ');
        sql.push_str(&rewritten);
    }

    // WHERE
    if !desc.wheres.is_empty() {
        let mut clauses = Vec::new();
        for (i, w) in desc.wheres.iter().enumerate() {
            let prefix = if i == 0 { "WHERE" } else {
                let t = w.get("type").and_then(|v| v.as_str()).unwrap_or("and");
                if t == "or" { "OR" } else { "AND" }
            };

            // Tagged variants: { kind: 'exists' | 'raw', ... }
            if let Some(kind) = w.get("kind").and_then(|v| v.as_str()) {
                if kind == "group" {
                    // Parenthesised sub-group of WHERE conditions.
                    let nested = w.get("conditions").and_then(|v| v.as_array())
                        .ok_or_else(|| "group clause requires 'conditions' array".to_string())?;
                    if nested.is_empty() { continue; }
                    // Recursively compile each nested clause by building a fake
                    // QueryDescription whose WHERE is the nested list. This reuses
                    // all the compilation logic (operators, raw, exists, in-sub).
                    let fake = QueryDescription {
                        table: desc.table.clone(),
                        from_subquery: None,
                        select: vec!["*".to_string()],
                        wheres: nested.clone(),
                        order_by: vec![], group_by: vec![], having: vec![],
                        limit: None, offset: None, distinct: false, distinct_on: vec![],
                        ctes: vec![], unions: vec![], select_subqueries: vec![],
                        joins: vec![], lock_mode: None, casts: desc.casts.clone(),
                    };
                    let sub_result = compile_query_with_dialect(&fake, dialect)?;
                    // sub_result.sql looks like: `SELECT * FROM "table" WHERE <...>`
                    // Extract everything after the first ` WHERE ` so we can inline
                    // it under our parenthesised group.
                    let inner = sub_result.sql
                        .split_once(" WHERE ")
                        .map(|(_, rest)| rest.to_string())
                        .unwrap_or_default();
                    if inner.is_empty() { continue; }
                    let remapped = remap_params(&inner, &sub_result.params, &mut params, &mut param_index);
                    clauses.push(format!("{} ({})", prefix, remapped));
                    continue;
                }

                if kind == "inSub" {
                    // col IN (SELECT ...) — correlated or not.
                    let column = w.get("column").and_then(|v| v.as_str())
                        .ok_or_else(|| "inSub clause requires 'column'".to_string())?;
                    let negated = w.get("negated").and_then(|v| v.as_bool()).unwrap_or(false);
                    let sub = w.get("subquery")
                        .ok_or_else(|| "inSub clause requires 'subquery'".to_string())?;
                    let sub_desc: QueryDescription = serde_json::from_value(sub.clone())
                        .map_err(|e| format!("Invalid inSub subquery: {}", e))?;
                    let sub_result = compile_query_with_dialect(&sub_desc, dialect)?;
                    let remapped = remap_params(&sub_result.sql, &sub_result.params, &mut params, &mut param_index);
                    let col = quote(column)?;
                    let op = if negated { "NOT IN" } else { "IN" };
                    clauses.push(format!("{} {} {} ({})", prefix, col, op, remapped));
                    continue;
                }

                if kind == "exists" {
                    let negated = w.get("negated").and_then(|v| v.as_bool()).unwrap_or(false);
                    let sub = w.get("subquery")
                        .ok_or_else(|| "EXISTS clause missing 'subquery' field".to_string())?;
                    let sub_desc: QueryDescription = serde_json::from_value(sub.clone())
                        .map_err(|e| format!("Invalid EXISTS subquery: {}", e))?;
                    let sub_result = compile_query_with_dialect(&sub_desc, dialect)?;
                    let remapped = remap_params(&sub_result.sql, &sub_result.params, &mut params, &mut param_index);
                    let keyword = if negated { "NOT EXISTS" } else { "EXISTS" };
                    clauses.push(format!("{} {} ({})", prefix, keyword, remapped));
                    continue;
                }

                if kind == "json" {
                    // JSON predicates (whereJsonPath / whereJsonSupersetOf /
                    // whereJsonSubsetOf). Every value — the path AND the compared
                    // value — is BOUND, never interpolated; only the column is a
                    // quoted identifier. Each dialect spells JSON access
                    // differently, and SQLite has no containment operator at all,
                    // so unsupported combinations are refused rather than emitted.
                    let column = w.get("column").and_then(|v| v.as_str())
                        .ok_or_else(|| "json clause requires 'column'".to_string())?;
                    let json_op = w.get("jsonOp").and_then(|v| v.as_str())
                        .ok_or_else(|| "json clause requires 'jsonOp'".to_string())?;
                    let negated = w.get("negated").and_then(|v| v.as_bool()).unwrap_or(false);
                    let col = quote(column)?;

                    let push = |value: &serde_json::Value, params: &mut Vec<serde_json::Value>, idx: &mut u32| {
                        params.push(value.clone());
                        let ph = dialect.placeholder(*idx);
                        *idx += 1;
                        ph
                    };

                    let expr = match json_op {
                        "path" => {
                            let path = w.get("path")
                                .ok_or_else(|| "whereJsonPath requires 'path'".to_string())?;
                            let raw_op = w.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
                            let op = validate_operator(raw_op)?;
                            let value = w.get("value")
                                .ok_or_else(|| "whereJsonPath requires 'value'".to_string())?;
                            let path_ph = push(path, &mut params, &mut param_index);
                            let value_ph = push(value, &mut params, &mut param_index);
                            match dialect {
                                Dialect::Sqlite => format!("json_extract({col}, {path_ph}) {op} {value_ph}"),
                                Dialect::Mysql => format!("JSON_UNQUOTE(JSON_EXTRACT({col}, {path_ph})) {op} {value_ph}"),
                                // #>> '{}' extracts the located scalar as text; the
                                // path binds as a jsonpath and the column casts to
                                // jsonb so plain `json` columns work too.
                                Dialect::Postgres => format!(
                                    "(jsonb_path_query_first({col}::jsonb, {path_ph}::jsonpath) #>> '{{}}') {op} {value_ph}"
                                ),
                            }
                        }
                        "superset" | "subset" => {
                            let value = w.get("value")
                                .ok_or_else(|| "whereJson{Superset,Subset}Of requires 'value'".to_string())?;
                            let value_ph = push(value, &mut params, &mut param_index);
                            let superset = json_op == "superset";
                            match dialect {
                                Dialect::Postgres => {
                                    let op = if superset { "@>" } else { "<@" };
                                    format!("{col}::jsonb {op} {value_ph}::jsonb")
                                }
                                Dialect::Mysql => {
                                    // JSON_CONTAINS(target, candidate): superset =
                                    // col contains value; subset = value contains col.
                                    if superset {
                                        format!("JSON_CONTAINS({col}, {value_ph})")
                                    } else {
                                        format!("JSON_CONTAINS({value_ph}, {col})")
                                    }
                                }
                                Dialect::Sqlite => {
                                    return Err(
                                        "E_UNSUPPORTED: SQLite has no JSON containment operator — whereJsonSupersetOf / whereJsonSubsetOf need Postgres or MySQL".into(),
                                    )
                                }
                            }
                        }
                        other => return Err(format!("E_UNSAFE_SQL: unknown json operation '{other}'")),
                    };

                    let expr = if negated { format!("NOT ({expr})") } else { expr };
                    clauses.push(format!("{prefix} {expr}"));
                    continue;
                }

                if kind == "raw" {
                    // whereRaw — the caller provides a SQL fragment and its own
                    // `?`-style bindings. The compiler re-indexes the placeholders
                    // so they don't clash with other clause params.
                    let fragment = w.get("sql").and_then(|v| v.as_str())
                        .ok_or_else(|| "whereRaw requires a 'sql' field".to_string())?;
                    let bindings: Vec<serde_json::Value> = w.get("bindings")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();

                    // Append every binding to the overall param list, then
                    // rewrite `?` placeholders in the fragment with dialect-correct
                    // placeholders (`?` for sqlite/mysql, `$N` for postgres).
                    let mut rewritten = String::with_capacity(fragment.len());
                    let mut chars = fragment.chars().peekable();
                    let mut binding_iter = bindings.into_iter();
                    while let Some(ch) = chars.next() {
                        if ch == '?' {
                            let value = binding_iter.next()
                                .ok_or_else(|| "whereRaw fragment has more '?' placeholders than bindings".to_string())?;
                            params.push(value);
                            rewritten.push_str(&dialect.placeholder(param_index));
                            param_index += 1;
                        } else {
                            rewritten.push(ch);
                        }
                    }
                    if binding_iter.next().is_some() {
                        return Err("whereRaw has more bindings than '?' placeholders".to_string());
                    }
                    clauses.push(format!("{} ({})", prefix, rewritten));
                    continue;
                }
            }

            let column = w.get("column").and_then(|v| v.as_str()).unwrap_or("");
            let raw_op = w.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
            let operator = validate_operator(raw_op)?;
            let col = quote(column)?;

            match operator {
                "IS NULL" => clauses.push(format!("{} {} IS NULL", prefix, col)),
                "IS NOT NULL" => clauses.push(format!("{} {} IS NOT NULL", prefix, col)),
                "IN" | "NOT IN" => {
                    if let Some(arr) = w.get("value").and_then(|v| v.as_array()) {
                        if arr.is_empty() {
                            let expr = if operator == "IN" { "1 = 0" } else { "1 = 1" };
                            clauses.push(format!("{} {}", prefix, expr));
                        } else {
                            let in_cast = desc.casts.get(column).and_then(|t| dialect.cast_for(t));
                            let placeholders: Vec<String> = arr.iter().map(|v| {
                                params.push(v.clone());
                                let p = dialect.placeholder(param_index);
                                param_index += 1;
                                match &in_cast {
                                    Some(cast) => format!("{}::{}", p, cast),
                                    None => p,
                                }
                            }).collect();
                            clauses.push(format!("{} {} {} ({})", prefix, col, operator, placeholders.join(", ")));
                        }
                    }
                }
                "BETWEEN" | "NOT BETWEEN" => {
                    // BETWEEN expects a 2-element array: [low, high]
                    let arr = w.get("value").and_then(|v| v.as_array())
                        .ok_or_else(|| format!("{} operator requires a 2-element array value", operator))?;
                    if arr.len() != 2 {
                        return Err(format!("{} operator requires exactly 2 values, got {}", operator, arr.len()));
                    }
                    let bt_cast = desc.casts.get(column).and_then(|t| dialect.cast_for(t));
                    let apply = |ph: String| match &bt_cast {
                        Some(cast) => format!("{}::{}", ph, cast),
                        None => ph,
                    };
                    params.push(arr[0].clone());
                    let low_ph = apply(dialect.placeholder(param_index));
                    param_index += 1;
                    params.push(arr[1].clone());
                    let high_ph = apply(dialect.placeholder(param_index));
                    param_index += 1;
                    clauses.push(format!("{} {} {} {} AND {}", prefix, col, operator, low_ph, high_ph));
                }
                "ILIKE" | "NOT ILIKE" => {
                    // Postgres has native ILIKE. SQLite/MySQL fake it: LOWER(col) LIKE LOWER(?)
                    // Injection-safe because LOWER() only wraps an already-quoted identifier.
                    let value = w.get("value")
                        .ok_or_else(|| format!("{} operator requires a value", operator))?;
                    params.push(value.clone());
                    let ph = dialect.placeholder(param_index);
                    param_index += 1;
                    match dialect {
                        crate::dialect::Dialect::Postgres => {
                            clauses.push(format!("{} {} {} {}", prefix, col, operator, ph));
                        }
                        _ => {
                            let like_op = if operator == "ILIKE" { "LIKE" } else { "NOT LIKE" };
                            clauses.push(format!("{} LOWER({}) {} LOWER({})", prefix, col, like_op, ph));
                        }
                    }
                }
                _ => {
                    if let Some(value) = w.get("value") {
                        params.push(value.clone());
                        let ph = dialect.placeholder(param_index);
                        let ph = match desc.casts.get(column).and_then(|t| dialect.cast_for(t)) {
                            Some(cast) => format!("{}::{}", ph, cast),
                            None => ph,
                        };
                        clauses.push(format!("{} {} {} {}", prefix, col, operator, ph));
                        param_index += 1;
                    }
                }
            }
        }
        sql += &format!(" {}", clauses.join(" "));
    }

    // GROUP BY
    if !desc.group_by.is_empty() {
        let cols: Result<Vec<String>, String> = desc.group_by.iter()
            .map(|c| match c {
                GroupByItem::Column(name) => quote(name),
                // Verbatim (groupByRaw) — gated behind atlas strict mode on the
                // TypeScript side, exactly like whereRaw/havingRaw.
                GroupByItem::Raw { raw } => Ok(raw.clone()),
            })
            .collect();
        sql += &format!(" GROUP BY {}", cols?.join(", "));
    }

    // HAVING — structured `{column, operator, value, type}` or raw
    // `{kind:"raw", sql, bindings, type}`. The first clause is prefixed HAVING;
    // subsequent ones honour their `type` (and/or), mirroring the WHERE loop.
    if !desc.having.is_empty() {
        let mut clauses = Vec::new();
        for (i, h) in desc.having.iter().enumerate() {
            let prefix = if i == 0 { "HAVING" } else {
                let t = h.get("type").and_then(|v| v.as_str()).unwrap_or("and");
                if t == "or" { "OR" } else { "AND" }
            };

            if h.get("kind").and_then(|v| v.as_str()) == Some("raw") {
                let fragment = h.get("sql").and_then(|v| v.as_str())
                    .ok_or_else(|| "havingRaw requires a 'sql' field".to_string())?;
                let bindings: Vec<serde_json::Value> = h.get("bindings")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut rewritten = String::with_capacity(fragment.len());
                let mut binding_iter = bindings.into_iter();
                for ch in fragment.chars() {
                    if ch == '?' {
                        let value = binding_iter.next()
                            .ok_or_else(|| "havingRaw fragment has more '?' placeholders than bindings".to_string())?;
                        params.push(value);
                        rewritten.push_str(&dialect.placeholder(param_index));
                        param_index += 1;
                    } else {
                        rewritten.push(ch);
                    }
                }
                if binding_iter.next().is_some() {
                    return Err("havingRaw has more bindings than '?' placeholders".to_string());
                }
                clauses.push(format!("{} ({})", prefix, rewritten));
                continue;
            }

            let column = h.get("column").and_then(|v| v.as_str()).unwrap_or("");
            let col = quote_having_expr(column)?;
            let raw_op = h.get("operator").and_then(|v| v.as_str()).unwrap_or("=");
            let having_op = validate_operator(raw_op)?;
            match having_op {
                "IS NULL" => clauses.push(format!("{} {} IS NULL", prefix, col)),
                "IS NOT NULL" => clauses.push(format!("{} {} IS NOT NULL", prefix, col)),
                "IN" | "NOT IN" => {
                    let arr = h.get("value").and_then(|v| v.as_array())
                        .ok_or_else(|| format!("HAVING {} requires an array value", having_op))?;
                    if arr.is_empty() {
                        let expr = if having_op == "IN" { "1 = 0" } else { "1 = 1" };
                        clauses.push(format!("{} {}", prefix, expr));
                    } else {
                        let placeholders: Vec<String> = arr.iter().map(|v| {
                            params.push(v.clone());
                            let p = dialect.placeholder(param_index);
                            param_index += 1;
                            p
                        }).collect();
                        clauses.push(format!("{} {} {} ({})", prefix, col, having_op, placeholders.join(", ")));
                    }
                }
                "BETWEEN" | "NOT BETWEEN" => {
                    let arr = h.get("value").and_then(|v| v.as_array())
                        .ok_or_else(|| format!("HAVING {} requires a 2-element array value", having_op))?;
                    if arr.len() != 2 {
                        return Err(format!("HAVING {} requires exactly 2 values, got {}", having_op, arr.len()));
                    }
                    params.push(arr[0].clone());
                    let low = dialect.placeholder(param_index);
                    param_index += 1;
                    params.push(arr[1].clone());
                    let high = dialect.placeholder(param_index);
                    param_index += 1;
                    clauses.push(format!("{} {} {} {} AND {}", prefix, col, having_op, low, high));
                }
                _ => {
                    params.push(h.get("value").cloned().unwrap_or(serde_json::Value::Null));
                    clauses.push(format!("{} {} {} {}", prefix, col, having_op, dialect.placeholder(param_index)));
                    param_index += 1;
                }
            }
        }
        sql += &format!(" {}", clauses.join(" "));
    }

    // Compound (set) operations — UNION / INTERSECT / EXCEPT, in call order.
    // These come BEFORE ORDER BY / LIMIT / OFFSET: a trailing ORDER BY / LIMIT
    // binds to the WHOLE compound (standard SQL), and every dialect rejects an
    // ORDER BY placed on the first branch before the set operator.
    //
    // The branch is NOT parenthesised. SQLite's grammar is
    // `select-core (UNION|INTERSECT|EXCEPT) select-core`, and a parenthesised
    // select is not a select-core — `SELECT 1 UNION (SELECT 2)` is a syntax
    // error there, which is why union() never actually ran on SQLite even
    // though its compiled string looked right.
    //
    // Postgres and MySQL do accept the parens, and they would isolate a
    // branch's own ORDER BY / LIMIT. Dropping them everywhere rather than only
    // on SQLite is deliberate: keeping them where they parse would mean the
    // same atlas query returns different rows depending on the dialect, and a
    // silent divergence is worse than the documented standard behaviour. Wrap
    // the branch in a subquery if you need it isolated.
    for u in &desc.unions {
        let remapped = remap_params(&u.sql, &u.params, &mut params, &mut param_index);
        let keyword = set_operator(u.op.as_deref(), u.all, dialect)?;
        sql += &format!(" {} {}", keyword, remapped);
    }

    // ORDER BY
    if !desc.order_by.is_empty() {
        let cols: Result<Vec<String>, String> = desc.order_by.iter()
            .map(|o| {
                // Verbatim (orderByRaw) — gated behind atlas strict mode on the
                // TypeScript side, exactly like whereRaw/havingRaw.
                if let Some(raw) = &o.raw {
                    return Ok(raw.clone());
                }
                let col = quote(&o.column)?;
                let dir = validate_direction(&o.direction)?;
                Ok(format!("{} {}", col, dir))
            })
            .collect();
        sql += &format!(" ORDER BY {}", cols?.join(", "));
    }

    // LIMIT / OFFSET
    if let Some(limit) = desc.limit {
        sql += &format!(" LIMIT {}", limit);
    }
    if let Some(offset) = desc.offset {
        sql += &format!(" OFFSET {}", offset);
    }

    // LOCK MODE — FOR UPDATE / FOR SHARE (+ Postgres FOR NO KEY UPDATE / FOR KEY
    // SHARE), with optional SKIP LOCKED / NOWAIT modifiers. Only on postgres+mysql.
    if let Some(mode) = &desc.lock_mode {
        let upper = mode.to_uppercase();
        // Peel the optional modifier suffix off the base lock clause.
        let (base, modifier) = if let Some(rest) = upper.strip_suffix(" SKIP LOCKED") {
            (rest.to_string(), Some("SKIP LOCKED"))
        } else if let Some(rest) = upper.strip_suffix(" NOWAIT") {
            (rest.to_string(), Some("NOWAIT"))
        } else {
            (upper.clone(), None)
        };
        if !matches!(
            base.as_str(),
            "FOR UPDATE" | "FOR SHARE" | "FOR NO KEY UPDATE" | "FOR KEY SHARE"
        ) {
            return Err(format!("Unsupported lock mode: {}", mode));
        }
        // FOR NO KEY UPDATE / FOR KEY SHARE are Postgres-only.
        let pg_only = matches!(base.as_str(), "FOR NO KEY UPDATE" | "FOR KEY SHARE");
        if pg_only && matches!(dialect, Dialect::Mysql) {
            return Err(format!("Lock mode '{}' is Postgres-only", base));
        }
        if !matches!(dialect, Dialect::Sqlite) {
            sql.push(' ');
            sql.push_str(&base);
            if let Some(m) = modifier {
                sql.push(' ');
                sql.push_str(m);
            }
        }
        // sqlite: silently no-op — TS side surfaces a Spectrum warning.
    }

    Ok(CompileResult { sql, params })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_desc(table: &str) -> QueryDescription {
        QueryDescription {
            table: table.to_string(),
            from_subquery: None,
            select: vec!["*".to_string()],
            wheres: vec![],
            order_by: vec![],
            group_by: vec![],
            having: vec![],
            limit: None,
            offset: None,
            distinct: false, distinct_on: vec![],
            ctes: vec![],
            unions: vec![],
            select_subqueries: vec![],
            joins: vec![],
            lock_mode: None,
            casts: Default::default(),
        }
    }

    #[test]
    fn test_from_subquery_derived_table() {
        // FROM (<subquery>) AS t, with the subquery's own param remapped, then an
        // outer WHERE param — placeholders must renumber left-to-right.
        let mut desc = simple_desc("ignored");
        desc.from_subquery = Some(FromSubquery {
            // As the TS side compiles it for postgres — `$1` placeholders.
            sql: "SELECT * FROM \"orders\" WHERE \"status\" = $1".into(),
            params: vec![serde_json::json!("open")],
            alias: "t".into(),
        });
        desc.wheres.push(serde_json::json!({
            "column": "total", "operator": ">", "value": 10, "type": "and"
        }));
        let r = compile_query_with_dialect(&desc, Dialect::Postgres).unwrap();
        assert!(
            r.sql.contains("FROM (SELECT * FROM \"orders\" WHERE \"status\" = $1) AS \"t\""),
            "{}", r.sql
        );
        assert!(r.sql.contains("WHERE \"total\" > $2"), "{}", r.sql);
        assert_eq!(r.params, vec![serde_json::json!("open"), serde_json::json!(10)]);
    }

    #[test]
    fn test_select_subquery_projection_with_count() {
        let mut desc = simple_desc("users");
        desc.select_subqueries.push(SubqueryProjection {
            alias: "posts_count".into(),
            subquery: Box::new(QueryDescription {
                table: "posts".into(),
                from_subquery: None,
                select: vec!["COUNT(*)".into()],
                wheres: vec![serde_json::json!({
                    "kind": "raw", "type": "and",
                    "sql": "\"posts\".\"user_id\" = \"users\".\"id\"", "bindings": []
                })],
                order_by: vec![], group_by: vec![], having: vec![],
                limit: None, offset: None, distinct: false, distinct_on: vec![],
                ctes: vec![], unions: vec![], select_subqueries: vec![], joins: vec![], lock_mode: None,
                casts: Default::default(),
            }),
        });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("(SELECT COUNT(*) FROM \"posts\""));
        assert!(result.sql.contains(") AS \"posts_count\""));
    }

    #[test]
    fn test_select_subquery_with_filter_uses_param_remap() {
        let mut desc = simple_desc("users");
        desc.select_subqueries.push(SubqueryProjection {
            alias: "published_posts".into(),
            subquery: Box::new(QueryDescription {
                table: "posts".into(),
                from_subquery: None,
                select: vec!["COUNT(*)".into()],
                wheres: vec![
                    serde_json::json!({
                        "kind": "raw", "type": "and",
                        "sql": "\"posts\".\"user_id\" = \"users\".\"id\"", "bindings": []
                    }),
                    serde_json::json!({
                        "column": "published", "operator": "=", "value": true, "type": "and"
                    }),
                ],
                order_by: vec![], group_by: vec![], having: vec![],
                limit: None, offset: None, distinct: false, distinct_on: vec![],
                ctes: vec![], unions: vec![], select_subqueries: vec![], joins: vec![], lock_mode: None,
                casts: Default::default(),
            }),
        });
        desc.wheres.push(serde_json::json!({
            "column": "status", "operator": "=", "value": "active", "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        // Subquery params come BEFORE WHERE params in the outer query's param list
        assert_eq!(result.params, vec![serde_json::json!(true), serde_json::json!("active")]);
    }

    #[test]
    fn test_simple_select() {
        let desc = simple_desc("orders");
        let result = compile_query(&desc).unwrap();
        assert_eq!(result.sql, "SELECT * FROM \"orders\"");
        assert!(result.params.is_empty());
    }

    #[test]
    fn test_select_with_where() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "=",
            "value": "active",
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert_eq!(result.sql, "SELECT * FROM \"orders\" WHERE \"status\" = ?");
        assert_eq!(result.params, vec![serde_json::json!("active")]);
    }

    #[test]
    fn test_select_with_where_in() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "IN",
            "value": ["active", "pending"],
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("IN (?, ?)"));
        assert_eq!(result.params.len(), 2);
    }

    #[test]
    fn test_empty_in() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "IN",
            "value": [],
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("1 = 0"));
    }

    #[test]
    fn test_order_by_limit_offset() {
        let mut desc = simple_desc("orders");
        desc.order_by.push(OrderByClause { column: "createdAt".to_string(), direction: "desc".to_string(), raw: None });
        desc.limit = Some(20);
        desc.offset = Some(40);
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("ORDER BY \"createdAt\" DESC"));
        assert!(result.sql.contains("LIMIT 20"));
        assert!(result.sql.contains("OFFSET 40"));
    }

    #[test]
    fn test_distinct() {
        let mut desc = simple_desc("orders");
        desc.distinct = true;
        desc.select = vec!["status".to_string()];
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("SELECT DISTINCT \"status\""));
    }

    #[test]
    fn test_group_by_having() {
        let mut desc = simple_desc("orders");
        desc.select = vec!["status".to_string(), "COUNT(*) AS count".to_string()];
        desc.group_by = vec![GroupByItem::Column("status".to_string())];
        desc.having.push(serde_json::json!({
            "column": "COUNT(*)", "operator": ">", "value": 5, "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("GROUP BY \"status\""));
        assert!(result.sql.contains("HAVING COUNT(*) > ?"));
        assert_eq!(result.params, vec![serde_json::json!(5)]);
    }

    #[test]
    fn test_having_in_and_between() {
        let mut desc = simple_desc("orders");
        desc.group_by = vec![GroupByItem::Column("status".to_string())];
        desc.having.push(serde_json::json!({
            "column": "status", "operator": "IN", "value": ["a", "b"], "type": "and"
        }));
        desc.having.push(serde_json::json!({
            "column": "COUNT(*)", "operator": "BETWEEN", "value": [2, 10], "type": "and"
        }));
        let r = compile_query(&desc).unwrap();
        assert!(r.sql.contains("HAVING \"status\" IN (?, ?)"), "{}", r.sql);
        assert!(r.sql.contains("AND COUNT(*) BETWEEN ? AND ?"), "{}", r.sql);
        assert_eq!(
            r.params,
            vec![serde_json::json!("a"), serde_json::json!("b"), serde_json::json!(2), serde_json::json!(10)]
        );
    }

    /// A raw term must keep its position among the plain ones — that is why
    /// `raw` lives on OrderByClause instead of in a separate list.
    #[test]
    fn test_order_by_raw_keeps_call_order() {
        let mut desc = simple_desc("users");
        desc.order_by = vec![
            OrderByClause { column: "a".into(), direction: "asc".into(), raw: None },
            OrderByClause { column: String::new(), direction: String::new(), raw: Some("b DESC NULLS LAST".into()) },
            OrderByClause { column: "c".into(), direction: "desc".into(), raw: None },
        ];
        let sql = compile_query(&desc).unwrap().sql;
        assert!(
            sql.contains("ORDER BY \"a\" ASC, b DESC NULLS LAST, \"c\" DESC"),
            "{sql}"
        );
    }

    #[test]
    fn test_group_by_raw_keeps_call_order() {
        let mut desc = simple_desc("users");
        desc.select = vec!["*".to_string()];
        desc.group_by = vec![
            GroupByItem::Column("a".into()),
            GroupByItem::Raw { raw: "DATE_TRUNC('day', created_at)".into() },
        ];
        let sql = compile_query(&desc).unwrap().sql;
        assert!(
            sql.contains("GROUP BY \"a\", DATE_TRUNC('day', created_at)"),
            "{sql}"
        );
    }

    /// The plain `["a"]` wire format predates GroupByItem and must keep working.
    #[test]
    fn test_group_by_still_accepts_plain_strings() {
        let desc: QueryDescription = serde_json::from_str(
            r#"{ "table": "users", "groupBy": ["status"] }"#,
        )
        .unwrap();
        assert!(compile_query(&desc).unwrap().sql.contains("GROUP BY \"status\""));
    }

    #[test]
    fn test_order_by_raw_deserialises_from_json() {
        let desc: QueryDescription = serde_json::from_str(
            r#"{ "table": "users", "orderBy": [{ "raw": "RANDOM()" }] }"#,
        )
        .unwrap();
        assert!(compile_query(&desc).unwrap().sql.contains("ORDER BY RANDOM()"));
    }

    fn set_op(op: &str, all: bool) -> UnionDefinition {
        UnionDefinition {
            sql: "SELECT id FROM archived".into(),
            params: vec![],
            all,
            op: Some(op.into()),
        }
    }

    #[test]
    fn test_intersect_and_except() {
        for (op, keyword) in [("intersect", "INTERSECT"), ("except", "EXCEPT")] {
            let mut desc = simple_desc("users");
            desc.unions.push(set_op(op, false));
            let sql = compile_query_with_dialect(&desc, Dialect::Postgres).unwrap().sql;
            assert!(sql.contains(&format!("{keyword} SELECT id FROM archived")), "{sql}");
        }
    }

    #[test]
    fn test_order_by_comes_after_compound() {
        // Regression: ORDER BY was emitted on the first branch, before the set
        // operator (`SELECT … ORDER BY … EXCEPT SELECT …`), which every dialect
        // rejects. A trailing ORDER BY must bind to the whole compound.
        let mut desc = simple_desc("users");
        desc.order_by.push(OrderByClause {
            column: "id".into(),
            direction: "asc".into(),
            raw: None,
        });
        desc.limit = Some(5);
        desc.unions.push(set_op("except", false));
        let sql = compile_query_with_dialect(&desc, Dialect::Sqlite).unwrap().sql;
        let except_at = sql.find("EXCEPT").unwrap();
        let order_at = sql.find("ORDER BY").unwrap();
        assert!(except_at < order_at, "ORDER BY must follow the compound: {sql}");
        assert!(sql.trim_end().ends_with("LIMIT 5"), "{sql}");
    }

    #[test]
    fn test_set_operations_with_all() {
        let mut desc = simple_desc("users");
        desc.unions.push(set_op("intersect", true));
        assert!(compile_query_with_dialect(&desc, Dialect::Postgres)
            .unwrap()
            .sql
            .contains("INTERSECT ALL"));
    }

    /// SQLite's compound operators are UNION, UNION ALL, INTERSECT and EXCEPT —
    /// `INTERSECT ALL` is a syntax error there, so refuse rather than emit it.
    #[test]
    fn test_sqlite_rejects_intersect_all_but_allows_union_all() {
        let mut desc = simple_desc("users");
        desc.unions.push(set_op("intersect", true));
        let err = compile_query_with_dialect(&desc, Dialect::Sqlite).unwrap_err();
        assert!(err.contains("E_UNSUPPORTED"), "{err}");

        let mut plain = simple_desc("users");
        plain.unions.push(set_op("intersect", false));
        assert!(compile_query_with_dialect(&plain, Dialect::Sqlite).is_ok());

        let mut union_all = simple_desc("users");
        union_all.unions.push(set_op("union", true));
        assert!(compile_query_with_dialect(&union_all, Dialect::Sqlite)
            .unwrap()
            .sql
            .contains("UNION ALL"));
    }

    /// Omitting `op` must stay a UNION — that is the pre-existing wire format.
    #[test]
    fn test_missing_op_still_means_union() {
        let desc: QueryDescription = serde_json::from_str(
            r#"{ "table": "users", "unions": [{ "sql": "SELECT 1", "params": [], "all": false }] }"#,
        )
        .unwrap();
        let sql = compile_query(&desc).unwrap().sql;
        assert!(sql.contains("UNION SELECT 1"), "{sql}");
    }

    #[test]
    fn test_unknown_set_operation_is_rejected() {
        let mut desc = simple_desc("users");
        desc.unions.push(set_op("drop", false));
        assert!(compile_query(&desc).is_err());
    }

    /// RECURSIVE belongs to the WITH clause: one recursive CTE marks them all.
    #[test]
    fn test_recursive_cte_marks_the_whole_with_clause() {
        let mut desc = simple_desc("users");
        desc.ctes.push(CteDefinition {
            name: "plain".into(),
            sql: "SELECT 1".into(),
            params: vec![],
            recursive: false,
            materialized: None,
        });
        desc.ctes.push(CteDefinition {
            name: "tree".into(),
            sql: "SELECT 1 UNION ALL SELECT 2".into(),
            params: vec![],
            recursive: true,
            materialized: None,
        });
        let sql = compile_query(&desc).unwrap().sql;
        assert!(sql.starts_with("WITH RECURSIVE \"plain\" AS ("), "{sql}");
    }

    #[test]
    fn test_non_recursive_cte_has_no_recursive_keyword() {
        let mut desc = simple_desc("users");
        desc.ctes.push(CteDefinition {
            name: "c".into(),
            sql: "SELECT 1".into(),
            params: vec![],
            recursive: false,
            materialized: None,
        });
        assert!(!compile_query(&desc).unwrap().sql.contains("RECURSIVE"));
    }

    #[test]
    fn test_materialized_cte_hints() {
        let cte = |materialized| CteDefinition {
            name: "c".into(),
            sql: "SELECT 1".into(),
            params: vec![],
            recursive: false,
            materialized,
        };

        let mut yes = simple_desc("users");
        yes.ctes.push(cte(Some(true)));
        assert!(compile_query_with_dialect(&yes, Dialect::Postgres)
            .unwrap()
            .sql
            .contains("\"c\" AS MATERIALIZED (SELECT 1)"));

        let mut no = simple_desc("users");
        no.ctes.push(cte(Some(false)));
        assert!(compile_query_with_dialect(&no, Dialect::Postgres)
            .unwrap()
            .sql
            .contains("\"c\" AS NOT MATERIALIZED (SELECT 1)"));

        // MySQL has no such hint — refuse rather than emit a syntax error.
        let mut my = simple_desc("users");
        my.ctes.push(cte(Some(true)));
        assert!(compile_query_with_dialect(&my, Dialect::Mysql).is_err());

        // Without the hint, MySQL compiles fine.
        let mut plain = simple_desc("users");
        plain.ctes.push(cte(None));
        assert!(compile_query_with_dialect(&plain, Dialect::Mysql).is_ok());
    }

    #[test]
    fn test_distinct_on_is_postgres_only() {
        let mut desc = simple_desc("users");
        desc.distinct_on = vec!["email".into()];
        assert!(compile_query_with_dialect(&desc, Dialect::Postgres)
            .unwrap()
            .sql
            .starts_with("SELECT DISTINCT ON (\"email\") "));

        // Elsewhere it would parse as DISTINCT over a parenthesised list and
        // quietly return different rows — refuse instead.
        for d in [Dialect::Sqlite, Dialect::Mysql] {
            assert!(compile_query_with_dialect(&desc, d).is_err());
        }
    }

    #[test]
    fn test_distinct_on_rejects_injected_identifier() {
        let mut desc = simple_desc("users");
        desc.distinct_on = vec!["email\"); DROP TABLE users; --".into()];
        assert!(compile_query_with_dialect(&desc, Dialect::Postgres).is_err());
    }

    fn json_where(json: serde_json::Value) -> QueryDescription {
        let mut desc = simple_desc("docs");
        desc.wheres.push(json);
        desc
    }

    #[test]
    fn test_where_json_path_per_dialect() {
        let clause = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "path",
            "column": "data", "path": "$.name", "operator": "=", "value": "ada"
        });
        let pg = compile_query_with_dialect(&json_where(clause.clone()), Dialect::Postgres).unwrap();
        assert!(
            pg.sql.contains("(jsonb_path_query_first(\"data\"::jsonb, $1::jsonpath) #>> '{}') = $2"),
            "{}", pg.sql
        );
        assert_eq!(pg.params, vec![serde_json::json!("$.name"), serde_json::json!("ada")]);

        let my = compile_query_with_dialect(&json_where(clause.clone()), Dialect::Mysql).unwrap();
        assert!(my.sql.contains("JSON_UNQUOTE(JSON_EXTRACT(`data`, ?)) = ?"), "{}", my.sql);

        let lite = compile_query_with_dialect(&json_where(clause), Dialect::Sqlite).unwrap();
        assert!(lite.sql.contains("json_extract(\"data\", ?) = ?"), "{}", lite.sql);
        // The path and value are BOUND, never interpolated.
        assert_eq!(lite.params, vec![serde_json::json!("$.name"), serde_json::json!("ada")]);
    }

    #[test]
    fn test_where_json_path_negated() {
        let clause = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "path", "negated": true,
            "column": "data", "path": "$.n", "operator": ">", "value": 5
        });
        let sql = compile_query_with_dialect(&json_where(clause), Dialect::Sqlite).unwrap().sql;
        assert!(sql.contains("NOT (json_extract(\"data\", ?) > ?)"), "{sql}");
    }

    #[test]
    fn test_where_json_containment_pg_and_mysql() {
        let sup = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "superset",
            "column": "tags", "value": "[\"a\"]"
        });
        let pg = compile_query_with_dialect(&json_where(sup.clone()), Dialect::Postgres).unwrap().sql;
        assert!(pg.contains("\"tags\"::jsonb @> $1::jsonb"), "{pg}");

        let my = compile_query_with_dialect(&json_where(sup), Dialect::Mysql).unwrap().sql;
        assert!(my.contains("JSON_CONTAINS(`tags`, ?)"), "{my}");

        let sub = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "subset",
            "column": "tags", "value": "[\"a\",\"b\"]"
        });
        let pg_sub = compile_query_with_dialect(&json_where(sub.clone()), Dialect::Postgres).unwrap().sql;
        assert!(pg_sub.contains("\"tags\"::jsonb <@ $1::jsonb"), "{pg_sub}");
        // Subset flips the JSON_CONTAINS argument order on MySQL.
        let my_sub = compile_query_with_dialect(&json_where(sub), Dialect::Mysql).unwrap().sql;
        assert!(my_sub.contains("JSON_CONTAINS(?, `tags`)"), "{my_sub}");
    }

    /// SQLite has no JSON containment operator — refuse rather than emit wrong SQL.
    #[test]
    fn test_json_containment_rejected_on_sqlite() {
        for op in ["superset", "subset"] {
            let clause = serde_json::json!({
                "kind": "json", "type": "and", "jsonOp": op,
                "column": "tags", "value": "[]"
            });
            let err = compile_query_with_dialect(&json_where(clause), Dialect::Sqlite).unwrap_err();
            assert!(err.contains("E_UNSUPPORTED"), "{err}");
        }
    }

    #[test]
    fn test_json_path_rejects_bad_operator() {
        let clause = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "path",
            "column": "data", "path": "$.n", "operator": "; DROP TABLE docs --", "value": 1
        });
        assert!(compile_query_with_dialect(&json_where(clause), Dialect::Sqlite).is_err());
    }

    #[test]
    fn test_json_where_column_is_quoted_against_injection() {
        let clause = serde_json::json!({
            "kind": "json", "type": "and", "jsonOp": "path",
            "column": "data\"; DROP TABLE docs; --", "path": "$.n", "operator": "=", "value": 1
        });
        assert!(compile_query_with_dialect(&json_where(clause), Dialect::Sqlite).is_err());
    }

    #[test]
    fn test_having_raw_and_or() {
        let mut desc = simple_desc("orders");
        desc.select = vec!["status".to_string()];
        desc.group_by = vec![GroupByItem::Column("status".to_string())];
        desc.having.push(serde_json::json!({
            "column": "COUNT(*)", "operator": ">", "value": 2, "type": "and"
        }));
        desc.having.push(serde_json::json!({
            "kind": "raw", "sql": "SUM(total) < ?", "bindings": [1000], "type": "or"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("HAVING COUNT(*) > ? OR (SUM(total) < ?)"), "sql was: {}", result.sql);
        assert_eq!(result.params, vec![serde_json::json!(2), serde_json::json!(1000)]);
    }

    #[test]
    fn test_cte() {
        let mut desc = simple_desc("active_orders");
        desc.ctes.push(CteDefinition {
            name: "active_orders".to_string(),
            sql: "SELECT * FROM \"orders\" WHERE \"status\" = ?".to_string(),
            params: vec![serde_json::json!("active")],
            recursive: false,
            materialized: None,
        });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WITH \"active_orders\" AS ("));
        assert!(result.sql.contains("WHERE \"status\" = ?"));
        assert_eq!(result.params, vec![serde_json::json!("active")]);
    }

    #[test]
    fn test_union() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status",
            "operator": "=",
            "value": "pending",
            "type": "and"
        }));
        desc.unions.push(UnionDefinition {
            sql: "SELECT * FROM \"orders\" WHERE \"status\" = ?".to_string(),
            params: vec![serde_json::json!("paid")],
            all: false,
            op: None,
        });
        let result = compile_query(&desc).unwrap();
        // Not parenthesised: SQLite rejects `… UNION (SELECT …)` outright, so the
        // old assertion was pinning SQL that could never execute there.
        assert!(result.sql.contains("UNION SELECT * FROM \"orders\""), "{}", result.sql);
        assert_eq!(result.params, vec![serde_json::json!("pending"), serde_json::json!("paid")]);
    }

    #[test]
    fn test_rejects_sql_injection_in_table() {
        let desc = simple_desc("orders; DROP TABLE users--");
        let result = compile_query(&desc);
        assert!(result.is_err());
    }

    #[test]
    fn test_rejects_sql_injection_in_column() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "id; DROP TABLE orders--",
            "operator": "=",
            "value": 1,
            "type": "and"
        }));
        let result = compile_query(&desc);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_null() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "deleted_at",
            "operator": "IS NULL",
            "value": null,
            "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("\"deleted_at\" IS NULL"));
        assert!(result.params.is_empty());
    }

    #[test]
    fn test_or_where() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({ "column": "status", "operator": "=", "value": "active", "type": "and" }));
        desc.wheres.push(serde_json::json!({ "column": "status", "operator": "=", "value": "pending", "type": "or" }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE \"status\" = ? OR \"status\" = ?"));
    }

    #[test]
    fn test_where_between() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "total", "operator": "BETWEEN", "value": [10, 100], "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE \"total\" BETWEEN ? AND ?"));
        assert_eq!(result.params, vec![serde_json::json!(10), serde_json::json!(100)]);
    }

    #[test]
    fn test_where_cast_uuid_pk_postgres() {
        // Regression: a WHERE comparison against a uuid/timestamp column must
        // cast the bound string param, else Postgres errors `operator does not
        // exist: uuid = text`. The cast hint rides `QueryDescription::casts`.
        let mut desc = simple_desc("users");
        desc.casts.insert("id".into(), "uuid".into());
        desc.wheres.push(serde_json::json!({
            "column": "id", "operator": "=", "value": "0191-uuid", "type": "and"
        }));
        let pg = compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).unwrap();
        assert!(pg.sql.contains("\"id\" = $1::uuid"), "pg sql was: {}", pg.sql);
        // IN list casts every element.
        let mut desc_in = simple_desc("users");
        desc_in.casts.insert("id".into(), "uuid".into());
        desc_in.wheres.push(serde_json::json!({
            "column": "id", "operator": "IN", "value": ["a", "b"], "type": "and"
        }));
        let pg_in = compile_query_with_dialect(&desc_in, crate::dialect::Dialect::Postgres).unwrap();
        assert!(pg_in.sql.contains("IN ($1::uuid, $2::uuid)"), "pg in sql was: {}", pg_in.sql);
        // BETWEEN casts BOTH bounds (regression: emitted `$1date` — the cast was
        // concatenated without `::` on a date column, so Postgres errored
        // "trailing junk after parameter at or near \"$1date\"").
        let mut desc_bt = simple_desc("events");
        desc_bt.casts.insert("day".into(), "date".into());
        desc_bt.wheres.push(serde_json::json!({
            "column": "day", "operator": "BETWEEN", "value": ["2026-01-01", "2026-12-31"], "type": "and"
        }));
        let pg_bt = compile_query_with_dialect(&desc_bt, crate::dialect::Dialect::Postgres).unwrap();
        assert!(pg_bt.sql.contains("BETWEEN $1::date AND $2::date"), "pg between sql was: {}", pg_bt.sql);
        // Never cast on sqlite (the driver coerces).
        let sq = compile_query_with_dialect(&desc, crate::dialect::Dialect::Sqlite).unwrap();
        assert!(!sq.sql.contains("::"), "sqlite sql was: {}", sq.sql);
    }

    #[test]
    fn test_where_not_between() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "total", "operator": "NOT BETWEEN", "value": [10, 100], "type": "and"
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("NOT BETWEEN ? AND ?"));
    }

    #[test]
    fn test_where_between_rejects_wrong_arity() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "total", "operator": "BETWEEN", "value": [10], "type": "and"
        }));
        assert!(compile_query(&desc).is_err());
    }

    #[test]
    fn test_where_ilike_on_postgres() {
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "column": "email", "operator": "ILIKE", "value": "%@ACME.COM", "type": "and"
        }));
        let result = compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).unwrap();
        assert!(result.sql.contains("\"email\" ILIKE $1"));
    }

    #[test]
    fn test_where_ilike_on_sqlite_falls_back_to_lower() {
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "column": "email", "operator": "ILIKE", "value": "%@acme.com", "type": "and"
        }));
        let result = compile_query_with_dialect(&desc, crate::dialect::Dialect::Sqlite).unwrap();
        assert!(result.sql.contains("LOWER(\"email\") LIKE LOWER(?)"));
    }

    #[test]
    fn test_where_raw_with_bindings() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "kind": "raw",
            "sql": "total > ? AND created_at < ?",
            "bindings": [100, "2026-01-01"],
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE (total > ? AND created_at < ?)"));
        assert_eq!(result.params.len(), 2);
    }

    #[test]
    fn test_where_exists_subquery() {
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "kind": "exists",
            "type": "and",
            "subquery": {
                "table": "comments",
                "select": ["*"],
                "wheres": [{
                    "kind": "raw",
                    "type": "and",
                    "sql": "\"comments\".\"user_id\" = \"users\".\"id\"",
                    "bindings": []
                }]
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE EXISTS (SELECT * FROM \"comments\""));
        assert!(result.sql.contains("\"comments\".\"user_id\" = \"users\".\"id\""));
    }

    #[test]
    fn test_where_not_exists_subquery() {
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "kind": "exists",
            "negated": true,
            "type": "and",
            "subquery": {
                "table": "comments",
                "select": ["*"],
                "wheres": [{
                    "kind": "raw",
                    "type": "and",
                    "sql": "\"comments\".\"user_id\" = \"users\".\"id\"",
                    "bindings": []
                }]
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE NOT EXISTS ("));
    }

    #[test]
    fn test_where_exists_with_count_threshold() {
        // has('comments', '>', 2) → EXISTS (SELECT * FROM comments WHERE <join> HAVING COUNT(*) > ?)
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "kind": "exists",
            "type": "and",
            "subquery": {
                "table": "comments",
                "select": ["*"],
                "wheres": [{
                    "kind": "raw",
                    "type": "and",
                    "sql": "\"comments\".\"user_id\" = \"users\".\"id\"",
                    "bindings": []
                }],
                "having": [{
                    "column": "COUNT(*)",
                    "operator": ">",
                    "value": 2,
                    "type": "and"
                }]
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("HAVING COUNT(*) > ?"));
        assert_eq!(result.params, vec![serde_json::json!(2)]);
    }

    #[test]
    fn test_where_exists_nested() {
        // users whereHas posts whereHas comments
        let mut desc = simple_desc("users");
        desc.wheres.push(serde_json::json!({
            "kind": "exists",
            "type": "and",
            "subquery": {
                "table": "posts",
                "select": ["*"],
                "wheres": [
                    { "kind": "raw", "type": "and",
                      "sql": "\"posts\".\"user_id\" = \"users\".\"id\"", "bindings": [] },
                    { "kind": "exists", "type": "and", "subquery": {
                        "table": "comments", "select": ["*"],
                        "wheres": [{ "kind": "raw", "type": "and",
                          "sql": "\"comments\".\"post_id\" = \"posts\".\"id\"", "bindings": [] }]
                    }}
                ]
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("EXISTS (SELECT * FROM \"posts\""));
        assert!(result.sql.contains("EXISTS (SELECT * FROM \"comments\""));
    }

    #[test]
    fn test_where_group_parenthesises_nested_conditions() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "kind": "group",
            "type": "and",
            "conditions": [
                { "column": "a", "operator": "=", "value": 1, "type": "and" },
                { "column": "b", "operator": "=", "value": 2, "type": "or" },
            ]
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE (\"a\" = ? OR \"b\" = ?)"));
        assert_eq!(result.params, vec![serde_json::json!(1), serde_json::json!(2)]);
    }

    #[test]
    fn test_where_group_composes_with_sibling_or() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "column": "status", "operator": "=", "value": "paid", "type": "and"
        }));
        desc.wheres.push(serde_json::json!({
            "kind": "group", "type": "or",
            "conditions": [
                { "column": "a", "operator": "=", "value": 1, "type": "and" },
                { "column": "b", "operator": "=", "value": 2, "type": "and" },
            ]
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE \"status\" = ? OR (\"a\" = ? AND \"b\" = ?)"));
    }

    #[test]
    fn test_where_in_subquery() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "kind": "inSub", "type": "and",
            "column": "user_id",
            "subquery": {
                "table": "users",
                "select": ["id"],
                "wheres": [{ "column": "active", "operator": "=", "value": true, "type": "and" }]
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("WHERE \"user_id\" IN (SELECT \"id\" FROM \"users\""));
        assert_eq!(result.params, vec![serde_json::json!(true)]);
    }

    #[test]
    fn test_where_not_in_subquery() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "kind": "inSub", "type": "and", "negated": true,
            "column": "user_id",
            "subquery": {
                "table": "banned",
                "select": ["user_id"],
            }
        }));
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("NOT IN (SELECT \"user_id\" FROM \"banned\""));
    }

    #[test]
    fn test_join_raw_fragment_inlined() {
        let mut desc = simple_desc("orders");
        desc.joins.push(JoinClause { sql: "INNER JOIN \"users\" ON \"users\".\"id\" = \"orders\".\"user_id\"".into(), params: vec![] });
        let result = compile_query(&desc).unwrap();
        assert!(result.sql.contains("FROM \"orders\" INNER JOIN \"users\" ON"));
    }

    #[test]
    fn test_join_rejects_dangerous_chars() {
        let mut desc = simple_desc("orders");
        desc.joins.push(JoinClause { sql: "INNER JOIN users; DROP TABLE orders --".into(), params: vec![] });
        assert!(compile_query(&desc).is_err());
    }

    #[test]
    fn test_lock_mode_postgres() {
        let mut desc = simple_desc("orders");
        desc.lock_mode = Some("FOR UPDATE".into());
        let result = compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).unwrap();
        assert!(result.sql.ends_with("FOR UPDATE"));
    }

    #[test]
    fn test_join_params_ordered_before_where() {
        // A JOIN with a bound `?` (onVal) followed by a WHERE with a bound value:
        // join param must take $1, the where param $2.
        let mut desc = simple_desc("orders");
        desc.joins = vec![JoinClause {
            sql: "INNER JOIN \"users\" ON \"users\".\"id\" = \"orders\".\"user_id\" AND \"orders\".\"status\" = ?".into(),
            params: vec![serde_json::json!("paid")],
        }];
        desc.wheres = vec![serde_json::json!({
            "type": "and", "column": "region", "operator": "=", "value": "eu"
        })];
        let r =
            compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).unwrap();
        // $1 appears in the JOIN, $2 in the WHERE; params in the same order.
        let join_pos = r.sql.find("$1").unwrap();
        let where_pos = r.sql.find("$2").unwrap();
        assert!(join_pos < where_pos);
        assert_eq!(r.params, vec![serde_json::json!("paid"), serde_json::json!("eu")]);
    }

    #[test]
    fn test_join_params_mismatch_errors() {
        let mut desc = simple_desc("orders");
        desc.joins = vec![JoinClause {
            sql: "INNER JOIN \"u\" ON \"u\".\"x\" = ?".into(),
            params: vec![], // placeholder but no bound value
        }];
        assert!(compile_query_with_dialect(&desc, crate::dialect::Dialect::Sqlite).is_err());
    }

    #[test]
    fn test_lock_mode_sqlite_silently_dropped() {
        let mut desc = simple_desc("orders");
        desc.lock_mode = Some("FOR UPDATE".into());
        let result = compile_query_with_dialect(&desc, crate::dialect::Dialect::Sqlite).unwrap();
        assert!(!result.sql.contains("FOR UPDATE"));
    }

    #[test]
    fn test_lock_mode_postgres_no_key_update_and_modifiers() {
        let mut desc = simple_desc("orders");
        desc.lock_mode = Some("FOR NO KEY UPDATE SKIP LOCKED".into());
        let pg = compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).unwrap();
        assert!(pg.sql.ends_with("FOR NO KEY UPDATE SKIP LOCKED"));

        let mut desc2 = simple_desc("orders");
        desc2.lock_mode = Some("FOR UPDATE NOWAIT".into());
        let pg2 = compile_query_with_dialect(&desc2, crate::dialect::Dialect::Postgres).unwrap();
        assert!(pg2.sql.ends_with("FOR UPDATE NOWAIT"));
    }

    #[test]
    fn test_lock_mode_no_key_update_rejected_on_mysql() {
        let mut desc = simple_desc("orders");
        desc.lock_mode = Some("FOR KEY SHARE".into());
        let err = compile_query_with_dialect(&desc, crate::dialect::Dialect::Mysql);
        assert!(err.is_err());
    }

    #[test]
    fn test_lock_mode_unknown_rejected() {
        let mut desc = simple_desc("orders");
        desc.lock_mode = Some("FOR DANCING".into());
        assert!(compile_query_with_dialect(&desc, crate::dialect::Dialect::Postgres).is_err());
    }

    #[test]
    fn test_where_raw_rejects_binding_mismatch() {
        let mut desc = simple_desc("orders");
        desc.wheres.push(serde_json::json!({
            "kind": "raw",
            "sql": "total > ? AND created_at < ?",
            "bindings": [100], // only 1 binding for 2 placeholders
        }));
        assert!(compile_query(&desc).is_err());
    }
}
