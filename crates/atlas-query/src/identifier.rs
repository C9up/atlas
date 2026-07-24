//! Identifier validation and quoting — prevents SQL injection.

use crate::dialect::Dialect;

/// Validate and quote a SQL identifier (table/column name).
/// Supports up to three dot-qualified segments: `"public"."orders"."id"`
/// (schema.table, table.column, or schema.table.column).
pub fn quote_identifier(name: &str) -> Result<String, String> {
    if name == "*" {
        return Ok(name.to_string());
    }
    // Split on dot for schema/table-qualified identifiers (max schema.table.column).
    let parts: Vec<&str> = name.splitn(4, '.').collect();
    if parts.len() > 3 {
        return Err(format!("Too many dot segments in identifier: '{}'", name));
    }
    for part in &parts {
        if part.is_empty() {
            return Err(format!("Empty segment in identifier: '{}'", name));
        }
        if part.contains('\0') || part.contains('"') {
            return Err(format!(
                "E_UNSAFE_IDENTIFIER: identifier contains an illegal character (quote or NUL): '{}'",
                name
            ));
        }
        if !part.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!(
                "E_UNSAFE_IDENTIFIER: invalid identifier '{}' — only letters, digits, and underscores are allowed",
                name
            ));
        }
    }
    Ok(parts.iter().map(|p| format!("\"{}\"", p)).collect::<Vec<_>>().join("."))
}

/// Dangerous SQL patterns — blocked in all expression contexts.
fn contains_dangerous_sql(expr: &str) -> bool {
    let lower = expr.to_lowercase();
    lower.contains(';')
        || lower.contains("--")
        || lower.contains("/*")
        || lower.contains("*/")
        || lower.contains("union ")
        || lower.contains("union\t")
        || lower.contains(" into ")
        || lower.contains("exec ")
        || lower.contains("execute ")
        || lower.contains("drop ")
        || lower.contains("alter ")
        || lower.contains("create ")
        || lower.contains("insert ")
        || lower.contains("update ")
        || lower.contains("delete ")
        || lower.contains("truncate ")
        || lower.contains("xp_")
        || lower.contains("sp_")
        || lower.contains("\\x00")
        // A `select` keyword anywhere in a column/aggregate expression is a
        // sub-select — never legitimate via select()/having() (use RawSql for
        // that). Token-match so `selected_at` (a real column) is NOT blocked
        // while `COALESCE((SELECT secret FROM users),0)` is, regardless of the
        // whitespace the payload uses around the parens.
        || contains_keyword(&lower, "select")
}

/// True when `keyword` appears as a standalone token in `lower`
/// (already-lowercased input) — bounded by non-identifier characters on
/// both sides, so `selected_at` doesn't match `select`.
fn contains_keyword(lower: &str, keyword: &str) -> bool {
    lower
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|tok| tok == keyword)
}

/// Allowed aggregate/window function prefixes (case-insensitive).
const ALLOWED_FUNCTIONS: &[&str] = &[
    "count", "sum", "avg", "min", "max",
    "coalesce", "nullif", "cast", "case",
    "row_number", "rank", "dense_rank", "ntile",
    "lag", "lead", "first_value", "last_value",
    "array_agg", "string_agg", "json_agg",
    "extract", "date_trunc", "now", "length",
    "lower", "upper", "trim", "replace", "substring",
    "round", "ceil", "floor", "abs",
    "exists",
];

/// Check if an expression starts with a known safe function name.
fn starts_with_allowed_function(expr: &str) -> bool {
    let lower = expr.to_lowercase();
    let trimmed = lower.trim();
    ALLOWED_FUNCTIONS.iter().any(|f| {
        trimmed.starts_with(f) && trimmed.as_bytes().get(f.len()).map_or(false, |c| *c == b'(')
    })
}

/// Quote a SELECT expression — allows known aggregate/window functions and aliases.
/// Rejects unknown expressions containing `(` to prevent SQL injection via arbitrary sub-selects.
/// Identifiers are quoted for `dialect` (backticks on MySQL, `"` elsewhere).
pub fn quote_select_expr(name: &str, dialect: Dialect) -> Result<String, String> {
    if name == "*" {
        return Ok(name.to_string());
    }
    if contains_dangerous_sql(name) {
        return Err(format!("Dangerous SQL in expression: {}", name));
    }
    // Expression with parentheses — must start with a known function
    if name.contains('(') {
        if starts_with_allowed_function(name) {
            return Ok(name.to_string());
        }
        return Err(format!(
            "Unknown function in SELECT expression: '{}'. Use RawSql for custom expressions.",
            name
        ));
    }
    // Alias without function: "column AS alias"
    if name.to_lowercase().contains(" as ") {
        let parts: Vec<&str> = name.splitn(2, " as ").collect();
        if parts.len() == 2 || name.to_lowercase().splitn(2, " as ").count() == 2 {
            let col_part = name.split_whitespace().next().unwrap_or(name);
            let alias_part = name.rsplitn(2, ' ').next().unwrap_or("");
            return Ok(format!(
                "{} AS {}",
                dialect.quote_ident(col_part)?,
                dialect.quote_ident(alias_part)?
            ));
        }
    }
    dialect.quote_ident(name)
}

/// Quote a HAVING expression — allows known aggregate functions.
/// Identifiers are quoted for `dialect` (backticks on MySQL, `"` elsewhere).
pub fn quote_having_expr(name: &str, dialect: Dialect) -> Result<String, String> {
    if contains_dangerous_sql(name) {
        return Err(format!("Dangerous SQL in expression: {}", name));
    }
    if name.contains('(') {
        if starts_with_allowed_function(name) {
            return Ok(name.to_string());
        }
        return Err(format!(
            "Unknown function in HAVING expression: '{}'. Use RawSql for custom expressions.",
            name
        ));
    }
    dialect.quote_ident(name)
}

/// Validate an SQL operator against an allowlist.
///
/// Accepts both comparison operators and multi-word operators like `BETWEEN`
/// and `NOT BETWEEN`. `ILIKE` is Postgres-specific — the compiler rewrites it
/// to `LIKE` with a case-insensitive collation for sqlite/mysql at compile time.
pub fn validate_operator(op: &str) -> Result<&str, String> {
    match op {
        "=" | "!=" | "<>" | ">" | ">=" | "<" | "<="
        | "LIKE" | "ILIKE" | "NOT LIKE" | "NOT ILIKE"
        | "IN" | "NOT IN"
        | "IS NULL" | "IS NOT NULL"
        | "BETWEEN" | "NOT BETWEEN" => Ok(op),
        _ => Err(format!("Invalid operator: '{}'", op)),
    }
}

/// Validate ORDER BY direction.
pub fn validate_direction(dir: &str) -> Result<&'static str, String> {
    match dir.to_uppercase().as_str() {
        "ASC" => Ok("ASC"),
        "DESC" => Ok("DESC"),
        _ => Err(format!("Invalid ORDER BY direction: '{}'", dir)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("status").unwrap(), "\"status\"");
        assert_eq!(quote_identifier("*").unwrap(), "*");
    }

    #[test]
    fn test_quote_identifier_schema_qualified() {
        assert_eq!(quote_identifier("public.orders").unwrap(), "\"public\".\"orders\"");
        // schema.table.column — three segments (a join projection on a qualified table).
        assert_eq!(
            quote_identifier("public.orders.id").unwrap(),
            "\"public\".\"orders\".\"id\""
        );
    }

    #[test]
    fn test_quote_identifier_rejects_injection() {
        assert!(quote_identifier("id; DROP TABLE orders--").is_err());
        assert!(quote_identifier("id\"").is_err());
        assert!(quote_identifier("id\0").is_err());
        assert!(quote_identifier("a.b.c.d").is_err()); // more than three segments
        assert!(quote_identifier(".leading").is_err()); // empty segment
    }

    #[test]
    fn test_quote_select_expr() {
        use Dialect::{Mysql, Sqlite};
        // Known functions pass through
        assert_eq!(quote_select_expr("COUNT(*)", Sqlite).unwrap(), "COUNT(*)");
        assert_eq!(quote_select_expr("SUM(amount)", Sqlite).unwrap(), "SUM(amount)");
        assert_eq!(quote_select_expr("COALESCE(name, 'unknown')", Sqlite).unwrap(), "COALESCE(name, 'unknown')");
        // Simple columns get quoted for the dialect (sqlite/pg `"`, MySQL backticks).
        assert_eq!(quote_select_expr("status", Sqlite).unwrap(), "\"status\"");
        assert_eq!(quote_select_expr("status", Mysql).unwrap(), "`status`");
        // Aliased column follows the dialect on BOTH sides.
        assert_eq!(quote_select_expr("name AS label", Mysql).unwrap(), "`name` AS `label`");
        assert_eq!(quote_select_expr("name AS label", Sqlite).unwrap(), "\"name\" AS \"label\"");
        // Dangerous patterns rejected
        assert!(quote_select_expr("1; DROP TABLE--", Sqlite).is_err());
        assert!(quote_select_expr("1 /* evil */", Sqlite).is_err());
        assert!(quote_select_expr("1 UNION SELECT * FROM users", Sqlite).is_err());
        // Unknown function rejected — must use RawSql
        assert!(quote_select_expr("evil_func(1)", Sqlite).is_err());
        // Sub-select smuggled through an ALLOWED function is rejected,
        // regardless of the whitespace around the parens (the previous
        // `contains_dangerous_sql` screen missed `select`).
        assert!(quote_select_expr("COALESCE((SELECT secret FROM users LIMIT 1),0)", Sqlite).is_err());
        assert!(quote_select_expr("COALESCE(  ( select x from t ) ,0)", Sqlite).is_err());
        assert!(quote_select_expr("CAST((SELECT 1) AS int)", Sqlite).is_err());
        // But a real column whose name merely contains "select" is fine.
        assert_eq!(quote_select_expr("selected_at", Sqlite).unwrap(), "\"selected_at\"");
        // And EXTRACT(... FROM ...) — which legitimately contains `from` —
        // still passes (we only block the `select` keyword token).
        assert_eq!(
            quote_select_expr("EXTRACT(year FROM created_at)", Sqlite).unwrap(),
            "EXTRACT(year FROM created_at)"
        );
    }

    #[test]
    fn test_validate_operator() {
        assert!(validate_operator("=").is_ok());
        assert!(validate_operator(">=").is_ok());
        assert!(validate_operator("LIKE").is_ok());
        assert!(validate_operator("= 1; DROP TABLE--").is_err());
    }

    #[test]
    fn test_validate_direction() {
        assert_eq!(validate_direction("asc").unwrap(), "ASC");
        assert_eq!(validate_direction("DESC").unwrap(), "DESC");
        assert!(validate_direction("asc; DROP TABLE--").is_err());
    }
}
