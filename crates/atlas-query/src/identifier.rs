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
    Ok(parts
        .iter()
        .map(|p| format!("\"{}\"", p))
        .collect::<Vec<_>>()
        .join("."))
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
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "coalesce",
    "nullif",
    "cast",
    "case",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "array_agg",
    "string_agg",
    "json_agg",
    "extract",
    "date_trunc",
    "now",
    "length",
    "lower",
    "upper",
    "trim",
    "replace",
    "substring",
    "round",
    "ceil",
    "floor",
    "abs",
    "exists",
];

/// Bare words allowed inside an expression: SQL that is grammar, not a call.
/// Anything else that is not a call must look like an identifier.
const ALLOWED_KEYWORDS: &[&str] = &[
    "as",
    "from",
    "distinct",
    "filter",
    "where",
    "over",
    "partition",
    "by",
    "order",
    "asc",
    "desc",
    "and",
    "or",
    "not",
    "null",
    "is",
    "case",
    "when",
    "then",
    "else",
    "end",
    "interval",
    "true",
    "false",
    "within",
    "group",
    "nulls",
    "first",
    "last",
    "rows",
    "range",
    "between",
    "unbounded",
    "preceding",
    "following",
    "current",
    "row",
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "week",
    "quarter",
    "epoch",
    "doy",
    "dow",
    "leading",
    "trailing",
    "both",
    "for",
];

/// Every function named anywhere in `expr` must be allow-listed, every bare word
/// must be a keyword or an identifier, and no comma may sit at depth zero.
///
/// Checking only the FIRST token — which is what this replaced — validated the
/// wrapper and let its contents through verbatim. `cast(pg_read_file('/etc/passwd')
/// as text)` passed because it starts with `cast`, and returned the file; so did
/// `abs(1), version()`, because a top-level comma appends an expression the caller
/// never named as a column. A deny-list cannot close that: it has to know every
/// dangerous function, and Postgres ships more each release.
fn expression_is_allowed(expr: &str) -> bool {
    let bytes: Vec<char> = expr.chars().collect();
    let mut i = 0usize;
    let mut depth = 0i32;
    while i < bytes.len() {
        let c = bytes[i];
        // String literal — skipped whole, including doubled quotes.
        if c == '\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == '\'' {
                    if bytes.get(i + 1) == Some(&'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            if i >= bytes.len() {
                return false; // unterminated literal
            }
            i += 1;
            continue;
        }
        // A quoted identifier — `"amount"` or `` `amount` ``. The compiler emits
        // these itself (an aggregate over an already-quoted column comes back
        // through here), so the token is read whole and its contents checked as
        // an identifier rather than reasoned about character by character.
        if c == '"' || c == '`' {
            let closing = c;
            let start = i + 1;
            i += 1;
            while i < bytes.len() && bytes[i] != closing {
                i += 1;
            }
            if i >= bytes.len() {
                return false; // unterminated identifier
            }
            let inner: String = bytes[start..i].iter().collect();
            if inner.is_empty() || quote_identifier(&inner).is_err() {
                return false;
            }
            i += 1;
            continue;
        }
        if c == '(' {
            depth += 1;
            i += 1;
            continue;
        }
        if c == ')' {
            depth -= 1;
            if depth < 0 {
                return false;
            }
            i += 1;
            continue;
        }
        // A comma outside every paren adds a second SELECT expression.
        if c == ',' && depth == 0 {
            return false;
        }
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_alphanumeric() || bytes[i] == '_') {
                i += 1;
            }
            let word: String = bytes[start..i].iter().collect();
            // A call is a word followed by `(`, whitespace allowed between.
            let mut j = i;
            while j < bytes.len() && bytes[j].is_whitespace() {
                j += 1;
            }
            let is_call = bytes.get(j) == Some(&'(');
            let lower = word.to_lowercase();
            if is_call {
                if !ALLOWED_FUNCTIONS.contains(&lower.as_str()) {
                    return false;
                }
            } else if !ALLOWED_KEYWORDS.contains(&lower.as_str())
                && quote_identifier(&word).is_err()
            {
                return false;
            }
            continue;
        }
        // Remaining characters: digits, whitespace, and the operators an
        // expression legitimately uses. Anything else (`;`, `$`, `@`, `\`, …) is
        // refused rather than reasoned about.
        if !(c.is_ascii_digit()
            || c.is_whitespace()
            // `,` only reaches here at depth > 0 — the depth-zero case was
            // refused above, because that is what appends an expression.
            || matches!(
                c,
                '*' | '+' | '-' | '/' | '%' | '.' | ':' | '|' | '<' | '>' | '=' | '!' | ','
            ))
        {
            return false;
        }
        i += 1;
    }
    depth == 0
}

/// Quote a SELECT expression — allows known aggregate/window functions and aliases.
/// Rejects unknown expressions containing `(` to prevent SQL injection via arbitrary sub-selects.
/// Identifiers are quoted for `dialect` (backticks on MySQL, `"` elsewhere).
pub fn quote_select_expr(name: &str, dialect: Dialect) -> Result<String, String> {
    if name == "*" {
        return Ok(name.to_string());
    }
    if contains_dangerous_sql(name) {
        return Err(format!(
            "E_INJECTION_PATTERN: expression carries a SQL pattern no caller writes by hand: {}",
            name
        ));
    }
    // Expression with parentheses — must start with a known function
    if name.contains('(') {
        if expression_is_allowed(name) {
            return Ok(name.to_string());
        }
        return Err(format!(
            "E_UNSAFE_EXPRESSION: '{}' names a function, a word or a separator this expression may not use. Use db.raw() for a custom expression.",
            name
        ));
    }
    // Alias without function: "column AS alias"
    if name.to_lowercase().contains(" as ") {
        let parts: Vec<&str> = name.splitn(2, " as ").collect();
        if parts.len() == 2 || name.to_lowercase().splitn(2, " as ").count() == 2 {
            let col_part = name.split_whitespace().next().unwrap_or(name);
            let alias_part = name.rsplit(' ').next().unwrap_or("");
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
        return Err(format!(
            "E_INJECTION_PATTERN: expression carries a SQL pattern no caller writes by hand: {}",
            name
        ));
    }
    if name.contains('(') {
        if expression_is_allowed(name) {
            return Ok(name.to_string());
        }
        return Err(format!(
            "E_UNSAFE_EXPRESSION: '{}' names a function, a word or a separator this expression may not use. Use db.raw() for a custom expression.",
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
        "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" | "LIKE" | "ILIKE" | "NOT LIKE"
        | "NOT ILIKE" | "IN" | "NOT IN" | "IS NULL" | "IS NOT NULL" | "BETWEEN" | "NOT BETWEEN" => {
            Ok(op)
        }
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
        assert_eq!(
            quote_identifier("public.orders").unwrap(),
            "\"public\".\"orders\""
        );
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
        assert_eq!(
            quote_select_expr("SUM(amount)", Sqlite).unwrap(),
            "SUM(amount)"
        );
        assert_eq!(
            quote_select_expr("COALESCE(name, 'unknown')", Sqlite).unwrap(),
            "COALESCE(name, 'unknown')"
        );
        // Simple columns get quoted for the dialect (sqlite/pg `"`, MySQL backticks).
        assert_eq!(quote_select_expr("status", Sqlite).unwrap(), "\"status\"");
        assert_eq!(quote_select_expr("status", Mysql).unwrap(), "`status`");
        // Aliased column follows the dialect on BOTH sides.
        assert_eq!(
            quote_select_expr("name AS label", Mysql).unwrap(),
            "`name` AS `label`"
        );
        assert_eq!(
            quote_select_expr("name AS label", Sqlite).unwrap(),
            "\"name\" AS \"label\""
        );
        // Dangerous patterns rejected
        assert!(quote_select_expr("1; DROP TABLE--", Sqlite).is_err());
        assert!(quote_select_expr("1 /* evil */", Sqlite).is_err());
        assert!(quote_select_expr("1 UNION SELECT * FROM users", Sqlite).is_err());
        // Unknown function rejected — must use RawSql
        assert!(quote_select_expr("evil_func(1)", Sqlite).is_err());
        // Sub-select smuggled through an ALLOWED function is rejected,
        // regardless of the whitespace around the parens (the previous
        // `contains_dangerous_sql` screen missed `select`).
        assert!(
            quote_select_expr("COALESCE((SELECT secret FROM users LIMIT 1),0)", Sqlite).is_err()
        );
        assert!(quote_select_expr("COALESCE(  ( select x from t ) ,0)", Sqlite).is_err());
        assert!(quote_select_expr("CAST((SELECT 1) AS int)", Sqlite).is_err());
        // But a real column whose name merely contains "select" is fine.
        assert_eq!(
            quote_select_expr("selected_at", Sqlite).unwrap(),
            "\"selected_at\""
        );
        // And EXTRACT(... FROM ...) — which legitimately contains `from` —
        // still passes (we only block the `select` keyword token).
        assert_eq!(
            quote_select_expr("EXTRACT(year FROM created_at)", Sqlite).unwrap(),
            "EXTRACT(year FROM created_at)"
        );
    }

    #[test]
    fn select_expression_is_checked_whole_not_by_its_first_token() {
        use Dialect::Sqlite;
        // Each of these passed when only the leading function name was checked
        // and the rest returned verbatim. The first one read the server's
        // /etc/passwd and handed it back as a column.
        assert!(quote_select_expr("cast(pg_read_file('/etc/passwd') as text)", Sqlite).is_err());
        assert!(quote_select_expr("abs(1), version()", Sqlite).is_err());
        assert!(quote_select_expr("length(current_setting('is_superuser'))", Sqlite).is_err());
        assert!(quote_select_expr("count(*), pg_sleep(10)", Sqlite).is_err());
        assert!(quote_select_expr("max(id) as a, min(id) as b", Sqlite).is_err());
        assert!(quote_select_expr("coalesce(lo_import('/etc/passwd'), 0)", Sqlite).is_err());
        // Unbalanced parens, and a quote left open, are refused rather than
        // handed to the server to interpret.
        assert!(quote_select_expr("count((1)", Sqlite).is_err());
        assert!(quote_select_expr("coalesce(name, 'x)", Sqlite).is_err());

        // And the expressions that were always legitimate still are.
        assert_eq!(quote_select_expr("COUNT(*)", Sqlite).unwrap(), "COUNT(*)");
        assert_eq!(
            quote_select_expr("COALESCE(name, 'unknown')", Sqlite).unwrap(),
            "COALESCE(name, 'unknown')"
        );
        assert_eq!(
            quote_select_expr("EXTRACT(year FROM created_at)", Sqlite).unwrap(),
            "EXTRACT(year FROM created_at)"
        );
        assert_eq!(
            quote_select_expr("upper(a.id::text)", Sqlite).unwrap(),
            "upper(a.id::text)"
        );
        assert_eq!(
            quote_select_expr("string_agg(name, ',')", Sqlite).unwrap(),
            "string_agg(name, ',')"
        );
        // A quoted identifier the compiler emitted itself, coming back through.
        assert_eq!(
            quote_select_expr("SUM(\"amount\") AS __scalar__", Sqlite).unwrap(),
            "SUM(\"amount\") AS __scalar__"
        );
    }

    #[test]
    fn having_expression_is_checked_whole_too() {
        use Dialect::Sqlite;
        assert!(quote_having_expr("count(pg_read_file('/etc/passwd'))", Sqlite).is_err());
        assert!(quote_having_expr("count(*), version()", Sqlite).is_err());
        assert_eq!(quote_having_expr("count(*)", Sqlite).unwrap(), "count(*)");
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
