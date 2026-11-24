//! Identifier validation and quoting — prevents SQL injection.

use crate::dialect::Dialect;
use crate::expression::{check_having_expression, check_select_expression};

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
        return Err(format!(
            "E_UNSAFE_IDENTIFIER: too many dot segments in identifier: '{}'",
            name
        ));
    }
    for part in &parts {
        if part.is_empty() {
            return Err(format!(
                "E_UNSAFE_IDENTIFIER: empty segment in identifier: '{}'",
                name
            ));
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
        // `#` opens a line comment on MySQL. No column, alias or aggregate this
        // guard admits needs one, so it is refused everywhere, inside a literal
        // included: the literal boundary is exactly what an attacker disputes.
        || lower.contains('#')
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

/// Quote a SELECT entry: a column, `column AS alias`, or an expression.
///
/// Anything written with a parenthesis is an expression and must hold in the
/// grammar of [`crate::expression`], which is what keeps a call to an unlisted
/// function, a sub-select or a second `FROM` out of the statement.
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
    if name.contains('(') {
        return match check_select_expression(name) {
            Ok(()) => Ok(name.to_string()),
            Err(reason) => Err(unsafe_expression(name, &reason)),
        };
    }
    // Alias without function: exactly `column AS alias`. Anything else around
    // the AS is refused: taking the first and last words of
    // `price * 2 AS double` compiled `"price" AS "double"`, a different
    // expression returned without a word.
    let tokens: Vec<&str> = name.split_whitespace().collect();
    if tokens.len() > 1 && tokens.iter().any(|t| t.eq_ignore_ascii_case("as")) {
        if let [column, keyword, alias] = tokens.as_slice() {
            if keyword.eq_ignore_ascii_case("as") {
                return Ok(format!(
                    "{} AS {}",
                    dialect.quote_ident(column)?,
                    dialect.quote_ident(alias)?
                ));
            }
        }
        return Err(format!(
            "E_UNSAFE_EXPRESSION: '{}' is not `column AS alias`. Use db.raw() for an expression.",
            name
        ));
    }
    dialect.quote_ident(name)
}

/// Quote a HAVING column: a column or an expression of the same grammar as
/// [`quote_select_expr`], without an alias.
/// Identifiers are quoted for `dialect` (backticks on MySQL, `"` elsewhere).
pub fn quote_having_expr(name: &str, dialect: Dialect) -> Result<String, String> {
    if contains_dangerous_sql(name) {
        return Err(format!(
            "E_INJECTION_PATTERN: expression carries a SQL pattern no caller writes by hand: {}",
            name
        ));
    }
    if name.contains('(') {
        return match check_having_expression(name) {
            Ok(()) => Ok(name.to_string()),
            Err(reason) => Err(unsafe_expression(name, &reason)),
        };
    }
    dialect.quote_ident(name)
}

fn unsafe_expression(expr: &str, reason: &str) -> String {
    format!("E_UNSAFE_EXPRESSION: '{expr}' {reason}. Use db.raw() for a custom expression.")
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
        _ => Err(format!(
            "E_ATLAS_INVALID_OPERATOR: invalid operator: '{}'",
            op
        )),
    }
}

/// Validate ORDER BY direction.
pub fn validate_direction(dir: &str) -> Result<&'static str, String> {
    match dir.to_uppercase().as_str() {
        "ASC" => Ok("ASC"),
        "DESC" => Ok("DESC"),
        _ => Err(format!(
            "E_UNSAFE_SQL: invalid ORDER BY direction: '{}'",
            dir
        )),
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
    fn a_quoted_or_qualified_function_name_is_never_a_call() {
        // Postgres and SQLite call a double-quoted name, MySQL a backticked
        // one: the allow-list only ever saw the bare spelling, so the quoted
        // one ran any server function.
        for dialect in [Dialect::Sqlite, Dialect::Postgres, Dialect::Mysql] {
            for expr in [
                "\"pg_read_file\"('/etc/passwd') as leak",
                "\"pg_sleep\"(1)",
                "\"pg_sleep\" (1)",
                "`sleep`(1)",
                "\"set_config\"('role', 'postgres', false) as x",
                "coalesce(\"pg_sleep\"(1), 0)",
                "\"pg_catalog\".\"pg_sleep\"(1)",
                "pg_catalog.pg_sleep(1)",
                // A schema-qualified allow-listed name resolves to whatever
                // function that schema defines under the name.
                "public.count(id)",
                "\"public\".count(id)",
                "public . upper(name)",
            ] {
                let err = quote_select_expr(expr, dialect).expect_err(expr);
                assert!(err.starts_with("E_UNSAFE_EXPRESSION"), "{expr}: {err}");
                let err = quote_having_expr(expr, dialect).expect_err(expr);
                assert!(err.starts_with("E_UNSAFE_EXPRESSION"), "{expr}: {err}");
            }
            // A quoted column inside an allowed call is still fine, whatever
            // the spacing.
            assert_eq!(
                quote_select_expr("count (\"x\")", dialect).unwrap(),
                "count (\"x\")"
            );
            assert_eq!(
                quote_having_expr("sum(\"orders\".\"total\")", dialect).unwrap(),
                "sum(\"orders\".\"total\")"
            );
            assert_eq!(
                quote_select_expr("max(`amount`) as top", dialect).unwrap(),
                "max(`amount`) as top"
            );
            assert_eq!(
                quote_select_expr("coalesce(\"a\", 0) + (\"b\" * 2)", dialect).unwrap(),
                "coalesce(\"a\", 0) + (\"b\" * 2)"
            );
        }
    }

    #[test]
    fn a_literal_the_dialects_end_in_different_places_is_refused() {
        // MySQL reads `'\''` as a complete literal holding a quote; the standard
        // rule reads an open literal. Everything after it ran as SQL on MySQL,
        // and `#` commented out the rest of the statement.
        let having = "count(id) > count('\\'') OR max(password_hash) > '' #')";
        let select = "count('\\'') + count(password_hash) #')";
        for dialect in [Dialect::Sqlite, Dialect::Postgres, Dialect::Mysql] {
            assert!(quote_having_expr(having, dialect).is_err());
            assert!(quote_select_expr(select, dialect).is_err());
            // The backslash alone, without the `#`, is refused by the scan.
            assert!(quote_select_expr("count('\\'') + count(password_hash)", dialect).is_err());
            // Postgres escape-string syntax gives the backslash the same meaning.
            assert!(quote_select_expr("coalesce(name, e'\\'')", dialect).is_err());
            // `#` is a MySQL comment wherever it sits.
            assert!(quote_select_expr("count(id) # trailing", dialect).is_err());
            assert!(quote_having_expr("count('#')", dialect).is_err());

            assert_eq!(quote_select_expr("count(*)", dialect).unwrap(), "count(*)");
            assert_eq!(
                quote_select_expr("coalesce(sum(total), 0)", dialect).unwrap(),
                "coalesce(sum(total), 0)"
            );
            assert_eq!(
                quote_having_expr("count(distinct user_id)", dialect).unwrap(),
                "count(distinct user_id)"
            );
            assert_eq!(
                quote_select_expr("coalesce(name, 'it''s')", dialect).unwrap(),
                "coalesce(name, 'it''s')"
            );
        }
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

    #[test]
    fn select_alias_refuses_an_expression_it_would_rewrite() {
        for name in [
            "price * 2 AS double",
            "price + tax as total",
            "a AS b AS c",
            "price AS",
            "AS total",
        ] {
            let err = quote_select_expr(name, Dialect::Postgres).expect_err(name);
            assert!(err.starts_with("E_UNSAFE_EXPRESSION"), "{err}");
        }
        assert_eq!(
            quote_select_expr("price AS total", Dialect::Postgres).unwrap(),
            "\"price\" AS \"total\""
        );
        assert_eq!(
            quote_select_expr("users.name   as  label", Dialect::Mysql).unwrap(),
            "`users`.`name` AS `label`"
        );
    }

    #[test]
    fn refusals_carry_their_code() {
        for bad in ["a.b.c.d", ".leading", "a..b"] {
            let err = quote_identifier(bad).unwrap_err();
            assert!(err.starts_with("E_UNSAFE_IDENTIFIER: "), "{err}");
        }
        let err = validate_operator("~~").unwrap_err();
        assert!(err.starts_with("E_ATLAS_INVALID_OPERATOR: "), "{err}");
        let err = validate_direction("sideways").unwrap_err();
        assert!(err.starts_with("E_UNSAFE_SQL: "), "{err}");
    }
}
