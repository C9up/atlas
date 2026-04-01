//! Identifier validation and quoting — prevents SQL injection.

/// Validate and quote a SQL identifier (table/column name).
/// Supports schema-qualified names: "public"."orders"
pub fn quote_identifier(name: &str) -> Result<String, String> {
    if name == "*" {
        return Ok(name.to_string());
    }
    // Split on dot for schema-qualified identifiers
    let parts: Vec<&str> = name.splitn(3, '.').collect();
    if parts.len() > 2 {
        return Err(format!("Too many dot segments in identifier: '{}'", name));
    }
    for part in &parts {
        if part.is_empty() {
            return Err(format!("Empty segment in identifier: '{}'", name));
        }
        if part.contains('\0') || part.contains('"') {
            return Err(format!("Identifier contains illegal characters: {}", name));
        }
        if !part.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(format!("Invalid identifier: '{}'. Only letters, numbers, and underscores allowed.", name));
        }
    }
    Ok(parts.iter().map(|p| format!("\"{}\"", p)).collect::<Vec<_>>().join("."))
}

/// Dangerous SQL patterns — blocked in all expression contexts.
fn contains_dangerous_sql(name: &str) -> bool {
    name.contains(';') || name.contains("--") || name.contains("/*") || name.contains("*/")
}

/// Quote a SELECT expression — allows aggregates, aliases, window functions.
pub fn quote_select_expr(name: &str) -> Result<String, String> {
    if name == "*" {
        return Ok(name.to_string());
    }
    if name.contains('(') || name.to_lowercase().contains(" as ") {
        if contains_dangerous_sql(name) {
            return Err(format!("Dangerous SQL in expression: {}", name));
        }
        return Ok(name.to_string());
    }
    quote_identifier(name)
}

/// Quote a HAVING expression — allows aggregates.
pub fn quote_having_expr(name: &str) -> Result<String, String> {
    if name.contains('(') {
        if contains_dangerous_sql(name) {
            return Err(format!("Dangerous SQL in expression: {}", name));
        }
        return Ok(name.to_string());
    }
    quote_identifier(name)
}

/// Validate an SQL operator against an allowlist.
pub fn validate_operator(op: &str) -> Result<&str, String> {
    match op {
        "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" | "LIKE" | "ILIKE" | "NOT LIKE" | "IN" | "NOT IN" | "IS NULL" | "IS NOT NULL" => Ok(op),
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
    }

    #[test]
    fn test_quote_identifier_rejects_injection() {
        assert!(quote_identifier("id; DROP TABLE orders--").is_err());
        assert!(quote_identifier("id\"").is_err());
        assert!(quote_identifier("id\0").is_err());
        assert!(quote_identifier("a.b.c").is_err()); // too many segments
        assert!(quote_identifier(".leading").is_err()); // empty segment
    }

    #[test]
    fn test_quote_select_expr() {
        assert_eq!(quote_select_expr("COUNT(*) AS total").unwrap(), "COUNT(*) AS total");
        assert_eq!(quote_select_expr("status").unwrap(), "\"status\"");
        assert!(quote_select_expr("1; DROP TABLE--").is_err());
        assert!(quote_select_expr("1 /* evil */").is_err());
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
