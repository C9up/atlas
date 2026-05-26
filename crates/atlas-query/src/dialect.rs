//! SQL dialect: identifier quoting + column type mapping.
//!
//! Drives all dialect differences in one place so the rest of the compiler
//! is dialect-agnostic.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    #[default]
    Sqlite,
    Postgres,
    Mysql,
}

impl Dialect {
    pub fn from_name(name: &str) -> Result<Self, String> {
        match name.to_lowercase().as_str() {
            "sqlite" => Ok(Dialect::Sqlite),
            "postgres" | "postgresql" => Ok(Dialect::Postgres),
            "mysql" | "mariadb" => Ok(Dialect::Mysql),
            _ => Err(format!("unknown dialect: {}", name)),
        }
    }

    /// Quote character pair for identifiers: (`"`, `"`) for sqlite/postgres, (`` ` ``, `` ` ``) for mysql.
    pub fn quote_char(&self) -> char {
        match self {
            Dialect::Sqlite | Dialect::Postgres => '"',
            Dialect::Mysql => '`',
        }
    }

    /// Validate and quote an identifier. Supports schema-qualified names ("schema.table").
    pub fn quote_ident(&self, name: &str) -> Result<String, String> {
        if name == "*" {
            return Ok(name.to_string());
        }
        let q = self.quote_char();
        let parts: Vec<&str> = name.splitn(3, '.').collect();
        if parts.len() > 2 {
            return Err(format!("too many dot segments in identifier: '{}'", name));
        }
        for part in &parts {
            if part.is_empty() {
                return Err(format!("empty segment in identifier: '{}'", name));
            }
            if part.contains('\0') || part.contains(q) {
                return Err(format!("Identifier contains illegal characters: {}", name));
            }
            if !part.chars().all(|c| c.is_alphanumeric() || c == '_') {
                return Err(format!(
                    "Invalid identifier: '{}'. Only letters, numbers, and underscores allowed.",
                    name
                ));
            }
        }
        Ok(parts.iter().map(|p| format!("{}{}{}", q, p, q)).collect::<Vec<_>>().join("."))
    }

    /// Parameter placeholder for a given index: `$N` for Postgres, `?` for sqlite/mysql.
    pub fn placeholder(&self, index: u32) -> String {
        match self {
            Dialect::Postgres => format!("${}", index),
            Dialect::Sqlite | Dialect::Mysql => "?".into(),
        }
    }

    /// Render DEFAULT <expr>. SQLite wraps in parentheses for function calls.
    pub fn wrap_default(&self, value: &str) -> String {
        match self {
            Dialect::Sqlite => format!("DEFAULT ({})", value),
            _ => format!("DEFAULT {}", value),
        }
    }

    /// Map a logical column type to the physical SQL type for this dialect.
    pub fn map_column_type(&self, spec: &ColumnTypeSpec) -> String {
        use ColumnTypeKind::*;
        let length = spec.length.unwrap_or(255);
        let precision = spec.precision.unwrap_or(10);
        let scale = spec.scale.unwrap_or(2);
        match (self, spec.kind) {
            (Dialect::Postgres, Uuid) => "UUID".into(),
            (Dialect::Sqlite, Uuid) => "TEXT".into(),
            (Dialect::Mysql, Uuid) => "CHAR(36)".into(),

            (Dialect::Postgres, String) => format!("VARCHAR({})", length),
            (Dialect::Sqlite, String) => "TEXT".into(),
            (Dialect::Mysql, String) => format!("VARCHAR({})", length),

            (_, Text) => "TEXT".into(),

            (Dialect::Mysql, Integer) => "INT".into(),
            (_, Integer) => "INTEGER".into(),

            (Dialect::Sqlite, BigInteger) => "INTEGER".into(),
            (_, BigInteger) => "BIGINT".into(),

            (Dialect::Sqlite, Decimal) => "REAL".into(),
            (_, Decimal) => format!("DECIMAL({}, {})", precision, scale),

            (Dialect::Postgres, Boolean) => "BOOLEAN".into(),
            (Dialect::Sqlite, Boolean) => "INTEGER".into(),
            (Dialect::Mysql, Boolean) => "TINYINT(1)".into(),

            (Dialect::Sqlite, Date) => "TEXT".into(),
            (_, Date) => "DATE".into(),

            (Dialect::Sqlite, Timestamp) => "TEXT".into(),
            (_, Timestamp) => "TIMESTAMP".into(),

            (Dialect::Postgres, Json) => "JSONB".into(),
            (Dialect::Sqlite, Json) => "TEXT".into(),
            (Dialect::Mysql, Json) => "JSON".into(),

            (Dialect::Postgres, Binary) => "BYTEA".into(),
            (Dialect::Sqlite, Binary) => "BLOB".into(),
            (Dialect::Mysql, Binary) => "BLOB".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ColumnTypeKind {
    Uuid,
    String,
    Text,
    Integer,
    BigInteger,
    Decimal,
    Boolean,
    Date,
    Timestamp,
    Json,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnTypeSpec {
    pub kind: ColumnTypeKind,
    #[serde(default)]
    pub length: Option<u32>,
    #[serde(default)]
    pub precision: Option<u32>,
    #[serde(default)]
    pub scale: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_name_works() {
        assert_eq!(Dialect::from_name("sqlite").unwrap(), Dialect::Sqlite);
        assert_eq!(Dialect::from_name("POSTGRES").unwrap(), Dialect::Postgres);
        assert_eq!(Dialect::from_name("mariadb").unwrap(), Dialect::Mysql);
        assert!(Dialect::from_name("oracle").is_err());
    }

    #[test]
    fn quoting_sqlite_postgres_uses_double_quotes() {
        assert_eq!(Dialect::Sqlite.quote_ident("users").unwrap(), "\"users\"");
        assert_eq!(Dialect::Postgres.quote_ident("users.id").unwrap(), "\"users\".\"id\"");
    }

    #[test]
    fn quoting_mysql_uses_backticks() {
        assert_eq!(Dialect::Mysql.quote_ident("users").unwrap(), "`users`");
    }

    #[test]
    fn quoting_rejects_injection() {
        assert!(Dialect::Sqlite.quote_ident("id\"; DROP").is_err());
        assert!(Dialect::Mysql.quote_ident("id`; DROP").is_err());
        assert!(Dialect::Sqlite.quote_ident("id; DROP").is_err());
    }

    #[test]
    fn star_passes_through() {
        assert_eq!(Dialect::Sqlite.quote_ident("*").unwrap(), "*");
    }

    #[test]
    fn default_wrapping() {
        assert_eq!(Dialect::Postgres.wrap_default("NOW()"), "DEFAULT NOW()");
        assert_eq!(Dialect::Sqlite.wrap_default("CURRENT_TIMESTAMP"), "DEFAULT (CURRENT_TIMESTAMP)");
    }

    #[test]
    fn column_types_per_dialect() {
        let string_255 = ColumnTypeSpec { kind: ColumnTypeKind::String, length: Some(50), precision: None, scale: None };
        assert_eq!(Dialect::Postgres.map_column_type(&string_255), "VARCHAR(50)");
        assert_eq!(Dialect::Sqlite.map_column_type(&string_255), "TEXT");
        assert_eq!(Dialect::Mysql.map_column_type(&string_255), "VARCHAR(50)");

        let uuid = ColumnTypeSpec { kind: ColumnTypeKind::Uuid, length: None, precision: None, scale: None };
        assert_eq!(Dialect::Postgres.map_column_type(&uuid), "UUID");
        assert_eq!(Dialect::Mysql.map_column_type(&uuid), "CHAR(36)");

        let decimal = ColumnTypeSpec { kind: ColumnTypeKind::Decimal, length: None, precision: Some(12), scale: Some(4) };
        assert_eq!(Dialect::Postgres.map_column_type(&decimal), "DECIMAL(12, 4)");
        assert_eq!(Dialect::Sqlite.map_column_type(&decimal), "REAL");
    }
}
