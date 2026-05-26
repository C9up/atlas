//! DDL compilers: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX.
//!
//! DDL statements don't bind params — they return plain SQL strings.

use crate::dialect::{ColumnTypeSpec, Dialect};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableSpec {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    #[serde(default)]
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnDef {
    pub name: String,
    #[serde(flatten)]
    pub type_spec: ColumnTypeSpec,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary: bool,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub references: Option<ForeignKeyRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyRef {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropTableSpec {
    pub table: String,
    #[serde(default)]
    pub if_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateIndexSpec {
    pub table: String,
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropIndexSpec {
    pub name: String,
    #[serde(default)]
    pub if_exists: bool,
}

/// Compile CREATE TABLE → one or more SQL statements (the CREATE itself + CREATE INDEX for each index).
pub fn compile_create_table(spec: &CreateTableSpec, dialect: Dialect) -> Result<Vec<String>, String> {
    if spec.columns.is_empty() {
        return Err("CREATE TABLE requires at least one column".into());
    }
    let table = dialect.quote_ident(&spec.table)?;

    let col_lines: Result<Vec<String>, String> = spec.columns.iter().map(|col| {
        let mut parts: Vec<String> = vec![dialect.quote_ident(&col.name)?];
        parts.push(dialect.map_column_type(&col.type_spec));
        if col.primary { parts.push("PRIMARY KEY".into()); }
        if !col.nullable { parts.push("NOT NULL".into()); }
        if col.unique { parts.push("UNIQUE".into()); }
        if let Some(default) = &col.default {
            parts.push(dialect.wrap_default(default));
        }
        if let Some(fk) = &col.references {
            parts.push(format!(
                "REFERENCES {}({})",
                dialect.quote_ident(&fk.table)?,
                dialect.quote_ident(&fk.column)?
            ));
        }
        Ok(format!("  {}", parts.join(" ")))
    }).collect();

    let if_not_exists = if spec.if_not_exists { "IF NOT EXISTS " } else { "" };
    let mut stmts = vec![format!(
        "CREATE TABLE {}{} (\n{}\n);",
        if_not_exists,
        table,
        col_lines?.join(",\n")
    )];

    for idx in &spec.indexes {
        stmts.push(compile_create_index(&CreateIndexSpec {
            table: spec.table.clone(),
            name: idx.name.clone(),
            columns: idx.columns.clone(),
            unique: idx.unique,
        }, dialect)?);
    }
    Ok(stmts)
}

pub fn compile_drop_table(spec: &DropTableSpec, dialect: Dialect) -> Result<String, String> {
    let table = dialect.quote_ident(&spec.table)?;
    let if_exists = if spec.if_exists { "IF EXISTS " } else { "" };
    Ok(format!("DROP TABLE {}{};", if_exists, table))
}

pub fn compile_create_index(spec: &CreateIndexSpec, dialect: Dialect) -> Result<String, String> {
    let unique = if spec.unique { "UNIQUE " } else { "" };
    let name = dialect.quote_ident(&spec.name)?;
    let table = dialect.quote_ident(&spec.table)?;
    let cols: Result<Vec<String>, String> = spec.columns.iter().map(|c| dialect.quote_ident(c)).collect();
    Ok(format!("CREATE {}INDEX {} ON {} ({});", unique, name, table, cols?.join(", ")))
}

pub fn compile_drop_index(spec: &DropIndexSpec, dialect: Dialect) -> Result<String, String> {
    let name = dialect.quote_ident(&spec.name)?;
    let if_exists = if spec.if_exists { "IF EXISTS " } else { "" };
    Ok(format!("DROP INDEX {}{};", if_exists, name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::ColumnTypeKind;

    fn col(name: &str, kind: ColumnTypeKind) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            type_spec: ColumnTypeSpec { kind, length: None, precision: None, scale: None },
            nullable: true,
            primary: false,
            unique: false,
            default: None,
            references: None,
        }
    }

    #[test]
    fn create_table_sqlite() {
        let spec = CreateTableSpec {
            table: "users".into(),
            columns: vec![
                ColumnDef { primary: true, nullable: false, ..col("id", ColumnTypeKind::Integer) },
                ColumnDef { nullable: false, ..col("name", ColumnTypeKind::String) },
            ],
            indexes: vec![],
            if_not_exists: false,
        };
        let stmts = compile_create_table(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("CREATE TABLE \"users\""));
        assert!(stmts[0].contains("\"id\" INTEGER PRIMARY KEY NOT NULL"));
        assert!(stmts[0].contains("\"name\" TEXT NOT NULL"));
    }

    #[test]
    fn create_table_postgres_varchar() {
        let spec = CreateTableSpec {
            table: "users".into(),
            columns: vec![ColumnDef {
                nullable: false,
                type_spec: ColumnTypeSpec { kind: ColumnTypeKind::String, length: Some(100), precision: None, scale: None },
                ..col("email", ColumnTypeKind::String)
            }],
            indexes: vec![],
            if_not_exists: false,
        };
        let stmts = compile_create_table(&spec, Dialect::Postgres).unwrap();
        assert!(stmts[0].contains("\"email\" VARCHAR(100) NOT NULL"));
    }

    #[test]
    fn create_table_with_fk_and_index() {
        let spec = CreateTableSpec {
            table: "orders".into(),
            columns: vec![
                ColumnDef { primary: true, nullable: false, ..col("id", ColumnTypeKind::Integer) },
                ColumnDef {
                    nullable: false,
                    references: Some(ForeignKeyRef { table: "users".into(), column: "id".into() }),
                    ..col("user_id", ColumnTypeKind::Integer)
                },
            ],
            indexes: vec![IndexDef { name: "idx_orders_user".into(), columns: vec!["user_id".into()], unique: false }],
            if_not_exists: false,
        };
        let stmts = compile_create_table(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("REFERENCES \"users\"(\"id\")"));
        assert_eq!(stmts[1], "CREATE INDEX \"idx_orders_user\" ON \"orders\" (\"user_id\");");
    }

    #[test]
    fn drop_table() {
        let spec = DropTableSpec { table: "old".into(), if_exists: true };
        assert_eq!(compile_drop_table(&spec, Dialect::Sqlite).unwrap(), "DROP TABLE IF EXISTS \"old\";");
    }

    #[test]
    fn create_index_unique() {
        let spec = CreateIndexSpec {
            table: "users".into(),
            name: "idx_users_email".into(),
            columns: vec!["email".into()],
            unique: true,
        };
        assert_eq!(
            compile_create_index(&spec, Dialect::Sqlite).unwrap(),
            "CREATE UNIQUE INDEX \"idx_users_email\" ON \"users\" (\"email\");"
        );
    }

    #[test]
    fn mysql_backticks_in_ddl() {
        let spec = DropTableSpec { table: "old".into(), if_exists: false };
        assert_eq!(compile_drop_table(&spec, Dialect::Mysql).unwrap(), "DROP TABLE `old`;");
    }

    #[test]
    fn rejects_injection_in_table_name() {
        let spec = DropTableSpec { table: "users; DROP TABLE admins--".into(), if_exists: false };
        assert!(compile_drop_table(&spec, Dialect::Sqlite).is_err());
    }
}
