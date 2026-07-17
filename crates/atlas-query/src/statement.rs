//! Top-level statement dispatcher — single entry point for all SQL generation.

use crate::builder::{compile_query_with_dialect, CompileResult, QueryDescription};
use crate::ddl::{
    compile_alter_table, compile_create_index, compile_create_table, compile_drop_index,
    compile_drop_table, compile_rename_table, AlterTableSpec, CreateIndexSpec, CreateTableSpec,
    DropIndexSpec, DropTableSpec, RenameTableSpec,
};
use crate::dialect::Dialect;
use crate::dml::{compile_delete, compile_insert, compile_update, compile_upsert, DeleteSpec, InsertSpec, UpdateSpec, UpsertSpec};
use serde::{Deserialize, Serialize};

/// A statement to compile. Tagged union — TypeScript sends `{ kind: "insert", ... }`.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StatementSpec {
    Select(QueryDescription),
    Insert(InsertSpec),
    Update(UpdateSpec),
    Delete(DeleteSpec),
    Upsert(UpsertSpec),
    CreateTable(CreateTableSpec),
    DropTable(DropTableSpec),
    AlterTable(AlterTableSpec),
    RenameTable(RenameTableSpec),
    CreateIndex(CreateIndexSpec),
    DropIndex(DropIndexSpec),
}

/// Compiled output. DML/SELECT return one `(sql, params)`; DDL can return multiple statements.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledStatement {
    /// One SQL string per statement (DDL may produce more than one).
    pub statements: Vec<String>,
    /// Bound parameters (empty for DDL).
    pub params: Vec<serde_json::Value>,
}

impl CompiledStatement {
    fn from_result(r: CompileResult) -> Self {
        Self { statements: vec![r.sql], params: r.params }
    }
    fn from_ddl(stmts: Vec<String>) -> Self {
        Self { statements: stmts, params: vec![] }
    }
}

pub fn compile_statement(spec: &StatementSpec, dialect: Dialect) -> Result<CompiledStatement, String> {
    match spec {
        StatementSpec::Select(desc) => compile_query_with_dialect(desc, dialect).map(CompiledStatement::from_result),
        StatementSpec::Insert(s) => compile_insert(s, dialect).map(CompiledStatement::from_result),
        StatementSpec::Update(s) => compile_update(s, dialect).map(CompiledStatement::from_result),
        StatementSpec::Delete(s) => compile_delete(s, dialect).map(CompiledStatement::from_result),
        StatementSpec::Upsert(s) => compile_upsert(s, dialect).map(CompiledStatement::from_result),
        StatementSpec::CreateTable(s) => compile_create_table(s, dialect).map(CompiledStatement::from_ddl),
        StatementSpec::DropTable(s) => compile_drop_table(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::AlterTable(s) => compile_alter_table(s, dialect).map(CompiledStatement::from_ddl),
        StatementSpec::RenameTable(s) => compile_rename_table(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::CreateIndex(s) => compile_create_index(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::DropIndex(s) => compile_drop_index(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::{ColumnTypeKind, ColumnTypeSpec};
    use crate::ddl::ColumnDef;
    use serde_json::json;

    #[test]
    fn dispatches_insert() {
        let spec = StatementSpec::Insert(InsertSpec {
            table: "users".into(),
            values: vec![("name".into(), json!("Alice"))],
            ..Default::default()
        });
        let r = compile_statement(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.statements.len(), 1);
        assert!(r.statements[0].starts_with("INSERT INTO"));
        assert_eq!(r.params, vec![json!("Alice")]);
    }

    #[test]
    fn dispatches_create_table() {
        let spec = StatementSpec::CreateTable(CreateTableSpec {
            table: "t".into(),
            columns: vec![ColumnDef {
                name: "id".into(),
                type_spec: ColumnTypeSpec { kind: ColumnTypeKind::Integer, length: None, precision: None, scale: None, values: None, raw_type: None },
                nullable: false,
                primary: true,
                auto_increment: false,
                unique: false,
                unsigned: false,
                default: None,
                references: None,
            }],
            indexes: vec![],
            if_not_exists: false,
        });
        let r = compile_statement(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.statements.len(), 1);
        assert!(r.statements[0].contains("CREATE TABLE \"t\""));
        assert!(r.params.is_empty());
    }

    #[test]
    fn json_roundtrip() {
        let json_str = r#"{"kind":"dropTable","table":"old","ifExists":true}"#;
        let spec: StatementSpec = serde_json::from_str(json_str).unwrap();
        let r = compile_statement(&spec, Dialect::Sqlite).unwrap();
        assert_eq!(r.statements[0], "DROP TABLE IF EXISTS \"old\";");
    }
}
