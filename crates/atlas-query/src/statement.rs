//! Top-level statement dispatcher — single entry point for all SQL generation.

use crate::builder::{compile_query_with_dialect, CompileResult, QueryDescription};
use crate::ddl::{
    compile_alter_table, compile_alter_view, compile_create_index, compile_create_schema,
    compile_create_table, compile_create_table_like, compile_create_view, compile_drop_index,
    compile_drop_schema, compile_drop_table, compile_drop_view, compile_refresh_materialized_view,
    compile_rename_table, compile_rename_view, AlterTableSpec, AlterViewSpec, CreateIndexSpec,
    CreateSchemaSpec, CreateTableLikeSpec, CreateTableSpec, CreateViewSpec, DropIndexSpec,
    DropSchemaSpec, DropTableSpec, DropViewSpec, RefreshMaterializedViewSpec, RenameTableSpec,
    RenameViewSpec,
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
    CreateView(CreateViewSpec),
    DropView(DropViewSpec),
    CreateSchema(CreateSchemaSpec),
    DropSchema(DropSchemaSpec),
    CreateTableLike(CreateTableLikeSpec),
    AlterView(AlterViewSpec),
    RenameView(RenameViewSpec),
    RefreshMaterializedView(RefreshMaterializedViewSpec),
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
        StatementSpec::CreateView(s) => compile_create_view(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::DropView(s) => compile_drop_view(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::CreateSchema(s) => compile_create_schema(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::DropSchema(s) => compile_drop_schema(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::CreateTableLike(s) => compile_create_table_like(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::AlterView(s) => compile_alter_view(s, dialect).map(CompiledStatement::from_ddl),
        StatementSpec::RenameView(s) => compile_rename_view(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
        StatementSpec::RefreshMaterializedView(s) => compile_refresh_materialized_view(s, dialect).map(|sql| CompiledStatement::from_ddl(vec![sql])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialect::{ColumnTypeKind, ColumnTypeSpec};
    use crate::ddl::{ColumnDef, TableOptions};
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
    fn dispatches_create_schema() {
        let spec: StatementSpec =
            serde_json::from_str(r#"{"kind":"createSchema","name":"reporting","ifNotExists":true}"#)
                .unwrap();
        let pg = compile_statement(&spec, Dialect::Postgres).unwrap();
        assert_eq!(pg.statements[0], "CREATE SCHEMA IF NOT EXISTS \"reporting\";");
        // SQLite has no schemas.
        assert!(compile_statement(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn dispatches_drop_schema() {
        let spec: StatementSpec = serde_json::from_str(
            r#"{"kind":"dropSchema","name":"reporting","ifExists":true,"cascade":true}"#,
        )
        .unwrap();
        // Postgres honours CASCADE.
        assert_eq!(
            compile_statement(&spec, Dialect::Postgres).unwrap().statements[0],
            "DROP SCHEMA IF EXISTS \"reporting\" CASCADE;"
        );
        // MySQL: no CASCADE on DROP SCHEMA.
        assert_eq!(
            compile_statement(&spec, Dialect::Mysql).unwrap().statements[0],
            "DROP SCHEMA IF EXISTS `reporting`;"
        );
    }

    #[test]
    fn dispatches_rename_view() {
        let spec: StatementSpec =
            serde_json::from_str(r#"{"kind":"renameView","from":"v1","to":"v2"}"#).unwrap();
        assert_eq!(
            compile_statement(&spec, Dialect::Postgres).unwrap().statements[0],
            "ALTER VIEW \"v1\" RENAME TO \"v2\";"
        );
        assert_eq!(
            compile_statement(&spec, Dialect::Mysql).unwrap().statements[0],
            "RENAME TABLE `v1` TO `v2`;"
        );
        assert!(compile_statement(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn dispatches_refresh_materialized_view() {
        let spec: StatementSpec = serde_json::from_str(
            r#"{"kind":"refreshMaterializedView","name":"stats","concurrently":true}"#,
        )
        .unwrap();
        assert_eq!(
            compile_statement(&spec, Dialect::Postgres).unwrap().statements[0],
            "REFRESH MATERIALIZED VIEW CONCURRENTLY \"stats\";"
        );
        assert!(compile_statement(&spec, Dialect::Sqlite).is_err());
    }

    #[test]
    fn dispatches_alter_view() {
        let spec: StatementSpec = serde_json::from_str(
            r#"{"kind":"alterView","view":"active_users","renames":[{"from":"id","to":"user_id"}]}"#,
        )
        .unwrap();
        assert_eq!(
            compile_statement(&spec, Dialect::Postgres).unwrap().statements[0],
            "ALTER VIEW \"active_users\" RENAME COLUMN \"id\" TO \"user_id\";"
        );
        // Postgres-only.
        assert!(compile_statement(&spec, Dialect::Sqlite).is_err());
        assert!(compile_statement(&spec, Dialect::Mysql).is_err());
    }

    #[test]
    fn dispatches_create_table_like() {
        let spec: StatementSpec =
            serde_json::from_str(r#"{"kind":"createTableLike","table":"copy","likeTable":"orig"}"#)
                .unwrap();
        assert_eq!(
            compile_statement(&spec, Dialect::Postgres).unwrap().statements[0],
            "CREATE TABLE \"copy\" (LIKE \"orig\" INCLUDING ALL);"
        );
        assert_eq!(
            compile_statement(&spec, Dialect::Mysql).unwrap().statements[0],
            "CREATE TABLE `copy` LIKE `orig`;"
        );
        assert_eq!(
            compile_statement(&spec, Dialect::Sqlite).unwrap().statements[0],
            "CREATE TABLE \"copy\" AS SELECT * FROM \"orig\" WHERE 0;"
        );
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
                comment: None,
                collate: None,
                position: None,
            }],
            indexes: vec![],
            if_not_exists: false, constraints: vec![], options: TableOptions::default(),
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
