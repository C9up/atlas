//! ream-query — SQL compiler in Rust.
//!
//! Receives a query description (JSON from TypeScript) and produces parameterized SQL.
//! Handles identifier quoting (dialect-aware), parameter indexing, injection prevention.
//!
//! @implements FR36

pub mod builder;
pub mod ddl;
pub mod dialect;
pub mod dml;
pub mod identifier;
pub mod statement;

pub use builder::{compile_query, compile_query_with_dialect, CompileResult, QueryDescription};
pub use ddl::{
    compile_alter_table, compile_create_index, compile_create_table, compile_drop_index,
    compile_drop_table, compile_rename_table, AlterOp, AlterTableSpec, ColumnDef, CreateIndexSpec,
    CreateTableSpec, DropIndexSpec, DropTableSpec, ForeignKeyRef, IndexDef, RenameTableSpec,
};
pub use dialect::{ColumnTypeKind, ColumnTypeSpec, Dialect};
pub use dml::{compile_delete, compile_insert, compile_update, DeleteSpec, InsertSpec, UpdateSpec};
pub use identifier::quote_identifier;
pub use statement::{compile_statement, CompiledStatement, StatementSpec};
