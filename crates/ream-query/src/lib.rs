//! ream-query — SQL query compiler in Rust.
//!
//! Receives a query description (JSON) and produces parameterized SQL.
//! Handles identifier quoting, parameter indexing, and SQL injection prevention.
//!
//! @implements FR36

pub mod builder;
pub mod identifier;

pub use builder::{QueryDescription, compile_query, CompileResult};
pub use identifier::quote_identifier;
