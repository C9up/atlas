//! NAPI bindings for ream-query — exposes SQL compiler to TypeScript.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::panic::catch_unwind;

/// Compile a full statement (SELECT / INSERT / UPDATE / DELETE / DDL) via a tagged JSON spec.
/// `dialect` is one of "sqlite" | "postgres" | "mysql".
/// Returns a JSON-encoded `CompiledStatement` ({ statements: [...], params: [...] }).
#[napi]
pub fn compile_statement(spec_json: String, dialect: String) -> Result<String> {
    let result = catch_unwind(|| {
        let dialect = atlas_query::Dialect::from_name(&dialect)?;
        let spec: atlas_query::StatementSpec = serde_json::from_str(&spec_json)
            .map_err(|e| format!("Invalid statement spec: {}", e))?;
        let compiled = atlas_query::compile_statement(&spec, dialect)?;
        serde_json::to_string(&compiled).map_err(|e| format!("Serialization error: {}", e))
    });

    match result {
        Ok(Ok(json)) => Ok(json),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason("Internal panic in statement compiler")),
    }
}

/// Quote a SQL identifier (validates and wraps in double quotes).
#[napi]
pub fn quote_ident(name: String) -> Result<String> {
    let result = catch_unwind(|| atlas_query::quote_identifier(&name));

    match result {
        Ok(Ok(quoted)) => Ok(quoted),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason("Internal panic in identifier quoter")),
    }
}

/// A raw fragment's named bindings rewritten to the `?` notation.
#[napi(object)]
pub struct NamedBindings {
    /// The fragment with `:name` as `?` and `:name:` as `??`.
    pub sql: String,
    /// The binding names in placeholder order.
    pub names: Vec<String>,
}

/// Resolve Knex named bindings (`:name`, `:name:`) with the same SQL scanner
/// as every other placeholder: only names in `keys` and only in SQL code —
/// never inside a string, quoted identifier, comment or dollar-quoted body.
#[napi]
pub fn named_to_positional(
    sql: String,
    keys: Vec<String>,
    dialect: String,
) -> Result<NamedBindings> {
    let result = catch_unwind(|| {
        let dialect = atlas_query::Dialect::from_name(&dialect)?;
        atlas_query::named_to_positional(&sql, |name| keys.iter().any(|k| k == name), dialect)
    });

    match result {
        Ok(Ok(rewrite)) => Ok(NamedBindings {
            sql: rewrite.sql,
            names: rewrite.names,
        }),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason(
            "Internal panic in named binding resolver",
        )),
    }
}
