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
        serde_json::to_string(&compiled)
            .map_err(|e| format!("Serialization error: {}", e))
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
    let result = catch_unwind(|| {
        atlas_query::quote_identifier(&name)
    });

    match result {
        Ok(Ok(quoted)) => Ok(quoted),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason("Internal panic in identifier quoter")),
    }
}
