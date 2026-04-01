//! NAPI bindings for ream-query — exposes SQL compiler to TypeScript.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::panic::catch_unwind;

/// Compile a query description (JSON string) into parameterized SQL.
#[napi]
pub fn compile_query(query_json: String) -> Result<String> {
    let result = catch_unwind(|| {
        let desc: ream_query::QueryDescription = serde_json::from_str(&query_json)
            .map_err(|e| format!("Invalid query description: {}", e))?;

        let compiled = ream_query::compile_query(&desc)?;

        serde_json::to_string(&compiled)
            .map_err(|e| format!("Serialization error: {}", e))
    });

    match result {
        Ok(Ok(json)) => Ok(json),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason("Internal panic in query compiler")),
    }
}

/// Quote a SQL identifier (validates and wraps in double quotes).
#[napi]
pub fn quote_ident(name: String) -> Result<String> {
    let result = catch_unwind(|| {
        ream_query::quote_identifier(&name)
    });

    match result {
        Ok(Ok(quoted)) => Ok(quoted),
        Ok(Err(e)) => Err(Error::from_reason(e)),
        Err(_) => Err(Error::from_reason("Internal panic in identifier quoter")),
    }
}
