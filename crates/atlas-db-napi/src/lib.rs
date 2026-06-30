//! # ream-db-napi
//!
//! NAPI bindings for the Ream database driver.
//! All DB operations are async — they return Promises to TypeScript.

use napi_derive::napi;
use std::sync::Arc;

/// NAPI-exposed database connection.
#[napi]
pub struct ReamDatabase {
    db: Arc<atlas_db::Database>,
}

#[napi]
impl ReamDatabase {
    /// Connect to a database. URL determines the driver:
    /// - "sqlite:path/to/db.sqlite" or "sqlite::memory:"
    /// - "postgres://user:pass@host/dbname"
    /// - "mysql://user:pass@host/dbname"
    #[napi(factory)]
    pub async fn connect(
        url: String,
        pool_min: Option<u32>,
        pool_max: Option<u32>,
        pragmas: Option<Vec<Vec<String>>>,
        connect_retries: Option<u32>,
        connect_backoff_ms: Option<u32>,
        connect_timeout_ms: Option<u32>,
    ) -> napi::Result<Self> {
        // The TS side hands `pragmas` as `[[key, value], ...]` (napi-rs
        // bridges `Array<[string, string]>` into `Vec<Vec<String>>`). Flatten
        // into a `Vec<(String, String)>` here, rejecting any inner tuple
        // that isn't exactly length-2 — bad input now surfaces at connect-
        // time instead of as a confusing pragma error mid-boot.
        let sqlite_pragmas = match pragmas {
            None => None,
            Some(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for entry in rows {
                    if entry.len() != 2 {
                        return Err(napi::Error::new(
                            napi::Status::InvalidArg,
                            "pragmas must be an Array<[string, string]>",
                        ));
                    }
                    let mut it = entry.into_iter();
                    let k = it.next().unwrap();
                    let v = it.next().unwrap();
                    out.push((k, v));
                }
                Some(out)
            }
        };
        let config = atlas_db::DbConfig {
            url,
            pool_min,
            pool_max,
            sqlite_pragmas,
            connect_retries,
            // napi bridges JS numbers as u32; widen to the crate's u64 fields.
            connect_backoff_ms: connect_backoff_ms.map(u64::from),
            connect_timeout_ms: connect_timeout_ms.map(u64::from),
        };
        let rt = ream_napi_core::shared_runtime();

        let db = rt.spawn(async move {
            atlas_db::Database::connect(&config).await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Execute a SELECT query. Returns JSON array of row objects.
    #[napi]
    pub async fn query(&self, sql: String, params_json: String) -> napi::Result<String> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Invalid params JSON: {}", e)))?;

        let rt = ream_napi_core::shared_runtime();
        let rows = rt.spawn(async move {
            db.query(&sql, &params).await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        // Convert Vec<DbRow> to JSON array of objects
        let json_rows: Vec<serde_json::Value> = rows.iter().map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = row.columns.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            serde_json::Value::Object(obj)
        }).collect();

        serde_json::to_string(&json_rows)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// Execute an INSERT/UPDATE/DELETE. Returns rows affected.
    #[napi]
    pub async fn execute(&self, sql: String, params_json: String) -> napi::Result<f64> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Invalid params JSON: {}", e)))?;

        let rt = ream_napi_core::shared_runtime();
        let result = rt.spawn(async move {
            db.execute(&sql, &params).await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        Ok(result.rows_affected as f64)
    }

    /// Run a batch of `[sql, params_json]` pairs atomically in a single transaction.
    /// Accepts JSON `[[sql, params], ...]` and returns the total affected rows.
    #[napi]
    pub async fn run_in_transaction(&self, batch_json: String) -> napi::Result<f64> {
        let db = self.db.clone();
        // Input: `[[sql: string, params: unknown[]], ...]`
        let raw: Vec<(String, Vec<serde_json::Value>)> = serde_json::from_str(&batch_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Invalid transaction batch JSON: {}", e)))?;

        let rt = ream_napi_core::shared_runtime();
        let affected = rt.spawn(async move {
            db.run_in_transaction(&raw).await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        Ok(affected as f64)
    }

    /// Open an interactive transaction pinned to a single pooled connection.
    /// Returns a handle whose `query`/`execute` run on that one connection;
    /// `commit`/`rollback` release it. This is what makes a TS
    /// read-then-decide-then-write atomic — `BEGIN`/`COMMIT` pulled through the
    /// pool would land on different connections and guarantee nothing.
    #[napi]
    pub async fn begin(&self, isolation_level: Option<String>) -> napi::Result<ReamTransaction> {
        let db = self.db.clone();
        let rt = ream_napi_core::shared_runtime();
        let tx = rt.spawn(async move { db.begin(isolation_level.as_deref()).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;
        Ok(ReamTransaction {
            tx: Arc::new(tokio::sync::Mutex::new(Some(tx))),
        })
    }

    /// Health check.
    #[napi]
    pub async fn ping(&self) -> napi::Result<()> {
        let db = self.db.clone();
        let rt = ream_napi_core::shared_runtime();
        rt.spawn(async move { db.ping().await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))
    }

    /// Get pool size.
    #[napi]
    pub fn pool_size(&self) -> u32 {
        self.db.pool_size()
    }

    /// Close the connection pool.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let db = self.db.clone();
        let rt = ream_napi_core::shared_runtime();
        rt.spawn(async move { db.close().await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?;
        Ok(())
    }
}

/// An interactive transaction handle (see [`ReamDatabase::begin`]). The pinned
/// `DbTransaction` lives in an async mutex so the separate NAPI calls
/// (execute… → commit) all hit the SAME connection; `commit`/`rollback` take it
/// out (the connection returns to the pool) and any later call sees a clear
/// "transaction already finished" error instead of a silent no-op.
#[napi]
pub struct ReamTransaction {
    tx: Arc<tokio::sync::Mutex<Option<atlas_db::DbTransaction>>>,
}

#[napi]
impl ReamTransaction {
    /// SELECT on the pinned connection. Returns a JSON array of row objects.
    #[napi]
    pub async fn query(&self, sql: String, params_json: String) -> napi::Result<String> {
        let tx = self.tx.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Invalid params JSON: {}", e)))?;

        let rt = ream_napi_core::shared_runtime();
        let rows = rt.spawn(async move {
            let mut guard = tx.lock().await;
            let pinned = guard
                .as_mut()
                .ok_or_else(|| "transaction already finished".to_string())?;
            pinned.query(&sql, &params).await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        let json_rows: Vec<serde_json::Value> = rows.iter().map(|row| {
            let obj: serde_json::Map<String, serde_json::Value> = row.columns.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            serde_json::Value::Object(obj)
        }).collect();

        serde_json::to_string(&json_rows)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// INSERT/UPDATE/DELETE on the pinned connection. Returns rows affected.
    #[napi]
    pub async fn execute(&self, sql: String, params_json: String) -> napi::Result<f64> {
        let tx = self.tx.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("Invalid params JSON: {}", e)))?;

        let rt = ream_napi_core::shared_runtime();
        let affected = rt.spawn(async move {
            let mut guard = tx.lock().await;
            let pinned = guard
                .as_mut()
                .ok_or_else(|| "transaction already finished".to_string())?;
            pinned.execute(&sql, &params).await.map(|r| r.rows_affected)
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        Ok(affected as f64)
    }

    /// Commit and release the connection back to the pool. Idempotent-safe: a
    /// second commit/rollback errors with "transaction already finished".
    #[napi]
    pub async fn commit(&self) -> napi::Result<()> {
        let tx = self.tx.clone();
        let rt = ream_napi_core::shared_runtime();
        rt.spawn(async move {
            let pinned = tx
                .lock()
                .await
                .take()
                .ok_or_else(|| "transaction already finished".to_string())?;
            pinned.commit().await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))
    }

    /// Roll back and release the connection back to the pool.
    #[napi]
    pub async fn rollback(&self) -> napi::Result<()> {
        let tx = self.tx.clone();
        let rt = ream_napi_core::shared_runtime();
        rt.spawn(async move {
            let pinned = tx
                .lock()
                .await
                .take()
                .ok_or_else(|| "transaction already finished".to_string())?;
            pinned.rollback().await
        }).await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))
    }
}
