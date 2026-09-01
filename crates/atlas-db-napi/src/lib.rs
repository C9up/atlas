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

        let db = rt
            .spawn(async move { atlas_db::Database::connect(&config).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Execute a SELECT query. Returns JSON array of row objects.
    #[napi]
    pub async fn query(&self, sql: String, params_json: String) -> napi::Result<String> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let rows = rt
            .spawn(async move { db.query(&sql, &params).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        // Convert Vec<DbRow> to JSON array of objects
        let json_rows: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = row
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        // `sonic_rs`, not `serde_json`, for the write side.
        //
        // Measured, alternating between the two to cancel machine drift: on a
        // 10 000-row read it wins 5 passes out of 5, median 37.5ms → 34.0ms
        // (-9%); at 100 rows the two are a coin flip and it is never
        // consistently worse, so there is no threshold to route on — a branch
        // here would buy nothing. The output is byte-identical, checked against
        // escapes, emoji, floats and the extremes of the safe-integer range.
        //
        // It costs 0.1MB in this binary and nothing in an application's
        // node_modules: this is a Rust crate compiled in, not a package a
        // consumer installs.
        sonic_rs::to_string(&json_rows)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// Like {@link query} with a server-side statement timeout (Lucid
    /// `timeout(ms, { cancel: true })`) — Postgres `statement_timeout`, MySQL
    /// `MAX_EXECUTION_TIME` (SELECT). Returns JSON array of row objects.
    #[napi]
    pub async fn query_timed(
        &self,
        sql: String,
        params_json: String,
        timeout_ms: u32,
    ) -> napi::Result<String> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let rows = rt
            .spawn(async move { db.query_timed(&sql, &params, timeout_ms).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        let json_rows: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = row
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string(&json_rows)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// Like {@link execute} with a server-side statement timeout (Postgres
    /// `statement_timeout`). Returns the JSON-serialized `ExecResult`.
    #[napi]
    pub async fn execute_timed(
        &self,
        sql: String,
        params_json: String,
        timeout_ms: u32,
    ) -> napi::Result<String> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let result = rt
            .spawn(async move { db.execute_timed(&sql, &params, timeout_ms).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        serde_json::to_string(&result)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// Execute an INSERT/UPDATE/DELETE. Returns the JSON-serialized `ExecResult`
    /// (`{ rows_affected, last_insert_id }`) so the JS side can read the MySQL/
    /// SQLite auto-increment id (Lucid's insert-without-returning shape).
    #[napi]
    pub async fn execute(&self, sql: String, params_json: String) -> napi::Result<String> {
        let db = self.db.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let result = rt
            .spawn(async move { db.execute(&sql, &params).await })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        serde_json::to_string(&result)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// Run a batch of `[sql, params_json]` pairs atomically in a single transaction.
    /// Accepts JSON `[[sql, params], ...]` and returns the total affected rows.
    #[napi]
    pub async fn run_in_transaction(&self, batch_json: String) -> napi::Result<f64> {
        let db = self.db.clone();
        // Input: `[[sql: string, params: unknown[]], ...]`
        let raw: Vec<(String, Vec<serde_json::Value>)> = serde_json::from_str(&batch_json)
            .map_err(|e| {
                napi::Error::new(
                    napi::Status::GenericFailure,
                    format!("Invalid transaction batch JSON: {}", e),
                )
            })?;

        let rt = ream_napi_core::shared_runtime();
        let affected = rt
            .spawn(async move { db.run_in_transaction(&raw).await })
            .await
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
        let tx = rt
            .spawn(async move { db.begin(isolation_level.as_deref()).await })
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
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let rows = rt
            .spawn(async move {
                let mut guard = tx.lock().await;
                let pinned = guard
                    .as_mut()
                    .ok_or_else(|| "transaction already finished".to_string())?;
                pinned.query(&sql, &params).await
            })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        let json_rows: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = row
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_json::to_string(&json_rows)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
    }

    /// INSERT/UPDATE/DELETE on the pinned connection. Returns the JSON-serialized
    /// `ExecResult` (`{ rows_affected, last_insert_id }`).
    #[napi]
    pub async fn execute(&self, sql: String, params_json: String) -> napi::Result<String> {
        let tx = self.tx.clone();
        let params: Vec<serde_json::Value> = serde_json::from_str(&params_json).map_err(|e| {
            napi::Error::new(
                napi::Status::GenericFailure,
                format!("Invalid params JSON: {}", e),
            )
        })?;

        let rt = ream_napi_core::shared_runtime();
        let result = rt
            .spawn(async move {
                let mut guard = tx.lock().await;
                let pinned = guard
                    .as_mut()
                    .ok_or_else(|| "transaction already finished".to_string())?;
                // MySQL SAVEPOINT/RELEASE/ROLLBACK TO can't be prepared (error 1295);
                // they must run over the text protocol via sqlx `raw_sql`, whose future
                // isn't `Send`. Run it synchronously on this worker thread with
                // `block_in_place` + `block_on` (documented tokio escape hatch) so the
                // spawned future stays `Send` while the statement still executes on the
                // shared runtime that owns the connection.
                if pinned.wants_text_protocol(&sql) {
                    tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(pinned.execute_text(&sql))
                    })
                } else {
                    pinned.execute(&sql, &params).await
                }
            })
            .await
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))?;

        serde_json::to_string(&result)
            .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))
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
        })
        .await
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
        })
        .await
        .map_err(|e| napi::Error::new(napi::Status::GenericFailure, format!("{}", e)))?
        .map_err(|e| napi::Error::new(napi::Status::GenericFailure, e))
    }
}

/// The one claim the `query` write path makes that is not self-evident: that
/// swapping `serde_json::to_string` for `sonic_rs::to_string` changed nothing
/// a caller can observe.
///
/// It was measured but never asserted, and "byte-identical" is exactly the
/// kind of claim that rots silently — a crate bump changes float formatting or
/// an escape rule and every consumer of this boundary gets a different string
/// with no test to say so. `JSON.parse` on the other side is forgiving about
/// most of it and merciless about the rest.
#[cfg(test)]
mod sonic_parity {
    use serde_json::{json, Value};

    /// Both serialisers, on the same value, must produce the same bytes.
    fn assert_same(value: &Value) {
        assert_eq!(
            sonic_rs::to_string(value).expect("sonic serialises"),
            serde_json::to_string(value).expect("serde serialises"),
            "diverged on {value:?}"
        );
    }

    #[test]
    fn strings_that_need_escaping() {
        for value in [
            json!("plain"),
            json!(""),
            json!("quote \" backslash \\ slash /"),
            json!("newline \n tab \t carriage \r"),
            json!("control \u{0000}\u{0001}\u{001f}"),
            json!("</script><script>alert(1)</script>"),
            json!("line \u{2028} paragraph \u{2029} separators"),
        ] {
            assert_same(&value);
        }
    }

    #[test]
    fn unicode_beyond_the_basic_plane() {
        for value in [
            json!("héllo wörld"),
            json!("日本語のテキスト"),
            json!("emoji 👨‍👩‍👧‍👦 with a zero-width joiner"),
            json!("astral 𝄞 clef"),
            json!("\u{FEFF}byte order mark"),
        ] {
            assert_same(&value);
        }
    }

    #[test]
    fn numbers_at_the_edges() {
        // The database hands back f64 and i64; the shapes that historically
        // differ between serialisers are the short decimals, the exponents and
        // the ends of the safe-integer range.
        for value in [
            json!(0),
            json!(-0.0),
            json!(0.1),
            json!(1e-7),
            json!(1.7976931348623157e308),
            json!(5e-324),
            json!(9_007_199_254_740_991_i64),
            json!(-9_007_199_254_740_991_i64),
            json!(i64::MAX),
            json!(i64::MIN),
            json!(u64::MAX),
        ] {
            assert_same(&value);
        }
    }

    #[test]
    fn the_row_shape_the_boundary_actually_sends() {
        // A `Vec<Value::Object>`, which is what `query` builds: nulls, nested
        // JSON columns, empty containers and mixed types in one array.
        let rows = json!([
            {
                "id": 1,
                "name": "Ada",
                "score": 99.5,
                "active": true,
                "deleted_at": null,
                "meta": { "tags": ["a", "b"], "nested": { "deep": [1, 2, 3] } }
            },
            { "id": 2, "name": "", "score": 0.0, "active": false, "meta": {} },
            { "id": 3, "name": "quote \" in a column", "meta": [] }
        ]);
        assert_same(&rows);
        assert_same(&json!([]));
        assert_same(&json!([{}]));
    }

    #[test]
    fn both_emit_keys_in_the_same_order() {
        // `serde_json::Map` is a `BTreeMap` here — the `preserve_order`
        // feature is off — so column keys have ALWAYS come out of this
        // boundary alphabetically rather than in the SELECT's order. That
        // predates the sonic swap and is unchanged by it; what this pins is
        // that the two serialisers agree on the order, since a divergence
        // there would silently reshuffle every result set.
        let mut row = serde_json::Map::new();
        row.insert("z_last".into(), json!(1));
        row.insert("a_first".into(), json!(2));
        row.insert("m_middle".into(), json!(3));
        let value = Value::Object(row);
        assert_same(&value);
        assert_eq!(
            sonic_rs::to_string(&value).expect("sonic serialises"),
            r#"{"a_first":2,"m_middle":3,"z_last":1}"#
        );
    }
}
