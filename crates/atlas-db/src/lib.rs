//! # ream-db
//!
//! Async database driver for the Ream framework.
//! Supports SQLite, PostgreSQL, and MySQL via sqlx.
//!
//! All database I/O runs in Rust — TypeScript receives results via NAPI Promises.
//!
//! @implements FR37

use serde::{Deserialize, Serialize};
use sqlx::any::AnyPoolOptions;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::sqlite::{
    SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{AnyPool, Column, Row, SqlitePool, TypeInfo};
use std::str::FromStr;

/// Database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    /// Connection URL: "sqlite:data/app.db", "postgres://user:pass@host/db", "mysql://user:pass@host/db"
    pub url: String,
    /// Minimum pool connections (default: 1)
    pub pool_min: Option<u32>,
    /// Maximum pool connections (default: 10)
    pub pool_max: Option<u32>,
    /// Sqlite-only: ordered `(key, value)` pragma pairs applied to every
    /// pooled connection at open-time via `SqliteConnectOptions::pragma`.
    /// Ignored for postgres / mysql URLs.
    pub sqlite_pragmas: Option<Vec<(String, String)>>,
    /// Number of EXTRA attempts to make if the initial connection fails
    /// (default: 0 — a single attempt, the historical behaviour). Useful when
    /// the DB starts a moment after the app (docker-compose / k8s) or for a
    /// transient network blip at boot.
    pub connect_retries: Option<u32>,
    /// Base backoff in milliseconds between connect attempts (default: 200).
    /// The delay grows exponentially (`base * 2^attempt`), capped at 30s.
    pub connect_backoff_ms: Option<u64>,
    /// Per-attempt acquire timeout in milliseconds (sqlx `acquire_timeout`).
    /// sqlx already retries connection establishment INTERNALLY up to this
    /// window (default ~30s), so lowering it makes each `connect_retries`
    /// attempt give up faster — e.g. 5 retries × 2s beats waiting 30s once.
    /// Unset ⇒ sqlx's default.
    pub connect_timeout_ms: Option<u64>,
}

/// A database connection pool. Sqlite gets its own variant so we can use
/// `SqliteConnectOptions::pragma()` — the only sqlx surface that
/// guarantees every connection in the pool starts in the requested
/// `journal_mode` / `synchronous` state. The `AnyPool` path stays for
/// postgres / mysql where pragmas don't apply.
pub enum Database {
    Any(AnyPool),
    Sqlite(SqlitePool),
    /// Postgres gets a native `PgPool` rather than the `AnyPool`: the sqlx
    /// `Any` driver has a fixed type-mapping table and rejects `jsonb`
    /// (and other rich PG types) at the column-meta stage, before a single
    /// value is decoded. A native pool decodes `jsonb` straight to
    /// `serde_json::Value`.
    Postgres(PgPool),
    /// MySQL gets a native `MySqlPool` for the same reason: the `Any` driver's
    /// fixed type table can't represent `DATETIME` / `DECIMAL` and rejects them
    /// at the column-meta stage (`Any driver does not support MySql type
    /// Datetime`). The native pool decodes them via chrono / bigdecimal.
    MySql(MySqlPool),
}

/// A single row result — columns as key-value pairs.
#[derive(Debug, Serialize, Deserialize)]
pub struct DbRow {
    pub columns: Vec<(String, serde_json::Value)>,
}

/// Result of an execute (INSERT/UPDATE/DELETE).
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecResult {
    pub rows_affected: u64,
}

impl Database {
    /// Connect to a database. Auto-detects driver from URL scheme.
    /// Supports: `sqlite:`, `postgres://`, `mysql://`.
    ///
    /// `pool_min` / `pool_max` are honoured (previously silently ignored — the
    /// default pool of 10 broke in-memory SQLite tests because each connection
    /// got its own isolated memory DB). When unset, defaults are 1 / 10.
    ///
    /// For sqlite URLs, `sqlite_pragmas` is threaded into
    /// `SqliteConnectOptions::pragma()` so every connection in the pool
    /// opens with the requested `journal_mode` / `synchronous` /
    /// `foreign_keys` / etc. state. This is the only sqlx surface that
    /// guarantees the pragma actually lands — running PRAGMA from the
    /// query path can silently no-op when another pooled connection
    /// holds the journal in a different mode.
    pub async fn connect(config: &DbConfig) -> Result<Self, String> {
        let retries = config.connect_retries.unwrap_or(0);
        let base_backoff = config.connect_backoff_ms.unwrap_or(200);
        let mut attempt: u32 = 0;
        loop {
            match Self::try_connect(config).await {
                Ok(db) => return Ok(db),
                Err(err) => {
                    if attempt >= retries {
                        return Err(err);
                    }
                    // Exponential backoff: base * 2^attempt, capped at 30s.
                    // `attempt` is clamped before the shift so it can't overflow,
                    // and `saturating_mul` guards a large base.
                    let shift = attempt.min(20);
                    let delay = base_backoff
                        .saturating_mul(1u64 << shift)
                        .min(30_000);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    attempt += 1;
                }
            }
        }
    }

    /// A single connection attempt. `connect` wraps this with optional
    /// retry + exponential backoff (`connect_retries` / `connect_backoff_ms`).
    async fn try_connect(config: &DbConfig) -> Result<Self, String> {
        let pool_min = config.pool_min.unwrap_or(1);
        let pool_max = config.pool_max.unwrap_or(10);

        if is_sqlite_url(&config.url) {
            let sqlite_url = strip_sqlite_scheme(&config.url);
            let mut opts = SqliteConnectOptions::from_str(sqlite_url)
                .map_err(|e| format!("Invalid sqlite URL: {}", e))?
                // Sqlite refuses to write to a file that doesn't yet exist
                // unless we ask for it. Matches what apps expect for the
                // first run on a fresh checkout.
                .create_if_missing(true);

            if let Some(pragmas) = &config.sqlite_pragmas {
                for (key, value) in pragmas {
                    // sqlx models a handful of common pragmas as typed
                    // setters (journal_mode, synchronous, foreign_keys,
                    // auto_vacuum). Calling `.pragma()` for those is a
                    // SILENT no-op — sqlx ignores it in favour of the
                    // typed slot. Route them through the typed API so
                    // the value actually lands on every connection.
                    let k_lower = key.to_ascii_lowercase();
                    let v_lower = value.to_ascii_lowercase();
                    opts = match k_lower.as_str() {
                        "journal_mode" => {
                            let mode = match v_lower.as_str() {
                                "wal" => SqliteJournalMode::Wal,
                                "delete" => SqliteJournalMode::Delete,
                                "truncate" => SqliteJournalMode::Truncate,
                                "persist" => SqliteJournalMode::Persist,
                                "memory" => SqliteJournalMode::Memory,
                                "off" => SqliteJournalMode::Off,
                                _ => return Err(format!(
                                    "Unsupported journal_mode value: {}", value
                                )),
                            };
                            opts.journal_mode(mode)
                        }
                        "synchronous" => {
                            let sync = match v_lower.as_str() {
                                "off" | "0" => SqliteSynchronous::Off,
                                "normal" | "1" => SqliteSynchronous::Normal,
                                "full" | "2" => SqliteSynchronous::Full,
                                "extra" | "3" => SqliteSynchronous::Extra,
                                _ => return Err(format!(
                                    "Unsupported synchronous value: {}", value
                                )),
                            };
                            opts.synchronous(sync)
                        }
                        "foreign_keys" => {
                            let on = matches!(v_lower.as_str(), "on" | "1" | "true");
                            opts.foreign_keys(on)
                        }
                        "auto_vacuum" => {
                            let av = match v_lower.as_str() {
                                "none" | "0" => SqliteAutoVacuum::None,
                                "full" | "1" => SqliteAutoVacuum::Full,
                                "incremental" | "2" => SqliteAutoVacuum::Incremental,
                                _ => return Err(format!(
                                    "Unsupported auto_vacuum value: {}", value
                                )),
                            };
                            opts.auto_vacuum(av)
                        }
                        _ => opts.pragma(key.clone(), value.clone()),
                    };
                }
            }

            let mut builder = SqlitePoolOptions::new()
                .min_connections(pool_min)
                .max_connections(pool_max);
            if let Some(ms) = config.connect_timeout_ms {
                builder = builder.acquire_timeout(std::time::Duration::from_millis(ms));
            }
            let pool = builder
                .connect_with(opts)
                .await
                .map_err(|e| format!("Database connection failed: {}", e))?;
            return Ok(Self::Sqlite(pool));
        }

        if is_postgres_url(&config.url) {
            let mut builder = PgPoolOptions::new()
                .min_connections(pool_min)
                .max_connections(pool_max);
            if let Some(ms) = config.connect_timeout_ms {
                builder = builder.acquire_timeout(std::time::Duration::from_millis(ms));
            }
            let pool = builder
                .connect(&config.url)
                .await
                .map_err(|e| format!("Database connection failed: {}", e))?;
            return Ok(Self::Postgres(pool));
        }

        if is_mysql_url(&config.url) {
            let mut builder = MySqlPoolOptions::new()
                .min_connections(pool_min)
                .max_connections(pool_max);
            if let Some(ms) = config.connect_timeout_ms {
                builder = builder.acquire_timeout(std::time::Duration::from_millis(ms));
            }
            let pool = builder
                .connect(&config.url)
                .await
                .map_err(|e| format!("Database connection failed: {}", e))?;
            return Ok(Self::MySql(pool));
        }

        sqlx::any::install_default_drivers();
        let url = config.url.parse()
            .map_err(|e| format!("Invalid DB URL: {}", e))?;
        let mut builder = AnyPoolOptions::new()
            .min_connections(pool_min)
            .max_connections(pool_max);
        if let Some(ms) = config.connect_timeout_ms {
            builder = builder.acquire_timeout(std::time::Duration::from_millis(ms));
        }
        let pool = builder
            .connect_with(url)
            .await
            .map_err(|e| format!("Database connection failed: {}", e))?;
        Ok(Self::Any(pool))
    }

    /// Execute a query that returns rows (SELECT).
    pub async fn query(&self, sql: &str, params: &[serde_json::Value]) -> Result<Vec<DbRow>, String> {
        match self {
            Self::Any(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_param(q, param);
                }
                let rows = q.fetch_all(pool)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(row_to_dbrow).collect()
            }
            Self::Sqlite(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_sqlite_param(q, param);
                }
                let rows = q.fetch_all(pool)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(sqlite_row_to_dbrow).collect()
            }
            Self::Postgres(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_pg_param(q, param);
                }
                let rows = q.fetch_all(pool)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(pg_row_to_dbrow).collect()
            }
            Self::MySql(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_mysql_param(q, param);
                }
                let rows = q.fetch_all(pool)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(mysql_row_to_dbrow).collect()
            }
        }
    }

    /// Execute a statement (INSERT/UPDATE/DELETE).
    pub async fn execute(&self, sql: &str, params: &[serde_json::Value]) -> Result<ExecResult, String> {
        match self {
            Self::Any(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_param(q, param);
                }
                let result = q.execute(pool)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: result.rows_affected() })
            }
            Self::Sqlite(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_sqlite_param(q, param);
                }
                let result = q.execute(pool)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: result.rows_affected() })
            }
            Self::Postgres(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_pg_param(q, param);
                }
                let result = q.execute(pool)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: result.rows_affected() })
            }
            Self::MySql(pool) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_mysql_param(q, param);
                }
                let result = q.execute(pool)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: result.rows_affected() })
            }
        }
    }

    /// Execute a batch of `(sql, params)` pairs atomically inside a single
    /// sqlx transaction. Either every statement commits or none do.
    ///
    /// Used by MigrationRunner to wrap a migration's `up()` / `down()` SQL
    /// together with the corresponding `_migrations` bookkeeping row — so a
    /// mid-migration failure cannot leave the schema and the tracking table
    /// in an inconsistent state.
    pub async fn run_in_transaction(
        &self,
        statements: &[(String, Vec<serde_json::Value>)],
    ) -> Result<u64, String> {
        match self {
            Self::Any(pool) => {
                let mut tx = pool.begin()
                    .await
                    .map_err(|e| format!("BEGIN failed: {}", e))?;
                let mut total: u64 = 0;
                for (sql, params) in statements {
                    let mut q = sqlx::query(sql);
                    for param in params {
                        q = bind_param(q, param);
                    }
                    let result = q.execute(&mut *tx)
                        .await
                        .map_err(|e| format!("Transaction aborted on '{}': {}", truncate(sql, 80), e))?;
                    total += result.rows_affected();
                }
                tx.commit()
                    .await
                    .map_err(|e| format!("COMMIT failed: {}", e))?;
                Ok(total)
            }
            Self::Sqlite(pool) => {
                let mut tx = pool.begin()
                    .await
                    .map_err(|e| format!("BEGIN failed: {}", e))?;
                let mut total: u64 = 0;
                for (sql, params) in statements {
                    let mut q = sqlx::query(sql);
                    for param in params {
                        q = bind_sqlite_param(q, param);
                    }
                    let result = q.execute(&mut *tx)
                        .await
                        .map_err(|e| format!("Transaction aborted on '{}': {}", truncate(sql, 80), e))?;
                    total += result.rows_affected();
                }
                tx.commit()
                    .await
                    .map_err(|e| format!("COMMIT failed: {}", e))?;
                Ok(total)
            }
            Self::Postgres(pool) => {
                let mut tx = pool.begin()
                    .await
                    .map_err(|e| format!("BEGIN failed: {}", e))?;
                let mut total: u64 = 0;
                for (sql, params) in statements {
                    let mut q = sqlx::query(sql);
                    for param in params {
                        q = bind_pg_param(q, param);
                    }
                    let result = q.execute(&mut *tx)
                        .await
                        .map_err(|e| format!("Transaction aborted on '{}': {}", truncate(sql, 80), e))?;
                    total += result.rows_affected();
                }
                tx.commit()
                    .await
                    .map_err(|e| format!("COMMIT failed: {}", e))?;
                Ok(total)
            }
            Self::MySql(pool) => {
                let mut tx = pool.begin()
                    .await
                    .map_err(|e| format!("BEGIN failed: {}", e))?;
                let mut total: u64 = 0;
                for (sql, params) in statements {
                    let mut q = sqlx::query(sql);
                    for param in params {
                        q = bind_mysql_param(q, param);
                    }
                    let result = q.execute(&mut *tx)
                        .await
                        .map_err(|e| format!("Transaction aborted on '{}': {}", truncate(sql, 80), e))?;
                    total += result.rows_affected();
                }
                tx.commit()
                    .await
                    .map_err(|e| format!("COMMIT failed: {}", e))?;
                Ok(total)
            }
        }
    }

    /// Close the pool.
    pub async fn close(&self) {
        match self {
            Self::Any(p) => p.close().await,
            Self::Sqlite(p) => p.close().await,
            Self::Postgres(p) => p.close().await,
            Self::MySql(p) => p.close().await,
        }
    }

    /// Health check.
    pub async fn ping(&self) -> Result<(), String> {
        match self {
            Self::Any(p) => p.acquire().await
                .map_err(|e| format!("Ping failed: {}", e))
                .map(|_| ()),
            Self::Sqlite(p) => p.acquire().await
                .map_err(|e| format!("Ping failed: {}", e))
                .map(|_| ()),
            Self::Postgres(p) => p.acquire().await
                .map_err(|e| format!("Ping failed: {}", e))
                .map(|_| ()),
            Self::MySql(p) => p.acquire().await
                .map_err(|e| format!("Ping failed: {}", e))
                .map(|_| ()),
        }
    }

    /// Get pool size info.
    pub fn pool_size(&self) -> u32 {
        match self {
            Self::Any(p) => p.size(),
            Self::Sqlite(p) => p.size(),
            Self::Postgres(p) => p.size(),
            Self::MySql(p) => p.size(),
        }
    }

    /// Open an INTERACTIVE transaction pinned to ONE pooled connection.
    ///
    /// Unlike [`run_in_transaction`](Self::run_in_transaction) (atomic but
    /// non-interactive — it can't read-then-decide-then-write), this returns a
    /// handle that holds its connection: every `query`/`execute` runs on the
    /// SAME connection, and the connection only returns to the pool on
    /// `commit`/`rollback`. Pulling `BEGIN`/`COMMIT` through the pool (a
    /// different connection per call) is NOT a transaction — it scatters
    /// statements across connections and can strand a row lock on an idle
    /// pooled connection, poisoning the pool until `acquire_timeout`.
    pub async fn begin(&self) -> Result<DbTransaction, String> {
        match self {
            Self::Any(p) => Ok(DbTransaction::Any(
                p.begin().await.map_err(|e| format!("BEGIN failed: {}", e))?,
            )),
            Self::Sqlite(p) => Ok(DbTransaction::Sqlite(
                p.begin().await.map_err(|e| format!("BEGIN failed: {}", e))?,
            )),
            Self::Postgres(p) => Ok(DbTransaction::Postgres(
                p.begin().await.map_err(|e| format!("BEGIN failed: {}", e))?,
            )),
            Self::MySql(p) => Ok(DbTransaction::MySql(
                p.begin().await.map_err(|e| format!("BEGIN failed: {}", e))?,
            )),
        }
    }
}

/// An interactive transaction pinned to a single pooled connection, created by
/// [`Database::begin`]. `query`/`execute` run on that one connection;
/// `commit`/`rollback` consume the handle and release the connection back to
/// the pool. Held across NAPI calls so TS can read-then-decide-then-write
/// atomically (gap-free numbering, multi-statement create/update/delete).
pub enum DbTransaction {
    Any(sqlx::Transaction<'static, sqlx::Any>),
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
}

impl DbTransaction {
    /// Run a SELECT on the pinned connection.
    pub async fn query(
        &mut self,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<Vec<DbRow>, String> {
        match self {
            Self::Any(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_param(q, param);
                }
                let rows = q
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(row_to_dbrow).collect()
            }
            Self::Sqlite(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_sqlite_param(q, param);
                }
                let rows = q
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(sqlite_row_to_dbrow).collect()
            }
            Self::Postgres(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_pg_param(q, param);
                }
                let rows = q
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(pg_row_to_dbrow).collect()
            }
            Self::MySql(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_mysql_param(q, param);
                }
                let rows = q
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| format!("Query failed: {}", e))?;
                rows.iter().map(mysql_row_to_dbrow).collect()
            }
        }
    }

    /// Run an INSERT/UPDATE/DELETE on the pinned connection.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<ExecResult, String> {
        match self {
            Self::Any(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_param(q, param);
                }
                let r = q
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: r.rows_affected() })
            }
            Self::Sqlite(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_sqlite_param(q, param);
                }
                let r = q
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: r.rows_affected() })
            }
            Self::Postgres(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_pg_param(q, param);
                }
                let r = q
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: r.rows_affected() })
            }
            Self::MySql(tx) => {
                let mut q = sqlx::query(sql);
                for param in params {
                    q = bind_mysql_param(q, param);
                }
                let r = q
                    .execute(&mut **tx)
                    .await
                    .map_err(|e| format!("Execute failed: {}", e))?;
                Ok(ExecResult { rows_affected: r.rows_affected() })
            }
        }
    }

    /// Commit the transaction and release the connection back to the pool.
    pub async fn commit(self) -> Result<(), String> {
        match self {
            Self::Any(tx) => tx.commit().await.map_err(|e| format!("COMMIT failed: {}", e)),
            Self::Sqlite(tx) => tx.commit().await.map_err(|e| format!("COMMIT failed: {}", e)),
            Self::Postgres(tx) => tx.commit().await.map_err(|e| format!("COMMIT failed: {}", e)),
            Self::MySql(tx) => tx.commit().await.map_err(|e| format!("COMMIT failed: {}", e)),
        }
    }

    /// Roll back the transaction and release the connection back to the pool.
    pub async fn rollback(self) -> Result<(), String> {
        match self {
            Self::Any(tx) => tx.rollback().await.map_err(|e| format!("ROLLBACK failed: {}", e)),
            Self::Sqlite(tx) => tx.rollback().await.map_err(|e| format!("ROLLBACK failed: {}", e)),
            Self::Postgres(tx) => tx.rollback().await.map_err(|e| format!("ROLLBACK failed: {}", e)),
            Self::MySql(tx) => tx.rollback().await.map_err(|e| format!("ROLLBACK failed: {}", e)),
        }
    }
}

fn is_sqlite_url(url: &str) -> bool {
    url.starts_with("sqlite:") || url.starts_with("sqlite://")
}

fn is_mysql_url(url: &str) -> bool {
    url.starts_with("mysql:") || url.starts_with("mariadb:")
}

fn is_postgres_url(url: &str) -> bool {
    url.starts_with("postgres:") || url.starts_with("postgresql:")
}

/// `SqliteConnectOptions::from_str` accepts either the bare path or the
/// full `sqlite:...` URL. We strip the scheme so both styles end up the
/// same — and so `sqlite::memory:` keeps working.
fn strip_sqlite_scheme(url: &str) -> &str {
    if let Some(rest) = url.strip_prefix("sqlite://") {
        rest
    } else if let Some(rest) = url.strip_prefix("sqlite:") {
        rest
    } else {
        url
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n { s.to_string() } else { format!("{}...", &s[..n]) }
}

fn bind_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Any, sqlx::any::AnyArguments<'q>> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => query.bind(s.as_str()),
        _ => query.bind(value.to_string()),
    }
}

fn bind_sqlite_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => query.bind(s.as_str()),
        _ => query.bind(value.to_string()),
    }
}

fn row_to_dbrow(row: &sqlx::any::AnyRow) -> Result<DbRow, String> {
    let mut columns = Vec::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name();
        let ordinal = col.ordinal();

        // Decode each column as `Option<T>` so that SQL NULL becomes `None`
        // and a real decode error (type mismatch, bad column) propagates up
        // instead of being silently coerced to JSON null. Corrupt reads used
        // to vanish into `unwrap_or(Null)` — the audit caught it as a
        // medium-severity integrity risk.
        let value: serde_json::Value = match type_name {
            "INTEGER" | "INT4" | "INT8" | "BIGINT" | "INT" => {
                match row.try_get::<Option<i64>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Value::from(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            "REAL" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
                match row.try_get::<Option<f64>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            "BOOLEAN" | "BOOL" => {
                match row.try_get::<Option<bool>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Value::Bool(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            // Unknown / dynamic type — e.g. SQLite PRAGMA results report
            // `type_info().name() == "NULL"` regardless of the real value.
            // Try integer → float → string in order (most common → least).
            _ => try_decode_any(row, ordinal)
                .map_err(|e| format!("Column '{}' (type {}): decode failed: {}", name, type_name, e))?,
        };
        columns.push((name, value));
    }
    Ok(DbRow { columns })
}

/// Mirror of `row_to_dbrow` for `SqliteRow`. Same decoding rules — try
/// each concrete type by ordinal and propagate decode failures as a
/// `Result::Err` rather than coercing to JSON null.
fn sqlite_row_to_dbrow(row: &sqlx::sqlite::SqliteRow) -> Result<DbRow, String> {
    let mut columns = Vec::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name();
        let ordinal = col.ordinal();
        let value: serde_json::Value = match type_name {
            "INTEGER" | "INT4" | "INT8" | "BIGINT" | "INT" => {
                match row.try_get::<Option<i64>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Value::from(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            "REAL" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
                match row.try_get::<Option<f64>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Number::from_f64(v)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            "BOOLEAN" | "BOOL" => {
                match row.try_get::<Option<bool>, _>(ordinal) {
                    Ok(Some(v)) => serde_json::Value::Bool(v),
                    Ok(None) => serde_json::Value::Null,
                    Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
                }
            }
            _ => try_decode_sqlite(row, ordinal)
                .map_err(|e| format!("Column '{}' (type {}): decode failed: {}", name, type_name, e))?,
        };
        columns.push((name, value));
    }
    Ok(DbRow { columns })
}

fn try_decode_sqlite(row: &sqlx::sqlite::SqliteRow, ordinal: usize) -> Result<serde_json::Value, sqlx::Error> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(ordinal) {
        return Ok(match v {
            Some(n) => serde_json::Value::from(n),
            None => serde_json::Value::Null,
        });
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(ordinal) {
        return Ok(match v {
            Some(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            None => serde_json::Value::Null,
        });
    }
    match row.try_get::<Option<String>, _>(ordinal)? {
        Some(s) => Ok(serde_json::Value::String(s)),
        None => Ok(serde_json::Value::Null),
    }
}

/// Strict dynamic decoder used for columns whose static type info is missing
/// or `"NULL"` (SQLite PRAGMA). Tries the common scalar shapes in order and
/// returns the first successful decode. Only errors out if every attempt fails.
fn try_decode_any(row: &sqlx::any::AnyRow, ordinal: usize) -> Result<serde_json::Value, sqlx::Error> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(ordinal) {
        return Ok(match v {
            Some(n) => serde_json::Value::from(n),
            None => serde_json::Value::Null,
        });
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(ordinal) {
        return Ok(match v {
            Some(f) => serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            None => serde_json::Value::Null,
        });
    }
    // Last resort — propagate the string decode error if it fails too.
    match row.try_get::<Option<String>, _>(ordinal)? {
        Some(s) => Ok(serde_json::Value::String(s)),
        None => Ok(serde_json::Value::Null),
    }
}

fn bind_pg_param<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => query.bind(s.as_str()),
        _ => query.bind(value.to_string()),
    }
}

fn bind_mysql_param<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    value: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => query.bind(s.as_str()),
        _ => query.bind(value.to_string()),
    }
}

/// Mirror of `row_to_dbrow` for a native `PgRow`. Postgres decode is strict
/// (an `int4` column refuses an `i64` target, unlike the lenient `Any`
/// driver), so each numeric type is matched to its exact Rust width. The
/// payoff is the `json`/`jsonb` arm: a native pool decodes them straight to
/// `serde_json::Value`, which the `Any` driver cannot represent at all.
fn pg_row_to_dbrow(row: &sqlx::postgres::PgRow) -> Result<DbRow, String> {
    let mut columns = Vec::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name();
        let ordinal = col.ordinal();
        let value: serde_json::Value = match type_name {
            "INT2" | "SMALLINT" => match row.try_get::<Option<i16>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::from(v),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "INT4" | "INT" | "SERIAL" => match row.try_get::<Option<i32>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::from(v),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "INT8" | "BIGINT" => match row.try_get::<Option<i64>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::from(v),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "FLOAT4" => match row.try_get::<Option<f32>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Number::from_f64(v as f64)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "FLOAT8" | "DOUBLE PRECISION" => match row.try_get::<Option<f64>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Number::from_f64(v)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "NUMERIC" | "DECIMAL" => match row.try_get::<Option<sqlx::types::BigDecimal>, _>(ordinal) {
                // Decode to a STRING to preserve arbitrary precision — an f64
                // would silently truncate an 18+-digit decimal. `normalized()`
                // drops the trailing zeros sqlx pads in from Postgres's base-10000
                // NBASE grouping (raw `to_string()` of `…789` yields `…7890`), so
                // the JS decimal adapter consumes the exact inserted value.
                Ok(Some(v)) => serde_json::Value::String(v.normalized().to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "BOOL" | "BOOLEAN" => match row.try_get::<Option<bool>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::Bool(v),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "JSON" | "JSONB" => match row.try_get::<Option<serde_json::Value>, _>(ordinal) {
                Ok(Some(v)) => v,
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            // Native non-text types: sqlx's strict Postgres decoder REFUSES to
            // hand these back as `String`, so they must be decoded to their real
            // Rust type and re-serialised to a string the JS side can hydrate
            // (`new Date(...)` / uuid string). Without these arms the default
            // `try_get::<String>` arm fails with "Rust type Option<String> (as
            // SQL TEXT) is not compatible with SQL type TIMESTAMP/UUID/...".
            "UUID" => match row.try_get::<Option<sqlx::types::Uuid>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "TIMESTAMP" => match row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(ordinal) {
                // No timezone in the column: emit an ISO 8601 string with a `Z`
                // suffix (the write path stores UTC), so `new Date(...)` reads it back unambiguously.
                Ok(Some(v)) => serde_json::Value::String(v.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "TIMESTAMPTZ" => match row.try_get::<Option<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.to_rfc3339()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "DATE" => match row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "TIME" => match row.try_get::<Option<sqlx::types::chrono::NaiveTime>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            // TEXT / VARCHAR / etc. — surface as a string.
            _ => try_decode_pg(row, ordinal)
                .map_err(|e| format!("Column '{}' (type {}): decode failed: {}", name, type_name, e))?,
        };
        columns.push((name, value));
    }
    Ok(DbRow { columns })
}

fn try_decode_pg(row: &sqlx::postgres::PgRow, ordinal: usize) -> Result<serde_json::Value, sqlx::Error> {
    match row.try_get::<Option<String>, _>(ordinal)? {
        Some(s) => Ok(serde_json::Value::String(s)),
        None => Ok(serde_json::Value::Null),
    }
}

/// Native `MySqlRow` decode — mirror of `pg_row_to_dbrow`. The sqlx `Any` driver
/// (which MySQL used to ride) can't represent `DATETIME` / `DECIMAL`, so those
/// need their real Rust type. MySQL has no native uuid (it's CHAR/BINARY) and no
/// bool (it's `TINYINT(1)`), so those flow through the integer/string fallback.
fn mysql_row_to_dbrow(row: &sqlx::mysql::MySqlRow) -> Result<DbRow, String> {
    let mut columns = Vec::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name();
        let ordinal = col.ordinal();
        let value: serde_json::Value = match type_name {
            "DATETIME" | "TIMESTAMP" => match row.try_get::<Option<sqlx::types::chrono::NaiveDateTime>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "DATE" => match row.try_get::<Option<sqlx::types::chrono::NaiveDate>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            // MySQL TIME spans -838:59:59..838:59:59 (a duration), which
            // `chrono::NaiveTime` can't hold — surface the raw string.
            "TIME" => match row.try_get::<Option<String>, _>(ordinal) {
                Ok(opt) => opt.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "DECIMAL" => match row.try_get::<Option<sqlx::types::BigDecimal>, _>(ordinal) {
                Ok(Some(v)) => serde_json::Value::String(v.normalized().to_string()),
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            "JSON" => match row.try_get::<Option<serde_json::Value>, _>(ordinal) {
                Ok(Some(v)) => v,
                Ok(None) => serde_json::Value::Null,
                Err(e) => return Err(format!("Column '{}' (type {}): decode failed: {}", name, type_name, e)),
            },
            _ => try_decode_mysql(row, ordinal)
                .map_err(|e| format!("Column '{}' (type {}): decode failed: {}", name, type_name, e))?,
        };
        columns.push((name, value));
    }
    Ok(DbRow { columns })
}

/// Tolerant decode for MySQL columns whose type we don't special-case: integers
/// of any width/signedness, floats, `TINYINT(1)` booleans, and text/blob/enum.
/// Tries in widening order and returns the first success — avoids enumerating
/// the full signed/unsigned/width matrix of MySQL integer type names.
fn try_decode_mysql(row: &sqlx::mysql::MySqlRow, ordinal: usize) -> Result<serde_json::Value, sqlx::Error> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(ordinal) {
        return Ok(v.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null));
    }
    if let Ok(v) = row.try_get::<Option<u64>, _>(ordinal) {
        return Ok(v.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null));
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(ordinal) {
        return Ok(v
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null));
    }
    if let Ok(v) = row.try_get::<Option<bool>, _>(ordinal) {
        return Ok(v.map(serde_json::Value::Bool).unwrap_or(serde_json::Value::Null));
    }
    match row.try_get::<Option<String>, _>(ordinal)? {
        Some(s) => Ok(serde_json::Value::String(s)),
        None => Ok(serde_json::Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqlite_connect() {
        let db = Database::connect(&DbConfig {
            url: "sqlite:file:testdb?mode=memory&cache=shared".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();
        db.ping().await.unwrap();
        db.close().await;
    }

    #[tokio::test]
    async fn test_connect_retries_apply_backoff() {
        // An invalid URL fails fast at the parse stage (no 30s sqlx acquire
        // wait), so the elapsed time isolates the backoff our loop adds.
        let start = std::time::Instant::now();
        let result = Database::connect(&DbConfig {
            url: "not-a-valid-url".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: Some(2),
            connect_backoff_ms: Some(20),
            connect_timeout_ms: None,
        })
        .await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "invalid URL must error");
        // 2 retries at base 20ms ⇒ 20 + 40 = 60ms of backoff minimum.
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "expected backoff to delay the failure, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_no_retries_fails_fast() {
        let start = std::time::Instant::now();
        let result = Database::connect(&DbConfig {
            url: "not-a-valid-url".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: Some(0),
            connect_backoff_ms: Some(20),
            connect_timeout_ms: None,
        })
        .await;
        let elapsed = start.elapsed();
        assert!(result.is_err());
        // No retry ⇒ no backoff sleeps; the invalid URL errors immediately.
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "no-retry connect should not sleep, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_sqlite_crud() {
        let db = Database::connect(&DbConfig {
            url: "sqlite:file:testdb?mode=memory&cache=shared".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();

        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)", &[]).await.unwrap();

        let result = db.execute(
            "INSERT INTO test (name) VALUES (?)",
            &[serde_json::Value::String("hello".into())],
        ).await.unwrap();
        assert_eq!(result.rows_affected, 1);

        let rows = db.query("SELECT id, name FROM test", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns[1].1, serde_json::Value::String("hello".into()));

        db.close().await;
    }

    #[tokio::test]
    async fn test_sqlite_wal_pragma_lands() {
        let path = format!("/tmp/atlas_wal_test_{}.db", std::process::id());
        let _ = std::fs::remove_file(&path);
        let db = Database::connect(&DbConfig {
            url: format!("sqlite:{}", &path),
            pool_min: Some(1),
            pool_max: Some(4),
            sqlite_pragmas: Some(vec![
                ("journal_mode".into(), "WAL".into()),
                ("synchronous".into(), "NORMAL".into()),
            ]),
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();

        db.execute("CREATE TABLE x (a INTEGER)", &[]).await.unwrap();
        let mode = db.query("PRAGMA journal_mode;", &[]).await.unwrap();
        let sync = db.query("PRAGMA synchronous;", &[]).await.unwrap();
        assert_eq!(
            mode[0].columns[0].1,
            serde_json::Value::String("wal".into()),
            "journal_mode pragma did not land"
        );
        assert_eq!(
            sync[0].columns[0].1,
            serde_json::Value::from(1),
            "synchronous=NORMAL pragma did not land"
        );

        db.close().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-shm", path));
        let _ = std::fs::remove_file(format!("{}-wal", path));
    }

    #[tokio::test]
    async fn test_sqlite_params() {
        let db = Database::connect(&DbConfig {
            url: "sqlite:file:testdb?mode=memory&cache=shared".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();

        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, age INTEGER)", &[]).await.unwrap();
        db.execute("INSERT INTO users (email, age) VALUES (?, ?)", &[
            serde_json::json!("test@example.com"),
            serde_json::json!(25),
        ]).await.unwrap();

        let rows = db.query("SELECT * FROM users WHERE age > ?", &[serde_json::json!(20)]).await.unwrap();
        assert_eq!(rows.len(), 1);

        db.close().await;
    }

    /// Postgres native-type decode regression (gated behind `ATLAS_PG_TEST_URL`).
    /// Repros the bug where a SELECT over a `uuid` / `timestamp` / `date` column
    /// failed at decode because every non-numeric/bool/json column fell through
    /// to `try_get::<Option<String>>` — sqlx's strict Postgres decoder rejects
    /// `String` for those native types. Run with e.g.:
    ///   ATLAS_PG_TEST_URL=postgres://atlas:atlas@127.0.0.1:5433/atlas \
    ///     cargo test -p atlas-db pg_native -- --nocapture
    #[tokio::test]
    async fn test_pg_native_type_decode() {
        let url = match std::env::var("ATLAS_PG_TEST_URL") {
            Ok(u) => u,
            Err(_) => return, // no PG configured — skip
        };
        let db = Database::connect(&DbConfig {
            url,
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();

        db.execute("DROP TABLE IF EXISTS atlas_decode_probe", &[]).await.unwrap();
        db.execute(
            "CREATE TABLE atlas_decode_probe (id uuid, created_at timestamp, updated_at timestamptz, day date, amount numeric(30,3), name text)",
            &[],
        ).await.unwrap();
        db.execute(
            "INSERT INTO atlas_decode_probe VALUES \
             ('00000000-0000-4000-8000-000000000001'::uuid, \
              '2026-06-09 12:34:56'::timestamp, \
              '2026-06-09 12:34:56Z'::timestamptz, \
              '2026-06-09'::date, \
              1234567890123456.789, 'ada')",
            &[],
        ).await.unwrap();

        let rows = db
            .query("SELECT id, created_at, updated_at, day, amount, name FROM atlas_decode_probe", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].columns;
        // Each native column decodes to a non-null string, not a decode error.
        assert!(matches!(&cols[0].1, serde_json::Value::String(s) if s.contains("0000-4000")), "uuid: {:?}", cols[0].1);
        assert!(matches!(&cols[1].1, serde_json::Value::String(s) if s.contains("2026-06-09")), "timestamp: {:?}", cols[1].1);
        assert!(matches!(&cols[2].1, serde_json::Value::String(s) if s.contains("2026-06-09")), "timestamptz: {:?}", cols[2].1);
        assert!(matches!(&cols[3].1, serde_json::Value::String(s) if s == "2026-06-09"), "date: {:?}", cols[3].1);
        // NUMERIC must round-trip to the EXACT decimal string (precision-safe).
        assert_eq!(cols[4].1, serde_json::Value::String("1234567890123456.789".into()), "numeric: {:?}", cols[4].1);
        assert_eq!(cols[5].1, serde_json::Value::String("ada".into()));

        db.execute("DROP TABLE atlas_decode_probe", &[]).await.unwrap();
        db.close().await;
    }

    /// MySQL native-type decode (gated behind `ATLAS_MYSQL_TEST_URL`). MySQL used
    /// to ride the `Any` pool, which can't even represent DATETIME/DECIMAL; this
    /// proves the native `MySqlPool` path decodes them. Run with e.g.:
    ///   ATLAS_MYSQL_TEST_URL=mysql://atlas:atlas@127.0.0.1:3307/atlas \
    ///     cargo test -p atlas-db mysql_native -- --nocapture
    #[tokio::test]
    async fn test_mysql_native_type_decode() {
        let url = match std::env::var("ATLAS_MYSQL_TEST_URL") {
            Ok(u) => u,
            Err(_) => return,
        };
        let db = Database::connect(&DbConfig {
            url,
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
            connect_retries: None,
            connect_backoff_ms: None,
            connect_timeout_ms: None,
        }).await.unwrap();

        db.execute("DROP TABLE IF EXISTS atlas_my_probe", &[]).await.unwrap();
        db.execute(
            "CREATE TABLE atlas_my_probe (id int, big bigint unsigned, created_at datetime, \
             day date, amount decimal(30,3), flag tinyint(1), name varchar(50))",
            &[],
        ).await.unwrap();
        db.execute(
            "INSERT INTO atlas_my_probe VALUES \
             (42, 18446744073709551615, '2026-06-09 12:34:56', '2026-06-09', \
              1234567890123456.789, 1, 'ada')",
            &[],
        ).await.unwrap();

        let rows = db
            .query("SELECT id, big, created_at, day, amount, flag, name FROM atlas_my_probe", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let c = &rows[0].columns;
        assert_eq!(c[0].1, serde_json::Value::from(42i64), "int: {:?}", c[0].1);
        // bigint unsigned max — must survive as a number via the u64 path.
        assert!(matches!(&c[1].1, serde_json::Value::Number(_)), "bigint unsigned: {:?}", c[1].1);
        assert!(matches!(&c[2].1, serde_json::Value::String(s) if s.contains("2026-06-09")), "datetime: {:?}", c[2].1);
        assert!(matches!(&c[3].1, serde_json::Value::String(s) if s == "2026-06-09"), "date: {:?}", c[3].1);
        assert_eq!(c[4].1, serde_json::Value::String("1234567890123456.789".into()), "decimal: {:?}", c[4].1);
        // TINYINT(1) is MySQL's "bool" — surfaces as an integer 0/1.
        assert!(matches!(&c[5].1, serde_json::Value::Number(_)), "tinyint: {:?}", c[5].1);
        assert_eq!(c[6].1, serde_json::Value::String("ada".into()));

        db.execute("DROP TABLE atlas_my_probe", &[]).await.unwrap();
        db.close().await;
    }
}
