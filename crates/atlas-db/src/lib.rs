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
}

/// A database connection pool. Sqlite gets its own variant so we can use
/// `SqliteConnectOptions::pragma()` — the only sqlx surface that
/// guarantees every connection in the pool starts in the requested
/// `journal_mode` / `synchronous` state. The `AnyPool` path stays for
/// postgres / mysql where pragmas don't apply.
pub enum Database {
    Any(AnyPool),
    Sqlite(SqlitePool),
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

            let pool = SqlitePoolOptions::new()
                .min_connections(pool_min)
                .max_connections(pool_max)
                .connect_with(opts)
                .await
                .map_err(|e| format!("Database connection failed: {}", e))?;
            return Ok(Self::Sqlite(pool));
        }

        sqlx::any::install_default_drivers();
        let url = config.url.parse()
            .map_err(|e| format!("Invalid DB URL: {}", e))?;
        let pool = AnyPoolOptions::new()
            .min_connections(pool_min)
            .max_connections(pool_max)
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
        }
    }

    /// Close the pool.
    pub async fn close(&self) {
        match self {
            Self::Any(p) => p.close().await,
            Self::Sqlite(p) => p.close().await,
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
        }
    }

    /// Get pool size info.
    pub fn pool_size(&self) -> u32 {
        match self {
            Self::Any(p) => p.size(),
            Self::Sqlite(p) => p.size(),
        }
    }
}

fn is_sqlite_url(url: &str) -> bool {
    url.starts_with("sqlite:") || url.starts_with("sqlite://")
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
        }).await.unwrap();
        db.ping().await.unwrap();
        db.close().await;
    }

    #[tokio::test]
    async fn test_sqlite_crud() {
        let db = Database::connect(&DbConfig {
            url: "sqlite:file:testdb?mode=memory&cache=shared".into(),
            pool_min: Some(1),
            pool_max: Some(1),
            sqlite_pragmas: None,
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
}
