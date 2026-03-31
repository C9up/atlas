/**
 * SQLite Adapter — implements DatabaseAdapter for better-sqlite3.
 *
 * Used in tests and for SQLite-backed applications.
 */

import type { DatabaseAdapter } from '../schema/MigrationRunner.js'

/**
 * SQLite adapter wrapping better-sqlite3.
 * Pass ':memory:' for an in-memory database (tests).
 */
export class SqliteAdapter implements DatabaseAdapter {
  private db: import('better-sqlite3').Database

  constructor(db: import('better-sqlite3').Database) {
    this.db = db
  }

  async execute(sql: string): Promise<void> {
    // SQLite doesn't support SERIAL — rewrite for compatibility
    const adapted = sql
      .replace(/"id" SERIAL PRIMARY KEY/g, '"id" INTEGER PRIMARY KEY AUTOINCREMENT')
      .replace(/SERIAL PRIMARY KEY/g, 'INTEGER PRIMARY KEY AUTOINCREMENT')
      .replace(/DEFAULT NOW\(\)/g, "DEFAULT (datetime('now'))")
      .replace(/COALESCE\(MAX\(batch\), 0\) as max/g, "COALESCE(MAX(batch), 0) as max")
    this.db.exec(adapted)
  }

  async query<T>(sql: string): Promise<T[]> {
    const adapted = sql
      .replace(/DEFAULT NOW\(\)/g, "DEFAULT (datetime('now'))")
    return this.db.prepare(adapted).all() as T[]
  }

  async close(): Promise<void> {
    this.db.close()
  }

  /** Get the raw better-sqlite3 database (for direct queries in tests). */
  raw(): import('better-sqlite3').Database {
    return this.db
  }
}
