/**
 * Migration Runner — discovers, executes, and tracks database migrations.
 *
 * @implements FR34
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import { AtlasError } from '../errors.js'
import type { Migration } from './Migration.js'

export interface DatabaseAdapter {
  /** Execute a SQL statement. */
  execute(sql: string): Promise<void>
  /** Query rows. */
  query<T>(sql: string): Promise<T[]>
  /** Close the connection. */
  close(): Promise<void>
}

export interface MigrationRecord {
  name: string
  batch: number
  executed_at: string
}

/**
 * Runs migrations against a database.
 */
export class MigrationRunner {
  private db: DatabaseAdapter
  private migrationsDir: string
  private driver: 'postgres' | 'sqlite'

  constructor(db: DatabaseAdapter, options?: { migrationsDir?: string; driver?: 'postgres' | 'sqlite' }) {
    this.db = db
    this.migrationsDir = options?.migrationsDir ?? 'database/migrations'
    this.driver = options?.driver ?? 'postgres'
  }

  /** Ensure the _migrations tracking table exists (driver-aware DDL). */
  async init(): Promise<void> {
    if (this.driver === 'sqlite') {
      await this.db.execute(`
        CREATE TABLE IF NOT EXISTS "_migrations" (
          "id" INTEGER PRIMARY KEY AUTOINCREMENT,
          "name" TEXT NOT NULL UNIQUE,
          "batch" INTEGER NOT NULL,
          "executed_at" TEXT NOT NULL DEFAULT (datetime('now'))
        );
      `)
    } else {
      await this.db.execute(`
        CREATE TABLE IF NOT EXISTS "_migrations" (
          "id" SERIAL PRIMARY KEY,
          "name" VARCHAR(255) NOT NULL UNIQUE,
          "batch" INTEGER NOT NULL,
          "executed_at" TIMESTAMP NOT NULL DEFAULT NOW()
        );
      `)
    }
  }

  /** Get the status of all migrations. */
  async status(): Promise<Array<{ name: string; status: 'applied' | 'pending'; batch?: number }>> {
    const applied = await this.db.query<MigrationRecord>('SELECT name, batch FROM "_migrations" ORDER BY name')
    const appliedMap = new Map(applied.map((r) => [r.name, r.batch]))

    const files = this.discoverFiles()
    return files.map((f) => ({
      name: f,
      status: appliedMap.has(f) ? 'applied' as const : 'pending' as const,
      batch: appliedMap.get(f),
    }))
  }

  /** Run all pending migrations. */
  async migrate(): Promise<string[]> {
    await this.init()

    const applied = await this.db.query<MigrationRecord>('SELECT name FROM "_migrations"')
    const appliedNames = new Set(applied.map((r) => r.name))
    const files = this.discoverFiles()
    const pending = files.filter((f) => !appliedNames.has(f))

    if (pending.length === 0) {
      return []
    }

    // Get next batch number
    const batchResult = await this.db.query<{ max: number }>('SELECT COALESCE(MAX(batch), 0) as max FROM "_migrations"')
    const batch = (batchResult[0]?.max ?? 0) + 1

    const executed: string[] = []

    for (const name of pending) {
      this.assertSafeName(name)
      const migration = await this.loadMigration(name)
      const statements = await migration.getUpSQL()

      for (const sql of statements) {
        await this.db.execute(sql)
      }

      await this.db.execute(
        `INSERT INTO "_migrations" (name, batch) VALUES ('${name.replace(/'/g, "''")}', ${batch})`,
      )

      executed.push(name)
    }

    return executed
  }

  /** Rollback the last batch of migrations. */
  async rollback(): Promise<string[]> {
    await this.init()

    const batchResult = await this.db.query<{ max: number }>('SELECT COALESCE(MAX(batch), 0) as max FROM "_migrations"')
    const batch = batchResult[0]?.max ?? 0

    if (batch === 0) {
      return []
    }

    const toRollback = await this.db.query<MigrationRecord>(
      `SELECT name FROM "_migrations" WHERE batch = ${batch} ORDER BY name DESC`,
    )

    const rolled: string[] = []

    for (const record of toRollback) {
      this.assertSafeName(record.name)
      const migration = await this.loadMigration(record.name)
      const statements = await migration.getDownSQL()

      for (const sql of statements) {
        await this.db.execute(sql)
      }

      await this.db.execute(`DELETE FROM "_migrations" WHERE name = '${record.name.replace(/'/g, "''")}'`)
      rolled.push(record.name)
    }

    return rolled
  }

  /** Discover migration files sorted by name. */
  private discoverFiles(): string[] {
    if (!fs.existsSync(this.migrationsDir)) {
      return []
    }

    return fs.readdirSync(this.migrationsDir)
      .filter((f) => f.endsWith('.ts') || f.endsWith('.js'))
      .sort()
      .map((f) => f.replace(/\.(ts|js)$/, ''))
  }

  /** Validate migration name — must match timestamp_name pattern. */
  private assertSafeName(name: string): void {
    if (/[/\\'";]/.test(name) || name.includes('..')) {
      throw new AtlasError('MIGRATION_INVALID', `Invalid migration name: ${name}`)
    }
  }

  /** Load and instantiate a migration class. */
  private async loadMigration(name: string): Promise<Migration> {
    this.assertSafeName(name)

    const resolved = path.resolve(this.migrationsDir, `${name}.ts`)
    const base = path.resolve(this.migrationsDir)
    if (!resolved.startsWith(base + path.sep) && resolved !== base) {
      throw new AtlasError('MIGRATION_INVALID', `Migration path escapes migrations directory: ${name}`)
    }

    const tsPath = path.join(this.migrationsDir, `${name}.ts`)
    const jsPath = path.join(this.migrationsDir, `${name}.js`)
    const filePath = fs.existsSync(tsPath) ? tsPath : jsPath

    if (!fs.existsSync(filePath)) {
      throw new AtlasError('MIGRATION_NOT_FOUND', `Migration file not found: ${name}`)
    }

    const mod = await import(path.resolve(filePath))
    const MigrationClass = mod.default

    if (!MigrationClass || typeof MigrationClass !== 'function') {
      throw new AtlasError('MIGRATION_INVALID', `Migration ${name} must export a default class`)
    }

    return new MigrationClass(this.driver)
  }
}
