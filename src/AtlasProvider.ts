import type { AppContext } from '@c9up/ream'
import { SqliteAdapter } from './adapters/SqliteAdapter.js'

export interface AtlasDatabaseConfig {
  client: 'sqlite' | 'postgres'
  connection: {
    filename?: string
  }
  migrations?: {
    path?: string
  }
}

export default class AtlasProvider {
  private db?: import('better-sqlite3').Database

  constructor(protected app: AppContext) {}

  register() {}

  async boot() {
    const config = this.app.config.get<AtlasDatabaseConfig>('database')
    if (!config) return

    if (config.client === 'sqlite') {
      const Database = (await import('better-sqlite3')).default
      const filename = config.connection.filename ?? ':memory:'

      if (filename !== ':memory:') {
        const { mkdirSync } = await import('node:fs')
        const { dirname } = await import('node:path')
        mkdirSync(dirname(filename), { recursive: true })
      }

      this.db = new Database(filename)
      this.db.pragma('journal_mode = WAL')
      this.db.pragma('foreign_keys = ON')

      const adapter = new SqliteAdapter(this.db)
      this.app.container.singleton('db', () => this.db)
      this.app.container.singleton('db.adapter', () => adapter)
      this.app.container.singleton(SqliteAdapter, () => adapter)

      if (config.migrations?.path) {
        await this.runMigrations(this.db, config.migrations.path)
      }
    }
  }

  async shutdown() {
    this.db?.close()
  }

  async start() {}
  async ready() {}

  private async runMigrations(db: import('better-sqlite3').Database, migrationsPath: string): Promise<void> {
    const { readdirSync, existsSync } = await import('node:fs')
    const { join } = await import('node:path')

    if (!existsSync(migrationsPath)) return

    db.exec(`CREATE TABLE IF NOT EXISTS "_migrations" ("name" TEXT PRIMARY KEY, "executed_at" TEXT NOT NULL DEFAULT (datetime('now')));`)

    const files = readdirSync(migrationsPath)
      .filter((f: string) => f.endsWith('.ts') || f.endsWith('.js'))
      .sort()

    for (const file of files) {
      const name = file.replace(/\.(ts|js)$/, '')
      if (db.prepare('SELECT name FROM _migrations WHERE name = ?').get(name)) continue

      const mod = await import(join(migrationsPath, file))
      const migration = new mod.default('sqlite')
      for (const sql of await migration.getUpSQL()) db.exec(sql)
      db.prepare('INSERT INTO _migrations (name) VALUES (?)').run(name)
    }
  }
}
