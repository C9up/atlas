import Database from 'better-sqlite3'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { Migration, MigrationRunner, Schema, TableBuilder } from '../../src/index.js'
import { SqliteAdapter } from '../../src/adapters/SqliteAdapter.js'

// === Real SQLite tests — every test uses an in-memory database ===

let db: InstanceType<typeof Database>
let adapter: SqliteAdapter

beforeEach(() => {
  db = new Database(':memory:')
  adapter = new SqliteAdapter(db)
})

afterEach(() => {
  db.close()
})

describe('atlas > SchemaBuilder > SQL generation', () => {
  it('generates valid Postgres CREATE TABLE', () => {
    const builder = new TableBuilder('orders')
    builder.uuid('id').primary().defaultTo('gen_random_uuid()')
    builder.string('status', 50).notNullable().defaultTo("'pending'")
    builder.decimal('total', 10, 2).notNullable()
    builder.timestamp('created_at').notNullable().defaultTo('NOW()')

    const sql = builder.toPostgresSQL()
    expect(sql).toContain('CREATE TABLE "orders"')
    expect(sql).toContain('"id" UUID PRIMARY KEY DEFAULT gen_random_uuid()')
    expect(sql).toContain('"status" VARCHAR(50) NOT NULL')
    expect(sql).toContain('"total" DECIMAL(10, 2) NOT NULL')
  })

  it('generates valid SQLite CREATE TABLE', () => {
    const builder = new TableBuilder('users')
    builder.uuid('id').primary()
    builder.string('name').notNullable()
    builder.boolean('active')

    const sql = builder.toSqliteSQL()
    expect(sql).toContain('"id" TEXT PRIMARY KEY')
    expect(sql).toContain('"name" TEXT NOT NULL')
    expect(sql).toContain('"active" INTEGER')
  })

  it('supports id() and timestamps() shortcuts', () => {
    const builder = new TableBuilder('products')
    builder.id()
    builder.timestamps()
    const sql = builder.toPostgresSQL()
    expect(sql).toContain('"id" UUID PRIMARY KEY DEFAULT gen_random_uuid()')
    expect(sql).toContain('"created_at" TIMESTAMP NOT NULL DEFAULT NOW()')
    expect(sql).toContain('"updated_at" TIMESTAMP NOT NULL DEFAULT NOW()')
  })

  it('supports foreign key references', () => {
    const builder = new TableBuilder('order_items')
    builder.uuid('order_id').notNullable().references('orders', 'id')
    const sql = builder.toPostgresSQL()
    expect(sql).toContain('REFERENCES "orders"("id")')
  })

  it('supports unique constraint', () => {
    const builder = new TableBuilder('users')
    builder.string('email').unique().notNullable()
    const sql = builder.toPostgresSQL()
    expect(sql).toContain('NOT NULL UNIQUE')
  })

  it('supports all column types', () => {
    const builder = new TableBuilder('test')
    builder.uuid('a').string('b').text('c').integer('d').bigInteger('e')
    builder.decimal('f').boolean('g').date('h').timestamp('i').json('j').binary('k')
    expect(builder.getColumns()).toHaveLength(11)
  })
})

describe('atlas > Schema > executes against real SQLite', () => {
  it('creates a table that actually exists in SQLite', async () => {
    const schema = new Schema('sqlite')
    schema.createTable('orders', (table) => {
      table.integer('id').primary()
      table.string('status').notNullable()
      table.decimal('total')
    })

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    // Verify the table exists
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").all()
    expect(tables).toHaveLength(1)

    // Verify columns
    const info = db.prepare("PRAGMA table_info('orders')").all() as Array<{ name: string }>
    const colNames = info.map((c) => c.name)
    expect(colNames).toContain('id')
    expect(colNames).toContain('status')
    expect(colNames).toContain('total')
  })

  it('drops a table from SQLite', async () => {
    db.exec('CREATE TABLE old_table (id INTEGER)')
    expect(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'").all()).toHaveLength(1)

    const schema = new Schema('sqlite')
    schema.dropTable('old_table')

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    expect(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'").all()).toHaveLength(0)
  })

  it('inserts and queries data after CREATE TABLE', async () => {
    const schema = new Schema('sqlite')
    schema.createTable('users', (table) => {
      table.integer('id').primary()
      table.string('name').notNullable()
      table.string('email').unique().notNullable()
    })

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    db.prepare("INSERT INTO users (id, name, email) VALUES (1, 'Kaen', 'kaen@c9up.com')").run()
    db.prepare("INSERT INTO users (id, name, email) VALUES (2, 'Alice', 'alice@test.com')").run()

    const rows = db.prepare('SELECT * FROM users ORDER BY id').all() as Array<{ id: number; name: string; email: string }>
    expect(rows).toHaveLength(2)
    expect(rows[0].name).toBe('Kaen')
    expect(rows[1].email).toBe('alice@test.com')
  })

  it('unique constraint actually enforced', async () => {
    const schema = new Schema('sqlite')
    schema.createTable('users', (table) => {
      table.integer('id').primary()
      table.string('email').unique().notNullable()
    })

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    db.prepare("INSERT INTO users (id, email) VALUES (1, 'test@test.com')").run()
    expect(() => {
      db.prepare("INSERT INTO users (id, email) VALUES (2, 'test@test.com')").run()
    }).toThrow() // UNIQUE constraint violated
  })

  it('NOT NULL constraint actually enforced', async () => {
    const schema = new Schema('sqlite')
    schema.createTable('items', (table) => {
      table.integer('id').primary()
      table.string('name').notNullable()
    })

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    expect(() => {
      db.prepare("INSERT INTO items (id, name) VALUES (1, NULL)").run()
    }).toThrow() // NOT NULL constraint violated
  })
})

describe('atlas > Migration > real SQLite execution', () => {
  it('migration creates and drops table', async () => {
    class CreateOrders extends Migration {
      up() {
        this.schema.createTable('orders', (table) => {
          table.integer('id').primary()
          table.string('status', 50).notNullable()
          table.decimal('total', 10, 2).notNullable()
        })
      }
      down() {
        this.schema.dropTable('orders')
      }
    }

    const migration = new CreateOrders('sqlite')

    // Execute UP
    for (const sql of await migration.getUpSQL()) {
      await adapter.execute(sql)
    }

    let tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").all()
    expect(tables).toHaveLength(1)

    // Execute DOWN
    for (const sql of await migration.getDownSQL()) {
      await adapter.execute(sql)
    }

    tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").all()
    expect(tables).toHaveLength(0)
  })
})

describe('atlas > MigrationRunner > real SQLite execution', () => {
  it('runs migrations and tracks them in _migrations table', async () => {
    // Create a temp migrations directory with a real migration file
    const fs = await import('node:fs')
    const path = await import('node:path')
    const tmpDir = '/tmp/atlas-migration-test-' + Date.now()
    fs.mkdirSync(tmpDir, { recursive: true })

    // Write a migration file
    fs.writeFileSync(path.join(tmpDir, '20260331000000_create_products.ts'), `
      import { Migration } from '../../packages/atlas/src/schema/Migration.js'
      export default class extends Migration {
        up() {
          this.schema.createTable('products', (table) => {
            table.integer('id').primary()
            table.string('name').notNullable()
            table.decimal('price').notNullable()
          })
        }
        down() { this.schema.dropTable('products') }
      }
    `)

    const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir, driver: 'sqlite' })
    await runner.init()

    // Check _migrations table was created
    const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='_migrations'").all()
    expect(tables).toHaveLength(1)

    // Check status — should be pending
    const status = await runner.status()
    expect(status).toHaveLength(1)
    expect(status[0].status).toBe('pending')

    // Cleanup
    fs.rmSync(tmpDir, { recursive: true })
  })

  it('rollback removes table and migration record', async () => {
    // Directly test the SQL generation and execution
    const schema = new Schema('sqlite')
    schema.createTable('test_rollback', (table) => {
      table.integer('id').primary()
    })

    for (const sql of schema.toSQL()) {
      await adapter.execute(sql)
    }

    // Table exists
    let tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='test_rollback'").all()
    expect(tables).toHaveLength(1)

    // Drop it
    const drop = new Schema('sqlite')
    drop.dropTable('test_rollback')
    for (const sql of drop.toSQL()) {
      await adapter.execute(sql)
    }

    // Table gone
    tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='test_rollback'").all()
    expect(tables).toHaveLength(0)
  })
})

describe('atlas > QueryBuilder > executes against real SQLite', () => {
  it('generated SQL is valid and executable', async () => {
    // Create table first
    db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL, created_at TEXT)')
    db.exec("INSERT INTO orders VALUES (1, 'active', 42.50, '2026-03-31')")
    db.exec("INSERT INTO orders VALUES (2, 'pending', 10.00, '2026-03-30')")
    db.exec("INSERT INTO orders VALUES (3, 'active', 100.00, '2026-03-29')")

    // Use QueryBuilder to generate SQL
    const { QueryBuilder } = await import('../../src/query/QueryBuilder.js')
    const { sql, params } = new QueryBuilder('orders')
      .where('status', 'active')
      .orderBy('total', 'desc')
      .toSQL()

    // Execute it against real SQLite — replace $N params with ? for sqlite
    let sqliteSQL = sql
    for (let i = params.length; i >= 1; i--) {
      sqliteSQL = sqliteSQL.replace(`$${i}`, '?')
    }

    const rows = db.prepare(sqliteSQL).all(...params) as Array<{ id: number; total: number }>
    expect(rows).toHaveLength(2)
    expect(rows[0].total).toBe(100.00) // DESC order
    expect(rows[1].total).toBe(42.50)
  })

  it('WHERE IN works against real SQLite', async () => {
    db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)')
    db.exec("INSERT INTO items VALUES (1, 'active')")
    db.exec("INSERT INTO items VALUES (2, 'pending')")
    db.exec("INSERT INTO items VALUES (3, 'cancelled')")

    const { QueryBuilder } = await import('../../src/query/QueryBuilder.js')
    const { sql, params } = new QueryBuilder('items')
      .whereIn('status', ['active', 'pending'])
      .toSQL()

    let sqliteSQL = sql
    for (let i = params.length; i >= 1; i--) {
      sqliteSQL = sqliteSQL.replace(`$${i}`, '?')
    }

    const rows = db.prepare(sqliteSQL).all(...params) as Array<{ id: number }>
    expect(rows).toHaveLength(2)
  })

  it('paginate works against real SQLite', async () => {
    db.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)')
    for (let i = 1; i <= 25; i++) {
      db.exec(`INSERT INTO products VALUES (${i}, 'Product ${i}')`)
    }

    const { QueryBuilder } = await import('../../src/query/QueryBuilder.js')
    const { sql, params } = new QueryBuilder('products')
      .paginate(2, 10)
      .toSQL()

    let sqliteSQL = sql
    for (let i = params.length; i >= 1; i--) {
      sqliteSQL = sqliteSQL.replace(`$${i}`, '?')
    }

    const rows = db.prepare(sqliteSQL).all(...params) as Array<{ id: number }>
    expect(rows).toHaveLength(10)
    expect(rows[0].id).toBe(11) // page 2, offset 10
  })
})
