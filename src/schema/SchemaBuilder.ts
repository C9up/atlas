/**
 * Schema Builder — fluent API for creating and modifying database tables.
 *
 * @implements FR34
 */

import { AtlasError } from '../errors.js'

export type ColumnType = 'string' | 'text' | 'integer' | 'bigInteger' | 'decimal' | 'boolean' | 'date' | 'timestamp' | 'uuid' | 'json' | 'binary'

export interface ColumnDefinition {
  name: string
  type: ColumnType
  length?: number
  precision?: number
  scale?: number
  nullable: boolean
  primary: boolean
  unique: boolean
  defaultValue?: string
  references?: { table: string; column: string }
}

/**
 * Table builder — used inside schema.createTable() callback.
 */
export class TableBuilder {
  readonly tableName: string
  private columns: ColumnDefinition[] = []
  private currentColumn?: ColumnDefinition

  constructor(tableName: string) {
    this.tableName = tableName
  }

  /** UUID column. */
  uuid(name: string): this {
    return this.addColumn(name, 'uuid')
  }

  /** String column (VARCHAR). */
  string(name: string, length = 255): this {
    this.addColumn(name, 'string')
    this.currentColumn!.length = length
    return this
  }

  /** Text column (unlimited length). */
  text(name: string): this {
    return this.addColumn(name, 'text')
  }

  /** Integer column. */
  integer(name: string): this {
    return this.addColumn(name, 'integer')
  }

  /** Big integer column. */
  bigInteger(name: string): this {
    return this.addColumn(name, 'bigInteger')
  }

  /** Decimal column. */
  decimal(name: string, precision = 10, scale = 2): this {
    this.addColumn(name, 'decimal')
    this.currentColumn!.precision = precision
    this.currentColumn!.scale = scale
    return this
  }

  /** Boolean column. */
  boolean(name: string): this {
    return this.addColumn(name, 'boolean')
  }

  /** Date column (date only, no time). */
  date(name: string): this {
    return this.addColumn(name, 'date')
  }

  /** Timestamp column. */
  timestamp(name: string): this {
    return this.addColumn(name, 'timestamp')
  }

  /** JSON column. */
  json(name: string): this {
    return this.addColumn(name, 'json')
  }

  /** Binary column. */
  binary(name: string): this {
    return this.addColumn(name, 'binary')
  }

  /** Shortcut: id column (UUID primary key with default). */
  id(): this {
    return this.uuid('id').primary().defaultTo('gen_random_uuid()')
  }

  /** Shortcut: created_at + updated_at timestamps. */
  timestamps(): this {
    this.timestamp('created_at').notNullable().defaultTo('NOW()')
    this.timestamp('updated_at').notNullable().defaultTo('NOW()')
    return this
  }

  // --- Column modifiers ---

  /** Mark as primary key. */
  primary(): this {
    if (this.currentColumn) this.currentColumn.primary = true
    return this
  }

  /** Mark as NOT NULL. */
  notNullable(): this {
    if (this.currentColumn) this.currentColumn.nullable = false
    return this
  }

  /** Mark as nullable (default). */
  nullable(): this {
    if (this.currentColumn) this.currentColumn.nullable = true
    return this
  }

  /** Mark as UNIQUE. */
  unique(): this {
    if (this.currentColumn) this.currentColumn.unique = true
    return this
  }

  /** Set default value (raw SQL expression). */
  defaultTo(value: string): this {
    if (this.currentColumn) this.currentColumn.defaultValue = value
    return this
  }

  /** Add foreign key reference. */
  references(table: string, column = 'id'): this {
    if (this.currentColumn) this.currentColumn.references = { table, column }
    return this
  }

  // --- SQL generation ---

  /** Get all column definitions. */
  getColumns(): ColumnDefinition[] {
    return [...this.columns]
  }

  /** Generate CREATE TABLE SQL for PostgreSQL. */
  toPostgresSQL(): string {
    const cols = this.columns.map((col) => {
      const parts: string[] = [`"${col.name}"`]

      // Type mapping
      switch (col.type) {
        case 'uuid': parts.push('UUID'); break
        case 'string': parts.push(`VARCHAR(${col.length ?? 255})`); break
        case 'text': parts.push('TEXT'); break
        case 'integer': parts.push('INTEGER'); break
        case 'bigInteger': parts.push('BIGINT'); break
        case 'decimal': parts.push(`DECIMAL(${col.precision ?? 10}, ${col.scale ?? 2})`); break
        case 'boolean': parts.push('BOOLEAN'); break
        case 'date': parts.push('DATE'); break
        case 'timestamp': parts.push('TIMESTAMP'); break
        case 'json': parts.push('JSONB'); break
        case 'binary': parts.push('BYTEA'); break
      }

      if (col.primary) parts.push('PRIMARY KEY')
      if (!col.nullable) parts.push('NOT NULL')
      if (col.unique) parts.push('UNIQUE')
      if (col.defaultValue) parts.push(`DEFAULT ${col.defaultValue}`)
      if (col.references) parts.push(`REFERENCES "${col.references.table}"("${col.references.column}")`)

      return `  ${parts.join(' ')}`
    })

    return `CREATE TABLE "${this.tableName}" (\n${cols.join(',\n')}\n);`
  }

  /** Generate CREATE TABLE SQL for SQLite. */
  toSqliteSQL(): string {
    const cols = this.columns.map((col) => {
      const parts: string[] = [`"${col.name}"`]

      switch (col.type) {
        case 'uuid': parts.push('TEXT'); break
        case 'string': parts.push('TEXT'); break
        case 'text': parts.push('TEXT'); break
        case 'integer': parts.push('INTEGER'); break
        case 'bigInteger': parts.push('INTEGER'); break
        case 'decimal': parts.push('REAL'); break
        case 'boolean': parts.push('INTEGER'); break
        case 'date': parts.push('TEXT'); break
        case 'timestamp': parts.push('TEXT'); break
        case 'json': parts.push('TEXT'); break
        case 'binary': parts.push('BLOB'); break
      }

      if (col.primary) parts.push('PRIMARY KEY')
      if (!col.nullable) parts.push('NOT NULL')
      if (col.unique) parts.push('UNIQUE')
      if (col.defaultValue) parts.push(`DEFAULT (${col.defaultValue})`)

      return `  ${parts.join(' ')}`
    })

    return `CREATE TABLE "${this.tableName}" (\n${cols.join(',\n')}\n);`
  }

  private addColumn(name: string, type: ColumnType): this {
    const col: ColumnDefinition = {
      name,
      type,
      nullable: true,
      primary: false,
      unique: false,
    }
    this.columns.push(col)
    this.currentColumn = col
    return this
  }
}

/**
 * Schema — top-level API for DDL operations.
 */
export class Schema {
  private driver: 'postgres' | 'sqlite'
  private statements: string[] = []

  constructor(driver: 'postgres' | 'sqlite' = 'postgres') {
    this.driver = driver
  }

  /** Create a new table. */
  createTable(name: string, callback: (table: TableBuilder) => void): this {
    const builder = new TableBuilder(name)
    callback(builder)
    this.statements.push(
      this.driver === 'postgres' ? builder.toPostgresSQL() : builder.toSqliteSQL(),
    )
    return this
  }

  /** Drop a table. */
  dropTable(name: string): this {
    this.statements.push(`DROP TABLE IF EXISTS "${name}";`)
    return this
  }

  /** Raw SQL statement. */
  raw(sql: string): this {
    this.statements.push(sql)
    return this
  }

  /** Reset statements (used by Migration before generating up/down SQL). */
  reset(): void {
    this.statements = []
  }

  /** Get all generated SQL statements. */
  toSQL(): string[] {
    return [...this.statements]
  }
}
