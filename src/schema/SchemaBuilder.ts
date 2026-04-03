/**
 * Schema Builder — fluent API for creating and modifying database tables.
 * Dialect-agnostic: works with PostgreSQL, SQLite, MySQL, MariaDB.
 *
 * @implements FR34
 */

import { getDialect } from '../dialects/Dialect.js'
import type { Dialect } from '../dialects/Dialect.js'

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

export interface IndexDefinition {
  name: string
  columns: string[]
  unique: boolean
}

/**
 * Table builder — used inside schema.createTable() callback.
 */
export class TableBuilder {
  readonly tableName: string
  private columns: ColumnDefinition[] = []
  private indexes: IndexDefinition[] = []
  private currentColumn?: ColumnDefinition

  constructor(tableName: string) {
    this.tableName = tableName
  }

  // ─── Column types ─────────────────────────────────────────

  uuid(name: string): this { return this.addColumn(name, 'uuid') }

  string(name: string, length = 255): this {
    this.addColumn(name, 'string')
    this.currentColumn!.length = length
    return this
  }

  text(name: string): this { return this.addColumn(name, 'text') }
  integer(name: string): this { return this.addColumn(name, 'integer') }
  bigInteger(name: string): this { return this.addColumn(name, 'bigInteger') }

  decimal(name: string, precision = 10, scale = 2): this {
    this.addColumn(name, 'decimal')
    this.currentColumn!.precision = precision
    this.currentColumn!.scale = scale
    return this
  }

  boolean(name: string): this { return this.addColumn(name, 'boolean') }
  date(name: string): this { return this.addColumn(name, 'date') }
  timestamp(name: string): this { return this.addColumn(name, 'timestamp') }
  json(name: string): this { return this.addColumn(name, 'json') }
  binary(name: string): this { return this.addColumn(name, 'binary') }

  // ─── Shortcuts ────────────────────────────────────────────

  id(): this {
    return this.uuid('id').primary().defaultTo('gen_random_uuid()')
  }

  timestamps(): this {
    this.timestamp('created_at').notNullable().defaultTo('NOW()')
    this.timestamp('updated_at').notNullable().defaultTo('NOW()')
    return this
  }

  // ─── Column modifiers ─────────────────────────────────────

  primary(): this {
    if (this.currentColumn) this.currentColumn.primary = true
    return this
  }

  notNullable(): this {
    if (this.currentColumn) this.currentColumn.nullable = false
    return this
  }

  nullable(): this {
    if (this.currentColumn) this.currentColumn.nullable = true
    return this
  }

  unique(): this {
    if (this.currentColumn) this.currentColumn.unique = true
    return this
  }

  defaultTo(value: string): this {
    if (this.currentColumn) this.currentColumn.defaultValue = value
    return this
  }

  references(table: string, column = 'id'): this {
    if (this.currentColumn) this.currentColumn.references = { table, column }
    return this
  }

  // ─── Indexes ──────────────────────────────────────────────

  index(columns: string | string[], name?: string): this {
    const cols = Array.isArray(columns) ? columns : [columns]
    this.indexes.push({ name: name ?? `idx_${this.tableName}_${cols.join('_')}`, columns: cols, unique: false })
    return this
  }

  uniqueIndex(columns: string | string[], name?: string): this {
    const cols = Array.isArray(columns) ? columns : [columns]
    this.indexes.push({ name: name ?? `idx_${this.tableName}_${cols.join('_')}_unique`, columns: cols, unique: true })
    return this
  }

  // ─── SQL generation ───────────────────────────────────────

  getColumns(): ColumnDefinition[] { return [...this.columns] }
  getIndexes(): IndexDefinition[] { return [...this.indexes] }

  /** Generate SQL statements for a given dialect. */
  toStatements(dialect: Dialect): string[] {
    const id = (name: string) => dialect.wrapIdentifier(name)

    const colDefs = this.columns.map((col) => {
      const parts: string[] = [id(col.name)]
      parts.push(dialect.mapColumnType(col.type, { length: col.length, precision: col.precision, scale: col.scale }))
      if (col.primary) parts.push('PRIMARY KEY')
      if (!col.nullable) parts.push('NOT NULL')
      if (col.unique) parts.push('UNIQUE')
      if (col.defaultValue) parts.push(dialect.wrapDefaultValue(col.defaultValue))
      if (col.references) parts.push(`REFERENCES ${id(col.references.table)}(${id(col.references.column)})`)
      return `  ${parts.join(' ')}`
    })

    const stmts = [`CREATE TABLE ${id(this.tableName)} (\n${colDefs.join(',\n')}\n);`]
    for (const idx of this.indexes) {
      const u = idx.unique ? 'UNIQUE ' : ''
      stmts.push(`CREATE ${u}INDEX ${id(idx.name)} ON ${id(this.tableName)} (${idx.columns.map(c => id(c)).join(', ')});`)
    }
    return stmts
  }

  /** @deprecated Use toStatements(dialect). */
  toPostgresSQL(): string { return this.toStatements(getDialect('postgres')).join('\n') }
  /** @deprecated Use toStatements(dialect). */
  toSqliteSQL(): string { return this.toStatements(getDialect('sqlite')).join('\n') }

  private addColumn(name: string, type: ColumnType): this {
    const col: ColumnDefinition = { name, type, nullable: true, primary: false, unique: false }
    this.columns.push(col)
    this.currentColumn = col
    return this
  }
}

/**
 * Schema — top-level API for DDL operations.
 */
export class Schema {
  private dialect: Dialect
  private statements: string[] = []

  constructor(driver: string = 'postgres') {
    this.dialect = getDialect(driver)
  }

  createTable(name: string, callback: (table: TableBuilder) => void): this {
    const builder = new TableBuilder(name)
    callback(builder)
    this.statements.push(...builder.toStatements(this.dialect))
    return this
  }

  dropTable(name: string): this {
    this.statements.push(`DROP TABLE IF EXISTS ${this.dialect.wrapIdentifier(name)};`)
    return this
  }

  createIndex(table: string, columns: string | string[], name?: string, unique = false): this {
    const id = (n: string) => this.dialect.wrapIdentifier(n)
    const cols = Array.isArray(columns) ? columns : [columns]
    const indexName = name ?? `idx_${table}_${cols.join('_')}`
    const u = unique ? 'UNIQUE ' : ''
    this.statements.push(`CREATE ${u}INDEX ${id(indexName)} ON ${id(table)} (${cols.map(c => id(c)).join(', ')});`)
    return this
  }

  dropIndex(name: string): this {
    this.statements.push(`DROP INDEX IF EXISTS ${this.dialect.wrapIdentifier(name)};`)
    return this
  }

  raw(sql: string): this {
    this.statements.push(sql)
    return this
  }

  reset(): void {
    this.statements = []
  }

  toSQL(): string[] {
    return [...this.statements]
  }
}
