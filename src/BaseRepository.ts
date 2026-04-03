/**
 * BaseRepository — Data Mapper with real CRUD via database adapter.
 *
 * Like AdonisJS Lucid but Data Mapper pattern:
 * - find(id), findBy(column, value), findOrFail(id)
 * - all(), query() for fluent queries
 * - create(data), save(entity), delete(entity)
 * - Domain events flushed after save
 *
 * @implements FR29, FR31, FR35
 */

import type { BaseEntity, DomainEvent } from './BaseEntity.js'
import { getEntityMetadata, getColumnMetadata, getPrimaryKey } from './decorators/entity.js'
import { AtlasError } from './errors.js'
import { ModelQuery } from './ModelQuery.js'
import { QueryBuilder } from './query/QueryBuilder.js'

type EntityConstructor<T extends BaseEntity> = new () => T

export interface DatabaseConnection {
  prepare(sql: string): {
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint }
    get(...params: unknown[]): unknown
    all(...params: unknown[]): unknown[]
  }
}

/**
 * Base repository — provides typed CRUD operations backed by a real DB connection.
 */
export class BaseRepository<T extends BaseEntity> {
  protected entityClass: EntityConstructor<T>
  protected tableName: string
  protected primaryKey: string
  protected columns: string[]
  protected db: DatabaseConnection

  /** Callback to dispatch domain events (set by framework integration). */
  onDomainEvents?: (events: DomainEvent[]) => Promise<void>

  constructor(entityClass: EntityConstructor<T>, db: DatabaseConnection) {
    this.entityClass = entityClass
    this.db = db
    const meta = getEntityMetadata(entityClass)
    if (!meta) {
      throw new AtlasError('NOT_ENTITY', `Class '${entityClass.name}' is not decorated with @Entity()`, {
        hint: 'Add @Entity(\'table_name\') decorator to the class.',
      })
    }
    this.tableName = meta.tableName
    this.primaryKey = getPrimaryKey(entityClass) ?? 'id'
    this.columns = getColumnMetadata(entityClass).map((c) => c.propertyKey)
  }

  /**
   * Create an executable query builder for this entity.
   *
   * Usage:
   *   repo.query().where('status', 'active').orderBy('created_at', 'desc').limit(10).exec()
   *   repo.query().where('email', email).first()
   */
  query(): ModelQuery<T> {
    return new ModelQuery<T>(this.tableName, this.db, (row) => this.hydrate(row))
  }

  // ─── Finders ──────────────────────────────────────────────

  /** Find by primary key. Returns null if not found. */
  find(id: string | number): T | null {
    const row = this.db.prepare(
      `SELECT * FROM "${this.tableName}" WHERE "${this.primaryKey}" = ?`
    ).get(id) as Record<string, unknown> | undefined
    if (!row) return null
    return this.hydrate(row)
  }

  /** Find by primary key or throw. */
  findOrFail(id: string | number): T {
    const entity = this.find(id)
    if (!entity) {
      throw new AtlasError('ROW_NOT_FOUND', `${this.entityClass.name} with ${this.primaryKey}='${id}' not found`)
    }
    return entity
  }

  /** Find by a column value. */
  findBy(column: string, value: unknown): T | null {
    const row = this.db.prepare(
      `SELECT * FROM "${this.tableName}" WHERE "${column}" = ?`
    ).get(value) as Record<string, unknown> | undefined
    if (!row) return null
    return this.hydrate(row)
  }

  /** Get all rows. */
  all(): T[] {
    const rows = this.db.prepare(`SELECT * FROM "${this.tableName}"`).all() as Record<string, unknown>[]
    return rows.map((row) => this.hydrate(row))
  }

  /** Get rows matching a where clause. */
  where(column: string, value: unknown): T[] {
    const rows = this.db.prepare(
      `SELECT * FROM "${this.tableName}" WHERE "${column}" = ? ORDER BY rowid DESC`
    ).all(value) as Record<string, unknown>[]
    return rows.map((row) => this.hydrate(row))
  }

  // ─── Create / Save / Delete ───────────────────────────────

  /** Create a new entity from data and persist it. */
  create(data: Partial<Record<string, unknown>>): T {
    const entity = new this.entityClass()
    for (const [key, value] of Object.entries(data)) {
      (entity as Record<string, unknown>)[key] = value
    }
    this.insert(entity)
    return entity
  }

  /** Save (insert or update) an entity. Dispatches domain events. */
  async save(entity: T): Promise<void> {
    const pk = (entity as Record<string, unknown>)[this.primaryKey]
    if (pk && this.find(pk as string)) {
      this.update(entity)
    } else {
      this.insert(entity)
    }

    // Dispatch domain events after commit
    const events = entity.getDomainEvents()
    if (events.length > 0 && this.onDomainEvents) {
      await this.onDomainEvents([...events])
    }
    entity.clearDomainEvents()
  }

  /** Delete an entity. */
  delete(entity: T): void {
    const pk = (entity as Record<string, unknown>)[this.primaryKey]
    this.db.prepare(`DELETE FROM "${this.tableName}" WHERE "${this.primaryKey}" = ?`).run(pk)
  }

  /** Update specific columns on an entity by ID. */
  updateById(id: string | number, data: Partial<Record<string, unknown>>): void {
    const snakeData: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(data)) {
      snakeData[camelToSnake(key)] = value
    }
    const sets = Object.keys(snakeData).map((c) => `"${c}" = ?`).join(', ')
    this.db.prepare(`UPDATE "${this.tableName}" SET ${sets} WHERE "${this.primaryKey}" = ?`)
      .run(...Object.values(snakeData), id)
  }

  /** Update specific columns on rows matching a condition. */
  updateWhere(column: string, columnValue: unknown, data: Partial<Record<string, unknown>>): void {
    const snakeData: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(data)) {
      snakeData[camelToSnake(key)] = value
    }
    const sets = Object.keys(snakeData).map((c) => `"${c}" = ?`).join(', ')
    this.db.prepare(`UPDATE "${this.tableName}" SET ${sets} WHERE "${column}" = ?`)
      .run(...Object.values(snakeData), columnValue)
  }

  // ─── Raw query ────────────────────────────────────────────

  /** Execute a raw SQL query and return hydrated entities. */
  raw(sql: string, ...params: unknown[]): T[] {
    const rows = this.db.prepare(sql).all(...params) as Record<string, unknown>[]
    return rows.map((row) => this.hydrate(row))
  }

  /** Execute a raw SQL query and return raw rows. */
  rawRows<R = Record<string, unknown>>(sql: string, ...params: unknown[]): R[] {
    return this.db.prepare(sql).all(...params) as R[]
  }

  // ─── Internals ────────────────────────────────────────────

  private insert(entity: T): void {
    const data = this.entityToRow(entity)
    const cols = Object.keys(data)
    const placeholders = cols.map(() => '?').join(', ')
    const sql = `INSERT INTO "${this.tableName}" (${cols.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`
    this.db.prepare(sql).run(...Object.values(data))
  }

  private update(entity: T): void {
    const data = this.entityToRow(entity)
    const pk = data[this.primaryKey]
    delete data[this.primaryKey]
    const sets = Object.keys(data).map((c) => `"${c}" = ?`).join(', ')
    const sql = `UPDATE "${this.tableName}" SET ${sets} WHERE "${this.primaryKey}" = ?`
    this.db.prepare(sql).run(...Object.values(data), pk)
  }

  /** Convert a DB row (snake_case) to an entity instance. */
  private hydrate(row: Record<string, unknown>): T {
    const entity = new this.entityClass()
    for (const [key, value] of Object.entries(row)) {
      const camelKey = snakeToCamel(key)
      if (camelKey in entity || key in entity) {
        (entity as Record<string, unknown>)[camelKey in entity ? camelKey : key] = value
      }
    }
    return entity
  }

  /** Convert an entity to a DB row (snake_case). */
  private entityToRow(entity: T): Record<string, unknown> {
    const row: Record<string, unknown> = {}
    const columnMeta = getColumnMetadata(this.entityClass)
    for (const col of columnMeta) {
      const value = (entity as Record<string, unknown>)[col.propertyKey]
      if (value !== undefined) {
        row[camelToSnake(col.propertyKey)] = value
      }
    }
    // Include primary key
    const pkValue = (entity as Record<string, unknown>)[this.primaryKey]
    if (pkValue !== undefined) {
      row[this.primaryKey] = pkValue
    }
    return row
  }

  getTableName(): string { return this.tableName }
  getPrimaryKeyColumn(): string { return this.primaryKey }
}

function snakeToCamel(s: string): string {
  return s.replace(/_([a-z])/g, (_, c) => c.toUpperCase())
}

function camelToSnake(s: string): string {
  return s.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`)
}
