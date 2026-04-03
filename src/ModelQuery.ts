/**
 * ModelQuery — executable query builder for repositories.
 *
 * Like AdonisJS Lucid Model.query():
 *   repo.query().where('status', 'active').orderBy('created_at', 'desc').limit(10).exec()
 *
 * Builds SQL fluently and executes against the database connection.
 */

import type { BaseEntity } from './BaseEntity.js'
import type { DatabaseConnection } from './BaseRepository.js'

export class ModelQuery<T extends BaseEntity> {
  private tableName: string
  private db: DatabaseConnection
  private hydrateFn: (row: Record<string, unknown>) => T
  private joins: Array<{ type: string; table: string; left: string; right: string }> = []
  private wheres: Array<{ column: string; operator: string; value: unknown }> = []
  private orWheres: Array<{ column: string; operator: string; value: unknown }> = []
  private orderBys: Array<{ column: string; direction: 'asc' | 'desc' }> = []
  private _select?: string
  private _limit?: number
  private _offset?: number

  constructor(tableName: string, db: DatabaseConnection, hydrateFn: (row: Record<string, unknown>) => T) {
    this.tableName = tableName
    this.db = db
    this.hydrateFn = hydrateFn
  }

  /** Select specific columns (default: table.*). */
  select(columns: string): this {
    this._select = columns
    return this
  }

  /** INNER JOIN another table. */
  innerJoin(table: string, left: string, right: string): this {
    this.joins.push({ type: 'INNER JOIN', table, left, right })
    return this
  }

  /** LEFT JOIN another table. */
  leftJoin(table: string, left: string, right: string): this {
    this.joins.push({ type: 'LEFT JOIN', table, left, right })
    return this
  }

  where(column: string, value: unknown): this
  where(column: string, operator: string, value: unknown): this
  where(column: string, operatorOrValue: unknown, value?: unknown): this {
    if (value === undefined) {
      this.wheres.push({ column, operator: '=', value: operatorOrValue })
    } else {
      this.wheres.push({ column, operator: operatorOrValue as string, value })
    }
    return this
  }

  orWhere(column: string, value: unknown): this
  orWhere(column: string, operator: string, value: unknown): this
  orWhere(column: string, operatorOrValue: unknown, value?: unknown): this {
    if (value === undefined) {
      this.orWheres.push({ column, operator: '=', value: operatorOrValue })
    } else {
      this.orWheres.push({ column, operator: operatorOrValue as string, value })
    }
    return this
  }

  whereNull(column: string): this {
    this.wheres.push({ column, operator: 'IS', value: null })
    return this
  }

  whereNotNull(column: string): this {
    this.wheres.push({ column, operator: 'IS NOT', value: null })
    return this
  }

  orderBy(column: string, direction: 'asc' | 'desc' = 'asc'): this {
    this.orderBys.push({ column, direction })
    return this
  }

  limit(n: number): this {
    this._limit = n
    return this
  }

  offset(n: number): this {
    this._offset = n
    return this
  }

  /** Execute and return all matching entities. */
  exec(): T[] {
    const { sql, params } = this.toSQL()
    const rows = this.db.prepare(sql).all(...params) as Record<string, unknown>[]
    return rows.map((row) => this.hydrateFn(row))
  }

  /** Execute and return the first matching entity or null. */
  first(): T | null {
    this._limit = 1
    const results = this.exec()
    return results[0] ?? null
  }

  /** Execute and return the first matching entity or throw. */
  firstOrFail(): T {
    const result = this.first()
    if (!result) throw new Error(`No ${this.tableName} found matching query`)
    return result
  }

  /** Build SQL + params. */
  toSQL(): { sql: string; params: unknown[] } {
    const selectCols = this._select ?? `"${this.tableName}".*`
    const parts: string[] = [`SELECT ${selectCols} FROM "${this.tableName}"`]
    const params: unknown[] = []

    for (const j of this.joins) {
      parts.push(`${j.type} "${j.table}" ON ${j.left} = ${j.right}`)
    }

    if (this.wheres.length > 0 || this.orWheres.length > 0) {
      const conditions: string[] = []
      for (const w of this.wheres) {
        if (w.value === null) {
          conditions.push(`"${w.column}" ${w.operator} NULL`)
        } else {
          conditions.push(`"${w.column}" ${w.operator} ?`)
          params.push(w.value)
        }
      }
      for (const w of this.orWheres) {
        if (w.value === null) {
          conditions.push(`OR "${w.column}" ${w.operator} NULL`)
        } else {
          conditions.push(`OR "${w.column}" ${w.operator} ?`)
          params.push(w.value)
        }
      }
      parts.push(`WHERE ${conditions.join(' AND ').replace(/AND OR /g, 'OR ')}`)
    }

    if (this.orderBys.length > 0) {
      parts.push(`ORDER BY ${this.orderBys.map(o => `"${o.column}" ${o.direction.toUpperCase()}`).join(', ')}`)
    }

    if (this._limit !== undefined) {
      parts.push(`LIMIT ${this._limit}`)
    }
    if (this._offset !== undefined) {
      parts.push(`OFFSET ${this._offset}`)
    }

    return { sql: parts.join(' '), params }
  }
}
