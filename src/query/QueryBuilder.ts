/**
 * Fluent Query Builder — with advanced features (CTE, subqueries, unions, window functions).
 *
 * @implements FR31, FR32, FR33, FR37
 */

export type WhereOperator = '=' | '!=' | '>' | '>=' | '<' | '<=' | 'LIKE' | 'IN' | 'NOT IN' | 'IS NULL' | 'IS NOT NULL'

export interface WhereClause {
  column: string
  operator: WhereOperator
  value: unknown
  type: 'and' | 'or'
}

export interface ExistsClause {
  kind: 'exists'
  subquery: QueryBuilder
  type: 'and' | 'or'
}

export interface OrderByClause {
  column: string
  direction: 'asc' | 'desc'
}

export interface CteDefinition {
  name: string
  query: QueryBuilder | RawSql
}

export interface QueryResult<T> {
  data: T[]
  total?: number
  page?: number
  perPage?: number
}

/** Typed raw SQL with parameterized values. */
export class RawSql {
  readonly sql: string
  readonly params: unknown[]

  constructor(sql: string, params: unknown[] = []) {
    this.sql = sql
    this.params = params
  }

  /** Tagged template literal for raw SQL with automatic parameterization. */
  static sql(strings: TemplateStringsArray, ...values: unknown[]): RawSql {
    let sql = ''
    const params: unknown[] = []
    let paramIndex = 1

    for (let i = 0; i < strings.length; i++) {
      sql += strings[i]
      if (i < values.length) {
        if (values[i] instanceof RawSql) {
          // Inline raw SQL fragments — re-index their $N placeholders
          const raw = values[i] as RawSql
          let remapped = raw.sql
          for (let j = raw.params.length; j >= 1; j--) {
            remapped = remapped.replace(new RegExp(`\\$${j}(?!\\d)`, 'g'), `$${paramIndex + j - 1}`)
          }
          sql += remapped
          params.push(...raw.params)
          paramIndex += raw.params.length
        } else {
          sql += `$${paramIndex++}`
          params.push(values[i])
        }
      }
    }

    return new RawSql(sql, params)
  }
}

/** Strict identifier validation — only allows column/table names. */
const STRICT_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_.]*$/

/** Quote a SQL identifier to prevent injection. Strict mode for WHERE/ORDER BY/GROUP BY. */
function quoteIdentifier(name: string): string {
  if (name === '*') return name
  if (/["\0]/.test(name)) {
    throw new Error(`[ATLAS_INVALID_IDENTIFIER] Identifier contains illegal characters: ${name}`)
  }
  return `"${name}"`
}

/** Quote for SELECT — allows expressions (aggregates, window functions, aliases). */
function quoteSelectExpr(name: string): string {
  if (name === '*') return name
  // Allow expressions containing parentheses or AS aliases
  if (name.includes('(') || /\s+[Aa][Ss]\s+/.test(name)) return name
  if (/["\0]/.test(name)) {
    throw new Error(`[ATLAS_INVALID_IDENTIFIER] Identifier contains illegal characters: ${name}`)
  }
  return `"${name}"`
}

/** Quote for HAVING — allows aggregate expressions. */
function quoteHavingExpr(name: string): string {
  if (name.includes('(')) return name
  if (/["\0]/.test(name)) {
    throw new Error(`[ATLAS_INVALID_IDENTIFIER] Identifier contains illegal characters: ${name}`)
  }
  return `"${name}"`
}

/** Validate a strict identifier (no expressions allowed). */
function validateStrictIdentifier(name: string, context: string): void {
  if (!STRICT_IDENTIFIER_RE.test(name)) {
    throw new Error(`[ATLAS_INVALID_IDENTIFIER] Invalid ${context} identifier: '${name}'. Only letters, numbers, underscores, and dots are allowed.`)
  }
}

/**
 * Fluent query builder — type-safe, composable, with advanced features.
 */
export class QueryBuilder<T = Record<string, unknown>> {
  private _table: string
  private _select: string[] = ['*']
  private _where: Array<WhereClause | ExistsClause> = []
  private _orderBy: OrderByClause[] = []
  private _groupBy: string[] = []
  private _having: WhereClause[] = []
  private _limit?: number
  private _offset?: number
  private _preload: string[] = []
  private _ctes: CteDefinition[] = []
  private _unions: Array<{ query: QueryBuilder; all: boolean }> = []
  private _distinct = false

  constructor(table: string) {
    this._table = table
  }

  /** SELECT DISTINCT. */
  distinct(): this {
    this._distinct = true
    return this
  }

  /** Select specific columns (supports expressions like 'COUNT(*) AS total'). */
  select(...columns: string[]): this {
    if (columns.length === 0) {
      throw new Error('[ATLAS_EMPTY_SELECT] select() requires at least one column')
    }
    this._select = columns
    return this
  }

  /** Add a WHERE condition. */
  where(column: string, operatorOrValue: WhereOperator | unknown, value?: unknown): this {
    if (value === undefined) {
      this._where.push({ column, operator: '=', value: operatorOrValue, type: 'and' })
    } else {
      this._where.push({ column, operator: operatorOrValue as WhereOperator, value, type: 'and' })
    }
    return this
  }

  /** Add an OR WHERE condition. */
  orWhere(column: string, operatorOrValue: WhereOperator | unknown, value?: unknown): this {
    if (value === undefined) {
      this._where.push({ column, operator: '=', value: operatorOrValue, type: 'or' })
    } else {
      this._where.push({ column, operator: operatorOrValue as WhereOperator, value, type: 'or' })
    }
    return this
  }

  /** WHERE column IN (values). */
  whereIn(column: string, values: unknown[]): this {
    this._where.push({ column, operator: 'IN', value: values, type: 'and' })
    return this
  }

  /** WHERE column IS NULL. */
  whereNull(column: string): this {
    this._where.push({ column, operator: 'IS NULL', value: null, type: 'and' })
    return this
  }

  /** WHERE column IS NOT NULL. */
  whereNotNull(column: string): this {
    this._where.push({ column, operator: 'IS NOT NULL', value: null, type: 'and' })
    return this
  }

  /** WHERE EXISTS (subquery). Subquery is deferred — toSQL() called at build time. */
  whereExists(subquery: QueryBuilder): this {
    this._where.push({ kind: 'exists', subquery, type: 'and' })
    return this
  }

  /** GROUP BY columns. */
  groupBy(...columns: string[]): this {
    this._groupBy.push(...columns)
    return this
  }

  /** HAVING condition (used after GROUP BY). Supports aggregate expressions. */
  having(column: string, operatorOrValue: WhereOperator | unknown, value?: unknown): this {
    if (value === undefined) {
      this._having.push({ column, operator: '=', value: operatorOrValue, type: 'and' })
    } else {
      this._having.push({ column, operator: operatorOrValue as WhereOperator, value, type: 'and' })
    }
    return this
  }

  /** ORDER BY column. */
  orderBy(column: string, direction: 'asc' | 'desc' = 'asc'): this {
    this._orderBy.push({ column, direction })
    return this
  }

  /** LIMIT results. */
  limit(n: number): this {
    if (n < 0) {
      throw new Error('[ATLAS_INVALID_LIMIT] limit must be >= 0')
    }
    this._limit = n
    return this
  }

  /** OFFSET results. */
  offset(n: number): this {
    if (n < 0) {
      throw new Error('[ATLAS_INVALID_OFFSET] offset must be >= 0')
    }
    this._offset = n
    return this
  }

  /** Paginate results. */
  paginate(page: number, perPage = 20): this {
    if (page < 1) {
      throw new Error('[ATLAS_INVALID_PAGE] page must be >= 1')
    }
    if (perPage < 1) {
      throw new Error('[ATLAS_INVALID_PER_PAGE] perPage must be >= 1')
    }
    this._limit = perPage
    this._offset = (page - 1) * perPage
    return this
  }

  /** Eager-load a relation. */
  preload(relation: string): this {
    this._preload.push(relation)
    return this
  }

  /** WITH (Common Table Expression). */
  with(name: string, query: QueryBuilder | RawSql): this {
    if (!name || !STRICT_IDENTIFIER_RE.test(name)) {
      throw new Error(`[ATLAS_INVALID_CTE_NAME] CTE name must be a valid identifier: '${name}'`)
    }
    this._ctes.push({ name, query })
    return this
  }

  /** UNION with another query (wrapped in parentheses). */
  union(query: QueryBuilder): this {
    this._unions.push({ query, all: false })
    return this
  }

  /** UNION ALL with another query (wrapped in parentheses). */
  unionAll(query: QueryBuilder): this {
    this._unions.push({ query, all: true })
    return this
  }

  /** Build the SQL query string. */
  toSQL(): { sql: string; params: unknown[] } {
    const params: unknown[] = []
    let paramIndex = 1

    // Helper to remap $N placeholders from a sub-query
    const remapParams = (subSql: string, subParams: unknown[]): string => {
      let remapped = subSql
      for (let i = subParams.length; i >= 1; i--) {
        remapped = remapped.replace(new RegExp(`\\$${i}(?!\\d)`, 'g'), `$${paramIndex + i - 1}`)
      }
      params.push(...subParams)
      paramIndex += subParams.length
      return remapped
    }

    let sql = ''

    // CTEs
    if (this._ctes.length > 0) {
      const ctes = this._ctes.map((cte) => {
        if (cte.query instanceof RawSql) {
          const remapped = remapParams(cte.query.sql, cte.query.params)
          return `${quoteIdentifier(cte.name)} AS (${remapped})`
        }
        const sub = cte.query.toSQL()
        const remapped = remapParams(sub.sql, sub.params)
        return `${quoteIdentifier(cte.name)} AS (${remapped})`
      })
      sql += `WITH ${ctes.join(', ')} `
    }

    // SELECT
    const selectCols = this._select.map((c) => quoteSelectExpr(c)).join(', ')
    sql += `SELECT ${this._distinct ? 'DISTINCT ' : ''}${selectCols} FROM ${quoteIdentifier(this._table)}`

    // WHERE
    if (this._where.length > 0) {
      const clauses: string[] = []
      for (let i = 0; i < this._where.length; i++) {
        const w = this._where[i]
        const prefix = i === 0 ? 'WHERE' : w.type === 'or' ? 'OR' : 'AND'

        // EXISTS clause — deferred subquery
        if ('kind' in w && w.kind === 'exists') {
          const sub = w.subquery.toSQL()
          const remapped = remapParams(sub.sql, sub.params)
          clauses.push(`${prefix} EXISTS (${remapped})`)
          continue
        }

        const wc = w as WhereClause
        const col = quoteIdentifier(wc.column)
        if (wc.operator === 'IS NULL') { clauses.push(`${prefix} ${col} IS NULL`); continue }
        if (wc.operator === 'IS NOT NULL') { clauses.push(`${prefix} ${col} IS NOT NULL`); continue }
        if (wc.operator === 'IN' || wc.operator === 'NOT IN') {
          const arr = wc.value as unknown[]
          if (!Array.isArray(arr)) {
            throw new Error(`[ATLAS_INVALID_IN] ${wc.operator} requires an array value`)
          }
          if (arr.length === 0) {
            clauses.push(wc.operator === 'IN' ? `${prefix} 1 = 0` : `${prefix} 1 = 1`)
            continue
          }
          const placeholders = arr.map(() => `$${paramIndex++}`).join(', ')
          params.push(...arr)
          clauses.push(`${prefix} ${col} ${wc.operator} (${placeholders})`)
          continue
        }
        params.push(wc.value)
        clauses.push(`${prefix} ${col} ${wc.operator} $${paramIndex++}`)
      }
      sql += ` ${clauses.join(' ')}`
    }

    // GROUP BY
    if (this._groupBy.length > 0) {
      sql += ` GROUP BY ${this._groupBy.map((c) => quoteIdentifier(c)).join(', ')}`
    }

    // HAVING — same operator handling as WHERE
    if (this._having.length > 0) {
      const havingClauses: string[] = []
      for (let i = 0; i < this._having.length; i++) {
        const w = this._having[i]
        const prefix = i === 0 ? 'HAVING' : 'AND'
        const col = quoteHavingExpr(w.column)
        if (w.operator === 'IS NULL') { havingClauses.push(`${prefix} ${col} IS NULL`); continue }
        if (w.operator === 'IS NOT NULL') { havingClauses.push(`${prefix} ${col} IS NOT NULL`); continue }
        params.push(w.value)
        havingClauses.push(`${prefix} ${col} ${w.operator} $${paramIndex++}`)
      }
      sql += ` ${havingClauses.join(' ')}`
    }

    // ORDER BY
    if (this._orderBy.length > 0) {
      sql += ` ORDER BY ${this._orderBy.map((o) => `${quoteIdentifier(o.column)} ${o.direction.toUpperCase()}`).join(', ')}`
    }

    // LIMIT / OFFSET
    if (this._limit !== undefined) {
      sql += ` LIMIT ${this._limit}`
    }
    if (this._offset !== undefined) {
      sql += ` OFFSET ${this._offset}`
    }

    // UNIONS (wrapped in parentheses)
    if (this._unions.length > 0) {
      for (const u of this._unions) {
        const sub = u.query.toSQL()
        const remapped = remapParams(sub.sql, sub.params)
        sql += u.all ? ` UNION ALL (${remapped})` : ` UNION (${remapped})`
      }
    }

    return { sql, params }
  }

  /** Get the table name. */
  getTable(): string {
    return this._table
  }

  /** Get preloaded relations. */
  getPreloads(): string[] {
    return [...this._preload]
  }
}
