import 'reflect-metadata'
import { describe, expect, it } from 'vitest'
import {
  BaseEntity,
  BaseRepository,
  BelongsTo,
  Column,
  Entity,
  HasMany,
  PrimaryKey,
  QueryBuilder,
  RawSql,
  getColumnMetadata,
  getEntityMetadata,
  getPrimaryKey,
  getRelationMetadata,
} from '../../src/index.js'

// === Test entities ===

@Entity('orders')
class Order extends BaseEntity {
  @PrimaryKey() id!: string
  @Column() status!: string
  @Column({ type: 'decimal' }) total!: number
  @Column() createdAt!: string

  @HasMany(() => OrderItem) items!: unknown[]
  @BelongsTo(() => User) user!: unknown

  markAsPaid() {
    this.status = 'paid'
    this.addDomainEvent('order.paid', { orderId: this.id })
  }
}

@Entity('order_items')
class OrderItem extends BaseEntity {
  @PrimaryKey() id!: string
  @Column() orderId!: string
  @Column() productName!: string
  @Column() quantity!: number
}

@Entity('users')
class User extends BaseEntity {
  @PrimaryKey() id!: string
  @Column() name!: string
  @Column() email!: string
}

// === Decorator tests ===

describe('atlas > @Entity decorator', () => {
  it('stores table name metadata', () => {
    const meta = getEntityMetadata(Order)
    expect(meta?.tableName).toBe('orders')
  })

  it('stores columns', () => {
    const columns = getColumnMetadata(Order)
    expect(columns.length).toBeGreaterThanOrEqual(4) // id, status, total, createdAt
    expect(columns.map(c => c.propertyKey)).toContain('status')
    expect(columns.map(c => c.propertyKey)).toContain('total')
  })

  it('stores primary key', () => {
    expect(getPrimaryKey(Order)).toBe('id')
  })

  it('stores relations', () => {
    const relations = getRelationMetadata(Order)
    expect(relations.length).toBe(2)
    expect(relations.find(r => r.propertyKey === 'items')?.type).toBe('hasMany')
    expect(relations.find(r => r.propertyKey === 'user')?.type).toBe('belongsTo')
  })
})

// === BaseEntity tests ===

describe('atlas > BaseEntity domain events', () => {
  it('accumulates domain events', () => {
    const order = new Order()
    order.id = '123'
    order.status = 'pending'
    order.markAsPaid()

    expect(order.hasDomainEvents()).toBe(true)
  })

  it('flushes domain events', () => {
    const order = new Order()
    order.id = '456'
    order.markAsPaid()

    const events = order.flushDomainEvents()
    expect(events.length).toBe(1)
    expect(events[0].name).toBe('order.paid')
    expect(events[0].data.orderId).toBe('456')

    // Events cleared after flush
    expect(order.hasDomainEvents()).toBe(false)
    expect(order.flushDomainEvents().length).toBe(0)
  })
})

// === BaseRepository tests ===

describe('atlas > BaseRepository', () => {
  it('creates query builder for entity table', () => {
    const repo = new BaseRepository(Order)
    const qb = repo.query()
    expect(qb.getTable()).toBe('orders')
  })

  it('reads table name and primary key', () => {
    const repo = new BaseRepository(Order)
    expect(repo.getTableName()).toBe('orders')
    expect(repo.getPrimaryKeyColumn()).toBe('id')
  })

  it('throws if class is not decorated with @Entity', () => {
    class NotAnEntity extends BaseEntity {}
    expect(() => new BaseRepository(NotAnEntity)).toThrow('ATLAS_NOT_ENTITY')
  })

  it('dispatches domain events on save', async () => {
    const dispatched: Array<{ name: string }> = []
    const repo = new BaseRepository(Order)
    repo.onDomainEvents = async (events) => {
      dispatched.push(...events)
    }

    const order = new Order()
    order.id = '789'
    order.markAsPaid()

    await repo.save(order)

    expect(dispatched.length).toBe(1)
    expect(dispatched[0].name).toBe('order.paid')
    // Events flushed after save
    expect(order.hasDomainEvents()).toBe(false)
  })
})

// === QueryBuilder tests ===

describe('atlas > QueryBuilder', () => {
  it('builds basic SELECT', () => {
    const { sql, params } = new QueryBuilder('orders').toSQL()
    expect(sql).toBe('SELECT * FROM "orders"')
    expect(params).toEqual([])
  })

  it('builds WHERE with params', () => {
    const { sql, params } = new QueryBuilder('orders')
      .where('status', 'active')
      .toSQL()
    expect(sql).toBe('SELECT * FROM "orders" WHERE "status" = $1')
    expect(params).toEqual(['active'])
  })

  it('builds WHERE with operator', () => {
    const { sql, params } = new QueryBuilder('orders')
      .where('total', '>', 100)
      .toSQL()
    expect(sql).toBe('SELECT * FROM "orders" WHERE "total" > $1')
    expect(params).toEqual([100])
  })

  it('builds multiple WHERE', () => {
    const { sql } = new QueryBuilder('orders')
      .where('status', 'active')
      .where('total', '>', 100)
      .toSQL()
    expect(sql).toContain('WHERE "status" = $1 AND "total" > $2')
  })

  it('builds OR WHERE', () => {
    const { sql } = new QueryBuilder('orders')
      .where('status', 'active')
      .orWhere('status', 'pending')
      .toSQL()
    expect(sql).toContain('WHERE "status" = $1 OR "status" = $2')
  })

  it('builds WHERE IN', () => {
    const { sql, params } = new QueryBuilder('orders')
      .whereIn('status', ['active', 'pending', 'paid'])
      .toSQL()
    expect(sql).toContain('IN ($1, $2, $3)')
    expect(params).toEqual(['active', 'pending', 'paid'])
  })

  it('builds WHERE NULL / NOT NULL', () => {
    const { sql } = new QueryBuilder('orders')
      .whereNull('deletedAt')
      .whereNotNull('createdAt')
      .toSQL()
    expect(sql).toContain('IS NULL')
    expect(sql).toContain('IS NOT NULL')
  })

  it('builds ORDER BY', () => {
    const { sql } = new QueryBuilder('orders')
      .orderBy('createdAt', 'desc')
      .toSQL()
    expect(sql).toContain('ORDER BY "createdAt" DESC')
  })

  it('builds LIMIT + OFFSET', () => {
    const { sql } = new QueryBuilder('orders')
      .limit(20)
      .offset(40)
      .toSQL()
    expect(sql).toContain('LIMIT 20')
    expect(sql).toContain('OFFSET 40')
  })

  it('builds paginate', () => {
    const { sql } = new QueryBuilder('orders')
      .paginate(3, 10)
      .toSQL()
    expect(sql).toContain('LIMIT 10')
    expect(sql).toContain('OFFSET 20') // (3-1) * 10
  })

  it('builds SELECT specific columns', () => {
    const { sql } = new QueryBuilder('orders')
      .select('id', 'status', 'total')
      .toSQL()
    expect(sql).toContain('SELECT "id", "status", "total"')
  })

  it('tracks preloads', () => {
    const qb = new QueryBuilder('orders')
      .preload('items')
      .preload('user')
    expect(qb.getPreloads()).toEqual(['items', 'user'])
  })

  it('handles empty whereIn as always-false', () => {
    const { sql, params } = new QueryBuilder('orders')
      .whereIn('status', [])
      .toSQL()
    expect(sql).toContain('1 = 0')
    expect(params).toEqual([])
  })

  it('builds NOT IN', () => {
    const { sql, params } = new QueryBuilder('orders')
      .where('status', 'NOT IN', ['cancelled', 'refunded'])
      .toSQL()
    expect(sql).toContain('NOT IN ($1, $2)')
    expect(params).toEqual(['cancelled', 'refunded'])
  })

  it('handles empty NOT IN as always-true', () => {
    const { sql } = new QueryBuilder('orders')
      .where('status', 'NOT IN', [])
      .toSQL()
    expect(sql).toContain('1 = 1')
  })

  it('rejects paginate with page < 1', () => {
    expect(() => new QueryBuilder('orders').paginate(0, 10)).toThrow('ATLAS_INVALID_PAGE')
    expect(() => new QueryBuilder('orders').paginate(-1, 10)).toThrow('ATLAS_INVALID_PAGE')
  })

  it('rejects negative limit', () => {
    expect(() => new QueryBuilder('orders').limit(-1)).toThrow('ATLAS_INVALID_LIMIT')
  })

  it('rejects negative offset', () => {
    expect(() => new QueryBuilder('orders').offset(-1)).toThrow('ATLAS_INVALID_OFFSET')
  })

  it('rejects select with no columns', () => {
    expect(() => new QueryBuilder('orders').select()).toThrow('ATLAS_EMPTY_SELECT')
  })

  it('rejects identifier with double-quote (SQL injection)', () => {
    expect(() => new QueryBuilder('orders"; DROP TABLE orders--').toSQL()).toThrow('ATLAS_INVALID_IDENTIFIER')
  })

  it('quotes identifiers in SQL output', () => {
    const { sql } = new QueryBuilder('orders')
      .where('status', 'active')
      .orderBy('createdAt', 'desc')
      .toSQL()
    expect(sql).toContain('"orders"')
    expect(sql).toContain('"status"')
    expect(sql).toContain('"createdAt"')
  })
})

describe('atlas > BaseEntity > getDomainEvents / clearDomainEvents', () => {
  it('getDomainEvents returns events without clearing', () => {
    const order = new Order()
    order.id = '1'
    order.status = 'pending'
    order.total = 42
    order.createdAt = '2026-03-29'
    order.markAsPaid()

    const events1 = order.getDomainEvents()
    expect(events1).toHaveLength(1)

    // Events still present after getDomainEvents
    const events2 = order.getDomainEvents()
    expect(events2).toHaveLength(1)
    expect(order.hasDomainEvents()).toBe(true)
  })

  it('clearDomainEvents removes all events', () => {
    const order = new Order()
    order.id = '1'
    order.status = 'pending'
    order.total = 42
    order.createdAt = '2026-03-29'
    order.markAsPaid()
    expect(order.hasDomainEvents()).toBe(true)

    order.clearDomainEvents()
    expect(order.hasDomainEvents()).toBe(false)
    expect(order.getDomainEvents()).toHaveLength(0)
  })
})

// === Advanced Query Builder tests (Epic 13) ===

describe('atlas > QueryBuilder advanced', () => {
  it('builds GROUP BY', () => {
    const { sql } = new QueryBuilder('orders')
      .select('status', 'COUNT(*) AS count')
      .groupBy('status')
      .toSQL()
    expect(sql).toContain('GROUP BY "status"')
    expect(sql).toContain('COUNT(*) AS count')
  })

  it('builds GROUP BY with HAVING', () => {
    const { sql, params } = new QueryBuilder('orders')
      .select('status', 'COUNT(*) AS count')
      .groupBy('status')
      .having('COUNT(*)', '>', 5)
      .toSQL()
    expect(sql).toContain('GROUP BY "status"')
    expect(sql).toContain('HAVING COUNT(*) > $1')
    expect(params).toEqual([5])
  })

  it('builds DISTINCT', () => {
    const { sql } = new QueryBuilder('orders')
      .select('status')
      .distinct()
      .toSQL()
    expect(sql).toContain('SELECT DISTINCT "status"')
  })

  it('builds CTE with QueryBuilder', () => {
    const activeOrders = new QueryBuilder('orders').where('status', 'active')
    const { sql, params } = new QueryBuilder('active_orders')
      .with('active_orders', activeOrders)
      .select('*')
      .toSQL()
    expect(sql).toContain('WITH "active_orders" AS (')
    expect(sql).toContain('WHERE "status" = $1')
    expect(sql).toContain('FROM "active_orders"')
    expect(params).toEqual(['active'])
  })

  it('builds CTE with RawSql', () => {
    const raw = new RawSql('SELECT id, total FROM orders WHERE total > $1', [100])
    const { sql, params } = new QueryBuilder('big_orders')
      .with('big_orders', raw)
      .select('*')
      .toSQL()
    expect(sql).toContain('WITH "big_orders" AS (')
    expect(sql).toContain('total > $1')
    expect(params).toEqual([100])
  })

  it('builds multiple CTEs', () => {
    const active = new QueryBuilder('orders').where('status', 'active')
    const paid = new QueryBuilder('orders').where('status', 'paid')
    const { sql, params } = new QueryBuilder('active_orders')
      .with('active_orders', active)
      .with('paid_orders', paid)
      .toSQL()
    expect(sql).toContain('WITH "active_orders" AS (')
    expect(sql).toContain('"paid_orders" AS (')
    expect(params).toEqual(['active', 'paid'])
  })

  it('builds UNION with parentheses', () => {
    const q2 = new QueryBuilder('orders').where('status', 'paid')
    const { sql, params } = new QueryBuilder('orders')
      .where('status', 'pending')
      .union(q2)
      .toSQL()
    expect(sql).toContain('UNION (')
    expect(sql).not.toContain('UNION ALL')
    expect(params).toEqual(['pending', 'paid'])
  })

  it('builds UNION ALL with parentheses', () => {
    const q2 = new QueryBuilder('orders').where('status', 'paid')
    const { sql } = new QueryBuilder('orders')
      .where('status', 'pending')
      .unionAll(q2)
      .toSQL()
    expect(sql).toContain('UNION ALL (')
  })

  it('builds WHERE EXISTS (subquery)', () => {
    const sub = new QueryBuilder('order_items')
      .select('1')
      .where('orderId', 'abc')
    const { sql, params } = new QueryBuilder('orders')
      .whereExists(sub)
      .toSQL()
    expect(sql).toContain('WHERE EXISTS (')
    expect(params).toEqual(['abc'])
  })

  it('builds WHERE + EXISTS combined with correct param indices', () => {
    const sub = new QueryBuilder('order_items')
      .select('1')
      .where('orderId', 'abc')
    const { sql, params } = new QueryBuilder('orders')
      .where('status', 'active')
      .whereExists(sub)
      .toSQL()
    expect(sql).toContain('"status" = $1')
    expect(sql).toContain('AND EXISTS (')
    expect(sql).toContain('"orderId" = $2')
    expect(params).toEqual(['active', 'abc'])
  })

  it('rejects invalid CTE name', () => {
    expect(() => new QueryBuilder('t').with('', new QueryBuilder('x'))).toThrow('ATLAS_INVALID_CTE_NAME')
    expect(() => new QueryBuilder('t').with('has space', new QueryBuilder('x'))).toThrow('ATLAS_INVALID_CTE_NAME')
  })

  it('HAVING handles IS NULL without pushing spurious params', () => {
    const { sql, params } = new QueryBuilder('orders')
      .select('status', 'COUNT(*) AS count')
      .groupBy('status')
      .having('COUNT(*)', '>', 5)
      .toSQL()
    expect(sql).toContain('HAVING COUNT(*) > $1')
    expect(params).toEqual([5])
    // No extra params
    expect(params).toHaveLength(1)
  })
})

describe('atlas > RawSql', () => {
  it('creates raw SQL with params', () => {
    const raw = new RawSql('SELECT * FROM orders WHERE id = $1', ['123'])
    expect(raw.sql).toBe('SELECT * FROM orders WHERE id = $1')
    expect(raw.params).toEqual(['123'])
  })

  it('tagged template auto-parameterizes values', () => {
    const id = '123'
    const status = 'active'
    const raw = RawSql.sql`SELECT * FROM orders WHERE id = ${id} AND status = ${status}`
    expect(raw.sql).toBe('SELECT * FROM orders WHERE id = $1 AND status = $2')
    expect(raw.params).toEqual(['123', 'active'])
  })

  it('tagged template inlines RawSql fragments', () => {
    const table = new RawSql('"orders"')
    const raw = RawSql.sql`SELECT * FROM ${table} WHERE id = ${'123'}`
    expect(raw.sql).toBe('SELECT * FROM "orders" WHERE id = $1')
    expect(raw.params).toEqual(['123'])
  })

  it('tagged template with no params', () => {
    const raw = RawSql.sql`SELECT 1`
    expect(raw.sql).toBe('SELECT 1')
    expect(raw.params).toEqual([])
  })

  it('tagged template re-indexes RawSql fragment params after scalar values', () => {
    const filter = RawSql.sql`total > ${100}`  // sql: "total > $1", params: [100]
    const full = RawSql.sql`SELECT * FROM orders WHERE status = ${'active'} AND ${filter}`
    // status = $1, total > $2 (re-indexed from $1 to $2)
    expect(full.sql).toBe('SELECT * FROM orders WHERE status = $1 AND total > $2')
    expect(full.params).toEqual(['active', 100])
  })

  it('prevents SQL injection through template values', () => {
    const malicious = "'; DROP TABLE orders; --"
    const raw = RawSql.sql`SELECT * FROM orders WHERE name = ${malicious}`
    // Value is parameterized, not interpolated
    expect(raw.sql).toBe('SELECT * FROM orders WHERE name = $1')
    expect(raw.params).toEqual([malicious])
    expect(raw.sql).not.toContain('DROP')
  })
})

describe('atlas > BaseRepository > domain event safety', () => {
  it('preserves events if onDomainEvents throws', async () => {
    const repo = new BaseRepository(Order)
    repo.onDomainEvents = async () => { throw new Error('dispatch failed') }

    const order = new Order()
    order.id = '1'
    order.status = 'pending'
    order.total = 42
    order.createdAt = '2026-03-29'
    order.markAsPaid()

    await expect(repo.save(order)).rejects.toThrow('dispatch failed')
    // Events should still be on the entity since dispatch failed
    expect(order.hasDomainEvents()).toBe(true)
    expect(order.getDomainEvents()).toHaveLength(1)
  })
})
