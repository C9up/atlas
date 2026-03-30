# @c9up/atlas

Data Mapper ORM for Node.js. Entity decorators, fluent QueryBuilder, domain events.

## Usage

```typescript
import { Entity, Column, PrimaryKey, BaseEntity, QueryBuilder } from '@c9up/atlas'

@Entity('orders')
class Order extends BaseEntity {
  @PrimaryKey() id!: string
  @Column() status!: string
  @Column({ type: 'decimal' }) total!: number
}

const { sql, params } = new QueryBuilder('orders')
  .where('status', 'active')
  .orderBy('createdAt', 'desc')
  .paginate(1, 20)
  .toSQL()
```

## Features

- `@Entity`, `@Column`, `@PrimaryKey`, `@BelongsTo`, `@HasMany`, `@ManyToMany`
- Fluent QueryBuilder with parameterized SQL (injection-safe)
- CTE (`.with()`), UNION, WHERE EXISTS, GROUP BY, HAVING, DISTINCT
- `RawSql` tagged template for edge cases
- Domain events accumulated on entities, dispatched after save
- BaseRepository with query builder access

## License

MIT
