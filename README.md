# @c9up/atlas

AdonisJS Lucid–style ORM for Node.js — Active Record **models** *and* a Data Mapper **repository**, entity decorators, a fluent QueryBuilder, and domain events.

## Active Record (AdonisJS Lucid parity)

Extend `BaseModel` for the Lucid-style Active Record API: static finders/creators
plus `instance.save()` / `instance.delete()`. `@Entity('table')` is **optional** —
the table name is inferred from the class name (`User` → `users`).

```typescript
import { BaseModel, Column, PrimaryKey } from '@c9up/atlas'

class User extends BaseModel {
  @PrimaryKey() declare id: number
  @Column() declare email: string
  @Column() declare role: string
}

const user  = await User.find(1)
const admin = await User.findBy({ role: 'admin' })   // object clause
const fresh = await User.create({ email: 'a@b.co', role: 'user' })

if (user) {
  user.email = 'new@b.co'
  await user.save()                    // UPDATE only the dirty columns
  console.log(user.$isPersisted, user.$isDirty())
}

// bulk, keyed by a unique column, in one transaction
await User.updateOrCreateMany('email', rows)

// per-instance serialization visibility
user?.makeHidden('role').toJSON()
```

Static surface: `find`, `findOrFail`, `findBy`, `findByOrFail`, `findMany`,
`findManyBy`, `all`, `first`, `firstOrFail`, `query`, `create`, `createMany`,
`firstOrCreate`, `firstOrNew`, `updateOrCreate`, `updateOrCreateMany`,
`fetchOrCreateMany`, `fetchOrNewUpMany`, `truncate`. Config: `static table`,
`static connection`.

## Data Mapper

Prefer separation of persistence from the model? Use `BaseRepository` directly —
same surface, with the connection injected.

```typescript
import { BaseRepository, Entity, Column, PrimaryKey, BaseEntity } from '@c9up/atlas'

@Entity('orders')
class Order extends BaseEntity {
  @PrimaryKey({ generated: 'uuid' }) declare id: string
  @Column() declare status: string
  @Column({ type: 'decimal' }) declare total: number
}

const repo   = new BaseRepository(Order, db)
const active = await repo.query().where('status', 'active').exec()
```

## Features

- **Two styles** — Active Record (`BaseModel`) and Data Mapper (`BaseRepository`), same repository underneath
- Zero-config conventions: table + primary key inferred, overridable via `static table` / `@Entity` / `@PrimaryKey`
- Model state: `$isPersisted`, `$isNew`, `$isLocal`, `$isDeleted`, `$dirty` / `isDirty()`, `$primaryKeyValue`
- Mass-assignment protection (`static fillable` / `guarded`) enforced on `create` / `createMany` / `updateOrCreate`
- Serialization: `static hidden`/`visible`, per-instance `makeHidden`/`makeVisible`, `@Column({ serializeAs, serialize })`, `@computed`
- `@Entity`, `@Column`, `@PrimaryKey`, `@BelongsTo`, `@HasMany`, `@ManyToMany` + relation preloading
- Fluent QueryBuilder with parameterized SQL (injection-safe): CTE (`.with()`), UNION, WHERE EXISTS, GROUP BY, HAVING, DISTINCT
- `RawSql` tagged template; domain events accumulated on entities, dispatched after save
- Interactive transactions, soft deletes, multi-dialect (sqlite / postgres / mysql)

## License

MIT
