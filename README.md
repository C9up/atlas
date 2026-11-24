# @c9up/atlas

An ORM for Node.js — Active Record **models** *and* a Data Mapper **repository**, entity decorators, a fluent QueryBuilder, and domain events.

> **Scope:** Atlas is the **agnostic core ORM/database** layer and nothing more.
> Everything around it comes from Ream's own packages — validation `@c9up/rune`,
> testing `@c9up/atlas/testing` + `@c9up/ream/testing`, health `@c9up/ream`
> `HealthCheck`. Atlas depends on none of them, and on no third-party framework.

## Installation

```bash
pnpm add @c9up/atlas
```

Atlas compiles queries and talks to the database through two Rust binaries: the query compiler (`index.<platform>.node`) and the database driver (`db.<platform>.node`). Both binaries of a platform ship in one npm package, which `@c9up/atlas` lists as an optional dependency at its exact version:

| Platform | Package |
|---|---|
| Linux x64 (glibc) | `@c9up/atlas-linux-x64-gnu` |
| Linux arm64 (glibc) | `@c9up/atlas-linux-arm64-gnu` |
| macOS x64 | `@c9up/atlas-darwin-x64` |
| macOS arm64 | `@c9up/atlas-darwin-arm64` |
| Windows x64 | `@c9up/atlas-win32-x64-msvc` |

Your package manager installs only the one that matches the machine (`os`, `cpu` and `libc` fields), so an install downloads one platform's binaries and needs no Rust toolchain. An install run with optional dependencies omitted (`--no-optional`, `--omit=optional`) skips it.

There is no JavaScript fallback. Without the package, the first compiled query throws `E_ATLAS_NAPI_NOT_FOUND` and the first connection throws `E_ATLAS_DB_DRIVER_NOT_FOUND`. Both messages name the package to install, for example `@c9up/atlas-linux-x64-gnu`; the load error itself is the exception's `cause`.

In a checkout of this repository, `pnpm build:napi` builds `index.<platform>.node` and `db.<platform>.node` at the package root, and atlas loads those files before looking for the platform package. `node scripts/platform-packages.mjs` rewrites the platform packages' manifests (`npm/<platform>/package.json`) and the optional dependencies after a version bump; CI fails when they are stale.

## Active Record

Extend `BaseModel` for the Active Record API: static finders/creators
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
  console.log(user.$isPersisted, user.$isDirty)
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
- Repository scopes: `new BaseRepository(Entity, db, { scope: (q, table) => … })` filters the repository's queries and every query its relations run (preloads, pivot and intermediate tables, `whereHas`, `load()` / `related()`) — the place for a tenant filter
- PostgreSQL `LISTEN` / `NOTIFY`: `const stop = await db.listen('jobs', (payload) => …)` on a connection of its own, reconnected and re-subscribed when it drops

## License

MIT
