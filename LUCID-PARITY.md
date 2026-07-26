# Atlas ↔ AdonisJS Lucid — parity scope

This is the **authoritative contract** for what `@c9up/atlas` targets against
AdonisJS Lucid. Measure audits against *this document*, not against the whole
Lucid surface — Lucid bundles the ORM **and** its Adonis-framework integration,
and Atlas deliberately splits those.

## The split

| Layer | Package | Scope |
| --- | --- | --- |
| **Core ORM / database** | `@c9up/atlas` (this package) | Agnostic. Tracks the Lucid **query builder, models, relations, migrations, transactions, and the `db` service** as closely as possible with the Rust/NAPI backend. Depends on **no** framework. |
| **Adonis/Lucid compatibility** | separate integration package (e.g. `@c9up/atlas-adonis`) | Full Adonis surface: health checks, VineJS rules, Japa/testUtils, and — if we choose to mirror the Lucid surface exactly — the complete connection manager. May depend on `@adonisjs/core`, `@vinejs/vine`, `@japa/*`. |

**Why the split:** every package under `packages/` is agnostic and publishable
on its own. Pulling `@adonisjs/core` / `@vinejs/vine` / Japa into Atlas would
break that invariant. Those live in the integration layer by design — their
absence from Atlas is **not** a parity gap.

## In scope for Atlas (covered)

Query builder (`select`/`from`/joins + `on*` family/CTEs/unions/aggregates,
the full `where*` surface incl. `whereNot` forms, `whereIn` tuple/subquery,
`whereJson`/`whereJsonPath`/`whereJsonSuperset`/`whereJsonSubset` + and/or/not
variants, `andWhere*` aliases, dialect-aware `ifDialect`/`unlessDialect`),
inspection (`toSQL()` → `?`-normalized, `.toNative()`, `toQuery()` interpolated,
`debug`/`reporterData`), lazy chainable DML (`insert`/`update`/`delete` with
`onConflict`/`merge`/`returning`/`timeout`/`comment`), models + relations
(has-one/has-many/belongs-to/many-to-many, `preload`, `pivotColumns([...])`,
`wherePivot`/`whereNotPivot` with operator forms, `groupLimit`/`groupOrderBy`),
transactions (managed/manual, nested `trx.transaction()` SAVEPOINT, `after()`
hooks, `trx.on()`/`once()`/`off()` EventEmitter), migrations, and the `db`
service (`query`/`from`/`table`/`insertQuery`/`rawQuery`/`raw`/`ref`/`modelQuery`/
`connection(name, {mode})`/`truncate`/`truncateAllTables`/advisory locks/
`db.manager` read+close surface). The `db:query` event is bridged onto the
app emitter.

## Named deviations — KEPT on purpose (not parity gaps)

| Lucid | Atlas | Why |
| --- | --- | --- |
| `knexQuery()` / `knexRawQuery()` | absent | No Knex backend — the compiler is Rust; there is no Knex builder to hand back. |
| `rawQuery()` → dialect driver result (`result.rows` on pg, tuple on mysql) | returns `T[]` | The Rust/NAPI boundary yields rows only (no `rowCount`/`fields` metadata); a normalized `T[]` is the deliberate cross-dialect shape. |
| `timeout(ms, { cancel: true })` server-side cancel | signature accepted, awaiter rejects on timeout | Server-side statement cancellation is a Rust/NAPI runtime limitation. |
| `TableBuilder` `comment`/`collate`/`specificType` | column vs table `comment()`/`tableComment()`, `collate()`/`tableCollate()`, restricted `specificType()` | Security/quality choices kept over literal source parity. |

## Known limitation

- **MySQL nested transactions (SAVEPOINT):** MySQL rejects `SAVEPOINT` /
  `RELEASE` / `ROLLBACK TO` over the prepared-statement protocol the driver uses
  (error 1295). `trx.transaction()` works on SQLite and PostgreSQL. Lifting this
  needs a text-protocol (`sqlx::raw_sql`) exec path in the NAPI layer — a real
  Rust chantier, tracked, not yet done.

## Belongs to the integration package (out of Atlas core)

- Adonis health checks (`DbCheck`, `DbConnectionCountCheck`).
- VineJS validation rules (`vine.string().unique()` / `.exists()`).
- Japa testing utilities (`testUtils.db()`, the `dbAssertions` plugin,
  `migrate`/`truncate`/`seed`/`wrapInGlobalTransaction`).
- The full Lucid **connection manager** surface (`connect`/`add`/`patch`/
  `release`, `ConnectionNode` state/config/pool, lifecycle events) — Atlas keeps
  a read + close view (`connections`/`has`/`get`/`isConnected`/`close`/
  `closeAll`); owning config→connection lifecycle is the provider/integration
  layer's job.
