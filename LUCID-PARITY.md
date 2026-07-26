# Atlas ↔ AdonisJS Lucid — parity scope

This is the **authoritative contract** for what `@c9up/atlas` targets against
AdonisJS Lucid. Measure audits against *this document*, not against the whole
Lucid surface — Lucid bundles the ORM **and** its Adonis-framework integration,
and Atlas deliberately splits those.

## The split

Atlas is the **agnostic core ORM/database** layer. The capabilities Lucid
delivers through *Adonis-specific libraries* (VineJS validation, Japa testing,
Adonis health checks) are already provided in the Ream ecosystem by **Ream's own
equivalent packages** — not as VineJS/Japa/Adonis bindings. Same reasoning as the
`knexQuery` deviation: Ream is not Knex/Adonis, so it ships an equivalent surface,
not a binding. Their absence from Atlas — and the absence of any `-adonis`
package — is **not** a parity gap.

| Lucid capability | Provided in Ream by | Note |
| --- | --- | --- |
| Core ORM / database (query builder, models, relations, migrations, transactions, `db` service) | **`@c9up/atlas`** (this package) | Agnostic, no framework dependency. |
| Validation rules (Lucid's `vine.string().unique()/.exists()`) | **`@c9up/rune`** (`.unique(check)` / `.exists(check)` + `validateAsync`) | DB-backed rules take a check callback that queries atlas, so rune stays framework-agnostic (like Lucid's `unique(async (db,value)=>…)`). |
| Test runner (Japa `@japa/runner`) | **`@c9up/helix`** | Vitest-compatible runner + `expect` + DSL + container overrides + time-travel. Ream's Japa equivalent. |
| Testing utilities (`testUtils.db()`, migrate/truncate/seed/wrapInGlobalTransaction, Japa `@japa/api-client`, `dbAssertions`) | **`@c9up/atlas/testing`** (`factory`, `useTransaction`, `truncateAll`, `Database`) + **`@c9up/ream/testing`** (`TestClient`) | Host-specific fakes (HTTP/DB) — helix keeps these OUT of itself by design (dep-light), exactly as Adonis splits `@japa/api-client` + `test_utils` from the Japa runner. |
| Health checks (Lucid's `DbCheck` / `DbConnectionCountCheck`) | **`@c9up/ream`** `HealthCheck` (Kubernetes `/health`) | Ream's own health surface — not `@adonisjs/core/health`. |

**Why:** every package under `packages/` is agnostic and publishable on its own.
Pulling `@adonisjs/core` / `@vinejs/vine` / Japa into Atlas would break that
invariant — and it is unnecessary, because Ream already covers these needs with
its own packages.

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

## Covered by Ream's own packages (not an Atlas gap, not Adonis bindings)

These Lucid capabilities exist in Ream via its own equivalent packages — see the
split table above:

- Health checks → `@c9up/ream` `HealthCheck` (not Adonis `DbCheck`).
- Validation rules → `@c9up/rune` (not VineJS macros).
- Testing utilities → `@c9up/atlas/testing` + `@c9up/ream/testing` (not Japa).

## Genuinely optional / open

- **Full Lucid connection manager surface** (`connect`/`add`/`patch`/`release`,
  `ConnectionNode` state/config/pool, lifecycle events). Atlas keeps a read +
  close view (`connections`/`has`/`get`/`isConnected`/`close`/`closeAll`); the
  full config→connection lifecycle is `AtlasProvider`'s job. Mirroring the exact
  Lucid manager surface is a deliberate open choice, not a required gap.
- **MySQL nested transactions** — see *Known limitation* above.
