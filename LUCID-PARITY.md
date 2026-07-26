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
`connection(name, {mode})`/`truncate`/`truncateAllTables`/advisory locks), and
the full `db.manager` connection manager (`connections` node map,
`add`/`connect`/`patch` [background-reconnect]/`release`/`close(name, release?)`/
`closeAll(release?)`, `ConnectionNode` with the full `registered`/`open`/
`migrating`/`closing`/`closed` states). The `db:query` and connection lifecycle
events (`db:connection:connect`/`disconnect`/`error` with `[error, node]`) are
bridged onto the app emitter.

## Named deviations — KEPT on purpose (not parity gaps)

| Lucid | Atlas | Why |
| --- | --- | --- |
| `knexQuery()` / `knexRawQuery()` | absent | No Knex backend — the compiler is Rust; there is no Knex builder to hand back. |
| `rawQuery()` → dialect driver result (`result.rows` on pg, tuple on mysql) | returns `T[]` | The Rust/NAPI boundary yields rows only (no `rowCount`/`fields` metadata); a normalized `T[]` is the deliberate cross-dialect shape. |
| `timeout(ms, { cancel: true })` — out-of-band `KILL QUERY` by connection PID | server-side statement cancel is **implemented** (Postgres `statement_timeout`; MySQL `max_execution_time` for SELECT), applied on a dedicated pooled connection and reset before release; SQLite has no server timeout (client race applies). Knex's separate-connection `KILL <pid>` variant is not reproduced. | sqlx exposes no backend PID / out-of-band cancel handle, so the Knex "kill from another connection" path is unavailable. The `statement_timeout`/`max_execution_time` route delivers the same effect (the server stops executing at the deadline) for pool queries. Inside an interactive transaction the pinned client falls back to the client-side race only. |
| `TableBuilder` `comment`/`collate`/`specificType` | column vs table `comment()`/`tableComment()`, `collate()`/`tableCollate()`, restricted `specificType()` | Security/quality choices kept over literal source parity. |
| Lazy connection open (Lucid registers configs at boot, opens each pool on first use) | registers every config at boot (`manager.add`) AND eagerly **connects** each pool | Deliberate **fail-fast**: a misconfigured/unreachable DB fails at deploy time, not on the first request (better prod safety). Lazy open is available on demand via `db.manager.add(name, config)` + `await db.manager.connect(name)`. |

## Covered by Ream's own packages (not an Atlas gap, not Adonis bindings)

These Lucid capabilities exist in Ream via its own equivalent packages — see the
split table above:

- Health checks → `@c9up/ream` `HealthCheck` (not Adonis `DbCheck`).
- Validation rules → `@c9up/rune` (not VineJS macros).
- Testing utilities → `@c9up/atlas/testing` + `@c9up/ream/testing` (not Japa).

## Nothing open

The core ORM/database surface, transactions (incl. MySQL nested SAVEPOINTs), and
the full connection manager are all in place. What remains are the *named
deviations* above — each forced by the Rust/NAPI backend or a deliberate
security/quality choice, none a missing capability.
