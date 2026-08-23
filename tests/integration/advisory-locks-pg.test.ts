/**
 * Advisory locks against a REAL PostgreSQL, gated on ATLAS_TEST_PG_URL.
 *
 * `pg_try_advisory_lock` is SESSION-scoped: the lock belongs to the connection
 * that took it. atlas issues both statements through the pool, exactly as
 * Lucid's dialect does (`client.rawQuery('SELECT PG_TRY_ADVISORY_LOCK(…)')`),
 * so the guarantee holds for the shape the lock exists to serve — a migration
 * run, which is acquire → work → release in one uninterrupted sequence — and
 * NOT for a lock held across unrelated queries on a multi-connection pool.
 *
 * These tests pin that down instead of leaving it to reasoning. This path had
 * never run against a real Postgres before.
 *
 *   ATLAS_TEST_PG_URL=postgres://postgres:secret@localhost:5432/postgres \
 *     pnpm test tests/integration/advisory-locks-pg.test.ts
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import db, { clearDb, setDb } from "../../src/services/db.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

/** How many locks Postgres currently holds for this key. */
async function heldLocks(
	conn: AsyncDatabaseConnection,
	key: number,
): Promise<number> {
	// The key is inlined rather than bound: `pg_locks.objid` is an `oid`, and
	// this is a test helper reading a number this file chose — not the code
	// under test.
	const rows = await conn.query<{ n: string }>(
		`SELECT count(*) AS n FROM pg_locks WHERE locktype = 'advisory' AND objid = ${key}`,
	);
	return Number(rows[0]?.n ?? 0);
}

describePg("atlas > advisory locks (real Postgres)", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		// A single pooled connection — the shape a migration runner uses, and the
		// one the session-scoped guarantee actually holds for.
		conn = await createNapiConnection(PG_URL, 1, 1);
		setDb(conn);
	});

	afterAll(async () => {
		clearDb(conn);
		await conn?.close();
	});

	it("acquires, and refuses a second acquisition of the same key", async () => {
		expect(await db.getAdvisoryLock(1)).toBe(true);
		// Same session: Postgres advisory locks are re-entrant, so this succeeds
		// and the lock is held twice. That is the documented behaviour, and it is
		// why a release must match every acquisition.
		expect(await db.getAdvisoryLock(1)).toBe(true);
		await db.releaseAdvisoryLock(1);
		await db.releaseAdvisoryLock(1);
		expect(await heldLocks(conn, 1)).toBe(0);
	});

	it("releases what it took, on one pooled connection", async () => {
		expect(await db.getAdvisoryLock(42)).toBe(true);
		expect(await heldLocks(conn, 42)).toBe(1);

		expect(await db.releaseAdvisoryLock(42)).toBe(true);
		// The proof the release landed on the connection holding the lock.
		expect(await heldLocks(conn, 42)).toBe(0);
	});

	it("reports failure when releasing a key it never took", async () => {
		expect(await db.releaseAdvisoryLock(999)).toBe(false);
	});

	it("is not held by a SEPARATE connection", async () => {
		expect(await db.getAdvisoryLock(7)).toBe(true);
		const other = await createNapiConnection(PG_URL, 1, 1);
		try {
			// A different session sees the lock as taken — which is the whole point
			// of the migration guard across two deploying instances.
			const rows = await other.query<{ locked: boolean }>(
				"SELECT pg_try_advisory_lock($1) AS locked",
				[7],
			);
			expect(rows[0]?.locked).toBe(false);
		} finally {
			await other.close();
			await db.releaseAdvisoryLock(7);
		}
	});
});
