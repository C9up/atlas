/**
 * Advisory locks against a REAL MySQL, gated on E_ATLAS_TEST_MYSQL_URL.
 *
 * MySQL spells them `GET_LOCK(name, 0)` / `RELEASE_LOCK(name)` and they are
 * connection-scoped, exactly like Postgres's. Same shape as the Postgres file
 * beside this one; this path had never run against a real MySQL either.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import db, { clearDb, setDb } from "../../src/services/db.js";

const MYSQL_URL = process.env.E_ATLAS_TEST_MYSQL_URL ?? "";
const describeMysql = MYSQL_URL ? describe : describe.skip;

describeMysql("atlas > advisory locks (real MySQL)", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		// One pooled connection — the shape a migration runner uses, and the one
		// the connection-scoped guarantee holds for.
		conn = await createNapiConnection(MYSQL_URL, 1, 1);
		setDb(conn);
	});

	afterAll(async () => {
		clearDb(conn);
		await conn?.close();
	});

	it("takes a lock and releases it", async () => {
		expect(await db.getAdvisoryLock("migrations")).toBe(true);
		expect(await db.releaseAdvisoryLock("migrations")).toBe(true);
	});

	it("reports failure when releasing a name it never took", async () => {
		expect(await db.releaseAdvisoryLock("never-taken")).toBe(false);
	});

	it("is not held by a SEPARATE connection", async () => {
		expect(await db.getAdvisoryLock("deploy")).toBe(true);
		const other = await createNapiConnection(MYSQL_URL, 1, 1);
		try {
			// A second deploying instance must see the lock as taken — the reason
			// the migration guard exists.
			const rows = await other.query<{ locked: number }>(
				"SELECT GET_LOCK('deploy', 0) AS locked",
			);
			expect(Number(rows[0]?.locked)).toBe(0);
		} finally {
			await other.close();
			await db.releaseAdvisoryLock("deploy");
		}
	});
});
