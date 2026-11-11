/**
 * The database assertions against a REAL MySQL, gated on ATLAS_TEST_MYSQL_URL.
 *
 * They quoted identifiers with double quotes, which MySQL reads as a string
 * literal: `SELECT COUNT(*) AS c FROM "users"` was a syntax error, so every
 * assertion failed on MySQL.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { createDbAssertions } from "../../src/testing/DbAssertions.js";

const MYSQL_URL = process.env.ATLAS_TEST_MYSQL_URL ?? "";
const describeMysql = MYSQL_URL ? describe : describe.skip;

describeMysql("atlas > db assertions on MySQL", () => {
	let conn: AsyncDatabaseConnection;

	beforeAll(async () => {
		conn = await createNapiConnection(MYSQL_URL, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS dba_users");
		await conn.execute(
			"CREATE TABLE dba_users (id INT PRIMARY KEY, email VARCHAR(50), joined DATE)",
		);
		await conn.execute(
			"INSERT INTO dba_users VALUES (1, 'a@b.c', '2026-01-02'), (2, 'd@e.f', NULL)",
		);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS dba_users");
		await conn.close();
	});

	it("runs every assertion against the real engine", async () => {
		const db = createDbAssertions(conn);
		await db.assertHas("dba_users", { email: "a@b.c", joined: "2026-01-02" });
		await db.assertMissing("dba_users", { email: "nobody@x.y" });
		await db.assertCount("dba_users", 2);
		await db.assertCount("dba_users", 1, { id: 2 });
		await expect(db.assertEmpty("dba_users")).rejects.toThrow(
			/to be empty, found 2/,
		);
	});
});
