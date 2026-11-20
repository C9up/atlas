/**
 * `timeout(ms, { cancel: true })` hands the connection back with the timeout
 * it had, against a REAL server, gated on ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL.
 *
 * The driver set the server timeout for the statement, then reset it to a
 * hard-coded 0. A session-level SET outranks the role, database and startup
 * defaults, so a `statement_timeout` the DBA configured on the role was
 * switched off on that pooled connection for the rest of its life: every
 * later query borrowed on it ran unbounded.
 *
 * Each pool holds ONE connection, so the statements after the timed one run
 * on the connection the timed one used.
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const MYSQL_URL = process.env.ATLAS_TEST_MYSQL_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;
const describeMysql = MYSQL_URL ? describe : describe.skip;

const PROBE_ROLE = "atlas_timeout_probe";

describePg("atlas > Postgres server timeout restores the session value", () => {
	let admin: AsyncDatabaseConnection;
	let conn: AsyncDatabaseConnection;

	const current = async () => {
		const rows = await conn.query<{ statement_timeout: string }>(
			"SHOW statement_timeout",
		);
		return rows[0]?.statement_timeout;
	};

	beforeAll(async () => {
		admin = await createNapiConnection(PG_URL, 1, 1);
		await admin.execute(`DROP ROLE IF EXISTS ${PROBE_ROLE}`);
		await admin.execute(`CREATE ROLE ${PROBE_ROLE} LOGIN PASSWORD 'probe'`);
		// The hardening the bug wiped: a default on the role itself.
		await admin.execute(
			`ALTER ROLE ${PROBE_ROLE} SET statement_timeout = '5s'`,
		);
		const url = new URL(PG_URL);
		url.username = PROBE_ROLE;
		url.password = "probe";
		conn = await createNapiConnection(url.toString(), 1, 1);
	});

	afterAll(async () => {
		await conn?.close();
		await admin?.execute(`DROP ROLE IF EXISTS ${PROBE_ROLE}`);
		await admin?.close();
	});

	it("keeps the role default after a timed query", async () => {
		expect(await current()).toBe("5s");
		await conn.query("SELECT 1 AS one", [], { serverTimeoutMs: 1000 });
		expect(await current()).toBe("5s");
	});

	it("keeps it after a timed query the server cancelled", async () => {
		await expect(
			conn.query("SELECT pg_sleep(2)", [], { serverTimeoutMs: 100 }),
		).rejects.toThrow(/statement timeout/i);
		expect(await current()).toBe("5s");
	});

	it("keeps it after a timed write", async () => {
		await conn.execute("SELECT pg_sleep(0)", [], { serverTimeoutMs: 1000 });
		expect(await current()).toBe("5s");
	});

	it("keeps a value the session set itself", async () => {
		await conn.execute("SET statement_timeout = '7s'");
		await conn.query("SELECT 1 AS one", [], { serverTimeoutMs: 1000 });
		expect(await current()).toBe("7s");
		await conn.execute("RESET statement_timeout");
		expect(await current()).toBe("5s");
	});
});

describeMysql("atlas > MySQL server timeout restores the session value", () => {
	let conn: AsyncDatabaseConnection;

	const current = async () => {
		const rows = await conn.query<{ value: unknown }>(
			"SELECT @@SESSION.max_execution_time AS value",
		);
		return Number(rows[0]?.value);
	};

	beforeAll(async () => {
		conn = await createNapiConnection(MYSQL_URL, 1, 1);
	});

	afterAll(async () => {
		await conn?.close();
	});

	it("keeps the session value after a timed query", async () => {
		await conn.execute("SET SESSION max_execution_time = 4321");
		await conn.query("SELECT 1 AS one", [], { serverTimeoutMs: 1000 });
		expect(await current()).toBe(4321);
	});

	it("keeps it after a timed query the server cancelled", async () => {
		await conn.execute("SET SESSION max_execution_time = 4321");
		// MySQL interrupts SLEEP() at the deadline and returns 1 instead of 0.
		const rows = await conn.query<{ slept: unknown }>(
			"SELECT SLEEP(2) AS slept",
			[],
			{ serverTimeoutMs: 100 },
		);
		expect(Number(rows[0]?.slept)).toBe(1);
		expect(await current()).toBe(4321);
	});
});
