/**
 * Raw-query parameter typing against a REAL PostgreSQL, gated on
 * ATLAS_TEST_PG_URL.
 *
 * Lucid sends every binding untyped and lets Postgres infer it from context, so
 * `db.rawQuery('… where id = ?', [uuid])` needs no cast (knex turns
 * the `?` into `$1` before it reaches the driver, which is the layer under test). sqlx cannot: it always
 * declares a concrete OID and always binds in BINARY format, so a JS string
 * used to arrive as `text` and Postgres refused `uuid = text`.
 *
 * atlas now asks Postgres which type it inferred for each `$n` and converts the
 * JSON value into it. These tests are the proof that a raw query behaves like
 * Lucid's — no `::type` anywhere in them.
 *
 *   ATLAS_TEST_PG_URL=postgres://postgres:secret@localhost:5432/postgres \
 *     pnpm test tests/integration/raw-param-typing-pg.test.ts
 */
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

const UUID = "550e8400-e29b-41d4-a716-446655440000";
const OTHER_UUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

describePg("atlas > raw query parameter typing (Lucid parity)", () => {
	let db: AsyncDatabaseConnection;

	beforeAll(async () => {
		db = await createNapiConnection(PG_URL, 1, 5);
		await db.execute("DROP TABLE IF EXISTS raw_params CASCADE");
		await db.execute(
			"CREATE TABLE raw_params (" +
				"id uuid PRIMARY KEY, " +
				"company_id uuid, " +
				"name text, " +
				"qty integer, " +
				"price numeric(12,2), " +
				"active boolean, " +
				"meta jsonb, " +
				"created_at timestamptz, " +
				"due_on date)",
		);
	});

	afterAll(async () => {
		await db.execute("DROP TABLE IF EXISTS raw_params CASCADE");
		await db?.close();
	});

	it("matches a uuid column from a plain JS string — the reported bug", async () => {
		await db.execute(
			"INSERT INTO raw_params (id, company_id, name) VALUES ($1, $2, $3)",
			[UUID, OTHER_UUID, "acme"],
		);
		const rows = await db.query(
			"SELECT name FROM raw_params WHERE company_id = $1",
			[OTHER_UUID],
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]?.name).toBe("acme");
	});

	it("still matches a text column", async () => {
		const rows = await db.query("SELECT id FROM raw_params WHERE name = $1", [
			"acme",
		]);
		expect(rows).toHaveLength(1);
	});

	it("matches an integer column from a JS number", async () => {
		await db.execute("UPDATE raw_params SET qty = $1 WHERE id = $2", [7, UUID]);
		const rows = await db.query(
			"SELECT qty FROM raw_params WHERE qty = $1",
			[7],
		);
		expect(rows[0]?.qty).toBe(7);
	});

	it("accepts a numeric written as a decimal string, without losing precision", async () => {
		await db.execute("UPDATE raw_params SET price = $1 WHERE id = $2", [
			"1234567890.12",
			UUID,
		]);
		const rows = await db.query("SELECT price FROM raw_params WHERE id = $1", [
			UUID,
		]);
		expect(rows[0]?.price).toBe("1234567890.12");
	});

	it("binds booleans, jsonb, timestamptz and date", async () => {
		await db.execute(
			"UPDATE raw_params SET active = $1, meta = $2, created_at = $3, due_on = $4 WHERE id = $5",
			[
				true,
				JSON.stringify({ tier: "gold" }),
				"2026-08-22T10:30:00.000Z",
				"2026-12-31",
				UUID,
			],
		);
		const rows = await db.query(
			"SELECT active, meta, created_at, due_on FROM raw_params WHERE active = $1",
			[true],
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]?.meta).toEqual({ tier: "gold" });
		expect(String(rows[0]?.due_on)).toContain("2026-12-31");
	});

	it("matches a uuid column against NULL without a cast", async () => {
		await db.execute("UPDATE raw_params SET company_id = $1 WHERE id = $2", [
			null,
			UUID,
		]);
		const rows = await db.query(
			"SELECT id FROM raw_params WHERE company_id IS NOT DISTINCT FROM $1",
			[null],
		);
		expect(rows).toHaveLength(1);
	});

	it("reports a malformed uuid by name instead of an operator error", async () => {
		await expect(
			db.query("SELECT id FROM raw_params WHERE id = $1", ["not-a-uuid"]),
		).rejects.toThrow(/is not a valid uuid/);
	});

	it("takes a full ISO instant for a date column — never stricter than PG", async () => {
		// REGRESSION (0.2.4): a chronos DateTime serialises to a full instant
		// even for a date-only column, and Postgres accepts
		// `'2026-03-10T00:00:00.000Z'::date` — so refusing it client-side made
		// atlas stricter than the database it drives.
		for (const value of [
			"2026-03-10",
			"2026-03-10T00:00:00Z",
			"2026-03-10T00:00:00.000Z",
			"2026-03-10T00:00:00.000+00:00",
			"2026-03-10 00:00:00",
		]) {
			await db.execute("UPDATE raw_params SET due_on = $1 WHERE id = $2", [
				value,
				UUID,
			]);
			const rows = await db.query(
				"SELECT due_on FROM raw_params WHERE id = $1",
				[UUID],
			);
			expect(String(rows[0]?.due_on)).toContain("2026-03-10");
		}
	});

	it("takes a full instant for a time column too", async () => {
		await db.execute("DROP TABLE IF EXISTS raw_time");
		await db.execute("CREATE TABLE raw_time (at time)");
		for (const value of ["09:30:00", "2026-03-10T09:30:00Z"]) {
			await db.execute("DELETE FROM raw_time");
			await db.execute("INSERT INTO raw_time (at) VALUES ($1)", [value]);
			const rows = await db.query("SELECT at FROM raw_time");
			expect(String(rows[0]?.at)).toContain("09:30");
		}
		await db.execute("DROP TABLE raw_time");
	});

	it("takes a bare date for a timestamptz column", async () => {
		await db.execute("UPDATE raw_params SET created_at = $1 WHERE id = $2", [
			"2026-03-10",
			UUID,
		]);
		const rows = await db.query(
			"SELECT created_at FROM raw_params WHERE id = $1",
			[UUID],
		);
		expect(String(rows[0]?.created_at)).toContain("2026-03-10");
	});

	it("still runs a query Postgres cannot infer (falls back, no regression)", async () => {
		const rows = await db.query("SELECT $1 AS echo", ["hello"]);
		expect(rows[0]?.echo).toBe("hello");
	});

	it("keeps an explicit ::cast working (the old workaround stays valid)", async () => {
		const rows = await db.query(
			"SELECT name FROM raw_params WHERE id = $1::uuid",
			[UUID],
		);
		expect(rows).toHaveLength(1);
	});
});
