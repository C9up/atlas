/**
 * Views weren't in the DDL surface at all. Schema now emits CREATE/DROP VIEW
 * through the Rust compiler (name validated + quoted, raw SELECT body like
 * `raw()`). These tests run the emitted SQL against a live sqlite connection
 * and query the view, rather than asserting SQL strings.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { Schema } from "../../src/schema/Schema.js";

let conn: AsyncDatabaseConnection;

async function runSchema(build: (s: Schema) => void): Promise<void> {
	const schema = new Schema("sqlite");
	build(schema);
	for (const sql of schema.toSQL()) {
		await conn.execute(sql);
	}
}

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
	);
	await conn.execute(
		"INSERT INTO users (name, active) VALUES ('a', 1), ('b', 0), ('c', 1)",
	);
});

afterEach(async () => {
	await conn?.close();
});

describe("Schema.createView", () => {
	it("creates a queryable view over a base table", async () => {
		await runSchema((s) =>
			s.createView(
				"active_users",
				"SELECT id, name FROM users WHERE active = 1",
			),
		);

		const rows = await conn.query<{ name: string }>(
			"SELECT name FROM active_users ORDER BY name",
		);
		expect(rows.map((r) => r.name)).toEqual(["a", "c"]);
	});

	it("honours an explicit column list", async () => {
		await runSchema((s) =>
			s.createView("u", "SELECT id, name FROM users WHERE active = 1", {
				columns: ["uid", "label"],
			}),
		);

		const rows = await conn.query<{ uid: number; label: string }>(
			"SELECT uid, label FROM u ORDER BY label",
		);
		expect(rows[0]).toEqual({ uid: 1, label: "a" });
	});

	it("drops a view via dropView", async () => {
		await runSchema((s) =>
			s.createView("v", "SELECT id FROM users").dropView("v"),
		);
		// Buffer built create-then-drop; the view must not exist afterwards.
		const views = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='view' AND name='v'",
		);
		expect(views[0]?.n).toBe(0);
	});
});

describe("Schema view dialect guards", () => {
	it("refuses OR REPLACE on sqlite (grammar has none)", () => {
		expect(() =>
			new Schema("sqlite").createViewOrReplace("v", "SELECT 1"),
		).toThrow(/OR REPLACE/i);
	});

	it("refuses materialized views on sqlite (Postgres-only)", () => {
		expect(() =>
			new Schema("sqlite").createMaterializedView("v", "SELECT 1"),
		).toThrow(/materialized/i);
	});

	it("rejects an injection attempt in the view name", () => {
		expect(() =>
			new Schema("sqlite").createView("v; DROP TABLE users--", "SELECT 1"),
		).toThrow();
	});
});
