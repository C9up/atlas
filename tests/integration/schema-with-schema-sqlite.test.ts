/**
 * Schema.withSchema() (Adonis Lucid/Knex parity): qualifies table names with a
 * schema. SQLite has no CREATE SCHEMA, but it addresses ATTACH-ed databases as
 * `schema.table`, so we ATTACH one and prove the qualified DDL lands there.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { Schema } from "../../src/schema/Schema.js";

let conn: AsyncDatabaseConnection;

async function run(build: (s: Schema) => void): Promise<void> {
	const schema = new Schema("sqlite");
	build(schema);
	for (const sql of schema.toSQL()) {
		await conn.execute(sql);
	}
}

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	// A second in-memory database addressable as the "reporting" schema.
	await conn.execute("ATTACH DATABASE ':memory:' AS reporting");
});

afterEach(async () => {
	await conn?.close();
});

describe("Schema.withSchema", () => {
	it("emits a schema-qualified table name", () => {
		const schema = new Schema("sqlite");
		schema.withSchema("reporting").createTable("metrics", (t) => {
			t.increments("id");
		});
		expect(schema.toSQL()[0]).toContain('"reporting"."metrics"');
	});

	it("creates the table inside the attached schema, not the main one", async () => {
		await run((s) =>
			s.withSchema("reporting").createTable("metrics", (t) => {
				t.increments("id");
				t.integer("value");
			}),
		);

		// It exists under the reporting schema...
		const inReporting = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM reporting.sqlite_master WHERE type='table' AND name='metrics'",
		);
		expect(inReporting[0]?.n).toBe(1);
		// ...and not in the main schema.
		const inMain = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='metrics'",
		);
		expect(inMain[0]?.n).toBe(0);

		// And it's writable through the qualified name.
		await conn.execute("INSERT INTO reporting.metrics (value) VALUES (42)");
		const rows = await conn.query<{ value: number }>(
			"SELECT value FROM reporting.metrics",
		);
		expect(rows[0]?.value).toBe(42);
	});

	it("does not qualify when no schema is set", () => {
		const schema = new Schema("sqlite");
		schema.createTable("plain", (t) => {
			t.increments("id");
		});
		expect(schema.toSQL()[0]).not.toContain("reporting");
		expect(schema.toSQL()[0]).toContain('"plain"');
	});
});
