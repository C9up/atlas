/**
 * Column types and modifiers the table builder was missing against Lucid/Knex:
 * `jsonb`, `mediumint`, `text(name, 'longtext')`, `specificType`, plus the
 * precision/length options on `float`/`double`/`time`/`timestamp`/`binary`.
 *
 * The dialect-specific renderings are asserted at the SQL level (they differ by
 * design); the SQLite cases execute against a real database.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { Schema } from "../../src/schema/Schema.js";

/** The single CREATE TABLE statement produced for `callback`, on `dialect`. */
function createSql(
	dialect: "sqlite" | "postgres" | "mysql",
	callback: (
		t: import("../../src/schema/TableBuilder.js").TableBuilder,
	) => void,
): string {
	const schema = new Schema(dialect);
	schema.createTable("t", callback);
	return schema.toSQL()[0] ?? "";
}

describe("column types (SQL per dialect)", () => {
	it("renders jsonb per dialect", () => {
		expect(createSql("postgres", (t) => t.jsonb("payload"))).toContain("JSONB");
		expect(createSql("mysql", (t) => t.jsonb("payload"))).toContain("JSON");
		expect(createSql("sqlite", (t) => t.jsonb("payload"))).toContain("TEXT");
	});

	it("widens mediumint on MySQL only", () => {
		expect(createSql("mysql", (t) => t.mediumint("n"))).toContain("MEDIUMINT");
		expect(createSql("postgres", (t) => t.mediumint("n"))).toContain("INTEGER");
	});

	it("widens text variants on MySQL only", () => {
		expect(createSql("mysql", (t) => t.text("body", "mediumtext"))).toContain(
			"MEDIUMTEXT",
		);
		expect(createSql("mysql", (t) => t.text("body", "longtext"))).toContain(
			"LONGTEXT",
		);
		expect(createSql("postgres", (t) => t.text("body", "longtext"))).toContain(
			"TEXT",
		);
		// The default stays a plain TEXT — the extra arg is opt-in.
		expect(createSql("mysql", (t) => t.text("body"))).toContain("`body` TEXT");
	});

	it("applies temporal precision except on SQLite", () => {
		expect(
			createSql("postgres", (t) => t.timestamp("at", { precision: 3 })),
		).toContain("TIMESTAMP(3)");
		expect(createSql("postgres", (t) => t.time("at", 3))).toContain("TIME(3)");
		expect(
			createSql("sqlite", (t) => t.timestamp("at", { precision: 3 })),
		).toContain("TEXT");
	});

	it("selects the tz-aware type via useTz", () => {
		expect(
			createSql("postgres", (t) => t.timestamp("at", { useTz: true })),
		).toContain("TIMESTAMPTZ");
		expect(
			createSql("postgres", (t) => t.dateTime("at", { useTz: true })),
		).toContain("TIMESTAMPTZ");
		expect(createSql("postgres", (t) => t.timestamp("at"))).toContain(
			'"at" TIMESTAMP',
		);
	});

	it("gives binary a length on MySQL only", () => {
		expect(createSql("mysql", (t) => t.binary("blob", 16))).toContain(
			"VARBINARY(16)",
		);
		expect(createSql("postgres", (t) => t.binary("blob", 16))).toContain(
			"BYTEA",
		);
		expect(createSql("sqlite", (t) => t.binary("blob"))).toContain("BLOB");
	});

	it("gives float precision on MySQL only", () => {
		expect(createSql("mysql", (t) => t.float("ratio", 8, 2))).toContain(
			"FLOAT(8, 2)",
		);
		expect(createSql("postgres", (t) => t.float("ratio", 8, 2))).toContain(
			"REAL",
		);
	});

	it("passes a specificType through verbatim", () => {
		expect(
			createSql("postgres", (t) => t.specificType("ip", "inet")),
		).toContain('"ip" inet');
		expect(
			createSql("postgres", (t) =>
				t.specificType("geom", "geometry(Point, 4326)"),
			),
		).toContain("geometry(Point, 4326)");
	});

	it("rejects a specificType that could break out of the DDL", () => {
		for (const evil of [
			"text); DROP TABLE users; --",
			"text'",
			"text--",
			"numeric(10",
		]) {
			expect(() =>
				createSql("postgres", (t) => t.specificType("x", evil)),
			).toThrow(/E_UNSAFE_SQL|E_SPECIFIC_TYPE_EMPTY/);
		}
	});
});

describe("column types (sqlite, executed)", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
	});

	it("creates a table using the new types and round-trips a row", async () => {
		const schema = new Schema("sqlite");
		schema.createTable("events", (t) => {
			t.increments("id");
			t.jsonb("payload");
			t.mediumint("seq");
			t.text("body", "longtext");
			t.specificType("kind", "TEXT");
			t.timestamp("at", { precision: 3 });
		});
		for (const sql of schema.toSQL()) await conn.execute(sql, []);

		await conn.execute(
			"INSERT INTO events (payload, seq, body, kind, at) VALUES (?, ?, ?, ?, ?)",
			['{"a":1}', 7, "long", "click", "2026-07-17T10:00:00"],
		);
		const rows = await conn.query<{ payload: string; seq: number }>(
			"SELECT payload, seq FROM events",
		);
		expect(rows[0]).toMatchObject({ payload: '{"a":1}', seq: 7 });
	});
});
