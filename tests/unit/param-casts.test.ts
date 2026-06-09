/**
 * Postgres parameter casts (sqlx text-bind fix). sqlx binds JS strings as
 * `text`, which Postgres won't coerce to timestamp/uuid/date. The compiler
 * emits `$N::<type>` for flagged columns on Postgres only — verified end-to-end
 * through the NAPI binding (proves the `casts` field crosses the serde boundary).
 */
import { describe, expect, it } from "vitest";
import { compileStatementNative } from "../../src/query/native.js";

const insertSpec = {
	kind: "insert",
	table: "users",
	rows: [
		[
			["id", "0191-uuid"],
			["created_at", "2026-06-09T00:00:00Z"],
			["name", "Ada"],
		],
	],
	casts: { id: "uuid", created_at: "timestamp" },
	returning: [],
};

describe("atlas > Postgres parameter casts", () => {
	it("emits $N::type for flagged columns on Postgres, untyped stay plain", () => {
		const pg = compileStatementNative(insertSpec, "postgres");
		expect(pg.statements[0]).toContain("$1::uuid");
		expect(pg.statements[0]).toContain("$2::timestamp");
		expect(pg.statements[0]).toContain("$3");
		expect(pg.statements[0]).not.toContain("$3::");
	});

	it("never casts on SQLite (driver coerces)", () => {
		const sqlite = compileStatementNative(insertSpec, "sqlite");
		expect(sqlite.statements[0]).not.toContain("::");
	});

	it("never casts on MySQL", () => {
		const mysql = compileStatementNative(insertSpec, "mysql");
		expect(mysql.statements[0]).not.toContain("::");
	});

	it("casts an UPDATE SET against a typed column on Postgres", () => {
		const updateSpec = {
			kind: "update",
			table: "users",
			set: [
				["updated_at", "2026-06-09T00:00:00Z"],
				["name", "Ada"],
			],
			wheres: [],
			casts: { updated_at: "timestamp" },
		};
		const pg = compileStatementNative(updateSpec, "postgres");
		expect(pg.statements[0]).toContain('"updated_at" = $1::timestamp');
		expect(pg.statements[0]).toContain('"name" = $2');
		expect(pg.statements[0]).not.toContain("$2::");
	});
});
