/**
 * #12 Tranche A: schema-builder parity with Lucid. New column types
 * (float/double/time/tinyint/smallint/enum), the `unsigned()` modifier, and
 * chainable foreign-key actions (`references().onDelete()`) must compile to
 * valid SQL and behave correctly on a real SQLite database.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { TableBuilder } from "../../src/schema/TableBuilder.js";

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("PRAGMA foreign_keys = ON"); // SQLite enforces FKs only when on
});

afterEach(async () => {
	await conn?.close();
});

describe("atlas > TableBuilder Tranche A parity (#12)", () => {
	it("creates a table with the new column types, enum CHECK, and FK onDelete", async () => {
		const users = new TableBuilder("users");
		users.increments("id");
		for (const sql of users.toStatements("sqlite")) await conn.execute(sql);

		const t = new TableBuilder("widgets");
		t.increments("id");
		t.float("rating");
		t.double("balance");
		t.time("starts_at");
		t.tinyint("flag");
		t.smallint("priority");
		t.integer("qty").unsigned(); // UNSIGNED is a MySQL-only no-op on SQLite
		t.enum("status", ["active", "inactive"]).notNullable();
		t.integer("owner_id").references("users", "id").onDelete("cascade");

		const stmts = t.toStatements("sqlite");
		const sql = stmts.join("\n");
		expect(sql).toContain("ON DELETE CASCADE");
		expect(sql).toContain(`CHECK ("status" IN ('active', 'inactive'))`);
		expect(sql).not.toContain("UNSIGNED"); // dropped on SQLite
		for (const s of stmts) await conn.execute(s);

		// The enum CHECK enforces the value set.
		await conn.execute("INSERT INTO users (id) VALUES (1)");
		await conn.execute(
			"INSERT INTO widgets (id, status, owner_id) VALUES (1, 'active', 1)",
		);
		await expect(
			conn.execute(
				"INSERT INTO widgets (id, status, owner_id) VALUES (2, 'bogus', 1)",
			),
		).rejects.toThrow();

		// ON DELETE CASCADE removes the child when the parent goes.
		await conn.execute("DELETE FROM users WHERE id = 1");
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM widgets",
		);
		expect(rows[0]?.n).toBe(0);
	});

	it("rejects an enum with no values at compile time", () => {
		const t = new TableBuilder("x");
		t.enum("s", []);
		expect(() => t.toStatements("sqlite")).toThrow(/enum/i);
	});
});
