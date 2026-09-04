/**
 * A refused statement reaches the host, classified.
 *
 * The builder binds every value, so an injection payload arriving AS A VALUE is
 * compared and never parsed — no event, and none needed. What this reports is
 * the other vector: user input that reached somewhere a value cannot go — a
 * column name, a sort direction, a SELECT expression.
 */
import "reflect-metadata";
import { afterEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	clearUnsafeStatementListeners,
	onUnsafeStatement,
	type UnsafeStatementEvent,
} from "../../src/events.js";
import db, { clearDb, setDb } from "../../src/services/db.js";

async function withDb<T>(fn: () => Promise<T> | T): Promise<T> {
	const conn: AsyncDatabaseConnection = await createNapiConnection(
		"sqlite::memory:",
		1,
		1,
	);
	await conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
	setDb(conn);
	try {
		return await fn();
	} finally {
		clearDb(conn);
		await conn.close();
	}
}

function recorder(): UnsafeStatementEvent[] {
	const seen: UnsafeStatementEvent[] = [];
	onUnsafeStatement((event) => seen.push(event));
	return seen;
}

describe("atlas > a refused statement is reported to the host", () => {
	afterEach(() => {
		clearUnsafeStatementListeners();
	});

	it("calls an injection pattern what it is", async () => {
		await withDb(async () => {
			const seen = recorder();
			// A sort column taken from a query string.
			await expect(
				db.from("users").select("name; DROP TABLE users --").exec(),
			).rejects.toThrow();
			expect(seen).toHaveLength(1);
			expect(seen[0]).toMatchObject({
				kind: "injection-pattern",
				code: "E_INJECTION_PATTERN",
			});
			expect(seen[0]?.message).toContain("DROP TABLE");
		});
	});

	it("calls a quote inside an identifier an injection pattern too", async () => {
		await withDb(async () => {
			const seen = recorder();
			await expect(
				db.from("users").select('name" , 1').exec(),
			).rejects.toThrow();
			expect(seen[0]?.kind).toBe("injection-pattern");
		});
	});

	it("calls an unknown function a shape problem, not an attack", async () => {
		await withDb(async () => {
			const seen = recorder();
			// A legacy schema or an application-defined function looks like this,
			// so it is a count to accumulate rather than a verdict on a person.
			await expect(
				db.from("users").select("my_func(name)").exec(),
			).rejects.toThrow();
			expect(seen[0]).toMatchObject({ kind: "invalid-shape" });
		});
	});

	it("says nothing when an injection payload arrives as a bound VALUE", async () => {
		await withDb(async () => {
			const seen = recorder();
			// Compared, never parsed. This is the common case, and it is not an
			// event: reporting it would drown the signal that matters.
			const rows = await db
				.from("users")
				.where("name", "x'; DROP TABLE users; --")
				.exec();
			expect(rows).toEqual([]);
			expect(seen).toEqual([]);
			// And the table is still there.
			expect(await db.from("users").exec()).toEqual([]);
		});
	});

	it("says nothing about a statement that compiles", async () => {
		await withDb(async () => {
			const seen = recorder();
			await db.from("users").select("name").exec();
			expect(seen).toEqual([]);
		});
	});
});
