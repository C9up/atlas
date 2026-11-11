/**
 * A failing statement in a transaction batch reports the database's error,
 * even when its SQL has a multi-byte character where the message cuts it.
 *
 * The SQL was cut at byte 80 for the message; inside a character that slice
 * panicked, and the caller saw "task panicked" instead of the real cause.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)");
});

afterEach(async () => {
	await conn.close();
});

describe("atlas > a batch error survives a multi-byte character in its SQL", () => {
	it.each(["é", "—", "🦀"])(
		"reports the database error around %s",
		async (ch) => {
			// The statement is 79 ASCII bytes, then the character straddles byte 80.
			const prefix = "INSERT INTO missing_table (body) VALUES ('";
			const sql = `${prefix}${"x".repeat(79 - prefix.length)}${ch}')`;
			await expect(
				conn.runInTransaction([
					{ sql: "INSERT INTO notes (body) VALUES ('kept?')" },
					{ sql },
				]),
			).rejects.toThrow(/no such table: missing_table/);
			expect(await conn.query("SELECT body FROM notes")).toEqual([]);
		},
	);
});
