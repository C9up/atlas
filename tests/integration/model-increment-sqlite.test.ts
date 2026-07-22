/**
 * Regression guard: `ModelQuery.increment` / `decrement` must emit an atomic
 * `SET col = col ± ?` — not silently store the raw `{ op, value }` JSON object.
 * The untagged Rust `SetValue` enum had `Value(serde_json::Value)` first, so the
 * `{ op, value }` object matched `Value` and the `Expression` arm was
 * unreachable; both the db builder and this model path corrupted numeric
 * columns with zero DB-level coverage. See atlas-query `dml.rs`.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Counter extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare hits: number;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE counters (id TEXT PRIMARY KEY, hits INTEGER)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await Counter.truncate();
	await Counter.create({ id: "a", hits: 10 });
	await Counter.create({ id: "b", hits: 10 });
});

describe("atlas > ModelQuery increment/decrement (Lucid, atomic)", () => {
	it("increment(column, amount) does col = col + ? on the matched row only", async () => {
		const affected = await Counter.query().where("id", "a").increment("hits", 5);
		expect(affected).toBe(1);
		// The matched row incremented; hits stays a NUMBER (not a JSON string).
		expect((await Counter.find("a"))?.hits).toBe(15);
		expect((await Counter.find("b"))?.hits).toBe(10);
	});

	it("decrement defaults to 1", async () => {
		await Counter.query().where("id", "b").decrement("hits");
		expect((await Counter.find("b"))?.hits).toBe(9);
	});

	it("increment({map}) updates several columns atomically", async () => {
		await Counter.query().where("id", "a").increment({ hits: 3 });
		expect((await Counter.find("a"))?.hits).toBe(13);
	});
});
