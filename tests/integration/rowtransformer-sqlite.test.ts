/**
 * `query().rowTransformer(cb)` (Adonis Lucid): the callback runs for every
 * hydrated instance after loading, mutating it in place — per-query decoration
 * without a model hook.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Widget extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"INSERT INTO widgets VALUES ('1', 'alpha'), ('2', 'beta')",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > ModelQuery.rowTransformer (Lucid)", () => {
	it("runs for every hydrated instance and mutates it in place", async () => {
		const rows = await Widget.query()
			.orderBy("id")
			.rowTransformer((w) => w.setExtra("upper", w.name.toUpperCase()))
			.exec();

		expect(rows.map((r) => r.getExtra("upper"))).toEqual(["ALPHA", "BETA"]);
	});

	it("applies multiple transformers in registration order", async () => {
		const order: string[] = [];
		await Widget.query()
			.rowTransformer(() => order.push("first"))
			.rowTransformer(() => order.push("second"))
			.limit(1)
			.exec();

		expect(order).toEqual(["first", "second"]);
	});
});
