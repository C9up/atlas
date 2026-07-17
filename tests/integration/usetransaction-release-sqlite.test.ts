/**
 * `model.useTransaction(trx)` releases its binding when the trx settles, exactly
 * like Lucid clears `$trx` on commit/rollback. Before the fix `#trx` stayed
 * pinned to the finished transaction client, so a reused instance's next save()
 * ran against a closed/committed trx instead of falling back to the pool.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// transaction() is optional on AsyncDatabaseConnection (test doubles may omit
// it); a real napi connection always provides it. Narrow without a cast.
function beginTx(c: AsyncDatabaseConnection) {
	if (!c.transaction)
		throw new Error("napi connection must provide transaction()");
	return c.transaction();
}

class UtWidget extends BaseModel {
	static override table = "ut_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ut_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM ut_widgets");
});

describe("atlas > model.useTransaction releases $trx when it settles (Lucid parity)", () => {
	it("clears $trx on commit so a reused instance falls back to the pool", async () => {
		const w = new UtWidget();
		w.id = "w1";
		w.name = "a";

		const trx = await beginTx(conn);
		w.useTransaction(trx);
		expect(w.$trx).toBe(trx);
		await w.save();
		await trx.commit();

		// After commit Lucid unbinds: the next save() must NOT target the finished
		// trx. It should succeed against the pool.
		expect(w.$trx).toBeUndefined();
		w.name = "b";
		await w.save();

		const reloaded = await UtWidget.$repo().findOrFail("w1");
		expect(reloaded.name).toBe("b");
	});

	it("clears $trx on rollback too", async () => {
		const w = new UtWidget();
		w.id = "w2";
		w.name = "x";

		const trx = await beginTx(conn);
		w.useTransaction(trx);
		await trx.rollback();

		expect(w.$trx).toBeUndefined();
	});
});
