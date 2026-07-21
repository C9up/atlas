/**
 * Static finders/creators accept `{ client: trx }` (Adonis Lucid): the operation
 * runs on the transaction client, so a rollback undoes everything and the
 * default connection never sees the work.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Account extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare balance: number;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > BaseModel static { client: trx } (Lucid)", () => {
	it("routes create/query/findOrFail through the trx, and a rollback undoes it", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		await expect(
			conn.transaction(async (trx) => {
				// Everything inside uses the trx client (poolMax=1 is pinned by it).
				await Account.create({ id: "a1", balance: 100 }, { client: trx });
				const inTx = await Account.query({ client: trx })
					.where("id", "a1")
					.first();
				expect(inTx?.balance).toBe(100);
				const found = await Account.findOrFail("a1", { client: trx });
				expect(found.id).toBe("a1");
				throw new Error("rollback please");
			}),
		).rejects.toThrow("rollback please");

		// Rolled back — the default connection sees nothing.
		expect(await Account.find("a1")).toBeNull();
	});

	it("commits when the managed transaction succeeds", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		await conn.transaction(async (trx) => {
			await Account.create({ id: "a2", balance: 50 }, { client: trx });
		});
		expect((await Account.findOrFail("a2")).balance).toBe(50);
	});
});
