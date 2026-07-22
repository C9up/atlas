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
import {
	clearDb,
	registerConnection,
	setDb,
	unregisterConnection,
} from "../../src/services/db.js";

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

	it("Model.transaction commits on success, rolls back on throw (Lucid)", async () => {
		await Account.transaction(async (trx) => {
			await Account.create({ id: "t1", balance: 7 }, { client: trx });
		});
		expect((await Account.findOrFail("t1")).balance).toBe(7);

		await expect(
			Account.transaction(async (trx) => {
				await Account.create({ id: "t2", balance: 9 }, { client: trx });
				throw new Error("boom");
			}),
		).rejects.toThrow("boom");
		expect(await Account.find("t2")).toBeNull();
	});

	it("findBy / updateOrCreate accept { client: trx }", async () => {
		await expect(
			Account.transaction(async (trx) => {
				await Account.updateOrCreate(
					{ id: "u1" },
					{ balance: 5 },
					{
						client: trx,
					},
				);
				const found = await Account.findBy("id", "u1", { client: trx });
				expect(found?.balance).toBe(5);
				throw new Error("rollback");
			}),
		).rejects.toThrow("rollback");
		// Rolled back through the trx — nothing persisted.
		expect(await Account.find("u1")).toBeNull();
	});

	it("an instance loaded via query({ client: trx }) saves INSIDE the trx ($trx)", async () => {
		if (typeof conn.transaction !== "function") throw new Error("no trx");
		await Account.create({ id: "s1", balance: 100 });

		const trx = await conn.transaction();
		const acc = await Account.query({ client: trx }).where("id", "s1").first();
		if (!acc) throw new Error("expected row");
		acc.balance = 999;
		// No explicit acc.useTransaction(trx) — the row was LOADED through the trx,
		// so its save() must stay in the transaction (Lucid $trx propagation).
		await acc.save();
		await trx.rollback();

		// The save was inside the trx, so the rollback undoes it.
		expect((await Account.findOrFail("s1")).balance).toBe(100);
	});

	it("model.useConnection(name) persists to the named connection (Lucid)", async () => {
		const other = await createNapiConnection("sqlite::memory:", 1, 1);
		await other.execute(
			"CREATE TABLE accounts (id TEXT PRIMARY KEY, balance INTEGER)",
		);
		registerConnection("analytics", other);
		try {
			const acc = new Account();
			acc.id = "x1";
			acc.balance = 42;
			await acc.useConnection("analytics").save();

			// The row lives in the named connection, not the default one.
			const inOther = await other.query<{ balance: number }>(
				"SELECT balance FROM accounts WHERE id = 'x1'",
			);
			expect(inOther[0]?.balance).toBe(42);
			expect(await Account.find("x1")).toBeNull();
		} finally {
			unregisterConnection("analytics", other);
			await other.close();
		}
	});
});
