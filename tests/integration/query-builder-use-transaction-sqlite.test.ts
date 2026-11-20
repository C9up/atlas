/**
 * `useTransaction(trx)` on the query builders (upstream parity: the model,
 * database, insert and raw builders all take one). A query is often built
 * first and bound to a transaction later; without the method that was a
 * TypeError, and the only ways in (`Model.query({ client: trx })`,
 * `trx.from()`) meant rewriting the call site.
 *
 * The pool holds ONE connection, which the transaction pins: a statement that
 * did not run on the transaction would wait for that connection instead.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BaseModel,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { DatabaseQueryBuilder } from "../../src/query/DatabaseQueryBuilder.js";
import type { AtlasDialect } from "../../src/query/native.js";
import { clearDb, createDbService, setDb } from "../../src/services/db.js";
import type { TransactionClient } from "../../src/Transaction.js";

class Wallet extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare balance: number;
}

@Entity("wallets")
class WalletEntity extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare balance: number;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE wallets (id TEXT PRIMARY KEY, balance INTEGER)",
	);
	await conn.execute("CREATE TABLE audit (note TEXT)");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM wallets");
	await conn.execute("DELETE FROM audit");
	await conn.execute("INSERT INTO wallets VALUES ('w1', 100)");
});

const balance = async () =>
	(await conn.query<{ balance: number }>("SELECT balance FROM wallets"))[0]
		?.balance;
const audits = async () =>
	(await conn.query<{ n: number }>("SELECT count(*) AS n FROM audit"))[0]?.n;

/** Run `work` in a managed transaction that is then rolled back. */
async function rolledBack(
	work: (trx: TransactionClient) => Promise<void>,
): Promise<void> {
	if (typeof conn.transaction !== "function") throw new Error("no trx");
	await expect(
		conn.transaction(async (trx) => {
			await work(trx);
			throw new Error("rollback");
		}),
	).rejects.toThrow("rollback");
}

describe("atlas > query builders useTransaction(trx)", () => {
	it("a model query built outside runs inside, and is rolled back with it", async () => {
		const query = Wallet.query().where("id", "w1");
		await rolledBack(async (trx) => {
			expect(query.useTransaction(trx)).toBe(query);
			await query.update({ balance: 0 });
			const inside = await Wallet.query().useTransaction(trx).firstOrFail();
			expect(inside.balance).toBe(0);
		});
		expect(await balance()).toBe(100);
	});

	it("a row read through a bound model query saves inside the transaction", async () => {
		await rolledBack(async (trx) => {
			const wallet = await Wallet.query()
				.useTransaction(trx)
				.where("id", "w1")
				.firstOrFail();
			expect(wallet.$trx).toBe(trx);
			wallet.balance = 1;
			await wallet.save();
		});
		expect(await balance()).toBe(100);
	});

	it("a repository query binds the same way", async () => {
		const repo = new BaseRepository(WalletEntity, conn);
		await rolledBack(async (trx) => {
			await repo.query().useTransaction(trx).where("id", "w1").delete();
			expect(await repo.query().useTransaction(trx).exec()).toEqual([]);
		});
		expect(await balance()).toBe(100);
	});

	it("the database, insert, write and raw builders bind to it", async () => {
		const db = createDbService(() => conn);
		await rolledBack(async (trx) => {
			await db.from("audit").useTransaction(trx).insert({ note: "a" });
			await db
				.insertQuery()
				.table("audit")
				.useTransaction(trx)
				.insert({ note: "b" });
			await db
				.from("wallets")
				.where("id", "w1")
				.update({ balance: 5 })
				.useTransaction(trx);
			const rows = await db
				.rawQuery("SELECT balance FROM wallets WHERE id = ?", ["w1"])
				.useTransaction(trx);
			expect(rows[0]?.balance).toBe(5);
			const count = await db.from("audit").useTransaction(trx).count("* as n");
			expect(count).toEqual([{ n: 2 }]);
		});
		expect(await balance()).toBe(100);
		expect(await audits()).toBe(0);
	});

	it("refuses a transaction of another dialect: the query was compiled for this one", () => {
		const builder = new DatabaseQueryBuilder(conn, "sqlite", "wallets");
		const postgres: AtlasDialect = "postgres";
		const other = { ...conn, dialect: postgres };
		expect(() => builder.useTransaction(other)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_DIALECT_MISMATCH" }),
		);
		expect(() => Wallet.query().useTransaction(other)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_DIALECT_MISMATCH" }),
		);
		expect(() =>
			createDbService(() => conn)
				.rawQuery("SELECT 1")
				.useTransaction(other),
		).toThrow(expect.objectContaining({ code: "E_ATLAS_DIALECT_MISMATCH" }));
	});
});
