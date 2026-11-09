/**
 * increment()/decrement() with an amount that is not a finite number.
 *
 * JSON writes NaN and Infinity as `null`, so `increment('balance', NaN)`
 * compiled to `balance = balance + NULL` and wiped the column. Every entry
 * point refuses such an amount and leaves the row as it was; the compile
 * chokepoint refuses any non-finite parameter as a backstop.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("wallets")
class Wallet extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare balance: number;
}

const BAD: unknown[] = [
	Number.NaN,
	Number.POSITIVE_INFINITY,
	-Infinity,
	"5",
	{},
];

let conn: AsyncDatabaseConnection;
let db: DbService;
let repo: BaseRepository<Wallet>;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE wallets (id INTEGER PRIMARY KEY, balance INTEGER)",
	);
	await conn.execute("INSERT INTO wallets VALUES (1, 100), (2, 100)");
	db = createDbService(() => conn);
	repo = new BaseRepository(Wallet, conn);
});

afterEach(async () => {
	await conn.close();
});

async function balances(): Promise<number[]> {
	const rows = await db.from("wallets").orderBy("id");
	return rows.map((r) => Number(r.balance));
}

describe("atlas > increment/decrement refuse a non-finite amount", () => {
	it.each(BAD)(
		"model query increment(%s) throws and keeps the row",
		async (bad) => {
			// Untyped input (a parsed request body) reaches the call as-is.
			const q = repo.query().where("id", 1);
			await expect(
				Reflect.apply(q.increment, q, ["balance", bad]),
			).rejects.toThrow(/E_ATLAS_INVALID_INCREMENT|finite number/);
			expect(await balances()).toEqual([100, 100]);
		},
	);

	it.each(BAD)("model query decrement({ balance: %s }) throws", async (bad) => {
		const patch: Record<string, number> = {};
		Reflect.set(patch, "balance", bad);
		await expect(repo.query().where("id", 1).decrement(patch)).rejects.toThrow(
			/E_ATLAS_INVALID_INCREMENT|finite number/,
		);
		expect(await balances()).toEqual([100, 100]);
	});

	it.each(BAD)(
		"repository increment(id, { balance: %s }) throws",
		async (bad) => {
			const patch: Record<string, number> = {};
			Reflect.set(patch, "balance", bad);
			await expect(repo.increment(2, patch)).rejects.toThrow(
				/E_ATLAS_INVALID_INCREMENT|finite number/,
			);
			await expect(repo.decrement(2, patch)).rejects.toThrow(
				/E_ATLAS_INVALID_INCREMENT|finite number/,
			);
			expect(await balances()).toEqual([100, 100]);
		},
	);

	it.each(BAD)("db builder increment({ balance: %s }) throws", async (bad) => {
		const patch: Record<string, number> = {};
		Reflect.set(patch, "balance", bad);
		await expect(
			db.from("wallets").where("id", 1).increment(patch),
		).rejects.toThrow(/E_ATLAS_INVALID_INCREMENT|finite number/);
		expect(await balances()).toEqual([100, 100]);
	});

	it("a finite amount still applies atomically", async () => {
		await repo.query().where("id", 1).increment("balance", 5);
		await repo.decrement(2, "balance", 3);
		await db.from("wallets").where("id", 1).increment("balance", 0.0);
		expect(await balances()).toEqual([105, 97]);
	});

	it("any non-finite parameter is refused at compile time", () => {
		expect(() =>
			db.from("wallets").where("balance", Number.NaN).toSQL(),
		).toThrow(/E_ATLAS_NON_FINITE_NUMBER|finite numbers/);
		expect(() => repo.query().where("balance", ">", Infinity).toSQL()).toThrow(
			/finite numbers/,
		);
	});
});
