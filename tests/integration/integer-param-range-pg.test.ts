/**
 * A JS number bound to a Postgres integer parameter must fit it, against a
 * REAL PostgreSQL, gated on ATLAS_TEST_PG_URL.
 *
 * The binder cast the number into the inferred type: `4294967297` became the
 * int4 `1`, so `find(4294967297)` returned row 1 and a delete by that id
 * removed it; a fraction became `0`. Postgres answers "out of range" for
 * both, and so does atlas now.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { createDbService } from "../../src/services/db.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

@Entity("int_range_items")
class Item extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare qty: number;
	@Column() declare small: number;
}

describePg("atlas > Postgres integer parameters are range-checked", () => {
	let conn: AsyncDatabaseConnection;
	let items: BaseRepository<Item>;

	beforeAll(async () => {
		setAtlasDialect("postgres");
		conn = await createNapiConnection(PG_URL, 1, 2);
		await conn.execute("DROP TABLE IF EXISTS int_range_items");
		await conn.execute(
			"CREATE TABLE int_range_items (id integer PRIMARY KEY, qty integer, small smallint)",
		);
		await conn.execute(
			"INSERT INTO int_range_items VALUES (1, 10, 1), (2, 20, 2)",
		);
		items = new BaseRepository(Item, conn);
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS int_range_items");
		await conn.close();
		setAtlasDialect("sqlite");
	});

	async function rows(): Promise<unknown[]> {
		return conn.query("SELECT id, qty, small FROM int_range_items ORDER BY id");
	}
	const UNTOUCHED = [
		{ id: 1, qty: 10, small: 1 },
		{ id: 2, qty: 20, small: 2 },
	];

	it.each([4294967297, 2147483648, -4294967295])(
		"find(%d) is refused instead of reading a wrapped id",
		async (id) => {
			await expect(items.find(id)).rejects.toThrow(/is not a valid integer/);
		},
	);

	it("a delete by an out-of-range id removes nothing", async () => {
		await expect(
			items.query().where("id", 4294967297).delete(),
		).rejects.toThrow(/is not a valid integer/);
		const db = createDbService(() => conn);
		await expect(
			db.from("int_range_items").where("id", 4294967298).delete(),
		).rejects.toThrow(/is not a valid integer/);
		expect(await rows()).toEqual(UNTOUCHED);
	});

	it("a fraction or an out-of-range value is not stored as 0 or wrapped", async () => {
		await expect(items.updateById(1, { qty: 1.5 })).rejects.toThrow(
			/'1.5' is not a valid integer/,
		);
		await expect(items.updateById(1, { small: 32768 })).rejects.toThrow(
			/is not a valid smallint/,
		);
		expect(await rows()).toEqual(UNTOUCHED);
	});

	it("still binds an integer that fits", async () => {
		const found = await items.find(2);
		expect(found?.qty).toBe(20);
	});
});
