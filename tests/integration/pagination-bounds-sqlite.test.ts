/**
 * paginate()/forPage()/limit()/offset() given a page size that is not a
 * number — `Number(request.qs().perPage)` with `?perPage=abc` or `Infinity`.
 *
 * `Math.max(1, NaN)` is NaN and JSON writes NaN/Infinity as `null`, which the
 * compiler reads as "no LIMIT": the whole table came back in one response, with
 * null pagination meta. Pagination input is cast like Lucid does (`Number()`),
 * floored and clamped to 1, and refused when it is not a finite integer.
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
import { QueryBuilder } from "../../src/query/QueryBuilder.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("w")
class W extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare n: number;
}

let conn: AsyncDatabaseConnection;
let db: DbService;
let repo: BaseRepository<W>;

beforeAll(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE w (id INTEGER PRIMARY KEY, n INTEGER)");
	const values = Array.from({ length: 50 }, (_, i) => `(${i + 1}, ${i})`);
	await conn.execute(`INSERT INTO w (id, n) VALUES ${values.join(", ")}`);
	db = createDbService(() => conn);
	repo = new BaseRepository(W, conn);
});

afterAll(async () => {
	await conn.close();
});

/** Call a method with untyped input, as a parsed query string hands it over. */
function call(target: object, method: string, ...args: unknown[]): unknown {
	return Reflect.apply(Reflect.get(target, method), target, args);
}

const NON_FINITE: unknown[] = [
	Number.NaN,
	Number.POSITIVE_INFINITY,
	"abc",
	1e300,
];

describe("atlas > pagination never drops the LIMIT", () => {
	it.each(NON_FINITE)("model paginate(1, %s) is refused", async (bad) => {
		await expect(call(repo.query(), "paginate", 1, bad)).rejects.toThrow(
			/E_ATLAS_INVALID_PAGINATION|pagination/,
		);
		await expect(call(repo.query(), "paginate", bad, 5)).rejects.toThrow(
			/pagination/,
		);
	});

	it.each(NON_FINITE)("db builder paginate(1, %s) is refused", async (bad) => {
		await expect(call(db.from("w"), "paginate", 1, bad)).rejects.toThrow(
			/pagination/,
		);
	});

	it.each(NON_FINITE)("forPage(1, %s) is refused on both builders", (bad) => {
		expect(() => call(repo.query(), "forPage", 1, bad)).toThrow(/pagination/);
		expect(() => call(db.from("w"), "forPage", 1, bad)).toThrow(/pagination/);
	});

	it.each([Number.NaN, Number.POSITIVE_INFINITY, -1, 2.5])(
		"db builder limit/offset(%s) is refused",
		(bad) => {
			expect(() => db.from("w").limit(bad)).toThrow(/non-negative integer/);
			expect(() => db.from("w").offset(bad)).toThrow(/non-negative integer/);
			expect(() => new QueryBuilder("w").limit(bad)).toThrow(
				/non-negative integer/,
			);
			expect(() => new QueryBuilder("w").offset(bad)).toThrow(
				/non-negative integer/,
			);
		},
	);

	it("numeric strings, fractions and values below 1 still paginate", async () => {
		const page = await call(repo.query().orderBy("id"), "paginate", "2", "20");
		expect(page).toMatchObject({ total: 50, perPage: 20, currentPage: 2 });
		const viaDb = await db.from("w").orderBy("id").paginate(2.7, 10.9);
		expect(viaDb.all().map((r) => Number(r.id))).toEqual([
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		]);
		const clamped = await repo.query().orderBy("id").paginate(-3, 0);
		expect(clamped.all().map((u) => u.id)).toEqual([1]);
		const rows = await db.from("w").orderBy("id").forPage(3, 5);
		expect(rows.map((r) => Number(r.id))).toEqual([11, 12, 13, 14, 15]);
	});

	it("the meta of a page is finite", async () => {
		const page = await repo.query().paginate(1, 7);
		expect(page.meta).toMatchObject({
			total: 50,
			perPage: 7,
			currentPage: 1,
			lastPage: 8,
		});
		expect(page.all()).toHaveLength(7);
	});
});
