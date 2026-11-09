/**
 * A `db.raw()` value in a write is inlined with its own bindings, as Lucid
 * (knex) does: query-builder insert/multiInsert/upsert, repository
 * save/create/createMany, and an update of a persisted entity. Before, it was
 * refused (and before that, bound as the JSON text `{"sql":…}`).
 *
 * Strict mode still refuses raw SQL on the model/repository write paths;
 * `db.ref()` passes.
 *
 * SQLite always runs; Postgres and MySQL run with ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL.
 */
import "reflect-metadata";
import {
	afterAll,
	afterEach,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it,
} from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasStrictMode } from "../../src/ModelQuery.js";
import type { AtlasDialect } from "../../src/query/native.js";
import { RawSql } from "../../src/query/QueryBuilder.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("raw_writes")
class RawWrite extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: unknown;
	@Column() declare n: unknown;
}

const targets: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL)
	targets.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
if (process.env.ATLAS_TEST_MYSQL_URL)
	targets.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);

describe.each(targets)(
	"atlas > db.raw() values in writes on %s",
	(dialect, url) => {
		let conn: AsyncDatabaseConnection;
		let db: DbService;
		let repo: BaseRepository<RawWrite>;

		beforeAll(async () => {
			conn = await createNapiConnection(url, 1, 1);
			db = createDbService(() => conn);
			repo = new BaseRepository(RawWrite, conn, { dialect });
		});

		beforeEach(async () => {
			await conn.execute("DROP TABLE IF EXISTS raw_writes");
			await conn.execute(
				"CREATE TABLE raw_writes (id INTEGER PRIMARY KEY, name VARCHAR(50), n INTEGER)",
			);
		});

		afterEach(() => {
			setAtlasStrictMode(false);
		});

		afterAll(async () => {
			await conn.execute("DROP TABLE IF EXISTS raw_writes");
			await conn.close();
		});

		async function rows(): Promise<
			Array<{ id: number; name: string; n: number }>
		> {
			const found = await db.from("raw_writes").orderBy("id");
			return found.map((r) => ({
				id: Number(r.id),
				name: String(r.name),
				n: Number(r.n),
			}));
		}

		it("insert() inlines a raw value with its bindings", async () => {
			await db.table("raw_writes").insert({
				id: 1,
				name: db.raw("upper(?)", ["ada"]),
				n: db.raw("? + ?", [2, 3]),
			});
			expect(await rows()).toEqual([{ id: 1, name: "ADA", n: 5 }]);
		});

		it("multiInsert() renumbers each row's raw bindings", async () => {
			await db.table("raw_writes").multiInsert([
				{ id: 2, name: db.raw("lower(?)", ["X"]), n: 1 },
				{ id: 3, name: db.raw("lower(:v)", { v: "Y" }), n: db.raw("1 + 1") },
			]);
			expect(await rows()).toEqual([
				{ id: 2, name: "x", n: 1 },
				{ id: 3, name: "y", n: 2 },
			]);
		});

		it("insert() takes RawSql.sql`` ($N notation)", async () => {
			await db
				.table("raw_writes")
				.insert({ id: 4, name: RawSql.sql`lower(${"ZED"})`, n: 9 });
			expect(await rows()).toEqual([{ id: 4, name: "zed", n: 9 }]);
		});

		it("an upsert row and its merge both take raw values", async () => {
			await db.table("raw_writes").insert({ id: 5, name: "old", n: 1 });
			await db
				.table("raw_writes")
				.insert({ id: 5, name: db.raw("lower(?)", ["NEW"]), n: 1 })
				.onConflict("id")
				.merge({ n: db.raw("? * 10", [7]) });
			expect(await rows()).toEqual([{ id: 5, name: "old", n: 70 }]);
		});

		it("repository create(), createMany() and save() inline raw values", async () => {
			await repo.create({ id: 6, name: db.raw("upper(?)", ["bob"]), n: 1 });
			await repo.createMany([
				{ id: 7, name: db.raw("lower(?)", ["CY"]), n: 2 },
				{ id: 8, name: "dee", n: db.raw("? + 1", [2]) },
			]);
			const found = await repo.find(6);
			if (!found) throw new Error("row 6 missing");
			found.n = db.raw("n + ?", [4]);
			await repo.save(found);
			expect(await rows()).toEqual([
				{ id: 6, name: "BOB", n: 5 },
				{ id: 7, name: "cy", n: 2 },
				{ id: 8, name: "dee", n: 3 },
			]);
		});

		it("strict mode refuses raw SQL on model writes but lets db.ref() through", async () => {
			await repo.create({ id: 9, name: "eve", n: 1 });
			setAtlasStrictMode(true);
			await expect(
				repo.create({ id: 10, name: db.raw("'x'"), n: 1 }),
			).rejects.toThrow(/strict mode/);
			const found = await repo.find(9);
			if (!found) throw new Error("row 9 missing");
			found.n = db.raw("n + 1");
			await expect(repo.save(found)).rejects.toThrow(/strict mode/);
			found.n = db.ref("id");
			await repo.save(found);
			expect(await rows()).toEqual([{ id: 9, name: "eve", n: 9 }]);
		});

		it("a raw whose bindings do not match its placeholders is refused", () => {
			expect(() =>
				db
					.table("raw_writes")
					.insert({ id: 11, name: new RawSql("upper(?)", []) })
					.toSQL(),
			).toThrow(/binding/);
		});
	},
);
