/**
 * The raw-fragment binding grammar, on every dialect available.
 *
 * A `db.raw()` / `RawSql` fragment keeps its own bindings wherever it is
 * embedded: a fragment `$1` used to point at the OUTER statement's first
 * parameter inside update()/merge(), so a column received the value bound for
 * another one. `rawQuery()` refuses a binding count that does not match its
 * placeholders instead of binding NULL, leaves unknown `:name` text alone, and
 * honours the `\?` escape. A `db.raw()` used as a where VALUE is inlined, not
 * bound as a JSON object (which made `whereNot` match every row).
 *
 * SQLite always runs; Postgres and MySQL run with ATLAS_TEST_PG_URL /
 * ATLAS_TEST_MYSQL_URL.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import type { AtlasDialect } from "../../src/query/native.js";
import { RawSql } from "../../src/query/QueryBuilder.js";
import { createDbService, type DbService } from "../../src/services/db.js";

@Entity("probe_users")
class ProbeUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare role: string;
	@Column() declare bio: string;
}

const targets: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL)
	targets.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
if (process.env.ATLAS_TEST_MYSQL_URL)
	targets.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);

describe.each(targets)("atlas > raw binding grammar on %s", (dialect, url) => {
	let conn: AsyncDatabaseConnection;
	let db: DbService;
	let repo: BaseRepository<ProbeUser>;

	beforeAll(async () => {
		conn = await createNapiConnection(url, 1, 1);
		db = createDbService(() => conn);
		repo = new BaseRepository(ProbeUser, conn, { dialect });
	});

	afterAll(async () => {
		await conn.execute("DROP TABLE IF EXISTS probe_users");
		await conn.close();
	});

	beforeEach(async () => {
		await conn.execute("DROP TABLE IF EXISTS probe_users");
		await conn.execute(
			"CREATE TABLE probe_users (id INTEGER PRIMARY KEY, role VARCHAR(50), bio VARCHAR(50))",
		);
		await conn.execute(
			"INSERT INTO probe_users (id, role, bio) VALUES (1, 'user', 'hello'), (2, 'admin', 'x')",
		);
	});

	async function row(id: number): Promise<Record<string, unknown>> {
		const rows = await db.from("probe_users").where("id", id);
		const [found] = rows;
		if (found === undefined) throw new Error(`row ${id} missing`);
		return found;
	}

	describe("a fragment keeps its own bindings inside update()/merge()", () => {
		it("named db.raw in a model update binds its own value", async () => {
			await repo
				.query()
				.where("id", 1)
				.update({ bio: "admin", role: db.raw(":r", { r: "member" }) });
			expect(await row(1)).toMatchObject({ role: "member", bio: "admin" });
		});

		it("?? identifier + value db.raw in an update", async () => {
			await repo
				.query()
				.where("id", 1)
				.update({ bio: "ZZ", role: db.raw("lower(??) || ?", ["bio", "!"]) });
			// SET expressions read the row as it was; MySQL reads `||` as OR
			// without PIPES_AS_CONCAT, and assigns left to right.
			if (dialect !== "mysql") {
				expect(await row(1)).toMatchObject({ role: "hello!", bio: "ZZ" });
			}
		});

		it("RawSql.sql`` in an update binds its template value", async () => {
			await repo
				.query()
				.where("id", 1)
				.update({ bio: "admin", role: RawSql.sql`lower(${"USER"})` });
			expect(await row(1)).toMatchObject({ role: "user", bio: "admin" });
		});

		it("named db.raw in a db builder update", async () => {
			await db
				.from("probe_users")
				.where("id", 1)
				.update({ bio: "admin", role: db.raw(":r", { r: "member" }) });
			expect(await row(1)).toMatchObject({ role: "member", bio: "admin" });
		});

		it("named db.raw in an upsert merge binds its own value", async () => {
			await db
				.table("probe_users")
				.insert({ id: 1, role: "x", bio: "admin" })
				.onConflict("id")
				.merge({ role: db.raw(":r", { r: "merged" }) });
			expect(await row(1)).toMatchObject({ role: "merged", bio: "hello" });
		});

		it("a raw whose bindings do not match its placeholders is refused", () => {
			expect(() =>
				repo
					.query()
					.where("id", 1)
					.update({ role: new RawSql("now()", ["dropped"]) })
					.toSQL(),
			).toThrow(/binding/);
			expect(() =>
				repo
					.query()
					.where("id", 1)
					.update({ role: new RawSql("$1", []) })
					.toSQL(),
			).toThrow(/\$1/);
		});
	});

	describe("rawQuery() binding resolution", () => {
		it("refuses fewer or more values than placeholders", async () => {
			await expect(db.rawQuery("SELECT ? AS a, ? AS b", [1])).rejects.toThrow(
				/binding/,
			);
			await expect(db.rawQuery("SELECT ? AS a", [1, 2])).rejects.toThrow(
				/binding/,
			);
		});

		it("refuses an undefined value instead of binding NULL", async () => {
			await expect(db.rawQuery("SELECT ? AS a", [undefined])).rejects.toThrow(
				/undefined/i,
			);
			await expect(
				db.rawQuery("SELECT :v AS a", { v: undefined }),
			).rejects.toThrow(/undefined/i);
		});

		it("leaves a :name that is not a binding key as written", async () => {
			const rows = await db.rawQuery<{ t: string; id: unknown }>(
				"SELECT '10:30:00' AS t, :id AS id",
				{ id: 7 },
			);
			expect(rows[0]?.t).toBe("10:30:00");
			expect(Number(rows[0]?.id)).toBe(7);
		});

		it("does not bind NULL for an unknown key", () => {
			const q = db.rawQuery("SELECT :missing AS m, :id AS id", { id: 7 });
			expect(q.toSQL().bindings).toEqual([7]);
			expect(q.toSQL().sql).toContain(":missing");
		});

		it("?? in whereRaw inlines a quoted identifier", async () => {
			const rows = await db
				.from("probe_users")
				.whereRaw("?? = ?", ["role", "admin"]);
			expect(rows.map((r) => Number(r.id))).toEqual([2]);
		});

		it("whereRaw refuses a binding count mismatch", () => {
			expect(() =>
				db.from("probe_users").whereRaw("id = ?", [1, 2]).toSQL(),
			).toThrow(/binding/);
		});

		if (dialect === "postgres") {
			it("\\? reaches Postgres as the jsonb operator", async () => {
				const rows = await db.rawQuery<{ has: boolean; v: unknown }>(
					"SELECT '{\"a\":1}'::jsonb \\? 'a' AS has, ? AS v",
					[5],
				);
				expect(rows[0]?.has).toBe(true);
				const viaWhere = await db
					.from("probe_users")
					.whereRaw("'{\"a\":1}'::jsonb \\? ?", ["a"])
					.where("id", 1);
				expect(viaWhere).toHaveLength(1);
			});
		}
	});

	describe("a db.raw() used as a where value is inlined", () => {
		it("whereNot(col, db.raw) excludes the matching rows (model query)", async () => {
			const rows = await repo.query().whereNot("role", db.raw("'admin'"));
			expect(rows.map((u) => u.role)).toEqual(["user"]);
		});

		it("whereNot(col, db.raw) excludes the matching rows (db builder)", async () => {
			const rows = await db
				.from("probe_users")
				.whereNot("role", db.raw("'admin'"));
			expect(rows.map((r) => r.role)).toEqual(["user"]);
		});

		it("where(col, db.raw with bindings) matches", async () => {
			const rows = await repo
				.query()
				.where("role", db.raw("lower(?)", ["USER"]));
			expect(rows.map((u) => u.id)).toEqual([1]);
			const viaDb = await db
				.from("probe_users")
				.where("role", "!=", db.raw("lower(?)", ["USER"]));
			expect(viaDb.map((r) => Number(r.id))).toEqual([2]);
		});

		it("orWhere and having take a raw value too", async () => {
			const rows = await repo
				.query()
				.where("id", 0)
				.orWhere("role", db.raw("'admin'"));
			expect(rows.map((u) => u.id)).toEqual([2]);
			const groups = await db
				.from("probe_users")
				.select("role")
				.groupBy("role")
				.having("role", "=", db.raw("'admin'"));
			expect(groups.map((g) => g.role)).toEqual(["admin"]);
		});

		it("a raw value is never bound as a parameter", () => {
			const { bindings } = repo
				.query()
				.whereNot("role", db.raw("'admin'"))
				.toSQL();
			expect(bindings).toEqual([]);
		});
	});
});
