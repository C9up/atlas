/**
 * `upsert()` on a repository built with a `scope` never updates a row outside
 * the scope.
 *
 * The conflict branch used to rewrite any row the conflict target matched: a
 * tenant's bulk upsert keyed on the id rewrote another tenant's row, and took
 * it over when the tenant column was among the updated ones. The update branch
 * now carries the scope as its condition on PostgreSQL and SQLite; MySQL's
 * `ON DUPLICATE KEY UPDATE` takes no condition, so an updating upsert is
 * refused there. Runs on SQLite always, and on Postgres/MySQL when
 * ATLAS_TEST_PG_URL / ATLAS_TEST_MYSQL_URL are set.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { BaseEntity, Column, Entity, PrimaryKey } from "../../src/index.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

@Entity("su_docs")
class SuDoc extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare tenantId: number;
	@Column() declare title: string;
}

describe.each(TARGETS)(
	"atlas > upsert() on a scoped repository on %s",
	(dialect, url) => {
		let conn: AsyncDatabaseConnection;
		let repo: BaseRepository<SuDoc>;

		async function rows(): Promise<Array<[number, number, string]>> {
			const out = await conn.query<{
				id: number;
				tenant_id: number;
				title: string;
			}>("SELECT id, tenant_id, title FROM su_docs ORDER BY id");
			return out.map((r) => [Number(r.id), Number(r.tenant_id), r.title]);
		}

		beforeAll(async () => {
			setAtlasDialect(dialect);
			conn = await createNapiConnection(url, 1, 2);
			await conn.execute("DROP TABLE IF EXISTS su_docs");
			await conn.execute(
				"CREATE TABLE su_docs (id integer PRIMARY KEY, tenant_id integer, title varchar(80))",
			);
			// The repository of a request made by tenant 1.
			repo = new BaseRepository(SuDoc, conn, {
				dialect,
				scope: (q) => {
					q.where("tenant_id", 1);
				},
			});
		});

		beforeEach(async () => {
			await conn.execute("DELETE FROM su_docs");
			await conn.execute(
				"INSERT INTO su_docs VALUES (1, 1, 'mine'), (2, 2, 'tenant 2 contract')",
			);
		});

		afterAll(async () => {
			await conn.execute("DROP TABLE IF EXISTS su_docs");
			await conn.close();
		});

		if (dialect === "mysql") {
			it("refuses an updating upsert, and writes nothing", async () => {
				await expect(
					repo.upsert(
						[{ id: 2, tenantId: 1, title: "rewritten" }],
						["id"],
						["title", "tenantId"],
					),
				).rejects.toMatchObject({ code: "E_UNSUPPORTED" });
				expect(await rows()).toEqual([
					[1, 1, "mine"],
					[2, 2, "tenant 2 contract"],
				]);
			});

			it("still runs an upsert that updates nothing (INSERT IGNORE)", async () => {
				await repo.upsert(
					[
						{ id: 2, tenantId: 1, title: "rewritten" },
						{ id: 3, tenantId: 1, title: "new" },
					],
					["id"],
				);
				expect(await rows()).toEqual([
					[1, 1, "mine"],
					[2, 2, "tenant 2 contract"],
					[3, 1, "new"],
				]);
			});
			return;
		}

		it("leaves a conflicting row outside the scope untouched", async () => {
			const affected = await repo.upsert(
				[{ id: 2, tenantId: 1, title: "rewritten by tenant 1" }],
				["id"],
				["title", "tenantId"],
			);
			expect(affected).toBe(0);
			expect(await rows()).toEqual([
				[1, 1, "mine"],
				[2, 2, "tenant 2 contract"],
			]);
			expect(await repo.find(2)).toBeNull();
		});

		it("still updates a conflicting row inside the scope and inserts a new one", async () => {
			await repo.upsert(
				[
					{ id: 1, tenantId: 1, title: "mine, updated" },
					{ id: 2, tenantId: 1, title: "rewritten" },
					{ id: 3, tenantId: 1, title: "new" },
				],
				["id"],
				["title"],
			);
			expect(await rows()).toEqual([
				[1, 1, "mine, updated"],
				[2, 2, "tenant 2 contract"],
				[3, 1, "new"],
			]);
		});

		it("an unscoped repository updates as before", async () => {
			const open = new BaseRepository(SuDoc, conn, { dialect });
			await open.upsert(
				[{ id: 2, tenantId: 2, title: "updated" }],
				["id"],
				["title"],
			);
			expect((await rows())[1]).toEqual([2, 2, "updated"]);
		});
	},
);
