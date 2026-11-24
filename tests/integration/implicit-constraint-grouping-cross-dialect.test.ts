/**
 * The constraints atlas adds itself are ANDed with the caller's clauses taken
 * as one group: a relation's key (`related().query()`), the correlation of
 * `whereHas()` / `withCount()` / `withAggregate()`, the parent keys of a
 * preload, the m2m pivot lookup, a repository scope and the `@SoftDeletes`
 * filter.
 *
 * Pushed flat, `where(a).orWhere(b)` read as `a OR (b AND <constraint>)`: a
 * user's `related('docs').query()` returned the documents of every other user,
 * its `update()` / `delete()` rewrote them, `whereHas()` matched parents with
 * no related row, and a soft-deleted account answered a login lookup by email
 * OR username. Runs on SQLite always, and on Postgres/MySQL when
 * ATLAS_TEST_PG_URL / ATLAS_TEST_MYSQL_URL are set.
 */
import "reflect-metadata";
import type { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import type { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { beforeFetch, beforeFind } from "../../src/decorators/hooks.js";
import {
	BaseModel,
	BelongsTo,
	Column,
	column,
	HasMany,
	HasManyThrough,
	ManyToMany,
	PrimaryKey,
	SoftDeletes,
} from "../../src/index.js";
import { ModelQuery } from "../../src/ModelQuery.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";
import { clearDb, setDb } from "../../src/services/db.js";

const TARGETS: Array<[AtlasDialect, string]> = [["sqlite", "sqlite::memory:"]];
if (process.env.ATLAS_TEST_PG_URL) {
	TARGETS.push(["postgres", process.env.ATLAS_TEST_PG_URL]);
}
if (process.env.ATLAS_TEST_MYSQL_URL) {
	TARGETS.push(["mysql", process.env.ATLAS_TEST_MYSQL_URL]);
}

class IcNote extends BaseModel {
	static override table = "ic_notes";
	@PrimaryKey() declare id: number;
	@Column() declare docId: number;
	@Column() declare body: string;
	@Column() declare flagged: number;
	@Column() declare urgent: number;
}

@SoftDeletes()
class IcDoc extends BaseModel {
	static override table = "ic_docs";
	@PrimaryKey() declare id: number;
	@Column() declare ownerId: number;
	@Column() declare title: string;
	@Column() declare published: number;
	@Column() declare pinned: number;
	@Column() declare views: number;
	@column.dateTime() declare deletedAt: DateTime | null;
	@BelongsTo(() => IcOwner, { foreignKey: "ownerId" }) declare owner: unknown;
}

class IcTeam extends BaseModel {
	static override table = "ic_teams";
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@Column() declare isOpen: number;
}

class IcOwner extends BaseModel {
	static override table = "ic_owners";
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
	@Column() declare admin: number;
	@HasMany(() => IcDoc, { foreignKey: "ownerId" }) declare docs: IcDoc[];
	@HasManyThrough(
		() => IcNote,
		() => IcDoc,
		{ firstKey: "ownerId", secondKey: "docId" },
	)
	declare notes: IcNote[];
	@ManyToMany(() => IcTeam, {
		pivotTable: "ic_owner_team",
		foreignKey: "owner_id",
		otherKey: "team_id",
	})
	declare teams: IcTeam[];
}

@SoftDeletes()
class IcAccount extends BaseModel {
	static override table = "ic_accounts";
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare username: string;
	@Column() declare credits: number;
	@Column() declare tenantId: number;
	@column.dateTime() declare deletedAt: DateTime | null;
}

/** A tenant filter written as a read hook — the hook sees the caller's clauses as one group. */
class IcTenantAccount extends BaseModel {
	static override table = "ic_accounts";
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare username: string;
	@Column() declare tenantId: number;

	@beforeFind()
	@beforeFetch()
	static tenant(q: ModelQuery<BaseEntity>): void {
		q.where("tenant_id", 1);
	}
}

/** `related(name).query()` is typed for every relation kind; narrow it. */
function asQuery(q: unknown): ModelQuery<BaseEntity> {
	if (!(q instanceof ModelQuery)) throw new Error("expected a ModelQuery");
	return q;
}

function pluck(rows: readonly BaseEntity[], key: string): unknown[] {
	return rows.map((r) => Reflect.get(r, key));
}

const TABLES = [
	"ic_notes",
	"ic_docs",
	"ic_teams",
	"ic_owner_team",
	"ic_owners",
	"ic_accounts",
];

describe.each(TARGETS)(
	"atlas > implicit constraints group the caller's clauses on %s",
	(dialect, url) => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			setAtlasDialect(dialect);
			conn = await createNapiConnection(url, 1, 2);
			for (const t of TABLES) await conn.execute(`DROP TABLE IF EXISTS ${t}`);
			await conn.execute(
				"CREATE TABLE ic_owners (id integer PRIMARY KEY, name varchar(50), admin integer)",
			);
			await conn.execute(
				"CREATE TABLE ic_docs (id integer PRIMARY KEY, owner_id integer, title varchar(50), published integer, pinned integer, views integer, deleted_at varchar(40))",
			);
			await conn.execute(
				"CREATE TABLE ic_notes (id integer PRIMARY KEY, doc_id integer, body varchar(50), flagged integer, urgent integer)",
			);
			await conn.execute(
				"CREATE TABLE ic_teams (id integer PRIMARY KEY, name varchar(50), is_open integer)",
			);
			await conn.execute(
				"CREATE TABLE ic_owner_team (owner_id integer, team_id integer)",
			);
			await conn.execute(
				"CREATE TABLE ic_accounts (id integer PRIMARY KEY, email varchar(50), username varchar(50), credits integer, tenant_id integer, deleted_at varchar(40))",
			);
			setDb(conn);
		});

		beforeEach(async () => {
			for (const t of TABLES) await conn.execute(`DELETE FROM ${t}`);
			await conn.execute(
				"INSERT INTO ic_owners VALUES (1, 'alice', 0), (2, 'bob', 1), (3, 'carol', 0)",
			);
			// alice: one published doc and one she deleted. bob: one PINNED
			// private doc and a draft. carol: nothing.
			await conn.execute(
				"INSERT INTO ic_docs VALUES (10, 1, 'alice public', 1, 0, 5, NULL), (11, 1, 'alice trashed', 1, 1, 7, '2026-01-01T00:00:00.000Z'), (20, 2, 'bob secret', 0, 1, 1000, NULL), (21, 2, 'bob draft', 0, 0, 1, NULL)",
			);
			await conn.execute(
				"INSERT INTO ic_notes VALUES (100, 10, 'alice note', 1, 0), (200, 20, 'bob urgent note', 0, 1)",
			);
			await conn.execute(
				"INSERT INTO ic_teams VALUES (100, 'alice team', 0), (200, 'bob team', 1)",
			);
			await conn.execute("INSERT INTO ic_owner_team VALUES (1, 100), (2, 200)");
			// A banned (soft-deleted) account, a live one, another tenant's.
			await conn.execute(
				"INSERT INTO ic_accounts VALUES (1, 'banned@x.io', 'banned', 0, 1, '2026-01-01T00:00:00.000Z'), (2, 'live@x.io', 'live', 0, 1, NULL), (3, 'other@x.io', 'other', 0, 2, NULL)",
			);
		});

		afterAll(async () => {
			clearDb(conn);
			for (const t of TABLES) await conn.execute(`DROP TABLE IF EXISTS ${t}`);
			await conn.close();
		});

		const login = (value: string): ModelQuery<IcAccount> =>
			IcAccount.query().where("email", value).orWhere("username", value);

		async function titlesOf(ids: string): Promise<string[]> {
			const rows = await conn.query<{ title: string }>(
				`SELECT title FROM ic_docs WHERE id IN (${ids}) ORDER BY id`,
			);
			return rows.map((r) => r.title);
		}

		describe("related().query()", () => {
			it("hasMany: an orWhere stays inside the parent's rows", async () => {
				const alice = await IcOwner.findOrFail(1);
				const q = asQuery(alice.related("docs").query())
					.where("published", 1)
					.orWhere("pinned", 1);
				expect(pluck(await q.orderBy("id").exec(), "title")).toEqual([
					"alice public",
				]);
			});

			it("belongsTo: an orWhere does not answer another owner", async () => {
				const doc = await IcDoc.findOrFail(10);
				const q = asQuery(doc.related("owner").query())
					.where("name", "nobody")
					.orWhere("admin", 1);
				expect(await q.exec()).toEqual([]);
			});

			it("many-to-many: an orWhere does not reach a team the parent is not in", async () => {
				const alice = await IcOwner.findOrFail(1);
				const q = asQuery(alice.related("teams").query())
					.where("is_open", 1)
					.orWhere("name", "no such team");
				expect(await q.exec()).toEqual([]);
			});

			it("hasManyThrough: an orWhere does not reach a note of another parent's doc", async () => {
				const alice = await IcOwner.findOrFail(1);
				const q = asQuery(alice.related("notes").query())
					.where("flagged", 1)
					.orWhere("urgent", 1);
				expect(pluck(await q.orderBy("id").exec(), "body")).toEqual([
					"alice note",
				]);
			});

			it("clearWhere() drops the caller's clauses, never the relation's key", async () => {
				const alice = await IcOwner.findOrFail(1);
				const q = asQuery(alice.related("docs").query())
					.where("published", 0)
					.clearWhere();
				expect(pluck(await q.exec(), "title")).toEqual(["alice public"]);
			});

			it("update() and delete() built from the relation stay inside it", async () => {
				const alice = await IcOwner.findOrFail(1);
				await asQuery(alice.related("docs").query())
					.where("published", 0)
					.orWhere("pinned", 1)
					.update({ title: "rewritten" });
				await asQuery(alice.related("docs").query())
					.where("title", "no such title")
					.orWhere("title", "bob draft")
					.delete();
				expect(await titlesOf("10, 11, 20, 21")).toEqual([
					"alice public",
					"alice trashed",
					"bob secret",
					"bob draft",
				]);
				const live = await conn.query<{ n: number }>(
					"SELECT COUNT(*) AS n FROM ic_docs WHERE deleted_at IS NULL",
				);
				expect(Number(live[0]?.n)).toBe(3);
			});

			it("increment() through the relation credits the parent's rows only", async () => {
				const alice = await IcOwner.findOrFail(1);
				await asQuery(alice.related("docs").query())
					.where("id", 10)
					.orWhere("id", 20)
					.increment("views", 1);
				const rows = await conn.query<{ id: number; views: number }>(
					"SELECT id, views FROM ic_docs WHERE id IN (10, 20) ORDER BY id",
				);
				expect(rows.map((r) => Number(r.views))).toEqual([6, 1000]);
			});

			it("many-to-many update()/delete() touch only the parent's related rows", async () => {
				const alice = await IcOwner.findOrFail(1);
				const updated = await asQuery(alice.related("teams").query()).update({
					name: "renamed",
				});
				expect(Number(updated)).toBe(1);
				await asQuery(alice.related("teams").query())
					.where("id", 200)
					.orWhere("is_open", 1)
					.delete();
				const rows = await conn.query<{ id: number; name: string }>(
					"SELECT id, name FROM ic_teams ORDER BY id",
				);
				expect(rows.map((r) => [Number(r.id), r.name])).toEqual([
					[100, "renamed"],
					[200, "bob team"],
				]);
			});

			it("a scoped repository's many-to-many write stays inside the parent and the scope", async () => {
				const repo = new BaseRepository(IcOwner, conn, {
					dialect,
					scope: (q, table) => {
						if (table === "ic_teams") q.where("is_open", 0);
					},
				});
				const alice = await repo.findOrFail(1);
				const n = await asQuery(alice.related("teams").query())
					.where("id", 100)
					.orWhere("id", 200)
					.update({ name: "scoped" });
				expect(Number(n)).toBe(1);
				const rows = await conn.query<{ name: string }>(
					"SELECT name FROM ic_teams ORDER BY id",
				);
				expect(rows.map((r) => r.name)).toEqual(["scoped", "bob team"]);
			});
		});

		describe("whereHas() / withCount() / withAggregate()", () => {
			it("whereHas: an orWhere does not match parents with no related row", async () => {
				const q = IcOwner.query().whereHas("docs", (sub) => {
					sub.where("published", 1).orWhere("pinned", 1);
				});
				expect(pluck(await q.orderBy("id").exec(), "name")).toEqual([
					"alice",
					"bob",
				]);
			});

			it("withCount: counts each parent's own rows, trashed ones left out", async () => {
				const owners = await IcOwner.query()
					.withCount("docs", (sub) => {
						sub.as("visible").where("published", 1).orWhere("pinned", 1);
					})
					.orderBy("id")
					.exec();
				expect(owners.map((o) => Number(o.getExtra("visible")))).toEqual([
					1, 1, 0,
				]);
			});

			it("withAggregate: sums each parent's own rows", async () => {
				const owners = await IcOwner.query()
					.withAggregate("docs", (sub) => {
						sub
							.sum("views")
							.as("reach")
							.where("published", 1)
							.orWhere("pinned", 1);
					})
					.orderBy("id")
					.exec();
				// Carol has no row: her sum is NULL, not another parent's views.
				expect(
					owners.map((o) => {
						const reach = o.getExtra("reach");
						return reach == null ? null : Number(reach);
					}),
				).toEqual([5, 1000, null]);
			});

			it("whereHas on a @SoftDeletes relation does not match by a trashed row", async () => {
				const q = IcOwner.query().whereHas("docs", (sub) => {
					sub.where("title", "alice trashed");
				});
				expect(await q.exec()).toEqual([]);
				const withTrashed = IcOwner.query().whereHas("docs", (sub) => {
					sub.withTrashed().where("title", "alice trashed");
				});
				expect(pluck(await withTrashed.exec(), "name")).toEqual(["alice"]);
			});
		});

		describe("@SoftDeletes", () => {
			it("first(): an email-or-username lookup does not find a trashed account", async () => {
				expect(await login("banned@x.io").first()).toBeNull();
				expect((await login("live").first())?.id).toBe(2);
			});

			it("paginate(): neither the total nor the page holds a trashed row", async () => {
				const page = await login("banned@x.io").paginate(1, 10);
				expect(page.total).toBe(0);
				expect(page.all()).toEqual([]);
			});

			it("preload(): the callback's orWhere does not bring a trashed child back", async () => {
				const owners = await IcOwner.query()
					.where("id", 1)
					.preload("docs", (q) => q.where("published", 0).orWhere("pinned", 1))
					.exec();
				expect(owners[0]?.docs).toEqual([]);
			});

			it("update() / increment() / delete() leave the trashed row alone", async () => {
				await login("banned@x.io").update({ username: "rewritten" });
				await IcAccount.query()
					.where("email", "banned@x.io")
					.orWhere("email", "nobody@x.io")
					.increment("credits", 50);
				await IcAccount.query()
					.where("email", "banned@x.io")
					.orWhere("email", "nobody@x.io")
					.delete();
				const rows = await conn.query<{
					username: string;
					credits: number;
					deleted_at: string;
				}>(
					"SELECT username, credits, deleted_at FROM ic_accounts WHERE id = 1",
				);
				expect(rows[0]?.username).toBe("banned");
				expect(Number(rows[0]?.credits)).toBe(0);
				expect(rows[0]?.deleted_at.startsWith("2026-01-01")).toBe(true);
			});

			it("onlyTrashed() groups the same way; withTrashed() adds no filter", async () => {
				const trashed = await IcAccount.query()
					.onlyTrashed()
					.where("email", "live@x.io")
					.orWhere("username", "banned")
					.exec();
				expect(pluck(trashed, "id")).toEqual([1]);
				const all = await IcAccount.query()
					.withTrashed()
					.where("email", "live@x.io")
					.orWhere("username", "banned")
					.orderBy("id")
					.exec();
				expect(pluck(all, "id")).toEqual([1, 2]);
			});

			it("restore() reaches only trashed rows, whatever the caller's orWhere", async () => {
				await conn.execute(
					"UPDATE ic_accounts SET deleted_at = '2026-02-02T00:00:00.000Z' WHERE id = 3",
				);
				const repo = new BaseRepository(IcAccount, conn, {
					dialect,
					scope: (q) => {
						q.where("tenant_id", 1);
					},
				});
				const n = await repo
					.query()
					.where("email", "banned@x.io")
					.orWhere("email", "other@x.io")
					.restore();
				expect(Number(n)).toBe(1);
				const rows = await conn.query<{
					id: number;
					deleted_at: string | null;
				}>("SELECT id, deleted_at FROM ic_accounts ORDER BY id");
				expect(rows.map((r) => [Number(r.id), r.deleted_at])).toEqual([
					[1, null],
					[2, null],
					[3, "2026-02-02T00:00:00.000Z"],
				]);
			});
		});

		describe("read hooks", () => {
			it("a filter a beforeFind / beforeFetch hook adds guards every OR branch", async () => {
				const q = (): ModelQuery<IcTenantAccount> =>
					IcTenantAccount.query()
						.where("email", "other@x.io")
						.orWhere("username", "other");
				expect(await q().first()).toBeNull();
				expect(await q().exec()).toEqual([]);
				const page = await q().paginate(1, 10);
				expect(page.all()).toEqual([]);
			});
		});

		describe("the compiled statement", () => {
			it("ANDed clauses stay flat; an OR is grouped before the guard", () => {
				expect(IcAccount.query().where("email", "a").toSQL().sql).not.toContain(
					"(",
				);
				const sql = login("a").toSQL().sql;
				expect(sql.replaceAll("`", '"')).toContain(
					'("email" = ? OR "username" = ?) AND "deleted_at" IS NULL',
				);
			});

			it("wrapExisting() closes the chained clauses into their own group", () => {
				const sql = IcAccount.query()
					.withTrashed()
					.where("email", "a")
					.orWhere("username", "a")
					.wrapExisting()
					.where("tenant_id", 1)
					.orWhere("tenant_id", 2)
					.toSQL()
					.sql.replaceAll("`", '"');
				expect(sql).toContain(
					'WHERE ("email" = ? OR "username" = ?) AND ("tenant_id" = ? OR "tenant_id" = ?)',
				);
			});
		});
	},
);
