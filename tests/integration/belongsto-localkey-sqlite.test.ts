/**
 * Two relation-config fixes against real SQLite:
 *
 *   P1 — @BelongsTo writes only via associate/dissociate. The FK is on THIS
 *        model, so create/save/createMany/saveMany would write the wrong side;
 *        they must throw, not silently corrupt (AdonisJS Lucid parity — Lucid's
 *        belongsTo client has no persist-through methods).
 *   P2 — @ManyToMany({ localKey }) must actually be honoured: the pivot FK stores
 *        `parent[localKey]`, not always the parent PK.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	BelongsTo,
	Column,
	ManyToMany,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class BtAuthor extends BaseModel {
	static override table = "bt_authors";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

class BtPost extends BaseModel {
	static override table = "bt_posts";
	@PrimaryKey() declare id: string;
	@Column() declare authorId: string | null;
	@BelongsTo(() => BtAuthor, { foreignKey: "author_id" })
	declare author: BtAuthor | null;
}

class LkRole extends BaseModel {
	static override table = "lk_roles";
	@PrimaryKey() declare id: string;
}

class LkUser extends BaseModel {
	static override table = "lk_users";
	@PrimaryKey() declare id: string;
	@Column() declare code: string;
	// The pivot FK references the parent's `code`, NOT its `id`.
	@ManyToMany(() => LkRole, {
		pivotTable: "lk_pivot",
		foreignKey: "user_code",
		otherKey: "role_id",
		localKey: "code",
	})
	declare roles: LkRole[];
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE bt_authors (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE bt_posts (id TEXT PRIMARY KEY, author_id TEXT)",
	);
	await conn.execute("CREATE TABLE lk_users (id TEXT PRIMARY KEY, code TEXT)");
	await conn.execute("CREATE TABLE lk_roles (id TEXT PRIMARY KEY)");
	await conn.execute("CREATE TABLE lk_pivot (user_code TEXT, role_id TEXT)");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	for (const t of [
		"bt_posts",
		"bt_authors",
		"lk_pivot",
		"lk_roles",
		"lk_users",
	]) {
		await conn.execute(`DELETE FROM ${t}`);
	}
});

describe("atlas > @BelongsTo writes only via associate/dissociate (P1)", () => {
	it("associate() sets the FK on THIS model and saves it; dissociate() clears it", async () => {
		const author = await BtAuthor.create({ id: "a1", name: "Ada" });
		const post = await BtPost.create({ id: "p1", authorId: null });

		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel.associate(author);

		expect(post.authorId).toBe("a1");
		const [linked] = await conn.query<Record<string, unknown>>(
			"SELECT author_id FROM bt_posts WHERE id = 'p1'",
		);
		expect(linked.author_id).toBe("a1");

		await rel.dissociate();
		expect(post.authorId).toBeNull();
		const [cleared] = await conn.query<Record<string, unknown>>(
			"SELECT author_id FROM bt_posts WHERE id = 'p1'",
		);
		expect(cleared.author_id).toBeNull();
	});

	it("create/save/createMany/saveMany throw (they'd write the wrong side)", async () => {
		const post = await BtPost.create({ id: "p2", authorId: null });
		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");

		await expect(rel.create({ id: "a9", name: "X" })).rejects.toThrow(
			/not supported on @BelongsTo/i,
		);
		await expect(rel.save(new BtAuthor())).rejects.toThrow(
			/not supported on @BelongsTo/i,
		);
		await expect(rel.createMany([{ id: "a9" }])).rejects.toThrow(
			/not supported on @BelongsTo/i,
		);
		await expect(rel.saveMany([new BtAuthor()])).rejects.toThrow(
			/not supported on @BelongsTo/i,
		);
		// No author row leaked from the rejected writes.
		expect(await BtAuthor.find("a9")).toBeNull();
	});

	it("associate() persists an UNSAVED owner first, then links the parent (Lucid)", async () => {
		const post = await BtPost.create({ id: "p3", authorId: null });
		const author = new BtAuthor();
		author.id = "a2";
		author.name = "Grace";
		expect(author.$isPersisted).toBe(false);

		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");
		await rel.associate(author);

		// The owner was saved (no orphan FK) and the parent now points at it.
		expect(author.$isPersisted).toBe(true);
		expect(await BtAuthor.find("a2")).not.toBeNull();
		expect(post.authorId).toBe("a2");
		const [linked] = await conn.query<Record<string, unknown>>(
			"SELECT author_id FROM bt_posts WHERE id = 'p3'",
		);
		expect(linked.author_id).toBe("a2");
	});

	it("associate() rejects an owner with no usable key (no silent no-op)", async () => {
		await BtAuthor.create({ id: "real", name: "R" });
		const post = await BtPost.create({ id: "p4", authorId: null });
		// A persisted keyless projection — associate() must refuse, not set the FK
		// to `undefined` (which the UPDATE would silently skip).
		const [proj] = await BtAuthor.query().select("COUNT(*) as n").exec();
		const rel = post.related("author");
		if (rel.type !== "belongsTo") throw new Error("expected belongsTo");

		await expect(rel.associate(proj)).rejects.toThrow(/owner .* has no id/i);
		expect(post.authorId).toBeNull(); // FK untouched
	});
});

describe("atlas > @ManyToMany({ localKey }) targets the local key, not the PK (P2)", () => {
	it("stores parent[localKey] in the pivot FK", async () => {
		const user = await LkUser.create({ id: "u1", code: "ABC" });
		await LkRole.create({ id: "r1" });

		const roles = user.related("roles");
		if (roles.type !== "manyToMany") throw new Error("expected m2m");
		await roles.attach(["r1"]);

		const [pivot] = await conn.query<Record<string, unknown>>(
			"SELECT user_code, role_id FROM lk_pivot",
		);
		// The FK carries the `code` ("ABC"), NOT the PK ("u1") — proof localKey is honoured.
		expect(pivot.user_code).toBe("ABC");
		expect(pivot.role_id).toBe("r1");
	});

	it("eager preload reads back a localKey-written pivot (write/read symmetry)", async () => {
		const user = await LkUser.create({ id: "u1", code: "ABC" });
		await LkRole.create({ id: "r1" });
		const roles = user.related("roles");
		if (roles.type !== "manyToMany") throw new Error("expected m2m");
		await roles.attach(["r1"]);

		// Before the fix, preload queried `user_code IN ('u1')` (the PK) while attach
		// wrote `user_code = 'ABC'` (the localKey) — the relation never read back.
		const [loaded] = await LkUser.query()
			.where("id", "u1")
			.preload("roles")
			.exec();
		expect(loaded.roles.map((r) => r.id)).toEqual(["r1"]);
	});

	it("the missing-key guard names the localKey, not 'id' (P3)", async () => {
		await LkUser.create({ id: "u2", code: "XYZ" });
		// A projection selecting the PK but NOT `code` → parent[localKey] is absent.
		const [proj] = await LkUser.query().select("id").exec();
		const roles = LkUser.$repo().relatedProxy(proj, "roles");
		if (roles.type !== "manyToMany") throw new Error("expected m2m");

		// Diagnostic must point at 'code' (the localKey the pivot FK references),
		// not the primary key 'id'.
		await expect(roles.attach(["r1"])).rejects.toThrow(/key \('code'\)/i);
	});
});
