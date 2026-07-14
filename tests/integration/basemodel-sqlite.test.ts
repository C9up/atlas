import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey, transaction } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// No @Entity — the table name is inferred from the class name (Widget → widgets).
class Widget extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@Column() declare kind: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE widgets (id TEXT PRIMARY KEY, name TEXT, kind TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > BaseModel (Active Record façade, sqlite)", () => {
	it("create() persists + static find() reads it back (table inferred, no @Entity)", async () => {
		const w = await Widget.create({ id: "1", name: "hammer", kind: "tool" });
		expect(w.$isPersisted).toBe(true);
		expect(w.$isLocal).toBe(true);

		const found = await Widget.find("1");
		expect(found?.name).toBe("hammer");
		expect(found?.$isLocal).toBe(false);

		const byKind = await Widget.findBy({ kind: "tool" });
		expect(byKind?.id).toBe("1");
	});

	it("instance save() updates dirty columns", async () => {
		const w = await Widget.find("1");
		if (!w) throw new Error("expected row");
		w.name = "mallet";
		await w.save();
		expect((await Widget.find("1"))?.name).toBe("mallet");
	});

	it("instance delete() sets $isDeleted and removes the row", async () => {
		const w = await Widget.find("1");
		if (!w) throw new Error("expected row");
		await w.delete();
		expect(w.$isDeleted).toBe(true);
		expect(await Widget.find("1")).toBeNull();
	});

	it("all() / first() / query() on the model class", async () => {
		await Widget.create({ id: "2", name: "a", kind: "x" });
		await Widget.create({ id: "3", name: "b", kind: "x" });
		expect((await Widget.all()).length).toBe(2);
		expect(await Widget.first()).not.toBeNull();
		const xs = await Widget.query().where("kind", "x").exec();
		expect(xs.length).toBe(2);
	});

	it("updateOrCreateMany / fetchOrCreateMany / fetchOrNewUpMany keyed by a unique column", async () => {
		await Widget.truncate();
		await Widget.create({ id: "k1", name: "orig", kind: "a" });

		// updateOrCreateMany: k1 updates, k2 inserts (one transaction).
		const uoc = await Widget.updateOrCreateMany("id", [
			{ id: "k1", name: "updated", kind: "a" },
			{ id: "k2", name: "new", kind: "b" },
		]);
		expect(uoc.length).toBe(2);
		expect((await Widget.find("k1"))?.name).toBe("updated");
		expect((await Widget.find("k2"))?.name).toBe("new");

		// fetchOrCreateMany: existing rows are returned UNTOUCHED, misses created.
		await Widget.fetchOrCreateMany("id", [
			{ id: "k1", name: "SHOULD-NOT-OVERWRITE", kind: "a" },
			{ id: "k3", name: "created", kind: "c" },
		]);
		expect((await Widget.find("k1"))?.name).toBe("updated");
		expect((await Widget.find("k3"))?.name).toBe("created");

		// fetchOrNewUpMany: a miss becomes an UNPERSISTED in-memory instance.
		const fresh = await Widget.fetchOrNewUpMany("id", [
			{ id: "k4", name: "ghost", kind: "d" },
		]);
		expect(fresh[0].$isPersisted).toBe(false);
		expect(await Widget.find("k4")).toBeNull();
	});

	it("save() throws on a deleted instance (Lucid $isDeleted guard)", async () => {
		await Widget.truncate();
		const w = await Widget.create({ id: "d1", name: "x", kind: "a" });
		await w.delete();
		expect(w.$isDeleted).toBe(true);
		await expect(w.save()).rejects.toThrow(/deleted/i);
	});

	it("$isDirty getter reflects unsaved changes (Lucid parity)", async () => {
		await Widget.truncate();
		const w = await Widget.create({ id: "dy1", name: "x", kind: "a" });
		expect(w.$isDirty).toBe(false); // clean snapshot right after persist
		w.name = "y";
		expect(w.$isDirty).toBe(true);
	});

	it("loadOnce() is a no-op when the relation is already populated", async () => {
		const w = new Widget();
		w.id = "lo1";
		// Pre-populate the relation slot; no REPO_REF is set, so if loadOnce tried
		// to actually load it would throw — proving it short-circuits.
		w.parent = { sentinel: true };
		await expect(w.loadOnce("parent")).resolves.toBe(w);
	});

	it("enableForceUpdate() re-persists current state even when nothing is dirty", async () => {
		await Widget.truncate();
		await Widget.create({ id: "fu1", name: "orig", kind: "a" });
		const w = await Widget.find("fu1");
		if (!w) throw new Error("expected row");
		// Mutate the row behind atlas's back — w stays non-dirty.
		await conn.execute("UPDATE widgets SET name = 'external' WHERE id = 'fu1'");
		await w.enableForceUpdate().save();
		// The forced UPDATE re-wrote w's in-memory 'orig' over the external change.
		expect((await Widget.find("fu1"))?.name).toBe("orig");
	});

	it("query().sideload() threads context onto every hydrated instance ($sideloaded)", async () => {
		await Widget.truncate();
		await Widget.create({ id: "s1", name: "a", kind: "z" });
		const [w] = await Widget.query()
			.where("kind", "z")
			.sideload({ tenantId: 42 })
			.exec();
		expect(w.$sideloaded).toEqual({ tenantId: 42 });
		// A plain query does not carry sideloaded context.
		const [plain] = await Widget.query().where("kind", "z").exec();
		expect(plain.$sideloaded).toEqual({});
	});

	it("query().pojo() returns raw rows without hydration", async () => {
		await Widget.truncate();
		await Widget.create({ id: "p1", name: "raw", kind: "z" });
		const rows = await Widget.query().where("kind", "z").pojo();
		expect(rows).toEqual([{ id: "p1", name: "raw", kind: "z" }]);
		expect(rows[0]).not.toBeInstanceOf(Widget);
	});

	it("useTransaction binds save() to a transaction (Lucid model.useTransaction)", async () => {
		await Widget.truncate();
		await transaction(conn, async (trx) => {
			const w = new Widget();
			w.id = "tx1";
			w.name = "n";
			w.kind = "k";
			expect(w.useTransaction(trx)).toBe(w); // chainable
			expect(w.$trx).toBe(trx);
			await w.save(); // runs inside trx
		});
		// committed → visible on the default connection
		expect((await Widget.find("tx1"))?.name).toBe("n");
	});
});
