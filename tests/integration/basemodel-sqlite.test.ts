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
