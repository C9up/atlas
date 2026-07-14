import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	beforeCreate,
	beforeDelete,
	beforeSave,
	Column,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// A counter the hooks bump — the Quietly variants must leave it untouched.
let hookCalls = 0;

class Gadget extends BaseModel {
	static override table = "gadgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@beforeSave()
	static bumpSave(): void {
		hookCalls++;
	}
	@beforeCreate()
	static bumpCreate(): void {
		hookCalls++;
	}
	@beforeDelete()
	static bumpDelete(): void {
		hookCalls++;
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE gadgets (id TEXT PRIMARY KEY, name TEXT)");
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	hookCalls = 0;
	await Gadget.truncate();
});

describe("atlas > *Quietly variants skip lifecycle hooks (Lucid parity)", () => {
	it("create() fires hooks; createQuietly() does not", async () => {
		await Gadget.create({ id: "1", name: "a" });
		expect(hookCalls).toBe(2); // beforeSave + beforeCreate

		hookCalls = 0;
		await Gadget.createQuietly({ id: "2", name: "b" });
		expect(hookCalls).toBe(0);
		// The row is still written — only the hooks are muted.
		expect((await Gadget.find("2"))?.name).toBe("b");
	});

	it("createManyQuietly() writes rows without hooks", async () => {
		await Gadget.createManyQuietly([
			{ id: "m1", name: "x" },
			{ id: "m2", name: "y" },
		]);
		expect(hookCalls).toBe(0);
		expect((await Gadget.all()).length).toBe(2);
	});

	it("saveQuietly() persists without firing beforeSave", async () => {
		const g = await Gadget.createQuietly({ id: "3", name: "c" });
		hookCalls = 0;
		g.name = "c2";
		await g.saveQuietly();
		expect(hookCalls).toBe(0);
		expect((await Gadget.find("3"))?.name).toBe("c2");
	});

	it("deleteQuietly() removes the row without firing beforeDelete", async () => {
		const g = await Gadget.createQuietly({ id: "4", name: "d" });
		hookCalls = 0;
		await g.deleteQuietly();
		expect(hookCalls).toBe(0);
		expect(await Gadget.find("4")).toBeNull();
	});
});
