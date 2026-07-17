import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	afterCreate,
	afterFind,
	afterSave,
	afterUpdate,
	BaseModel,
	beforeCreate,
	beforeFind,
	beforeSave,
	beforeUpdate,
	Column,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

// AdonisJS/Lucid order: the SPECIFIC before-hook fires BEFORE the general
// `beforeSave`; the AFTER side is specific-then-`afterSave`.
const order: string[] = [];

class Gadget extends BaseModel {
	static override table = "ho_gadgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@beforeCreate() static bc(): void {
		order.push("beforeCreate");
	}
	@beforeUpdate() static bu(): void {
		order.push("beforeUpdate");
	}
	@beforeSave() static bs(): void {
		order.push("beforeSave");
	}
	@afterCreate() static ac(): void {
		order.push("afterCreate");
	}
	@afterUpdate() static au(): void {
		order.push("afterUpdate");
	}
	@afterSave() static as(): void {
		order.push("afterSave");
	}
	// save() must NOT fire these — it decides insert-vs-update from $isPersisted,
	// never a find() SELECT (Lucid parity). Registered so the exact-array
	// assertions below catch any spurious read-hook firing.
	@beforeFind() static bf(): void {
		order.push("beforeFind");
	}
	@afterFind() static af(): void {
		order.push("afterFind");
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ho_gadgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(() => {
	order.length = 0;
});

describe("atlas > persistence hook order (Lucid parity, sqlite e2e)", () => {
	it("create(): beforeCreate → beforeSave → INSERT → afterCreate → afterSave", async () => {
		await Gadget.create({ id: "g1", name: "a" });
		expect(order).toEqual([
			"beforeCreate",
			"beforeSave",
			"afterCreate",
			"afterSave",
		]);
	});

	it("save() insert: beforeCreate → beforeSave → INSERT → afterCreate → afterSave", async () => {
		const g = new Gadget();
		g.id = "g2";
		g.name = "b";
		await g.save();
		expect(order).toEqual([
			"beforeCreate",
			"beforeSave",
			"afterCreate",
			"afterSave",
		]);
	});

	it("save() update: beforeUpdate → beforeSave → UPDATE → afterUpdate → afterSave", async () => {
		const g = await Gadget.create({ id: "g3", name: "c" });
		order.length = 0;
		g.name = "c2";
		await g.save();
		expect(order).toEqual([
			"beforeUpdate",
			"beforeSave",
			"afterUpdate",
			"afterSave",
		]);
	});
});
