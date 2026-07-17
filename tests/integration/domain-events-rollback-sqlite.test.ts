import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	beforeCreate,
	Column,
	type DomainEvent,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

const dispatched: DomainEvent[] = [];

class Rw extends BaseModel {
	static override table = "rw_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@beforeCreate() static stamp(w: Rw): void {
		w.addDomainEvent("rw.created", { id: w.id });
		// The poison row aborts the batch mid-flight → the managed transaction
		// rolls back, so the earlier row must NOT have emitted its event.
		if (w.name === "BOOM") throw new Error("boom");
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE rw_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > domain events flush only AFTER a managed batch commits", () => {
	it("a rolled-back updateOrCreateMany dispatches NO events and persists NO rows", async () => {
		dispatched.length = 0;
		const repo = Rw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		await expect(
			repo.updateOrCreateMany("id", [
				{ id: "g1", name: "good" },
				{ id: "b1", name: "BOOM" },
			]),
		).rejects.toThrow(/boom/i);

		// Before the fix, g1's event dispatched in-loop before the batch rolled back.
		expect(dispatched).toEqual([]);
		// And the rollback means g1 never persisted.
		expect(await Rw.find("g1")).toBeNull();
	});

	it("a successful updateOrCreateMany dispatches every row's event after commit", async () => {
		dispatched.length = 0;
		const repo = Rw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		await repo.updateOrCreateMany("id", [
			{ id: "ok1", name: "a" },
			{ id: "ok2", name: "b" },
		]);

		expect(dispatched.map((e) => e.data.id)).toEqual(["ok1", "ok2"]);
		expect(await Rw.find("ok1")).not.toBeNull();
		expect(await Rw.find("ok2")).not.toBeNull();
	});
});
