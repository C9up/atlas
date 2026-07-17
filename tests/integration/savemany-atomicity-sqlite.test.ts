/**
 * saveMany() is all-or-nothing, like Lucid's createMany + every batch helper
 * (managed transaction, verified against the Lucid CRUD docs). Before the fix,
 * dirty entities were saved one-by-one OUTSIDE any transaction, so a mid-batch
 * failure left the earlier rows already updated.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	beforeCreate,
	beforeUpdate,
	Column,
	HasMany,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class AtomLog extends BaseModel {
	static override table = "atom_logs";
	@PrimaryKey() declare id: string;
	@Column() declare widgetId: string;
}

class AtomWidget extends BaseModel {
	static override table = "atom_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => AtomLog, { foreignKey: "widget_id" })
	declare logs: AtomLog[];
	@beforeCreate() static stamp(w: AtomWidget): void {
		w.addDomainEvent("widget.created", { id: w.id });
	}
	@beforeUpdate() static guard(w: AtomWidget): void {
		// Poison the SECOND update so it fails AFTER the first already ran.
		if (w.name === "BOOM") throw new Error("boom-update");
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE atom_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE atom_logs (id TEXT PRIMARY KEY, widget_id TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM atom_logs");
	await conn.execute("DELETE FROM atom_widgets");
});

describe("atlas > saveMany() is all-or-nothing (Lucid parity)", () => {
	it("rolls back the whole batch when a later dirty update fails mid-batch", async () => {
		const repo = AtomWidget.$repo();
		const a = await repo.create({ id: "w1", name: "a-init" });
		const b = await repo.create({ id: "w2", name: "b-init" });

		// Both become dirty; the second poisons its beforeUpdate hook.
		a.name = "a-updated";
		b.name = "BOOM";

		await expect(repo.saveMany([a, b])).rejects.toThrow(/boom-update/);

		// Atomic: the first update must have rolled back with the failed one.
		expect((await repo.find("w1"))?.name).toBe("a-init");
		expect((await repo.find("w2"))?.name).toBe("b-init");
	});

	it("commits the whole batch on success", async () => {
		const repo = AtomWidget.$repo();
		const a = await repo.create({ id: "w1", name: "a-init" });
		const b = await repo.create({ id: "w2", name: "b-init" });
		a.name = "a-ok";
		b.name = "b-ok";

		await repo.saveMany([a, b]);

		expect((await repo.find("w1"))?.name).toBe("a-ok");
		expect((await repo.find("w2"))?.name).toBe("b-ok");
	});

	it("rolls back a fresh+dirty mix and reverts the fresh instance to not-persisted (no orphan FK)", async () => {
		const repo = AtomWidget.$repo();
		const existing = await repo.create({ id: "e1", name: "e-init" });
		const fresh = new AtomWidget();
		fresh.id = "f1";
		fresh.name = "fresh";
		existing.name = "BOOM"; // poisons the dirty update AFTER the fresh insert

		await expect(repo.saveMany([fresh, existing])).rejects.toThrow(
			/boom-update/,
		);

		// DB rolled back the whole mix: the fresh insert AND the dirty update.
		expect(await repo.find("f1")).toBeNull();
		expect((await repo.find("e1"))?.name).toBe("e-init");

		// The fresh instance's INSERT was rolled back, so it must report $isNew again.
		// Otherwise related().create() would see $isPersisted === true, skip re-saving
		// the parent, and write a child whose FK points at a phantom row.
		expect(fresh.$isPersisted).toBe(false);

		// Its beforeCreate-queued "widget.created" event was dropped: the batch rolled
		// back, so leaving it would double-publish when the parent is re-saved below.
		expect(fresh.getDomainEvents()).toEqual([]);

		// The REPO_REF was reset to the durable repo (NOT the finished trx), so the
		// instance is fully usable. related().create() now re-saves the parent first
		// (because it reads $isNew), then the child — no dangling FK.
		await fresh.related("logs").create({ id: "l1" });
		expect(await repo.find("f1")).not.toBeNull(); // parent re-persisted, real row
		expect((await AtomLog.find("l1"))?.widgetId).toBe("f1"); // child FK is valid

		// The dirty instance's row still exists (rolled back to its old value), so it
		// stays $isPersisted — only the FRESH one is reverted.
		expect(existing.$isPersisted).toBe(true);
	});
});
