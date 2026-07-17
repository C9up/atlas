/**
 * A transaction rollback must drop only the domain events the rolled-back write
 * queued — NOT events the caller queued on the instance BEFORE the write. Before
 * the fix every rollback path called clearDomainEvents(), wiping pre-existing
 * (caller-authored) events too (#8). The fix snapshots an event "floor" before
 * each write and truncates back to it on rollback.
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
	Column,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { transaction } from "../../src/Transaction.js";

class EvWidget extends BaseModel {
	static override table = "ev_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@beforeCreate() static stamp(w: EvWidget): void {
		// The batch's OWN event (should be dropped on rollback).
		w.addDomainEvent("ev.created", { id: w.id });
		if (w.name === "BOOM") throw new Error("boom"); // aborts the batch → rollback
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ev_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM ev_widgets");
});

describe("atlas > rollback keeps pre-existing domain events (#8)", () => {
	it("saveMany rollback drops the batch's events but keeps caller-queued ones", async () => {
		const repo = EvWidget.$repo();
		const a = new EvWidget();
		a.id = "a1";
		a.name = "good";
		// Caller queues an event BEFORE the batch — describes work outside the tx.
		a.addDomainEvent("widget.imported", { id: "a1" });
		const boom = new EvWidget();
		boom.id = "b1";
		boom.name = "BOOM";

		await expect(repo.saveMany([a, boom])).rejects.toThrow(/boom/i);

		const names = a.getDomainEvents().map((e) => e.name);
		// Pre-existing survives; the batch's own beforeCreate event is dropped.
		expect(names).toContain("widget.imported");
		expect(names).not.toContain("ev.created");
	});

	it("single save() in a manual transaction keeps a caller-queued event on rollback", async () => {
		const w = new EvWidget();
		w.id = "s1";
		w.name = "good";
		w.addDomainEvent("widget.imported", { id: "s1" });

		await transaction(conn, async (trx) => {
			await EvWidget.$repo().useTransaction(trx).save(w);
			throw new Error("boom"); // roll the manual transaction back
		}).catch(() => {});

		// The deferred rollback restores to the pre-save floor: the caller's event
		// survives (this save queued none of its own — no throwing hook fired).
		expect(w.getDomainEvents().map((e) => e.name)).toContain("widget.imported");
	});
});
