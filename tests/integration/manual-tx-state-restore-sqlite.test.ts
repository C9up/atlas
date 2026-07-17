/**
 * A MANUAL transaction (`repo.useTransaction(trx)`) must restore in-memory entity
 * state once the transaction ends, mirroring Lucid — which resets a model's `$trx`
 * on BOTH commit and rollback. Before the fix, an entity persisted through the
 * trx-bound repo kept its REPO_REF pointing at the finished transaction (so a later
 * related()/refresh() threw "transaction already finished"), kept `$isPersisted`
 * after a rollback (so related().create() skipped re-saving the parent and orphaned
 * the FK), and kept its queued domain events (so a re-save double-published them).
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
	HasMany,
	PrimaryKey,
	transaction,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class TxLog extends BaseModel {
	static override table = "tx_logs";
	@PrimaryKey() declare id: string;
	@Column() declare widgetId: string;
}

class TxWidget extends BaseModel {
	static override table = "tx_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => TxLog, { foreignKey: "widget_id" })
	declare logs: TxLog[];
	@beforeCreate() static stamp(w: TxWidget): void {
		w.addDomainEvent("widget.created", { id: w.id });
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE tx_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	await conn.execute(
		"CREATE TABLE tx_logs (id TEXT PRIMARY KEY, widget_id TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM tx_logs");
	await conn.execute("DELETE FROM tx_widgets");
});

describe("atlas > manual transaction restores entity state on completion", () => {
	it("re-points REPO_REF to the durable repo after a manual commit (related() works post-commit)", async () => {
		const repo = TxWidget.$repo();
		let widget: TxWidget | undefined;

		await transaction(conn, async (trx) => {
			widget = await repo.useTransaction(trx).create({ id: "w1", name: "A" });
		});

		// The trx is finished. related() must run on the durable connection, NOT the
		// now-closed transaction ("transaction already finished").
		await widget?.related("logs").create({ id: "l1" });
		expect((await TxLog.find("l1"))?.widgetId).toBe("w1");
	});

	it("reverts a fresh insert AND clears its events after a manual rollback (no orphan, no stale event)", async () => {
		const repo = TxWidget.$repo();
		const published: string[] = [];
		repo.onDomainEvents = async (ev) => {
			published.push(...ev.map((e) => e.name));
		};

		let widget: TxWidget | undefined;
		await expect(
			transaction(conn, async (trx) => {
				widget = await repo.useTransaction(trx).create({ id: "w1", name: "A" });
				throw new Error("boom");
			}),
		).rejects.toThrow(/boom/);

		// Rolled back: nothing published, no row.
		expect(published).toEqual([]);
		expect(await repo.find("w1")).toBeNull();

		// The fresh INSERT was rolled back, so the instance must report $isNew again —
		// otherwise related().create() reads $isPersisted === true, skips re-saving the
		// parent, and writes a child with a phantom FK.
		expect(widget?.$isPersisted).toBe(false);

		// The queued "widget.created" event was cleared — leaving it would double-publish
		// on the re-save below (whose beforeCreate hook re-queues the same event).
		expect(widget?.getDomainEvents()).toEqual([]);

		// REPO_REF re-pointed to the durable repo → related().create() re-saves the
		// parent first (reads $isNew), then the child → real row, valid FK, no orphan.
		await widget?.related("logs").create({ id: "l1" });
		expect(await repo.find("w1")).not.toBeNull();
		expect((await TxLog.find("l1"))?.widgetId).toBe("w1");

		// The re-save published exactly once (the stale event did not linger).
		expect(published).toEqual(["widget.created"]);
	});

	it("keeps $isPersisted on a rolled-back UPDATE — only fresh inserts revert", async () => {
		const repo = TxWidget.$repo();
		const w = await repo.create({ id: "w1", name: "init" }); // committed durably

		await expect(
			transaction(conn, async (trx) => {
				w.name = "changed";
				await repo.useTransaction(trx).save(w);
				throw new Error("boom");
			}),
		).rejects.toThrow(/boom/);

		// The row still exists (its UPDATE rolled back to 'init'), so the instance must
		// stay persisted — reverting would be wrong here.
		expect(w.$isPersisted).toBe(true);
		expect((await repo.find("w1"))?.name).toBe("init");
	});
});
