/**
 * Domain events must flush POST-COMMIT in a MANUAL transaction too, not just in
 * managed batches (#inManagedTx). BaseEntity documents post-commit flush. Before
 * the fix, `repo.useTransaction(trx).create(...)` dispatched inline, so a later
 * rollback still published side effects for rows that never persisted.
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
	transaction,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class EvtWidget extends BaseModel {
	static override table = "evt_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@beforeCreate() static stamp(w: EvtWidget): void {
		w.addDomainEvent("widget.created", { id: w.id });
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE evt_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM evt_widgets");
});

describe("atlas > manual transaction defers domain events to commit", () => {
	it("a rolled-back manual transaction publishes NO events", async () => {
		const repo = EvtWidget.$repo();
		const dispatched: string[] = [];
		repo.onDomainEvents = async (ev) => {
			dispatched.push(...ev.map((e) => e.name));
		};

		await expect(
			transaction(conn, async (trx) => {
				const r = repo.useTransaction(trx);
				await r.create({ id: "w1", name: "A" });
				throw new Error("boom");
			}),
		).rejects.toThrow(/boom/);

		expect(dispatched).toEqual([]); // rolled back → nothing published
		expect(await repo.find("w1")).toBeNull(); // row rolled back too
	});

	it("a committed manual transaction publishes events AFTER commit", async () => {
		const repo = EvtWidget.$repo();
		const dispatched: string[] = [];
		repo.onDomainEvents = async (ev) => {
			dispatched.push(...ev.map((e) => e.name));
		};

		await transaction(conn, async (trx) => {
			const r = repo.useTransaction(trx);
			await r.create({ id: "w2", name: "B" });
			// Not yet published — still inside the transaction.
			expect(dispatched).toEqual([]);
		});

		expect(dispatched).toEqual(["widget.created"]); // published post-commit
		expect((await repo.find("w2"))?.name).toBe("B");
	});
});
