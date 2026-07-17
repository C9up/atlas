/**
 * Domain events must flush only after the ROOT transaction is durable — the
 * nested-transaction case the rollback test couldn't reach.
 *
 * When a repo already bound to an external `TransactionClient` runs a managed
 * op (updateOrCreateMany → #inManagedTx), atlas opens only a SAVEPOINT, so the
 * inner "commit" is a RELEASE, NOT durability. Flushing then would emit events
 * for rows the OUTER transaction can still roll back. The after-commit hook must
 * forward to the outer trx and fire only when IT commits.
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
	type DomainEvent,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

const dispatched: DomainEvent[] = [];

// `transaction` is optional on AsyncDatabaseConnection (test doubles may omit
// it); a real napi connection always provides it. Narrow without a cast.
function dbTransaction(c: AsyncDatabaseConnection) {
	if (!c.transaction) {
		throw new Error("napi connection must provide transaction()");
	}
	return c.transaction;
}

class Tw extends BaseModel {
	static override table = "tw_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;

	@beforeCreate() static stamp(w: Tw): void {
		w.addDomainEvent("tw.created", { id: w.id });
	}

	@beforeUpdate() static stampUpdate(w: Tw): void {
		w.addDomainEvent("tw.updated", { id: w.id });
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE tw_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM tw_widgets");
	dispatched.length = 0;
});

describe("atlas > domain events flush only after the ROOT transaction commits", () => {
	it("an EXTERNAL transaction that rolls back emits NO events (nested SAVEPOINT case)", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		const trx = await dbTransaction(conn)();
		await repo.useTransaction(trx).updateOrCreateMany("id", [
			{ id: "g1", name: "a" },
			{ id: "g2", name: "b" },
		]);
		// The inner managed op "committed" its SAVEPOINT — but the outer trx has
		// NOT committed yet, so nothing must have been emitted.
		expect(dispatched).toEqual([]);

		await trx.rollback();
		// The outer rollback discards the forwarded after-commit hooks → still none,
		// and no rows persisted.
		expect(dispatched).toEqual([]);
		expect(await Tw.find("g1")).toBeNull();
	});

	it("re-points a returned entity's ref at the durable repo after the outer rollback", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		const trx = await dbTransaction(conn)();
		const [r1] = await repo
			.useTransaction(trx)
			.updateOrCreateMany("id", [{ id: "r1", name: "a" }]);
		await trx.rollback();

		expect(await Tw.find("r1")).toBeNull(); // row rolled back

		// The inner managed op re-attached REPO_REF eagerly to the outer-trx repo. Once
		// the OUTER trx resolved (here: rolled back), the ref was re-pointed at the
		// durable repo, so refresh() runs on a LIVE connection and reports the row is
		// gone — instead of throwing "transaction already finished" on a dead trx.
		const err = await r1.refresh().catch((e: unknown) => e);
		expect(err).toBeInstanceOf(Error);
		expect(String(err)).not.toMatch(/transaction already finished/);

		// The row was FRESHLY inserted inside the managed op (the insert tracker proves
		// it), so the outer rollback reverts it to $isNew — a later related().create()
		// re-saves it instead of writing a child with a phantom FK. (An entity that had
		// been found + updated instead would stay persisted; only fresh inserts revert.)
		expect(r1.$isPersisted).toBe(false);
	});

	it("reverts ONLY the fresh insert on outer rollback, not a found+updated row", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};
		// Pre-existing durable row: it existed BEFORE the outer transaction.
		await repo.create({ id: "keep", name: "old" });

		const trx = await dbTransaction(conn)();
		const [kept, fresh] = await repo
			.useTransaction(trx)
			.updateOrCreateMany("id", [
				{ id: "keep", name: "new" }, // found → UPDATE (row already existed)
				{ id: "born", name: "x" }, // absent → fresh INSERT
			]);
		await trx.rollback();

		// Both rows reflect the rollback: 'keep' is back to its old value, 'born' gone.
		expect((await Tw.find("keep"))?.name).toBe("old");
		expect(await Tw.find("born")).toBeNull();

		// The tracker discriminates: the fresh INSERT reverts to $isNew; the found row —
		// whose row still exists — stays persisted (reverting it would be wrong).
		expect(fresh.$isPersisted).toBe(false);
		expect(kept.$isPersisted).toBe(true);

		// But the found+updated row's beforeUpdate hook queued a "tw.updated" event on
		// the rolled-back UPDATE — that write never committed, so the event MUST be
		// dropped too (not just the fresh insert's), else a later re-save double-publishes.
		expect(kept.getDomainEvents()).toEqual([]);
		expect(fresh.getDomainEvents()).toEqual([]);
	});

	it("an EXTERNAL transaction that commits emits every event once, after commit", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async (events) => {
			dispatched.push(...events);
		};

		const trx = await dbTransaction(conn)();
		await repo.useTransaction(trx).updateOrCreateMany("id", [
			{ id: "ok1", name: "a" },
			{ id: "ok2", name: "b" },
		]);
		expect(dispatched).toEqual([]); // deferred until the outer commit

		await trx.commit();
		expect(dispatched.map((e) => e.data.id)).toEqual(["ok1", "ok2"]);
		expect(await Tw.find("ok1")).not.toBeNull();
	});
});

describe("atlas > onDomainEvents failure: swallowed post-commit, propagated inline", () => {
	it("a post-commit (transactional) dispatch failure does NOT fail the committed op", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async () => {
			throw new Error("bus down");
		};
		// updateOrCreateMany dispatches through trx.after('commit') — the write is
		// already durable, so a bus failure is swallowed (Lucid after-hook parity):
		// the caller must NOT see a rejection on a transaction that committed.
		await expect(
			repo.updateOrCreateMany("id", [{ id: "sw1", name: "a" }]),
		).resolves.toBeDefined();
		expect(await Tw.find("sw1")).not.toBeNull();
	});

	it("a non-transactional create DOES propagate a dispatch failure (inline)", async () => {
		const repo = Tw.$repo();
		repo.onDomainEvents = async () => {
			throw new Error("bus down");
		};
		// create() dispatches inline (no managed transaction), so the failure
		// surfaces to the caller — the observable difference from the path above.
		await expect(repo.create({ id: "sw2", name: "b" })).rejects.toThrow(
			/bus down/i,
		);
		// The row was still written (dispatch runs after the INSERT).
		expect(await Tw.find("sw2")).not.toBeNull();
	});
});
