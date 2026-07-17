/**
 * A relation write (`parent.related('x').create/save`) persists an UNSAVED parent
 * first, on the SAME transaction as the child (Lucid parity). If a later step fails
 * — here the child's own hook throws after the parent was already inserted — the
 * transaction rolls back the rows, and the parent's in-memory state must be restored
 * too: reverted to $isNew (its INSERT was rolled back), REPO_REF re-pointed at the
 * durable repo, and its queued domain events dropped. Before the fix the parent stayed
 * $isPersisted with a REPO_REF bound to the finished transaction and a stale event
 * queue, so reusing it orphaned the FK and double-published.
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
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class RwItem extends BaseModel {
	static override table = "rw_items";
	@PrimaryKey() declare id: string;
	@Column() declare ownerId: string;
	@Column() declare label: string;
	@beforeCreate() static guard(i: RwItem): void {
		// Poison the child insert AFTER the parent was already saved on the trx.
		if (i.label === "BOOM") throw new Error("boom-child");
	}
}

class RwOwner extends BaseModel {
	static override table = "rw_owners";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@HasMany(() => RwItem, { foreignKey: "owner_id" })
	declare items: RwItem[];
	@beforeCreate() static stamp(o: RwOwner): void {
		o.addDomainEvent("owner.created", { id: o.id });
	}
}

// The child FK references the parent's `code` (a non-PK localKey), so a fresh parent
// saved with `code` unset passes the INSERT but fails the localKey check right after —
// the same hook-registration-order gotcha as associate()'s ownerKey, in withParentSaved.
class LkItem extends BaseModel {
	static override table = "lk_items";
	@PrimaryKey() declare id: string;
	@Column() declare ownerCode: string;
}

class LkOwner extends BaseModel {
	static override table = "lk_owners";
	@PrimaryKey() declare id: string;
	@Column() declare code: string | null;
	@HasMany(() => LkItem, { foreignKey: "owner_code", localKey: "code" })
	declare items: LkItem[];
	@beforeCreate() static stamp(o: LkOwner): void {
		o.addDomainEvent("owner.created", { id: o.id });
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE rw_owners (id TEXT PRIMARY KEY, name TEXT)");
	await conn.execute(
		"CREATE TABLE rw_items (id TEXT PRIMARY KEY, owner_id TEXT, label TEXT)",
	);
	await conn.execute("CREATE TABLE lk_owners (id TEXT PRIMARY KEY, code TEXT)");
	await conn.execute(
		"CREATE TABLE lk_items (id TEXT PRIMARY KEY, owner_code TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM rw_items");
	await conn.execute("DELETE FROM rw_owners");
	await conn.execute("DELETE FROM lk_items");
	await conn.execute("DELETE FROM lk_owners");
});

describe("atlas > relation write restores an unsaved parent on rollback", () => {
	it("reverts the parent to $isNew, clears its events, and keeps it reusable", async () => {
		const repo = RwOwner.$repo();
		const published: string[] = [];
		repo.onDomainEvents = async (ev) => {
			published.push(...ev.map((e) => e.name));
		};

		const owner = new RwOwner();
		owner.id = "o1";
		owner.name = "Ada";
		expect(owner.$isPersisted).toBe(false);

		// Parent gets saved on the trx, then the child's BOOM hook throws → rollback.
		await expect(
			repo.relatedProxy(owner, "items").create({ id: "i1", label: "BOOM" }),
		).rejects.toThrow(/boom-child/);

		// Rows rolled back, nothing published.
		expect(await repo.find("o1")).toBeNull();
		expect(await RwItem.$repo().find("i1")).toBeNull();
		expect(published).toEqual([]);

		// The parent's INSERT was rolled back → it must report $isNew again, its queued
		// "owner.created" event dropped, and its REPO_REF re-pointed at the durable repo.
		expect(owner.$isPersisted).toBe(false);
		expect(owner.getDomainEvents()).toEqual([]);

		// Reuse it: a clean child write re-saves the parent (reads $isNew) then the
		// child → real parent row, valid FK, and the event published EXACTLY once
		// (the stale one did not linger).
		await repo.relatedProxy(owner, "items").create({ id: "i2", label: "ok" });
		expect(await repo.find("o1")).not.toBeNull();
		expect((await RwItem.$repo().find("i2"))?.ownerId).toBe("o1");
		expect(published).toEqual(["owner.created"]);
	});

	it("reverts a freshly-saved parent when the localKey check fails AFTER the save", async () => {
		const repo = LkOwner.$repo();

		// The parent is savable (has a PK) but its `code` — the localKey the child FK
		// references — is unset. withParentSaved INSERTs the parent, THEN reads `code`,
		// finds it absent, and throws. The parent insert must roll back AND its in-memory
		// state be restored, even though the throw lands between the save and the (now
		// earlier-registered) rollback hook.
		const owner = new LkOwner();
		owner.id = "o1";
		owner.code = null; // no localKey value
		expect(owner.$isPersisted).toBe(false);

		await expect(
			repo.relatedProxy(owner, "items").create({ id: "i1" }),
		).rejects.toThrow();

		// The parent INSERT rolled back, and its in-memory state was restored.
		expect(await repo.find("o1")).toBeNull();
		expect(owner.$isPersisted).toBe(false);
		expect(owner.getDomainEvents()).toEqual([]);

		// Reusable: set the missing localKey and write cleanly.
		owner.code = "C1";
		await repo.relatedProxy(owner, "items").create({ id: "i2" });
		expect(await repo.find("o1")).not.toBeNull();
		expect((await LkItem.$repo().find("i2"))?.ownerCode).toBe("C1");
	});
});
