/**
 * createMany() must be all-or-nothing, like saveMany and every Lucid batch helper:
 * the multi-row INSERT *and* its afterCreate/afterSave hooks run inside ONE managed
 * transaction. Before the fix createMany ran the INSERT then the after-hooks with no
 * surrounding transaction, so an afterCreate hook that threw left the rows committed
 * while the call rejected — a partial write and a Lucid-parity break.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { afterCreate, BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class CmWidget extends BaseModel {
	static override table = "cm_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
	@afterCreate() static guard(w: CmWidget): void {
		// Poison the after-hook of the SECOND row — the INSERT already ran for both.
		if (w.name === "BOOM") throw new Error("boom-after");
	}
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE cm_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM cm_widgets");
});

describe("atlas > createMany() is all-or-nothing (Lucid parity)", () => {
	it("rolls back the whole batch when an afterCreate hook throws", async () => {
		const repo = CmWidget.$repo();

		await expect(
			repo.createMany([
				{ id: "c1", name: "ok" },
				{ id: "c2", name: "BOOM" }, // its afterCreate hook throws
			]),
		).rejects.toThrow(/boom-after/);

		// The INSERT for BOTH rows already ran before the hook threw — the managed
		// transaction must have rolled them back, leaving NOTHING committed.
		expect(await repo.find("c1")).toBeNull();
		expect(await repo.find("c2")).toBeNull();
	});

	it("commits the whole batch on success", async () => {
		const repo = CmWidget.$repo();

		const created = await repo.createMany([
			{ id: "c1", name: "a" },
			{ id: "c2", name: "b" },
		]);

		expect(created.map((w) => w.id)).toEqual(["c1", "c2"]);
		expect((await repo.find("c1"))?.name).toBe("a");
		expect((await repo.find("c2"))?.name).toBe("b");
	});
});
