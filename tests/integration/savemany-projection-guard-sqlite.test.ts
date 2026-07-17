/**
 * saveMany() classifies fresh-vs-dirty by `$isPersisted`, NOT by an empty
 * `$original`. An aggregate/alias PROJECTION is hydrated persisted but with
 * `$original = {}`; the old empty-$original test misrouted it into the fresh INSERT
 * batch, turning a keyless projection into an INSERT and bypassing the
 * E_MISSING_PRIMARY_KEY guard that unit save() enforces.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class PjWidget extends BaseModel {
	static override table = "pj_widgets";
	@PrimaryKey() declare id: string;
	@Column() declare name: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE pj_widgets (id TEXT PRIMARY KEY, name TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM pj_widgets");
});

describe("atlas > saveMany() rejects a persisted keyless projection (no phantom INSERT)", () => {
	it("routes a persisted projection through save()'s guard instead of INSERTing it", async () => {
		const repo = PjWidget.$repo();
		await repo.create({ id: "w1", name: "a" });
		await repo.create({ id: "w2", name: "b" });

		// A persisted aggregate projection: hydrated with $isPersisted === true but no
		// recognized column → $original === {}.
		const [proj] = await repo.query().select("COUNT(*) as n").exec();
		expect(proj.$isPersisted).toBe(true);

		// It must NOT be treated as a fresh row and INSERTed — save()'s guard rejects a
		// persisted instance loaded without its primary key.
		await expect(repo.saveMany([proj])).rejects.toThrow(
			/primary key|E_MISSING_PRIMARY_KEY/i,
		);

		// No stray row leaked in (still exactly the two originals).
		const all = await repo.query().exec();
		expect(all.length).toBe(2);
	});
});
