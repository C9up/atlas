/**
 * update()/delete() now accept the SAME complex WHERE predicates as reads —
 * nested groups (and whereExists/sub-queries) — because the DML compiler reuses
 * the SELECT compiler's WHERE lowering. These were previously rejected.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Account extends BaseModel {
	@PrimaryKey() declare id: string;
	@Column() declare status: string;
	@Column() declare flag: number;
}

let conn: AsyncDatabaseConnection;

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE accounts (id TEXT PRIMARY KEY, status TEXT, flag INTEGER)",
	);
	await conn.execute(
		"INSERT INTO accounts VALUES ('1','active',0), ('2','pending',0), ('3','banned',0)",
	);
	setDb(conn);
});

afterEach(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > update/delete with complex WHERE (Lucid)", () => {
	it("update() accepts a nested OR group", async () => {
		const n = await Account.query()
			.where((q) => q.where("status", "active").orWhere("status", "pending"))
			.update({ flag: 1 });
		expect(n).toBe(2);

		const flagged = await Account.query().where("flag", 1).exec();
		expect(flagged.map((a) => a.id).sort()).toEqual(["1", "2"]);
	});

	it("delete() accepts a nested OR group", async () => {
		const n = await Account.query()
			.where((q) => q.where("status", "banned").orWhere("status", "pending"))
			.delete();
		expect(n).toBe(2);

		const left = await Account.all();
		expect(left.map((a) => a.id)).toEqual(["1"]);
	});
});
