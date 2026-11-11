/**
 * save() of a new instance whose key is already taken fails; it does not
 * overwrite the existing row.
 *
 * save() documented a fallback that turned the INSERT into an UPDATE on a
 * duplicate key. It never fired (the driver's error carried no code it
 * matched) and, had it fired, an id taken from a request body would have
 * rewritten someone else's row. The duplicate-key error now simply reaches
 * the caller, the way the database reports it.
 */
import "reflect-metadata";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { setAtlasDialect } from "../../src/query/native.js";

@Entity("dup_users")
class DupUser extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare role: string;
}

let conn: AsyncDatabaseConnection;
let users: BaseRepository<DupUser>;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE dup_users (id INTEGER PRIMARY KEY, role TEXT)",
	);
	await conn.execute("INSERT INTO dup_users VALUES (1, 'owner')");
	users = new BaseRepository(DupUser, conn);
});

afterEach(async () => {
	await conn.close();
});

describe("atlas > save() of a new instance with a taken key", () => {
	it("rejects with the database error and leaves the row alone", async () => {
		const intruder = new DupUser();
		intruder.id = 1;
		intruder.role = "attacker";
		await expect(users.save(intruder)).rejects.toThrow(/UNIQUE constraint/);
		expect(intruder.$isPersisted).toBe(false);
		expect(await conn.query("SELECT id, role FROM dup_users")).toEqual([
			{ id: 1, role: "owner" },
		]);
	});
});
