import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";

@Entity("p_users")
class PUser extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column() declare email: string;
	@Column() declare role: string;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE p_users (id TEXT PRIMARY KEY, email TEXT, role TEXT)",
	);
});

afterAll(async () => {
	await conn?.close();
});

describe("atlas parity > finders (object clause) + truncate + state flags (sqlite)", () => {
	it("findBy / findManyBy accept a Lucid object clause", async () => {
		const repo = new BaseRepository(PUser, conn);
		await repo.create({ id: "1", email: "a@x", role: "admin" });
		await repo.create({ id: "2", email: "b@x", role: "user" });
		await repo.create({ id: "3", email: "c@x", role: "user" });

		const admin = await repo.findBy({ role: "admin" });
		expect(admin?.id).toBe("1");

		const users = await repo.findManyBy({ role: "user" });
		expect(users.map((u) => u.id).sort()).toEqual(["2", "3"]);
	});

	it("state flags: create → persisted+local, find → not local, delete → deleted", async () => {
		const repo = new BaseRepository(PUser, conn);
		const created = await repo.create({ id: "10", email: "d@x", role: "user" });
		expect(created.$isPersisted).toBe(true);
		expect(created.$isNew).toBe(false);
		expect(created.$isLocal).toBe(true);

		const fetched = await repo.find("10");
		if (!fetched) throw new Error("expected row");
		expect(fetched.$isPersisted).toBe(true);
		expect(fetched.$isLocal).toBe(false);
		expect(fetched.$primaryKeyValue).toBe("10");

		await repo.delete(fetched);
		expect(fetched.$isDeleted).toBe(true);
		expect(await repo.find("10")).toBeNull();
	});

	it("truncate empties the table", async () => {
		const repo = new BaseRepository(PUser, conn);
		await repo.truncate();
		expect(await repo.all()).toHaveLength(0);
	});
});
