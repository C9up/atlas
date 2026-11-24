/**
 * The found row of `updateOrCreate()` / `updateOrCreateMany()` /
 * `related().updateOrCreate()` takes its payload through `merge()`.
 *
 * Each payload key used to be set on the row as written: `{ IsAdmin: 1 }`
 * passed `guarded = ['isAdmin']` and was snake-cased into `is_admin` by the
 * UPDATE, a column the model never declared was written, `{ Id: 4242 }`
 * rewrote the primary key and `{ UserId: 2 }` moved a child to another parent.
 */
import "reflect-metadata";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, HasMany, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class MaPost extends BaseModel {
	static override table = "ma_posts";
	@PrimaryKey() declare id: number;
	@Column() declare userId: number;
	@Column() declare title: string;
}

class MaUser extends BaseModel {
	static override table = "ma_users";
	static guarded = ["isAdmin"];
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare name: string;
	@Column() declare isAdmin: number;
	@HasMany(() => MaPost, { foreignKey: "userId" }) declare posts: MaPost[];
}

/** A narrow model over the same table: `balance` and `is_admin` are not declared. */
class MaProfile extends BaseModel {
	static override table = "ma_users";
	@PrimaryKey() declare id: number;
	@Column() declare email: string;
	@Column() declare name: string;
}

let conn: AsyncDatabaseConnection;

async function userRow(
	id: number,
): Promise<Record<string, unknown> | undefined> {
	const rows = await conn.query<Record<string, unknown>>(
		"SELECT * FROM ma_users WHERE id = ?",
		[id],
	);
	return rows[0];
}

const UNTOUCHED = { id: 1, name: "mallory", is_admin: 0, balance: 0 };

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE ma_users (id INTEGER PRIMARY KEY, email TEXT, name TEXT, is_admin INTEGER DEFAULT 0, balance INTEGER DEFAULT 0)",
	);
	await conn.execute(
		"CREATE TABLE ma_posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)",
	);
	setDb(conn);
});

beforeEach(async () => {
	await conn.execute("DELETE FROM ma_users");
	await conn.execute("DELETE FROM ma_posts");
	await conn.execute(
		"INSERT INTO ma_users (id, email, name) VALUES (1, 'mallory@x.io', 'mallory'), (2, 'victim@x.io', 'victim')",
	);
	await conn.execute("INSERT INTO ma_posts VALUES (10, 1, 'mine')");
});

afterAll(async () => {
	clearDb(conn);
	await conn.close();
});

describe("updateOrCreate() on a found row", () => {
	const update = (body: Record<string, unknown>): Promise<MaUser> =>
		MaUser.updateOrCreate({ email: "mallory@x.io" }, body);

	it("refuses the guarded property under any spelling", async () => {
		for (const body of [{ isAdmin: 1 }, { is_admin: 1 }]) {
			await expect(update(body)).rejects.toMatchObject({
				code: "E_MASS_ASSIGNMENT",
			});
		}
		for (const body of [{ IsAdmin: 1 }, { Is_admin: 1 }]) {
			await expect(update(body)).rejects.toMatchObject({
				code: "E_EXTRA_PROPERTIES",
			});
		}
		expect(await userRow(1)).toMatchObject(UNTOUCHED);
	});

	it("refuses a column the model does not declare, and a misspelt primary key", async () => {
		for (const body of [{ balance: 999999 }, { Id: 4242 }, { is_admin: 1 }]) {
			await expect(
				MaProfile.updateOrCreate({ email: "mallory@x.io" }, body),
			).rejects.toMatchObject({ code: "E_EXTRA_PROPERTIES" });
		}
		expect(await userRow(1)).toMatchObject(UNTOUCHED);
		expect(await userRow(4242)).toBeUndefined();
	});

	it("assigns a declared property or its column name, and ignores a relation key", async () => {
		await update({ name: "renamed", posts: [] });
		expect((await userRow(1))?.name).toBe("renamed");
		await MaProfile.updateOrCreate(
			{ email: "mallory@x.io" },
			{ name: "again" },
		);
		expect((await userRow(1))?.name).toBe("again");
	});

	it("keeps an unknown key in $extras with allowExtraProperties, never in SQL", async () => {
		const row = await MaProfile.updateOrCreate(
			{ email: "mallory@x.io" },
			{ name: "x", balance: 999999 },
			{ allowExtraProperties: true },
		);
		expect(row.$extras.balance).toBe(999999);
		expect(await userRow(1)).toMatchObject({ name: "x", balance: 0 });
	});

	it("the new-row branch refuses an unknown key too", async () => {
		await expect(
			MaProfile.updateOrCreate({ email: "new@x.io" }, { balance: 5 }),
		).rejects.toMatchObject({ code: "E_UNKNOWN_COLUMN" });
		const created = await MaProfile.updateOrCreate(
			{ email: "new@x.io" },
			{ name: "new" },
		);
		expect((await userRow(created.id))?.name).toBe("new");
	});
});

describe("updateOrCreateMany() on found rows", () => {
	it("refuses an undeclared or guarded key and writes nothing", async () => {
		await expect(
			MaUser.updateOrCreateMany("email", [
				{ email: "mallory@x.io", name: "a" },
				{ email: "victim@x.io", IsAdmin: 1 },
			]),
		).rejects.toMatchObject({ code: "E_EXTRA_PROPERTIES" });
		await expect(
			MaProfile.updateOrCreateMany("email", [
				{ email: "mallory@x.io", balance: 1 },
			]),
		).rejects.toMatchObject({ code: "E_EXTRA_PROPERTIES" });
		expect(await userRow(1)).toMatchObject(UNTOUCHED);
		expect(await userRow(2)).toMatchObject({ is_admin: 0, balance: 0 });
	});

	it("still updates declared columns, keyed by property or column name", async () => {
		const rows = await MaUser.updateOrCreateMany("email", [
			{ email: "mallory@x.io", name: "m2" },
		]);
		expect(rows.map((r) => r.id)).toEqual([1]);
		expect((await userRow(1))?.name).toBe("m2");
	});
});

describe("related().updateOrCreate() on a found row", () => {
	it("refuses a respelt foreign key and keeps the child on its parent", async () => {
		const me = await MaUser.findOrFail(1);
		const posts = me.related("posts");
		if (posts.type !== "hasMany") throw new Error("expected a hasMany proxy");
		for (const values of [{ UserId: 2 }, { User_id: 2 }]) {
			await expect(
				posts.updateOrCreate({ title: "mine" }, values),
			).rejects.toMatchObject({ code: "E_EXTRA_PROPERTIES" });
		}
		const rows = await conn.query<{ user_id: number }>(
			"SELECT user_id FROM ma_posts WHERE id = 10",
		);
		expect(rows[0]?.user_id).toBe(1);
	});
});

describe("save() of a found row", () => {
	it("writes declared columns only, whatever own property the instance holds", async () => {
		const row = await MaProfile.findOrFail(1);
		Reflect.set(row, "IsAdmin", 1);
		Reflect.set(row, "balance", 999999);
		row.name = "saved";
		await row.save();
		expect(await userRow(1)).toMatchObject({
			name: "saved",
			is_admin: 0,
			balance: 0,
		});
	});
});
