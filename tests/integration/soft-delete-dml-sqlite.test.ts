import "reflect-metadata";
import type { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import {
	BaseModel,
	Column,
	column,
	PrimaryKey,
	SoftDeletes,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

@SoftDeletes()
class Note extends BaseModel {
	static override table = "sd_notes";
	@PrimaryKey() declare id: string;
	@Column() declare tag: string;
	@Column() declare priority: number;
	@column.dateTime() declare deletedAt: DateTime | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE sd_notes (id TEXT PRIMARY KEY, tag TEXT, priority INTEGER, deleted_at TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

beforeEach(async () => {
	await conn.execute("DELETE FROM sd_notes");
});

describe("atlas > bulk DML honours @SoftDeletes scope", () => {
	it("bulk delete() SOFT-deletes (stamps deleted_at, row stays; excluded from default reads)", async () => {
		await Note.create({ id: "n1", tag: "x", priority: 1 });
		await Note.create({ id: "n2", tag: "y", priority: 1 });

		const affected = await Note.query().where("tag", "x").delete();
		expect(affected).toBe(1);

		// Row is NOT hard-deleted — deleted_at is set.
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT deleted_at FROM sd_notes WHERE id = 'n1'",
		);
		expect(raw.deleted_at).not.toBeNull();
		// And it's excluded from the default (non-trashed) read scope.
		expect(await Note.find("n1")).toBeNull();
	});

	it("bulk update() does NOT touch trashed rows (read/write scope symmetry)", async () => {
		await Note.create({ id: "a", tag: "t", priority: 1 });
		await Note.create({ id: "b", tag: "t", priority: 1 });
		await Note.query().where("id", "a").delete(); // soft-delete a

		await Note.query().where("tag", "t").update({ priority: 9 });

		const [rawA] = await conn.query<Record<string, unknown>>(
			"SELECT priority FROM sd_notes WHERE id = 'a'",
		);
		const [rawB] = await conn.query<Record<string, unknown>>(
			"SELECT priority FROM sd_notes WHERE id = 'b'",
		);
		// The trashed row is untouched; only the live row is updated.
		expect(rawA.priority).toBe(1);
		expect(rawB.priority).toBe(9);
	});

	it("forceDelete() hard-deletes the row", async () => {
		await Note.create({ id: "f", tag: "z", priority: 1 });
		await Note.query().where("id", "f").forceDelete();
		const rows = await conn.query<Record<string, unknown>>(
			"SELECT * FROM sd_notes WHERE id = 'f'",
		);
		expect(rows.length).toBe(0);
	});

	it("restore() clears deleted_at and brings the row back into default reads", async () => {
		await Note.create({ id: "r", tag: "w", priority: 1 });
		await Note.query().where("id", "r").delete();
		expect(await Note.find("r")).toBeNull();

		const restored = await Note.query().where("id", "r").restore();
		expect(restored).toBe(1);
		expect(await Note.find("r")).not.toBeNull();
	});
});
