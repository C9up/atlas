import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Task extends BaseModel {
	static override table = "tasks";
	@PrimaryKey() declare id: string;
	@Column() declare title: string;
	@column.dateTime({ autoUpdate: true }) declare updatedAt: DateTime | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, updated_at TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > autoUpdate timestamp (Lucid parity)", () => {
	it("a clean save() does NOT bump updatedAt (no-op, no query)", async () => {
		await Task.create({ id: "1", title: "a" });
		const t = await Task.find("1");
		if (!t) throw new Error("expected row");
		const before = t.updatedAt?.toISO();

		// No mutation → save() must be a no-op; updatedAt stays exactly as-is.
		await t.save();
		expect(t.$isDirty).toBe(false);
		const reread = await Task.find("1");
		expect(reread?.updatedAt?.toISO()).toBe(before);
	});

	it("a dirty save() stamps updatedAt", async () => {
		const t = await Task.find("1");
		if (!t) throw new Error("expected row");
		// Force a distinct instant so the bump is observable regardless of clock ms.
		t.updatedAt = new DateTime("2000-01-01T00:00:00Z");
		await t.save(); // updatedAt is now dirty AND autoUpdate re-stamps to "now"
		const reread = await Task.find("1");
		expect(reread?.updatedAt?.toISO()).not.toBe("2000-01-01T00:00:00Z");
		expect(Number.isNaN(Date.parse(reread?.updatedAt?.toISO() ?? ""))).toBe(
			false,
		);
	});
});
