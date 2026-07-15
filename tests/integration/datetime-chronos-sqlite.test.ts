import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, column, PrimaryKey } from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

class Meeting extends BaseModel {
	static override table = "meetings";
	@PrimaryKey() declare id: string;
	@column.dateTime() declare startsAt: DateTime | null;
	@column.dateTime({ autoCreate: true }) declare createdAt: DateTime | null;
}

let conn: AsyncDatabaseConnection;

beforeAll(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE meetings (id TEXT PRIMARY KEY, starts_at TEXT, created_at TEXT)",
	);
	setDb(conn);
});

afterAll(async () => {
	clearDb(conn);
	await conn?.close();
});

describe("atlas > @column.dateTime round-trips a Chronos DateTime (sqlite e2e)", () => {
	it("writes a DateTime as ISO and reads it back as a DateTime", async () => {
		const startsAt = new DateTime("2026-06-09T12:34:56Z");
		await Meeting.create({ id: "m1", startsAt });

		// Stored as an ISO string in the DB column.
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT starts_at FROM meetings WHERE id = 'm1'",
		);
		expect(typeof raw.starts_at).toBe("string");
		expect(Date.parse(String(raw.starts_at))).toBe(
			Date.parse("2026-06-09T12:34:56Z"),
		);

		// Hydrated back into a DateTime, same instant.
		const found = await Meeting.find("m1");
		expect(found?.startsAt).toBeInstanceOf(DateTime);
		expect(found?.startsAt?.equals(startsAt)).toBe(true);
	});

	it("autoCreate populates createdAt with a DateTime", async () => {
		const m = await Meeting.create({ id: "m2" });
		expect(m.createdAt).toBeInstanceOf(DateTime);
		const reread = await Meeting.find("m2");
		expect(reread?.createdAt).toBeInstanceOf(DateTime);
	});

	it("fluent query().update() lowers a DateTime to ISO (prepare not bypassed)", async () => {
		await Meeting.create({
			id: "u1",
			startsAt: new DateTime("2020-01-01T00:00:00Z"),
		});
		await Meeting.query()
			.where("id", "u1")
			.update({ startsAt: new DateTime("2026-06-09T12:34:56Z") });
		const [raw] = await conn.query<Record<string, unknown>>(
			"SELECT starts_at FROM meetings WHERE id = 'u1'",
		);
		// Bound as an ISO string, not a [object DateTime].
		expect(typeof raw.starts_at).toBe("string");
		expect(Date.parse(String(raw.starts_at))).toBe(
			Date.parse("2026-06-09T12:34:56Z"),
		);
	});

	it("re-wrapping a date column with an equal instant does not flag it dirty", async () => {
		await Meeting.create({
			id: "d1",
			startsAt: new DateTime("2026-06-09T12:34:56Z"),
		});
		const m = await Meeting.find("d1");
		if (!m) throw new Error("expected row");
		expect(m.$isDirty).toBe(false);
		// Same instant, new instance → must stay clean (value compare, not reference).
		m.startsAt = new DateTime(m.startsAt?.toISO() ?? "");
		expect(m.isDirty("startsAt")).toBe(false);
		expect(m.$isDirty).toBe(false);
	});

	it("toJSON() serializes a date column to an ISO string (Lucid parity)", async () => {
		const m = await Meeting.create({
			id: "j1",
			startsAt: new DateTime("2026-06-09T12:34:56Z"),
		});
		const json = m.toJSON();
		expect(typeof json.startsAt).toBe("string");
		expect(Date.parse(String(json.startsAt))).toBe(
			Date.parse("2026-06-09T12:34:56Z"),
		);
	});

	it("repo.updateWhere() prepares a DateTime filter value (matches query().where())", async () => {
		const at = new DateTime("2026-06-09T12:34:56Z");
		await Meeting.create({ id: "uw1", startsAt: at });
		// Filter by the DateTime instance — must be prepared to ISO to match the
		// stored value, exactly like query().where('startsAt', at).update(...).
		await Meeting.$repo().updateWhere("startsAt", at, {
			createdAt: new DateTime("2000-01-01T00:00:00Z"),
		});
		const m = await Meeting.find("uw1");
		expect(m?.createdAt?.toISO()).toBe("2000-01-01T00:00:00Z");
	});

	it("prepare normalizes a DB column-name key (updateWhere('starts_at', DateTime))", async () => {
		const at = new DateTime("2026-06-09T12:34:56Z");
		await Meeting.create({ id: "norm1", startsAt: at });
		// Passing the DB column name (not the `startsAt` property) must still route
		// through the date adapter (prepare is keyed by property via the reverse map).
		await Meeting.$repo().updateWhere("starts_at", at, {
			createdAt: new DateTime("1999-01-01T00:00:00Z"),
		});
		const m = await Meeting.find("norm1");
		expect(m?.createdAt?.toISO()).toBe("1999-01-01T00:00:00Z");
	});
});
