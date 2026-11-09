/**
 * Dirty tracking compares a chronos DateTime column by instant.
 *
 * chronos renders `toISO()` with the value's own zone offset, so comparing
 * ISO strings flagged the same moment in another zone as a change: a save
 * issued a pointless UPDATE, fired update hooks and bumped `@Version`.
 */
import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { describe, expect, it } from "vitest";
import { BaseEntity, column, PrimaryKey } from "../../src/index.js";

class Meeting extends BaseEntity {
	@PrimaryKey() declare id: number;
	@column.dateTime() declare startsAt: DateTime;
}

function persisted(at: DateTime): Meeting {
	const meeting = new Meeting();
	meeting.id = 1;
	meeting.startsAt = at;
	meeting.markAsPersisted();
	return meeting;
}

describe("atlas > dirty tracking of DateTime columns", () => {
	it("the same instant in another zone is not a change", () => {
		const utc = DateTime.fromISO("2026-01-01T09:00:00.000Z").toUTC();
		const meeting = persisted(utc);
		meeting.startsAt = utc.setZone("Europe/Paris");
		expect(meeting.startsAt.toISO()).not.toBe(utc.toISO());
		expect(meeting.isDirty("startsAt")).toBe(false);
		expect(meeting.$dirty).toEqual({});
	});

	it("a value rebuilt from the same instant is not a change", () => {
		const at = DateTime.fromISO("2026-03-10T12:30:00.000+02:00");
		const meeting = persisted(at);
		meeting.startsAt = DateTime.fromMillis(at.toMillis());
		expect(meeting.isDirty()).toBe(false);
	});

	it("a different instant is a change", () => {
		const at = DateTime.fromISO("2026-01-01T09:00:00.000Z");
		const meeting = persisted(at);
		meeting.startsAt = at.plus({ milliseconds: 1 });
		expect(meeting.isDirty("startsAt")).toBe(true);
		expect(Object.keys(meeting.$dirty)).toEqual(["startsAt"]);
	});
});
