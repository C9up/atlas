/**
 * `@column.dateTime()` / `@column.date()` must hydrate the DB value (an ISO
 * string, as returned by the Rust decode) into a Chronos `DateTime` on read —
 * mirroring Adonis Lucid, which hydrates date columns to a Luxon DateTime (here
 * `@c9up/chronos` plays Luxon's role).
 */
import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("sessions")
class Session extends BaseEntity {
	@PrimaryKey({ generated: "uuid" }) declare id: string;
	@column.dateTime() declare expiresAt: DateTime | null;
	@column.date() declare day: DateTime | null;
}

function repoReturning(row: Record<string, unknown>): BaseRepository<Session> {
	const conn = wrapPrepareMock({
		prepare: () => ({
			run: () => ({ changes: 0 }),
			all: () => [row],
		}),
	});
	return new BaseRepository(Session, conn);
}

describe("atlas > dateTime hydration", () => {
	it("hydrates a @column.dateTime ISO string into a Chronos DateTime on read", async () => {
		const repo = repoReturning({
			id: "u1",
			expires_at: "2026-06-09T12:34:56Z",
			day: "2026-06-09",
		});
		const found = await repo.find("u1");
		const exp = found?.expiresAt;
		expect(exp).toBeInstanceOf(DateTime);
		if (exp instanceof DateTime) {
			expect(Date.parse(exp.toISO())).toBe(Date.parse("2026-06-09T12:34:56Z"));
		}
		expect(found?.day).toBeInstanceOf(DateTime);
	});

	it("leaves null date columns as null", async () => {
		const repo = repoReturning({ id: "u2", expires_at: null, day: null });
		const found = await repo.find("u2");
		expect(found?.expiresAt).toBeNull();
	});
});
