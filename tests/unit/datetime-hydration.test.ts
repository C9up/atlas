/**
 * `@column.dateTime()` / `@column.date()` must hydrate the DB value (an ISO
 * string, as returned by the Rust decode) into a JS `Date` on read — otherwise
 * `row.expiresAt.getTime()` throws "is not a function". Mirrors Adonis Lucid
 * hydrating date columns to a Luxon DateTime (atlas standardises on native Date).
 */
import "reflect-metadata";
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
	@column.dateTime() declare expiresAt: Date | null;
	@column.date() declare day: Date | null;
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
	it("hydrates a @column.dateTime ISO string into a JS Date on read", async () => {
		const repo = repoReturning({
			id: "u1",
			expires_at: "2026-06-09T12:34:56Z",
			day: "2026-06-09",
		});
		const found = await repo.find("u1");
		const exp = found?.expiresAt;
		expect(exp).toBeInstanceOf(Date);
		if (exp instanceof Date) {
			expect(exp.getTime()).toBe(Date.parse("2026-06-09T12:34:56Z"));
		}
		expect(found?.day).toBeInstanceOf(Date);
	});

	it("leaves null date columns as null", async () => {
		const repo = repoReturning({ id: "u2", expires_at: null, day: null });
		const found = await repo.find("u2");
		expect(found?.expiresAt).toBeNull();
	});
});
