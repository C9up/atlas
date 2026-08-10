import "reflect-metadata";
import { DateTime } from "@c9up/chronos";
import { describe, expect, it } from "vitest";
import { AtlasError } from "../../src/errors.js";
import {
	BaseEntity,
	BaseRepository,
	column,
	Entity,
	PrimaryKey,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

/**
 * Branch parity with Lucid's `prepareDateColumn` / `prepareDateTimeColumn`
 * (`@adonisjs/lucid/src/orm/decorators/date.js:20`): a string passes through,
 * a `DateTime` is formatted, anything else throws naming `Model.attribute`.
 * atlas used to reject every string, which sent callers writing wrapper
 * helpers for values Lucid accepts as-is.
 */
@Entity("events")
class Event extends BaseEntity {
	@PrimaryKey() declare id: number;
	@column.dateTime() declare startsAt: DateTime | null;
	@column.date() declare day: DateTime | null;
}

function capturingDb() {
	const captured: { sql: string; params: unknown[] }[] = [];
	return {
		captured,
		prepare(sql: string) {
			return {
				run: (...params: unknown[]) => {
					captured.push({ sql, params });
					return { changes: 1, lastInsertRowid: 1 };
				},
				get: (...params: unknown[]) => {
					captured.push({ sql, params });
					return { id: 1 };
				},
				all: (...params: unknown[]) => {
					captured.push({ sql, params });
					return [{ id: 1 }];
				},
			};
		},
	};
}

const write = async (values: Record<string, unknown>) => {
	const db = capturingDb();
	const repo = new BaseRepository(Event, wrapPrepareMock(db));
	await repo.create(values);
	return db.captured.at(-1)?.params ?? [];
};

describe("atlas > date column prepare (Lucid parity)", () => {
	it("passes a date-only string straight through, like Lucid", async () => {
		await expect(write({ day: "2026-08-10" })).resolves.toContain("2026-08-10");
	});

	it("passes an offset-bearing datetime string through", async () => {
		await expect(
			write({ startsAt: "2026-08-10T12:00:00Z" }),
		).resolves.toContain("2026-08-10T12:00:00Z");
		await expect(
			write({ startsAt: "2026-08-10T12:00:00+02:00" }),
		).resolves.toContain("2026-08-10T12:00:00+02:00");
	});

	it("rejects a NAIVE datetime, naming the column — the one deviation", async () => {
		// No Z and no offset: the read path would resolve it in the runtime's local
		// zone, so the same row means different instants on a laptop and in CI.
		await expect(write({ startsAt: "2026-08-10T12:00:00" })).rejects.toThrow(
			AtlasError,
		);
		await expect(write({ startsAt: "2026-08-10T12:00:00" })).rejects.toThrow(
			/Event\.startsAt/,
		);
	});

	it("still serialises a chronos DateTime", async () => {
		const params = await write({
			startsAt: new DateTime("2026-08-10T12:00:00Z"),
		});
		expect(
			params.some((p) => String(p).startsWith("2026-08-10T12:00:00")),
		).toBe(true);
	});

	it("accepts a JS Date — named superset over Lucid, which throws", async () => {
		const params = await write({ startsAt: new Date("2026-08-10T12:00:00Z") });
		expect(params).toContain("2026-08-10T12:00:00.000Z");
	});

	it("leaves null alone", async () => {
		await expect(write({ startsAt: null })).resolves.toContain(null);
	});
});
