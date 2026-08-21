/**
 * Postgres casts on the RAW query builder (`db.from(...)`).
 *
 * sqlx binds JS strings as `text`, and Postgres refuses `date >= text`. The
 * compiler already emits `$N::<type>` when a cast is known — but the cast map
 * is only published as a SIDE EFFECT of constructing a `BaseRepository`. A
 * table with no entity behind it (raw SQL migrations, a reporting view) never
 * gets one, and every date/uuid comparison on it fails at runtime.
 *
 * `registerTableCasts` is the way out, so it has to be reachable from the
 * package entry point — not just from `query/native.js` internally.
 */
import { describe, expect, it } from "vitest";
import { registerTableCasts as publicRegisterTableCasts } from "../../src/index.js";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";
import { clearCastRegistry } from "../../src/query/native.js";

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};

function betweenSql(table: string): string {
	return new DatabaseQueryBuilder(exec, "postgres", table)
		.whereBetween("day", ["2026-01-01", "2026-12-31"])
		.toSQL()
		.toNative().sql;
}

describe("atlas > raw query builder > Postgres casts", () => {
	it("emits NO cast for a table nothing has declared — the bind stays `text` and Postgres rejects it", () => {
		clearCastRegistry();
		// Characterisation, not an endorsement: atlas will not guess a column's
		// type from its value. Declaring it is the supported fix.
		expect(betweenSql("events")).toContain('"day" BETWEEN $1 AND $2');
	});

	it("casts BOTH bounds once the table's casts are declared", () => {
		clearCastRegistry();
		publicRegisterTableCasts("events", { day: "date" });
		expect(betweenSql("events")).toContain(
			'"day" BETWEEN $1::date AND $2::date',
		);
	});

	it("exposes registerTableCasts from the package entry point", () => {
		expect(typeof publicRegisterTableCasts).toBe("function");
	});
});
