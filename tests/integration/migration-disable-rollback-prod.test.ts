/**
 * `disableRollbacksInProduction` (Adonis Lucid parity): when set and
 * NODE_ENV === 'production', rollback/reset/refresh refuse to run unless forced.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { setAtlasDialect } from "../../src/query/native.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";

function toAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: (sql, params) => conn.query(sql, params),
		close: () => conn.close(),
		runInTransaction: (batch) => conn.runInTransaction(batch),
	};
}

let conn: AsyncDatabaseConnection;
const originalEnv = process.env.NODE_ENV;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
});

afterEach(async () => {
	await conn?.close();
	process.env.NODE_ENV = originalEnv;
});

function runner(disable: boolean): MigrationRunner {
	return new MigrationRunner(toAdapter(conn), {
		migrationsDir: "/nonexistent",
		disableRollbacksInProduction: disable,
	});
}

describe("disableRollbacksInProduction", () => {
	it("blocks rollback in production when enabled", async () => {
		process.env.NODE_ENV = "production";
		await expect(runner(true).rollback()).rejects.toThrow(
			/disabled in production/i,
		);
	});

	it("blocks reset in production when enabled", async () => {
		process.env.NODE_ENV = "production";
		await expect(runner(true).reset()).rejects.toThrow(
			/disabled in production/i,
		);
	});

	it("blocks the destructive fresh() and wipe() in production too", async () => {
		process.env.NODE_ENV = "production";
		// These DROP tables — at least as destructive as rollback, so the same
		// guard must apply (it previously did not).
		await expect(runner(true).fresh()).rejects.toThrow(
			/disabled in production/i,
		);
		await expect(runner(true).wipe()).rejects.toThrow(
			/disabled in production/i,
		);
		// refresh() rolls back + re-migrates → also guarded.
		await expect(runner(true).refresh()).rejects.toThrow(
			/disabled in production/i,
		);
	});

	it("allows forced fresh()/wipe() in production", async () => {
		process.env.NODE_ENV = "production";
		await expect(runner(true).wipe({ force: true })).resolves.toBeUndefined();
	});

	it("allows a forced rollback even in production", async () => {
		process.env.NODE_ENV = "production";
		// No migrations applied → nothing to roll back, but it must not throw.
		await expect(runner(true).rollback({ force: true })).resolves.toEqual([]);
	});

	it("does not block outside production", async () => {
		process.env.NODE_ENV = "development";
		await expect(runner(true).rollback()).resolves.toEqual([]);
	});

	it("does not block when the option is off, even in production", async () => {
		process.env.NODE_ENV = "production";
		await expect(runner(false).rollback()).resolves.toEqual([]);
	});
});
