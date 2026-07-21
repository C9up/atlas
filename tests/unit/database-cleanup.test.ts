import { describe, expect, it } from "vitest";
import type { AsyncDatabaseConnection } from "../../src/adapters/NapiDbAdapter.js";
import {
	truncateAll,
	useTransaction,
} from "../../src/testing/DatabaseCleanup.js";

interface ExecRecord {
	sql: string;
	params?: unknown[];
}

function makeAsyncDb(opts?: { tables?: string[] }): {
	db: AsyncDatabaseConnection;
	executes: ExecRecord[];
	queries: string[];
} {
	const executes: ExecRecord[] = [];
	const queries: string[] = [];

	// transaction is optional on AsyncDatabaseConnection and not exercised here
	// (these tests drive savepoints through db.execute), so the fake omits it.
	const db: AsyncDatabaseConnection = {
		dialect: "sqlite",
		async execute(sql, params) {
			executes.push({ sql, params });
			return { rowsAffected: 0 };
		},
		async query<T>(sql: string): Promise<T[]> {
			queries.push(sql);
			const tables = opts?.tables ?? [];
			return tables.map((name) => ({ name }) as unknown as T);
		},
		async ping() {},
		async close() {},
		async runInTransaction(batch): Promise<number> {
			for (const stmt of batch) {
				executes.push({ sql: stmt.sql, params: [...(stmt.params ?? [])] });
			}
			return batch.length;
		},
	};
	return { db, executes, queries };
}

describe("atlas > DatabaseCleanup > useTransaction", () => {
	it("opens a savepoint and returns a cleanup that rolls back + releases it", async () => {
		const { db, executes } = makeAsyncDb();

		const cleanup = await useTransaction(db);
		expect(executes[0]?.sql).toBe("SAVEPOINT test_savepoint");
		expect(executes).toHaveLength(1);

		await cleanup();
		expect(executes[1]?.sql).toBe("ROLLBACK TO SAVEPOINT test_savepoint");
		expect(executes[2]?.sql).toBe("RELEASE SAVEPOINT test_savepoint");
	});
});

describe("atlas > DatabaseCleanup > truncateAll", () => {
	it("probes the sqlite catalog and emits a DELETE per user table", async () => {
		const { db, executes, queries } = makeAsyncDb({
			tables: ["users", "posts"],
		});

		await truncateAll(db);

		// One dialect-aware catalog probe (SQLite → sqlite_master).
		expect(queries).toHaveLength(1);
		expect(queries[0]).toContain("sqlite_master");

		// A DELETE per table, run on ONE pinned connection together with the
		// transaction-safe FK suspension (sqlite → defer_foreign_keys, checked at
		// COMMIT once every table is empty), so delete order doesn't matter and a
		// pool can't scatter the toggle away from the deletes.
		const sql = executes.map((e) => e.sql);
		expect(sql).toContain("PRAGMA defer_foreign_keys = ON");
		expect(sql.filter((s) => /DELETE FROM/i.test(s))).toHaveLength(2);
	});

	it("filters ream_ framework tables in JS, not the catalog query", async () => {
		const { db, executes } = makeAsyncDb({
			tables: ["users", "ream_migrations"],
		});

		await truncateAll(db);

		const deleted = executes
			.map((e) => e.sql)
			.filter((s) => /DELETE FROM/i.test(s));
		expect(deleted).toHaveLength(1);
		expect(deleted[0]).toMatch(/"users"/);
	});

	it("skips rows where name is not a string (defensive against driver glitches)", async () => {
		const { db, executes } = makeAsyncDb();
		// Override query to return malformed rows.
		db.query = async () =>
			[
				{ name: 42 },
				{ name: undefined },
				{ name: "valid_table" },
			] as unknown as never[];

		await truncateAll(db);
		const deleted = executes
			.map((e) => e.sql)
			.filter((s) => /DELETE FROM/i.test(s));
		expect(deleted).toHaveLength(1);
	});

	it("is a no-op — not even an FK toggle — when there are no user tables", async () => {
		const { db, executes } = makeAsyncDb({ tables: [] });
		await truncateAll(db);
		expect(executes).toHaveLength(0);
	});
});
