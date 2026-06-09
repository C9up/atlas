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
	it("queries sqlite_master and emits a DELETE per user table", async () => {
		const { db, executes, queries } = makeAsyncDb({
			tables: ["users", "posts"],
		});

		await truncateAll(db);

		// One sqlite_master probe.
		expect(queries).toHaveLength(1);
		expect(queries[0]).toContain("sqlite_master");
		expect(queries[0]).toContain("ream\\_%"); // ream_* system-table exclusion sentinel

		// Two user tables → at least two DELETE executes (no SAVEPOINTs in this path).
		expect(executes.length).toBeGreaterThanOrEqual(2);
		const sqlJoined = executes.map((e) => e.sql).join("\n");
		expect(sqlJoined).toMatch(/DELETE FROM/i);
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
		expect(executes).toHaveLength(1);
		expect(executes[0]?.sql).toMatch(/DELETE FROM/i);
	});

	it("is a no-op when sqlite_master returns no user tables", async () => {
		const { db, executes } = makeAsyncDb({ tables: [] });
		await truncateAll(db);
		expect(executes).toHaveLength(0);
	});
});
