import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AtlasError } from "../../src/errors.js";
import {
	type BatchStmt,
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";

interface ExecCall {
	sql: string;
	params?: unknown[];
}

interface FakeAdapterOptions {
	/** Rows to return for `query` calls, in FIFO order. */
	queueQuery?: unknown[][];
	/** Skip implementing runInTransaction (forces the non-atomic fallback). */
	noTransaction?: boolean;
}

function createFakeAdapter(opts: FakeAdapterOptions = {}): {
	adapter: DatabaseAdapter;
	executes: ExecCall[];
	queries: ExecCall[];
	transactions: BatchStmt[][];
} {
	const executes: ExecCall[] = [];
	const queries: ExecCall[] = [];
	const transactions: BatchStmt[][] = [];
	const queue = [...(opts.queueQuery ?? [])];

	const adapter: DatabaseAdapter = {
		async execute(sql, params) {
			executes.push({ sql, params });
		},
		async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
			queries.push({ sql, params });
			const next = queue.shift() ?? [];
			return next as T[];
		},
		async close() {
			// no-op
		},
	};

	if (!opts.noTransaction) {
		adapter.runInTransaction = async (batch) => {
			transactions.push([...batch]);
			for (const stmt of batch) {
				await adapter.execute(stmt.sql, stmt.params);
			}
			return batch.length;
		};
	}

	return { adapter, executes, queries, transactions };
}

async function makeMigrationFixtures(
	dir: string,
	files: Record<string, string>,
): Promise<void> {
	await fsp.mkdir(dir, { recursive: true });
	for (const [name, body] of Object.entries(files)) {
		await fsp.writeFile(path.join(dir, name), body, "utf8");
	}
}

const MIG_001 = `
import { Migration } from '${pathToFileURL(path.resolve(__dirname, "../../src/schema/Migration.ts")).href}'
export default class CreateUsers extends Migration {
  async up() { this.schema.createTable('users', (t) => { t.id(); t.string('email', 100); }) }
  async down() { this.schema.dropTable('users') }
}
`;

const MIG_002 = `
import { Migration } from '${pathToFileURL(path.resolve(__dirname, "../../src/schema/Migration.ts")).href}'
export default class CreatePosts extends Migration {
  async up() { this.schema.createTable('posts', (t) => { t.id(); t.string('title', 200) }) }
  async down() { this.schema.dropTable('posts') }
}
`;

describe("atlas > MigrationRunner > init", () => {
	it("creates the ream_migrations tracking table via the adapter", async () => {
		const { adapter, executes } = createFakeAdapter();
		const runner = new MigrationRunner(adapter);
		await runner.init();
		expect(executes).toHaveLength(1);
		expect(executes[0]?.sql).toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?ream_migrations/i,
		);
		// Auto-increment PK (Lucid `increments`) — SQLite identity form.
		expect(executes[0]?.sql).toContain("INTEGER PRIMARY KEY AUTOINCREMENT");
	});
});

describe("atlas > MigrationRunner > tableName validation", () => {
	const invalidNames = [
		"",
		"1migrations",
		"my migrations",
		"my-migrations",
		"schema.migrations",
		'my"mig',
		"DROP--",
	];

	for (const bad of invalidNames) {
		it(`rejects ${JSON.stringify(bad)} with MIGRATION_INVALID_TABLE_NAME`, () => {
			const { adapter } = createFakeAdapter();
			expect(() => new MigrationRunner(adapter, { tableName: bad })).toThrow(
				expect.objectContaining({
					name: "AtlasError",
					code: "ATLAS_MIGRATION_INVALID_TABLE_NAME",
				}),
			);
		});
	}

	it("accepts a valid identifier and exposes it to emitted SQL", async () => {
		const { adapter, executes } = createFakeAdapter();
		const runner = new MigrationRunner(adapter, {
			tableName: "schema_versions",
		});
		await runner.init();
		expect(executes[0]?.sql).toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?schema_versions/i,
		);
	});
});

describe("atlas > MigrationRunner > custom tableName", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("emits INSERT against the custom tracking table on migrate", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, transactions } = createFakeAdapter({
			queueQuery: [
				[], // applied names
				[{ max: 0 }], // currentBatch
			],
		});
		const runner = new MigrationRunner(adapter, {
			migrationsDir: tmpDir,
			tableName: "schema_versions",
		});

		await runner.migrate();
		const allSql = transactions.flat().map((b) => b.sql);
		expect(
			allSql.some((s) => /INSERT INTO\s+["`]?schema_versions/i.test(s)),
		).toBe(true);
		expect(allSql.some((s) => /INSERT INTO\s+["`]?ream_migrations/i.test(s))).toBe(
			false,
		);
	});

	it("emits DELETE against the custom tracking table on rollback", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, transactions } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // currentBatch
				[{ name: "0001_x" }], // toRollback rows
			],
		});
		const runner = new MigrationRunner(adapter, {
			migrationsDir: tmpDir,
			tableName: "schema_versions",
		});

		await runner.rollback();
		const allSql = transactions.flat().map((b) => b.sql);
		expect(
			allSql.some((s) => /DELETE FROM\s+["`]?schema_versions/i.test(s)),
		).toBe(true);
	});

	it("defaults to ream_migrations when tableName is omitted (regression pin)", async () => {
		const { adapter, executes } = createFakeAdapter();
		const runner = new MigrationRunner(adapter);
		await runner.init();
		expect(executes[0]?.sql).toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?ream_migrations/i,
		);
	});

	it("emits SELECT against the custom tracking table on status (status>select)", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, queries } = createFakeAdapter({
			queueQuery: [[{ name: "0001_x", batch: 1 }]],
		});
		const runner = new MigrationRunner(adapter, {
			migrationsDir: tmpDir,
			tableName: "schema_versions",
		});
		await runner.status();
		const allSql = queries.map((q) => q.sql).join("\n");
		expect(allSql).toMatch(/FROM\s+["`]?schema_versions/i);
		expect(allSql).not.toMatch(/FROM\s+["`]?ream_migrations/i);
	});

	it("emits SELECT MAX(batch) against the custom tracking table on migrate (#currentBatch>select)", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, queries } = createFakeAdapter({
			queueQuery: [
				[], // applied names (migrate>select)
				[{ max: 0 }], // #currentBatch>select
			],
		});
		const runner = new MigrationRunner(adapter, {
			migrationsDir: tmpDir,
			tableName: "schema_versions",
		});
		await runner.migrate();
		const allSql = queries.map((q) => q.sql).join("\n");
		expect(allSql).toMatch(/MAX\(.*?\).*FROM\s+["`]?schema_versions/i);
		expect(allSql).not.toMatch(/FROM\s+["`]?ream_migrations/i);
	});

	it("emits SELECT against the custom tracking table on rollback (rollback>select)", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, queries } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // #currentBatch>select
				[{ name: "0001_x" }], // rollback>select (toRollback rows)
			],
		});
		const runner = new MigrationRunner(adapter, {
			migrationsDir: tmpDir,
			tableName: "schema_versions",
		});
		await runner.rollback();
		const allSql = queries.map((q) => q.sql).join("\n");
		expect(allSql).toMatch(/FROM\s+["`]?schema_versions/i);
		expect(allSql).not.toMatch(/FROM\s+["`]?ream_migrations/i);
	});
});

describe("atlas > MigrationRunner > status", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("returns 'pending' for files not in ream_migrations and 'applied' for those that are", async () => {
		await makeMigrationFixtures(tmpDir, {
			"0001_create_users.ts": MIG_001,
			"0002_add_name.ts": MIG_002,
		});
		const { adapter } = createFakeAdapter({
			queueQuery: [
				// status() queries the ream_migrations table once (after init).
				[{ name: "0001_create_users", batch: 1 }],
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const status = await runner.status();
		expect(status).toEqual([
			{ name: "0001_create_users", status: "applied", batch: 1 },
			{ name: "0002_add_name", status: "pending", batch: undefined },
		]);
	});

	it("returns an empty array when the migrations directory does not exist", async () => {
		const { adapter } = createFakeAdapter({ queueQuery: [[]] });
		const runner = new MigrationRunner(adapter, {
			migrationsDir: path.join(tmpDir, "missing"),
		});
		expect(await runner.status()).toEqual([]);
	});
});

describe("atlas > MigrationRunner > migrate", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("returns [] when there is nothing pending", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter } = createFakeAdapter({
			queueQuery: [
				// migrate() reads applied names — return the file as already applied.
				[{ name: "0001_x" }],
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });
		expect(await runner.migrate()).toEqual([]);
	});

	it("runs every pending migration inside a transaction batch when supported", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, transactions } = createFakeAdapter({
			queueQuery: [
				// applied names: empty
				[],
				// currentBatch MAX(batch): no rows
				[{ max: 0 }],
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const executed = await runner.migrate();
		expect(executed).toEqual(["0001_x"]);
		// The batch must include the migration's DDL + the ream_migrations INSERT.
		expect(transactions.length).toBeGreaterThanOrEqual(1);
		const all = transactions.flat();
		expect(all.some((b) => b.sql.includes("INSERT INTO"))).toBe(true);
	});

	it("falls back to sequential execute when runInTransaction is unsupported (with warning)", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, executes } = createFakeAdapter({
			queueQuery: [[], [{ max: 0 }]],
			noTransaction: true,
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		try {
			const executed = await runner.migrate();
			expect(executed).toEqual(["0001_x"]);
			// init() emits CREATE TABLE; migrate() emits the migration DDL + INSERT.
			expect(executes.length).toBeGreaterThanOrEqual(2);
			expect(warn).toHaveBeenCalledWith(
				expect.stringMatching(/runInTransaction/),
			);
		} finally {
			warn.mockRestore();
		}
	});

	it("rejects a tracking-row migration name containing '..' on rollback", async () => {
		// Inject a malicious row into ream_migrations and confirm rollback refuses
		// to load a migration whose name would traverse the migrations dir.
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // currentBatch
				[{ name: "../escape" }], // toRollback returns the malicious name
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		await expect(runner.rollback()).rejects.toThrow(AtlasError);
	});
});

describe("atlas > MigrationRunner > rollback / reset / refresh", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("rollback returns [] when no batch has been applied", async () => {
		const { adapter } = createFakeAdapter({
			queueQuery: [
				// init: no query
				// rollback's #currentBatch -> 0
				[{ max: 0 }],
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });
		expect(await runner.rollback()).toEqual([]);
	});

	it("rollback runs down() for each migration in the latest batch and deletes the row", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter, transactions } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // currentBatch
				[{ name: "0001_x" }], // toRollback rows
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const rolled = await runner.rollback();
		expect(rolled).toEqual(["0001_x"]);
		const allSql = transactions.flat().map((b) => b.sql);
		expect(allSql.some((s) => s.includes("DELETE FROM"))).toBe(true);
	});

	it("reset rolls back every batch until #currentBatch returns 0", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // reset's while -> currentBatch
				[{ max: 1 }], // rollback's #currentBatch
				[{ name: "0001_x" }], // rollback's toRollback rows
				[{ max: 0 }], // reset's while -> next iteration -> 0, exit
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });
		const rolled = await runner.reset();
		expect(rolled).toEqual(["0001_x"]);
	});

	it("refresh = reset + migrate", async () => {
		await makeMigrationFixtures(tmpDir, { "0001_x.ts": MIG_001 });
		const { adapter } = createFakeAdapter({
			queueQuery: [
				// init for reset
				[{ max: 0 }], // first currentBatch loop check, exit immediately
				// migrate path:
				[], // applied names
				[{ max: 0 }], // currentBatch
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const result = await runner.refresh();
		expect(result.rolled).toEqual([]);
		expect(result.executed).toEqual(["0001_x"]);
	});
});

describe("atlas > MigrationRunner > dryRun", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("returns the SQL of pending migrations without executing them", async () => {
		await makeMigrationFixtures(tmpDir, {
			"0001_x.ts": MIG_001,
			"0002_y.ts": MIG_002,
		});
		const { adapter, transactions } = createFakeAdapter({
			queueQuery: [
				[{ name: "0001_x" }], // already applied → only 0002 should appear
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });

		const result = await runner.dryRun();
		expect(result).toHaveLength(1);
		expect(result[0]?.name).toBe("0002_y");
		expect(result[0]?.sql.length).toBeGreaterThan(0);
		// dryRun MUST NOT trigger a migrate transaction.
		expect(transactions).toEqual([]);
	});
});

describe("atlas > MigrationRunner > #loadMigration error paths", () => {
	let tmpDir: string;
	beforeEach(async () => {
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mig-"));
	});
	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("throws AtlasError when a migration file is missing", async () => {
		// File listed in the tracking table but absent on disk.
		const { adapter } = createFakeAdapter({
			queueQuery: [
				[{ max: 1 }], // currentBatch
				[{ name: "0001_phantom" }], // toRollback
			],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });
		await expect(runner.rollback()).rejects.toThrow(AtlasError);
	});

	it("throws AtlasError when the migration module has no default export", async () => {
		await makeMigrationFixtures(tmpDir, {
			"0001_no_default.ts": "export const x = 1\n",
		});
		const { adapter } = createFakeAdapter({
			queueQuery: [[], [{ max: 0 }]],
		});
		const runner = new MigrationRunner(adapter, { migrationsDir: tmpDir });
		await expect(runner.migrate()).rejects.toThrow(AtlasError);
	});
});
