import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AtlasProvider, {
	type AtlasAppContext,
	type AtlasDatabaseConfig,
} from "../../src/AtlasProvider.js";

interface ExecCall {
	sql: string;
	params?: unknown[];
}

interface FakeConnection {
	execute(sql: string, params?: unknown[]): Promise<void>;
	query<T>(sql: string, params?: unknown[]): Promise<T[]>;
	close(): Promise<void>;
	runInTransaction(
		batch: readonly { sql: string; params?: unknown[] }[],
	): Promise<number>;
}

const executes: ExecCall[] = [];

vi.mock("../../src/adapters/NapiDbAdapter.js", () => ({
	createNapiConnection: async (): Promise<FakeConnection> => ({
		async execute(sql, params) {
			executes.push({ sql, params });
		},
		async query<T>(): Promise<T[]> {
			return [];
		},
		async close() {
			// no-op
		},
		async runInTransaction(batch) {
			for (const stmt of batch) {
				executes.push({ sql: stmt.sql, params: stmt.params });
			}
			return batch.length;
		},
	}),
}));

interface SingletonRecord {
	token: unknown;
	factory: () => unknown;
}

function makeApp(authConfig: AtlasDatabaseConfig | undefined): {
	app: AtlasAppContext;
	bindings: SingletonRecord[];
} {
	const bindings: SingletonRecord[] = [];
	const app: AtlasAppContext = {
		container: {
			singleton(token, factory) {
				bindings.push({ token, factory });
			},
		},
		config: {
			get<T = unknown>(key: string): T | undefined {
				if (key === "database" && authConfig) return authConfig as T;
				return undefined;
			},
		},
	};
	return { app, bindings };
}

describe("atlas > AtlasProvider", () => {
	it("boot is a no-op (no container bindings) when 'database' config is absent — protects boot path from crashing during partial config", async () => {
		const { app, bindings } = makeApp(undefined);
		await new AtlasProvider(app).boot();
		expect(bindings).toEqual([]);
	});
});

describe("atlas > AtlasProvider > migrations.table plumbing", () => {
	let tmpDir: string;

	beforeEach(async () => {
		executes.length = 0;
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-provider-"));
	});

	afterEach(async () => {
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("threads database.migrations.table into the MigrationRunner so CREATE TABLE targets the custom name", async () => {
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir, table: "schema_versions" },
		});
		await new AtlasProvider(app).boot();

		const createStmt = executes.find((e) =>
			/CREATE TABLE IF NOT EXISTS/i.test(e.sql),
		);
		expect(createStmt).toBeDefined();
		expect(createStmt?.sql).toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?schema_versions/i,
		);
		expect(createStmt?.sql).not.toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?ream_migrations/i,
		);
	});

	it("falls back to ream_migrations when database.migrations.table is omitted", async () => {
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir },
		});
		await new AtlasProvider(app).boot();

		const createStmt = executes.find((e) =>
			/CREATE TABLE IF NOT EXISTS/i.test(e.sql),
		);
		expect(createStmt?.sql).toMatch(
			/CREATE TABLE IF NOT EXISTS\s+["`]?ream_migrations/i,
		);
	});

	it("skips boot-migration when REAM_SKIP_BOOT_MIGRATE=1 (the CLI drives it)", async () => {
		const prev = process.env.REAM_SKIP_BOOT_MIGRATE;
		process.env.REAM_SKIP_BOOT_MIGRATE = "1";
		try {
			const { app } = makeApp({
				url: "sqlite:memory",
				migrations: { path: tmpDir },
			});
			await new AtlasProvider(app).boot();
			// No migration pass ⇒ not even the tracking-table CREATE runs on boot.
			const createStmt = executes.find((e) =>
				/CREATE TABLE IF NOT EXISTS/i.test(e.sql),
			);
			expect(createStmt).toBeUndefined();
		} finally {
			if (prev === undefined) delete process.env.REAM_SKIP_BOOT_MIGRATE;
			else process.env.REAM_SKIP_BOOT_MIGRATE = prev;
		}
	});
});
