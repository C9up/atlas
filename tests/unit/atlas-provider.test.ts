import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AtlasProvider, {
	type AtlasAppContext,
	type AtlasDatabaseConfig,
} from "../../src/AtlasProvider.js";
import { getConnection } from "../../src/services/db.js";

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
let lastLockToken: unknown;
let closeCount = 0;

vi.mock("../../src/adapters/NapiDbAdapter.js", () => ({
	createNapiConnection: async (): Promise<FakeConnection> => ({
		async execute(sql, params) {
			executes.push({ sql, params });
			// Capture the migration-lock token so the read-back below reflects it.
			if (/_lock/i.test(sql) && /is_locked = 1/.test(sql)) {
				lastLockToken = params?.[0];
			}
		},
		async query<T>(sql?: string): Promise<T[]> {
			// Lock-aware: the acquire SELECT reads back the token we captured, so
			// #acquireLock sees itself as the winner. Everything else is empty.
			if (sql && /locked_by/i.test(sql)) {
				return [{ locked_by: lastLockToken }] as T[];
			}
			return [];
		},
		async close() {
			closeCount++;
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

describe("atlas > AtlasProvider > db:query emitter bridge (AdonisJS parity)", () => {
	afterEach(async () => {
		const { clearDbQueryListeners } = await import("../../src/events.js");
		clearDbQueryListeners();
	});

	it("bridges atlas query events onto the app emitter as 'db:query'", async () => {
		const emitted: Array<[string, unknown]> = [];
		const emitter = { emit: (e: string, d: unknown) => emitted.push([e, d]) };
		const { app } = makeApp({ url: "sqlite::memory:" });
		app.container.resolve = (token) =>
			token === "events" ? emitter : undefined;
		const provider = new AtlasProvider(app);
		await provider.boot();

		const { emitDbQuery } = await import("../../src/events.js");
		const event = { sql: "SELECT 1", bindings: [], duration: 1 };
		emitDbQuery(event);
		expect(emitted).toEqual([["db:query", event]]);

		// After shutdown the bridge is detached (no double-emit on re-boot).
		await provider.shutdown();
		emitDbQuery(event);
		expect(emitted).toHaveLength(1);
	});
});

describe("atlas > Lucid-shaped config aliases", () => {
	it("accepts `connection` (default selector) + `pool` + `migrations.paths`", async () => {
		const { app, bindings } = makeApp({
			url: "sqlite::memory:",
			connection: "main",
			connections: {
				main: { url: "sqlite::memory:", pool: { min: 1, max: 5 } },
			},
			migrations: { paths: ["database/migrations"] },
		});
		await new AtlasProvider(app).boot();
		// The `connection` selector resolved `main` → the db services are bound.
		expect(bindings.some((b) => b.token === "db")).toBe(true);
		await new AtlasProvider(app).shutdown();
	});

	it("accepts Lucid's per-connection `connection` key as the URL (alias of `url`)", async () => {
		const { app, bindings } = makeApp({
			connection: "main",
			connections: {
				// Lucid names the per-connection URL key `connection`, not `url`.
				main: { connection: "sqlite::memory:" },
			},
		});
		await new AtlasProvider(app).boot();
		expect(bindings.some((b) => b.token === "db")).toBe(true);
		await new AtlasProvider(app).shutdown();
	});

	it("exports BaseSchema (Lucid's migration base class) as an alias of Migration", async () => {
		const mod = await import("../../src/index.js");
		expect(mod.BaseSchema).toBe(mod.Migration);
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

describe("atlas > AtlasProvider > boot-migration production guard", () => {
	let tmpDir: string;
	const prevEnv = process.env.NODE_ENV;
	beforeEach(async () => {
		executes.length = 0;
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-provider-"));
	});
	afterEach(async () => {
		process.env.NODE_ENV = prevEnv;
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	function migratedOnBoot(): boolean {
		return executes.some((e) => /CREATE TABLE IF NOT EXISTS/i.test(e.sql));
	}

	it("does NOT auto-migrate on boot in production by default", async () => {
		process.env.NODE_ENV = "production";
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir },
		});
		await new AtlasProvider(app).boot();
		expect(migratedOnBoot()).toBe(false);
	});

	it("DOES auto-migrate in production when explicitly opted in", async () => {
		process.env.NODE_ENV = "production";
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir, autoRunInProduction: true },
		});
		await new AtlasProvider(app).boot();
		expect(migratedOnBoot()).toBe(true);
	});

	it("auto-migrates on boot outside production (dev convenience)", async () => {
		process.env.NODE_ENV = "development";
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir },
		});
		await new AtlasProvider(app).boot();
		expect(migratedOnBoot()).toBe(true);
	});
});

describe("atlas > AtlasProvider > connection lifecycle", () => {
	beforeEach(() => {
		executes.length = 0;
		closeCount = 0;
	});

	it("registers named connections at boot and UNregisters them at shutdown", async () => {
		const { app } = makeApp({ url: "sqlite:memory" });
		const provider = new AtlasProvider(app);
		await provider.boot();

		// boot populated the named-connection registry (finding #4 of round 1).
		expect(getConnection("primary")).toBeDefined();

		await provider.shutdown();

		// shutdown UNregisters, so nobody can fetch a now-closed handle.
		expect(getConnection("primary")).toBeUndefined();
		expect(closeCount).toBeGreaterThan(0);
	});

	it("closes opened connections when a post-open boot step fails", async () => {
		// The pools open, then the default-connection lookup fails — the opened
		// connection must not leak.
		const { app } = makeApp({
			url: "sqlite:memory",
			connections: { primary: { url: "sqlite:memory" } },
			default: "ghost",
		});
		await expect(new AtlasProvider(app).boot()).rejects.toThrow(/ghost/);

		expect(closeCount).toBeGreaterThan(0);
		// And the registry was rolled back too.
		expect(getConnection("primary")).toBeUndefined();
	});
});
