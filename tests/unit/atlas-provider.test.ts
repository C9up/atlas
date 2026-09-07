import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import AtlasProvider, {
	type AtlasAppContext,
	type AtlasDatabaseConfig,
} from "../../src/AtlasProvider.js";
import { connectionManager, getConnection } from "../../src/services/db.js";

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
	createNapiConnection: async (url?: string): Promise<FakeConnection> => {
		// Sentinel for the boot-failure test: a URL containing "fail" rejects.
		if (url?.includes("fail")) {
			throw new Error(`cannot connect: ${url}`);
		}
		return {
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
		};
	},
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

	it("reports a rejecting listener instead of ending the process", async () => {
		// `@adonisjs/events` declares `emit(): Promise<void>` and rethrows when a
		// listener fails and the application registered no error handler. This
		// bridge is driven by a driver callback nobody awaits, so that rejection
		// had nowhere to go — a query-log listener taking down the request it
		// was observing. The interface said `void`, which accepts a
		// promise-returning function, so nothing looked wrong.
		const written: string[] = [];
		const rejections: unknown[] = [];
		const originalWrite = process.stderr.write.bind(process.stderr);
		const onUnhandled = (reason: unknown): void => {
			rejections.push(reason);
		};
		process.stderr.write = (chunk: string | Uint8Array): boolean => {
			written.push(String(chunk));
			return true;
		};
		process.on("unhandledRejection", onUnhandled);
		try {
			const emitter = {
				emit: async () => {
					throw new Error("the query logger blew up");
				},
			};
			const { app } = makeApp({ url: "sqlite::memory:" });
			app.container.resolve = (token) =>
				token === "events" ? emitter : undefined;
			const provider = new AtlasProvider(app);
			await provider.boot();

			const { emitDbQuery } = await import("../../src/events.js");
			expect(() =>
				emitDbQuery({ sql: "SELECT 1", bindings: [], duration: 1 }),
			).not.toThrow();
			await new Promise((resolve) => setTimeout(resolve, 15));

			expect(rejections).toEqual([]);
			expect(written.join("")).toContain("query logger blew up");
			await provider.shutdown();
		} finally {
			process.stderr.write = originalWrite;
			process.off("unhandledRejection", onUnhandled);
		}
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
		// Filter to db:query — boot also emits db:connection:* lifecycle events now.
		const dbQuery = () => emitted.filter(([e]) => e === "db:query");
		emitDbQuery(event);
		expect(dbQuery()).toEqual([["db:query", event]]);

		// After shutdown the bridge is detached (no double-emit on re-boot).
		await provider.shutdown();
		emitDbQuery(event);
		expect(dbQuery()).toHaveLength(1);
	});

	it("bridges connection lifecycle onto the app emitter as 'db:connection:*'", async () => {
		const emitted: Array<[string, unknown]> = [];
		const emitter = { emit: (e: string, d: unknown) => emitted.push([e, d]) };
		const { app } = makeApp({ url: "sqlite::memory:" });
		app.container.resolve = (token) =>
			token === "events" ? emitter : undefined;
		const provider = new AtlasProvider(app);
		await provider.boot();
		// Boot opened + registered a connection → a db:connection:connect fired.
		const connects = emitted.filter(([e]) => e === "db:connection:connect");
		expect(connects.length).toBeGreaterThan(0);
		await provider.shutdown();
	});

	it("emits db:connection:error ([error, node]) when a boot connection fails", async () => {
		const emitted: Array<[string, unknown]> = [];
		const emitter = { emit: (e: string, d: unknown) => emitted.push([e, d]) };
		const { app } = makeApp({
			connection: "main",
			connections: {
				main: { url: "sqlite::memory:" },
				bad: { url: "sqlite:fail" }, // the mock rejects a "fail" URL
			},
		});
		app.container.resolve = (token) =>
			token === "events" ? emitter : undefined;
		await expect(new AtlasProvider(app).boot()).rejects.toThrow(/fail/);
		const errs = emitted.filter(([e]) => e === "db:connection:error");
		expect(errs.length).toBeGreaterThan(0);
		const [, payload] = errs[0] ?? [];
		expect(Array.isArray(payload)).toBe(true); // [error, node]
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
		const provider = new AtlasProvider(app);
		provider.register();
		await provider.boot();
		// The `connection` selector resolved `main` → the db services are bound.
		expect(bindings.some((b) => b.token === "db")).toBe(true);
		await provider.shutdown();
	});

	it("accepts Lucid's per-connection `connection` key as the URL (alias of `url`)", async () => {
		const { app, bindings } = makeApp({
			connection: "main",
			connections: {
				// Lucid names the per-connection URL key `connection`, not `url`.
				main: { connection: "sqlite::memory:" },
			},
		});
		const provider = new AtlasProvider(app);
		provider.register();
		await provider.boot();
		expect(bindings.some((b) => b.token === "db")).toBe(true);
		await provider.shutdown();
	});

	it("binds the connection tokens at REGISTER, before anything is opened", async () => {
		// They used to be bound inside boot(), after the pools opened, so whether
		// `container.make('db')` resolved depended on which provider booted
		// first: one reaching for the database in its own boot() found the token
		// unbound and read that as "atlas is not installed" rather than "not open
		// yet". Upstream binds in register() and opens nothing there.
		const { app, bindings } = makeApp({
			connection: "main",
			connections: { main: { url: "sqlite::memory:" } },
		});
		const provider = new AtlasProvider(app);

		provider.register();

		for (const token of ["db", "atlas.db", "db:main", "atlas.db:main"]) {
			expect(
				bindings.some((b) => b.token === token),
				token,
			).toBe(true);
		}
		// And resolving one before boot fails by name, rather than answering a
		// handle to a pool that was never opened.
		const factory = bindings.find((b) => b.token === "db")?.factory;
		if (!factory) throw new Error("expected the `db` factory");
		expect(() => factory()).toThrow(/is not available/);
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
			// `autoRun` opt-in: boot does not migrate on its own any more, and the
			// table name is what this test is about.
			migrations: { path: tmpDir, table: "schema_versions", autoRun: true },
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
			migrations: { path: tmpDir, autoRun: true },
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
				// `autoRun` ON, so the assertion is about precedence and not
				// about the default: the CLI's own run must still win.
				migrations: { path: tmpDir, autoRun: true },
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

	it("does NOT migrate on boot, in any environment, by default", async () => {
		// Upstream migrates from `migration:run` and nowhere else. Boot is the
		// wrong moment twice over: `warmUp()` boots too, so a route listing
		// mutated the schema, and every replica of a rolling deploy raced to
		// migrate the same database.
		for (const env of ["production", "development", "test"]) {
			executes.length = 0;
			process.env.NODE_ENV = env;
			const { app } = makeApp({
				url: "sqlite:memory",
				migrations: { path: tmpDir },
			});
			await new AtlasProvider(app).boot();
			expect(migratedOnBoot(), `NODE_ENV=${env}`).toBe(false);
		}
	});

	it("migrates on boot only when a host asks for it", async () => {
		process.env.NODE_ENV = "development";
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir, autoRun: true },
		});
		await new AtlasProvider(app).boot();
		expect(migratedOnBoot()).toBe(true);
	});

	it("opens no connection and migrates nothing while the app is being inspected", async () => {
		// `ream inspect`, a route listing and a codegen pass all go through
		// `warmUp()`, which runs register, boot and start. A pool opened there
		// made every one of them need a reachable database, and `shutdown()`
		// never fires on that path, so the pool stayed open.
		process.env.NODE_ENV = "development";
		const { app } = makeApp({
			url: "sqlite:memory",
			migrations: { path: tmpDir, autoRun: true },
		});
		const inspecting = { ...app, getMode: () => "warmup" };
		const provider = new AtlasProvider(inspecting);
		await provider.boot();
		await provider.start();
		expect(executes).toEqual([]);
	});

	it("stays quiet about the skip when the CLI drives migrations (REAM_SKIP_BOOT_MIGRATE=1)", async () => {
		process.env.NODE_ENV = "production";
		const prev = process.env.REAM_SKIP_BOOT_MIGRATE;
		process.env.REAM_SKIP_BOOT_MIGRATE = "1";
		const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
		try {
			const { app } = makeApp({
				url: "sqlite:memory",
				migrations: { path: tmpDir },
			});
			await new AtlasProvider(app).boot();
			expect(
				warn.mock.calls.some(
					([m]) => typeof m === "string" && m.includes("were NOT run"),
				),
			).toBe(false);
		} finally {
			warn.mockRestore();
			if (prev === undefined) delete process.env.REAM_SKIP_BOOT_MIGRATE;
			else process.env.REAM_SKIP_BOOT_MIGRATE = prev;
		}
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

/**
 * A failed boot must leave nothing behind for the next attempt.
 *
 * `manager.add()` is a NO-OP on a name it already knows. A boot that registered
 * every config, failed to open one, closed the pools it had opened and then
 * threw left those configs in place — so a retry in the same process, with the
 * config corrected, silently reopened the OLD settings and failed the same way,
 * with nothing to explain why the fix had no effect.
 */
describe("atlas > rolling back a failed boot", () => {
	it("forgets the configs it registered, so a retry sees the new ones", async () => {
		// The mock connector rejects any URL containing "fail".
		const bad = {
			connection: "primary",
			connections: { primary: { url: "sqlite:fail" } },
		};
		const { app } = makeApp(bad);
		const provider = new AtlasProvider(app);
		provider.register();
		await expect(provider.boot()).rejects.toThrow();

		// Nothing left registered under that name: the next `add` is free to
		// install the corrected settings rather than being ignored.
		expect(connectionManager().has("primary")).toBe(false);
	});
});
