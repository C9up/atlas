/**
 * The migration settings of a project set up by `configure`.
 *
 * The stub writes `migrations` inside the connection, where the commands read
 * it and where the reference config shape keeps it. The provider read only the
 * top level of the database config, so a generated project's `tableName`,
 * `paths` and `autoRun` were never seen at boot: the provider tracked runs in
 * `ream_migrations` and `migration:run` in the table the project named, and an
 * `autoRun` container came up on an empty schema.
 *
 * Driven end to end: configure renders the real stub, the file it writes is
 * imported as the application would import it, and the provider boots it
 * against a real sqlite database.
 */
import { readFileSync } from "node:fs";
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import AtlasProvider, {
	type AtlasAppContext,
	type AtlasDatabaseConfig,
} from "../../src/AtlasProvider.js";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { configure } from "../../src/configure.js";

const ATLAS_INDEX = pathToFileURL(
	path.resolve(__dirname, "../../src/index.ts"),
).href;
const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

const saved = {
	DATABASE_URL: process.env.DATABASE_URL,
	NODE_ENV: process.env.NODE_ENV,
	REAM_ENV: process.env.REAM_ENV,
	REAM_SKIP_BOOT_MIGRATE: process.env.REAM_SKIP_BOOT_MIGRATE,
};

function restore(name: keyof typeof saved): void {
	const value = saved[name];
	if (value === undefined) delete process.env[name];
	else process.env[name] = value;
}

let project: string;
let dbFile: string;
let provider: AtlasProvider | undefined;
let probe: AsyncDatabaseConnection | undefined;

beforeEach(async () => {
	project = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-configured-"));
	dbFile = path.join(project, "app.db");
	// The stub reads its URL from the environment, the way a deployed app is
	// configured, so the file it writes is booted unedited apart from the
	// migration keys under test.
	process.env.DATABASE_URL = `sqlite:${dbFile}`;
	// `autoRun` is ignored in development and when the CLI drives migrations;
	// neither is the case under test.
	process.env.NODE_ENV = "test";
	delete process.env.REAM_ENV;
	delete process.env.REAM_SKIP_BOOT_MIGRATE;
	await fsp.mkdir(path.join(project, "database", "migrations"), {
		recursive: true,
	});
	await fsp.writeFile(
		path.join(project, "database", "migrations", "001_create_users.ts"),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('users', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('users') }
}`,
	);
});

afterEach(async () => {
	await provider?.shutdown();
	provider = undefined;
	await probe?.close();
	probe = undefined;
	for (const name of [
		"DATABASE_URL",
		"NODE_ENV",
		"REAM_ENV",
		"REAM_SKIP_BOOT_MIGRATE",
	] as const) {
		restore(name);
	}
	await fsp.rm(project, { recursive: true, force: true });
});

/** Read a stub the way `codemods.makeUsingStub` does: front matter, then body. */
function renderStub(
	stubsRoot: string,
	stubPath: string,
): { to: string; body: string } {
	const raw = readFileSync(path.resolve(stubsRoot, stubPath), "utf8");
	const [, front = "", body = ""] = raw.split(/^---\r?\n/m, 3);
	const to = /^to:\s*(.+)$/m.exec(front)?.[1]?.trim() ?? "";
	return { to, body };
}

/**
 * Run `configure` into the project, then edit the generated connection's
 * `migrations` block the way a user would.
 */
async function configureProject(migrationKeys: string): Promise<string> {
	const written: Array<{ to: string; body: string }> = [];
	await configure({
		async addProvider() {},
		async registerCommand() {},
		async addEnvVars() {},
		async writeFile(to, content) {
			written.push({ to, body: content });
		},
		async makeUsingStub(stubsRoot, stubPath) {
			const { to, body } = renderStub(stubsRoot, stubPath);
			written.push({ to, body });
			return { path: to, contents: body };
		},
	});
	const generated = written.find((f) => f.to === "config/database.ts");
	if (generated === undefined) throw new Error("configure wrote no config");

	const pathsLine = "paths: ['database/migrations'],";
	expect(generated.body).toContain(pathsLine);
	const migrationsDir = path.join(project, "database", "migrations");
	const edited = generated.body
		// The package is not installed in the temporary project: point the
		// import at this checkout.
		.replace("from '@c9up/atlas'", `from '${ATLAS_INDEX}'`)
		.replace(
			pathsLine,
			`paths: [${JSON.stringify(migrationsDir)}], ${migrationKeys}`,
		);
	const target = path.join(project, generated.to);
	await fsp.mkdir(path.dirname(target), { recursive: true });
	await fsp.writeFile(target, edited);
	return target;
}

async function loadConfig(file: string): Promise<AtlasDatabaseConfig> {
	const mod: { default: AtlasDatabaseConfig } = await import(
		pathToFileURL(file).href
	);
	return mod.default;
}

function appFor(config: AtlasDatabaseConfig): AtlasAppContext {
	const store = new Map<string, unknown>([["database", config]]);
	// The host's reader is generic over what the caller expects; an overload
	// states that without asserting a type at the call.
	function get<T = unknown>(key: string): T | undefined;
	function get(key: string): unknown {
		return store.get(key);
	}
	return { container: { singleton() {} }, config: { get } };
}

async function tables(): Promise<string[]> {
	probe ??= await createNapiConnection(`sqlite:${dbFile}`, 1, 1);
	const rows = await probe.query<{ name: string }>(
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
	);
	return rows.map((r) => r.name);
}

describe("atlas > the migrations block configure writes reaches the provider", () => {
	it("honours the connection's tableName and autoRun at boot", async () => {
		const file = await configureProject(
			"tableName: 'schema_versions', autoRun: true,",
		);
		provider = new AtlasProvider(appFor(await loadConfig(file)));
		await provider.boot();

		const names = await tables();
		// autoRun was seen: the migration ran.
		expect(names).toContain("users");
		// And it was tracked where the project said, not in the default table.
		expect(names).toContain("schema_versions");
		expect(names).not.toContain("ream_migrations");
	});

	it("does not migrate when the connection leaves autoRun out", async () => {
		const file = await configureProject("tableName: 'schema_versions',");
		provider = new AtlasProvider(appFor(await loadConfig(file)));
		await provider.boot();

		expect(await tables()).not.toContain("users");
	});

	it("lets the connection's block win over a top-level one, key by key", async () => {
		const file = await configureProject("tableName: 'schema_versions',");
		const config = await loadConfig(file);
		// A top-level block is the single-connection form, still read as a
		// fallback: its `autoRun` applies, its `tableName` is overridden.
		provider = new AtlasProvider(
			appFor({
				...config,
				migrations: { tableName: "top_level_table", autoRun: true },
			}),
		);
		await provider.boot();

		const names = await tables();
		expect(names).toContain("users");
		expect(names).toContain("schema_versions");
		expect(names).not.toContain("top_level_table");
	});
});
