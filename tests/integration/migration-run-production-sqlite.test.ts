/**
 * `migration:run` in production asks before migrating (Lucid
 * `takeProductionConsent`), unless `--force` is passed. A forward migration
 * can be as destructive as a rollback; it used to run with no question asked.
 * A cancelled or unavailable prompt (non-interactive run) counts as "no".
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { migrationRunCommand } from "../../src/console/migrationCommands.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { runCommand } from "../helpers/runCommand.js";

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

let conn: AsyncDatabaseConnection;
let dir: string;
const previousEnv = process.env.NODE_ENV;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	setDb(conn);
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-prod-run-"));
	await fsp.writeFile(
		path.join(dir, "001_users.ts"),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('users', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('users') }
}`,
	);
	process.exitCode = 0;
});

afterEach(async () => {
	process.env.NODE_ENV = previousEnv;
	clearDb(conn);
	await conn.close();
	await fsp.rm(dir, { recursive: true, force: true });
	process.exitCode = 0;
});

async function usersExists(): Promise<boolean> {
	const rows = await conn.query<{ n: number }>(
		"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='users'",
	);
	return Number(rows[0]?.n) === 1;
}

function prompt(answer: boolean | Error): {
	asked: string[];
	confirm(message: string): Promise<boolean>;
} {
	const asked: string[] = [];
	return {
		asked,
		async confirm(message: string) {
			asked.push(message);
			if (answer instanceof Error) throw answer;
			return answer;
		},
	};
}

describe("atlas > migration:run in production", () => {
	it("does not migrate when the operator declines", async () => {
		const declined = prompt(false);
		await runCommand(migrationRunCommand({ migrationsDir: dir }), {
			app: { inProduction: true },
			prompt: declined,
		});
		expect(declined.asked).toEqual([
			"You are in production environment. Want to continue running migrations?",
		]);
		expect(await usersExists()).toBe(false);
		expect(process.exitCode).toBe(0);
	});

	it("treats a cancelled or missing prompt as no", async () => {
		await runCommand(migrationRunCommand({ migrationsDir: dir }), {
			app: { inProduction: true },
			prompt: prompt(new Error("non-interactive")),
		});
		expect(await usersExists()).toBe(false);
		process.env.NODE_ENV = "production";
		await runCommand(migrationRunCommand({ migrationsDir: dir }));
		expect(await usersExists()).toBe(false);
	});

	it("migrates when the operator agrees", async () => {
		await runCommand(migrationRunCommand({ migrationsDir: dir }), {
			app: { inProduction: true },
			prompt: prompt(true),
		});
		expect(await usersExists()).toBe(true);
	});

	it("migrates with --force without asking", async () => {
		const unused = prompt(false);
		await runCommand(migrationRunCommand({ migrationsDir: dir }), {
			app: { inProduction: true },
			prompt: unused,
			force: true,
		});
		expect(unused.asked).toEqual([]);
		expect(await usersExists()).toBe(true);
	});

	it("migrates outside production without asking", async () => {
		const unused = prompt(false);
		await runCommand(migrationRunCommand({ migrationsDir: dir }), {
			app: { inProduction: false },
			prompt: unused,
		});
		expect(unused.asked).toEqual([]);
		expect(await usersExists()).toBe(true);
	});

	it("declares a --force flag", () => {
		const flags = migrationRunCommand({ migrationsDir: dir }).flags ?? [];
		expect(flags.map((f) => f.flagName)).toContain("force");
	});
});
