/**
 * The commands that destroy data ask in production, as `migration:run` does.
 *
 * `db:wipe`, `migration:fresh`, `migration:rollback`, `migration:reset` and
 * `migration:refresh` called the runner directly. With
 * `disableRollbacksInProduction: false` nothing stood between the operator and
 * an empty production database: no question, no flag. Each now takes the
 * production consent unless `--force` is passed; the option, on by default,
 * still refuses first, without a question.
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
import type { AtlasCommandClass } from "../../src/console/contract.js";
import {
	dbWipeCommand,
	type MigrationCommandOptions,
	migrationFreshCommand,
	migrationRefreshCommand,
	migrationResetCommand,
	migrationRollbackCommand,
	migrationRunCommand,
} from "../../src/console/migrationCommands.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { runCommand } from "../helpers/runCommand.js";

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

const MIGRATIONS_QUESTION =
	"You are in production environment. Want to continue running migrations?";
const WIPE_QUESTION =
	"You are in production environment. Want to continue wiping the database?";

let conn: AsyncDatabaseConnection;
let dir: string;
const saved = {
	NODE_ENV: process.env.NODE_ENV,
	REAM_ENV: process.env.REAM_ENV,
};

function restoreEnv(): void {
	for (const [key, value] of Object.entries(saved)) {
		if (value === undefined) delete process.env[key];
		else process.env[key] = value;
	}
}

beforeEach(async () => {
	restoreEnv();
	delete process.env.NODE_ENV;
	delete process.env.REAM_ENV;
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	setDb(conn);
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-prod-destroy-"));
	await fsp.writeFile(
		path.join(dir, "001_users.ts"),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('users', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('users') }
}`,
	);
	// Applied outside production, with one row of data.
	await runCommand(migrationRunCommand({ migrationsDir: dir }), {
		app: { inProduction: false },
	});
	await conn.execute("INSERT INTO users (id) VALUES (1)");
	process.exitCode = 0;
});

afterEach(async () => {
	restoreEnv();
	clearDb(conn);
	await conn.close();
	await fsp.rm(dir, { recursive: true, force: true });
	process.exitCode = 0;
});

/** Whether the row of data survived. */
async function rowSurvived(): Promise<boolean> {
	const tables = await conn.query<{ n: number }>(
		"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='users'",
	);
	if (Number(tables[0]?.n) !== 1) return false;
	const rows = await conn.query<{ n: number }>(
		"SELECT COUNT(*) AS n FROM users",
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

const COMMANDS: Array<
	[string, (o: MigrationCommandOptions) => AtlasCommandClass, string]
> = [
	["db:wipe", dbWipeCommand, WIPE_QUESTION],
	["migration:fresh", migrationFreshCommand, WIPE_QUESTION],
	["migration:rollback", migrationRollbackCommand, MIGRATIONS_QUESTION],
	["migration:reset", migrationResetCommand, MIGRATIONS_QUESTION],
	["migration:refresh", migrationRefreshCommand, MIGRATIONS_QUESTION],
];

describe.each(COMMANDS)(
	"atlas > %s in production",
	(_name, makeCommand, question) => {
		const optedOut = (): AtlasCommandClass =>
			makeCommand({ migrationsDir: dir, disableRollbacksInProduction: false });

		it("asks once and does nothing when the operator declines", async () => {
			process.env.REAM_ENV = "production";
			const declined = prompt(false);
			await runCommand(optedOut(), {
				app: { inProduction: true },
				prompt: declined,
			});
			expect(declined.asked).toEqual([question]);
			expect(await rowSurvived()).toBe(true);
		});

		it("treats a failed or missing prompt as no", async () => {
			process.env.REAM_ENV = "production";
			await runCommand(optedOut(), {
				app: { inProduction: true },
				prompt: prompt(new Error("non-interactive")),
			});
			await runCommand(optedOut(), { app: { inProduction: true } });
			expect(await rowSurvived()).toBe(true);
		});

		it("runs when the operator agrees", async () => {
			process.env.REAM_ENV = "production";
			const agreed = prompt(true);
			await runCommand(optedOut(), {
				app: { inProduction: true },
				prompt: agreed,
			});
			expect(agreed.asked).toEqual([question]);
			expect(await rowSurvived()).toBe(false);
		});

		it("runs with --force without asking", async () => {
			process.env.REAM_ENV = "production";
			const unused = prompt(false);
			await runCommand(optedOut(), {
				app: { inProduction: true },
				prompt: unused,
				force: true,
			});
			expect(unused.asked).toEqual([]);
			expect(await rowSurvived()).toBe(false);
		});

		it("runs outside production without asking", async () => {
			const unused = prompt(false);
			await runCommand(optedOut(), {
				app: { inProduction: false },
				prompt: unused,
			});
			expect(unused.asked).toEqual([]);
			expect(await rowSurvived()).toBe(false);
		});

		it("with the option on, refuses before any question", async () => {
			process.env.REAM_ENV = "production";
			const unused = prompt(true);
			await expect(
				runCommand(makeCommand({ migrationsDir: dir }), {
					app: { inProduction: true },
					prompt: unused,
				}),
			).rejects.toMatchObject({ code: "E_ROLLBACK_DISABLED_IN_PRODUCTION" });
			expect(unused.asked).toEqual([]);
			expect(await rowSurvived()).toBe(true);
		});
	},
);
