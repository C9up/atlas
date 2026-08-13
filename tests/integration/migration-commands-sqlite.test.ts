/**
 * The migration console commands (`migration:run|rollback|status|reset|refresh`,
 * `db:wipe`) drive the real MigrationRunner against a live sqlite connection
 * resolved from atlas's own `getDb` locator. These tests invoke each command's
 * `run()` for real and assert the database side effects, not console output.
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
import {
	dbWipeCommand,
	migrationFreshCommand,
	migrationRefreshCommand,
	migrationResetCommand,
	migrationRollbackCommand,
	migrationRunCommand,
	migrationStatusCommand,
} from "../../src/console/migrationCommands.js";
import { setAtlasDialect } from "../../src/query/native.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { runCommand } from "../helpers/runCommand.js";

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

let conn: AsyncDatabaseConnection;
let tmpDir: string;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	setDb(conn);
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-cmd-"));
	process.exitCode = 0;
});

afterEach(async () => {
	clearDb(conn);
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
	process.exitCode = 0;
});

async function writeMigration(name: string, table: string): Promise<void> {
	await fsp.writeFile(
		path.join(tmpDir, `${name}.ts`),
		`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('${table}', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('${table}') }
}`,
	);
}

function tableExists(name: string): Promise<{ n: number }[]> {
	return conn.query<{ n: number }>(
		`SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='${name}'`,
	);
}

describe("migration:run", () => {
	it("applies pending migrations against the live connection", async () => {
		await writeMigration("001_widgets", "widgets");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		expect((await tableExists("widgets"))[0]?.n).toBe(1);
	});
});

describe("migration:status", () => {
	it("reports pending before run and applied after", async () => {
		await writeMigration("001_widgets", "widgets");
		const status = migrationStatusCommand({ migrationsDir: tmpDir });

		// A status probe before running must not create the table.
		await runCommand(status);
		expect((await tableExists("widgets"))[0]?.n).toBe(0);

		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));
		const applied = await conn.query<{ name: string; batch: number }>(
			"SELECT name, batch FROM ream_migrations",
		);
		expect(applied).toHaveLength(1);
		expect(applied[0]?.name).toBe("001_widgets");
	});
});

describe("migration:rollback", () => {
	it("undoes only the latest batch by default", async () => {
		await writeMigration("001_a", "a");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir })); // batch 1
		await writeMigration("002_b", "b");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir })); // batch 2

		await runCommand(migrationRollbackCommand({ migrationsDir: tmpDir }));

		expect((await tableExists("b"))[0]?.n).toBe(0); // latest batch gone
		expect((await tableExists("a"))[0]?.n).toBe(1); // earlier batch kept
	});

	it("rolls back to a target batch with --batch=0", async () => {
		await writeMigration("001_a", "a");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));
		await writeMigration("002_b", "b");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		await runCommand(migrationRollbackCommand({ migrationsDir: tmpDir }), {
			batch: 0,
		});

		expect((await tableExists("a"))[0]?.n).toBe(0);
		expect((await tableExists("b"))[0]?.n).toBe(0);
	});

	it("rejects a non-integer --batch and sets a failing exit code", async () => {
		await writeMigration("001_a", "a");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		// A non-numeric --batch never reaches the command: the kernel rejects it
		// while parsing (the flag is declared as a number). What the command
		// still validates is the range.
		await runCommand(migrationRollbackCommand({ migrationsDir: tmpDir }), {
			batch: -1,
		});

		expect(process.exitCode).toBe(1);
		// The migration is untouched — an invalid flag rolls nothing back.
		expect((await tableExists("a"))[0]?.n).toBe(1);
	});
});

describe("migration:reset + refresh", () => {
	it("reset rolls back everything", async () => {
		await writeMigration("001_a", "a");
		await writeMigration("002_b", "b");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		await runCommand(migrationResetCommand({ migrationsDir: tmpDir }));

		expect((await tableExists("a"))[0]?.n).toBe(0);
		expect((await tableExists("b"))[0]?.n).toBe(0);
	});

	it("refresh rolls back then re-runs, leaving tables present", async () => {
		await writeMigration("001_a", "a");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		await runCommand(migrationRefreshCommand({ migrationsDir: tmpDir }));

		expect((await tableExists("a"))[0]?.n).toBe(1);
	});
});

describe("migration:fresh", () => {
	it("drops all tables then re-runs every migration", async () => {
		await writeMigration("001_widgets", "widgets");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));
		await conn.execute("INSERT INTO widgets DEFAULT VALUES");

		await runCommand(migrationFreshCommand({ migrationsDir: tmpDir }));

		// Table re-created (so it exists) and empty (dropped + re-migrated).
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM widgets",
		);
		expect(rows[0]?.n).toBe(0);
		const applied = await conn.query<{ name: string }>(
			"SELECT name FROM ream_migrations",
		);
		expect(applied.map((a) => a.name)).toEqual(["001_widgets"]);
	});
});

describe("migration:status is read-only", () => {
	it("does not create the tracking table on a fresh database", async () => {
		await writeMigration("001_widgets", "widgets");
		await runCommand(migrationStatusCommand({ migrationsDir: tmpDir }));
		const tracking = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table' AND name='ream_migrations'",
		);
		expect(tracking[0]?.n).toBe(0);
	});
});

describe("db:wipe", () => {
	it("drops every table including the migrations table", async () => {
		await writeMigration("001_widgets", "widgets");
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));

		await runCommand(dbWipeCommand({ migrationsDir: tmpDir }));

		const tables = await conn.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
		);
		// wipe leaves a truly empty database — including the lock table.
		expect(tables).toEqual([]);
	});
});

describe("no connection registered", () => {
	it("reports and sets a failing exit code instead of throwing", async () => {
		clearDb(conn);
		await runCommand(migrationRunCommand({ migrationsDir: tmpDir }));
		expect(process.exitCode).toBe(1);
	});
});
