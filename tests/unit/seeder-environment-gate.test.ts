/**
 * `db:seed` gates `static environment` on the environment the other guards
 * read, never on an absent one.
 *
 * It compared the raw `NODE_ENV`: with only `REAM_ENV=production`, or with
 * neither variable, no environment was set and every restricted seeder ran —
 * a development seeder with a known admin password included. `NODE_ENV=prod`
 * skipped the production seeder, the alias table unread.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { dbSeedCommand } from "../../src/console/seederCommands.js";
import { runSeederDirectory } from "../../src/schema/Seeder.js";
import { clearDb, setDb } from "../../src/services/db.js";
import { currentEnvironment } from "../../src/utils/environment.js";

let conn: AsyncDatabaseConnection;
let dir: string;
const saved = {
	NODE_ENV: process.env.NODE_ENV,
	REAM_ENV: process.env.REAM_ENV,
};

function setEnv(values: { NODE_ENV?: string; REAM_ENV?: string }): void {
	for (const key of ["NODE_ENV", "REAM_ENV"] as const) {
		const value = values[key];
		if (value === undefined) delete process.env[key];
		else process.env[key] = value;
	}
}

const seeder = (marker: string, environment: string[]): string =>
	`export default class {
  static environment = ${JSON.stringify(environment)};
  constructor(db) { this.db = db; }
  async run() {
    await this.db.execute("INSERT INTO seed_gate_log (marker) VALUES (?)", ["${marker}"]);
  }
}
`;

async function markers(): Promise<string[]> {
	const rows = await conn.query<{ marker: string }>(
		"SELECT marker FROM seed_gate_log ORDER BY marker",
	);
	return rows.map((r) => r.marker);
}

/** Run `db:seed`, optionally with the application the kernel hands over. */
async function seed(app?: { nodeEnvironment: string }): Promise<string[]> {
	const Command = dbSeedCommand({ seedersDir: dir });
	const command = new Command();
	if (app) Reflect.set(command, "app", app);
	await command.run();
	return markers();
}

beforeAll(async () => {
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-seed-gate-"));
	await fsp.writeFile(
		path.join(dir, "1_dev_admin.mjs"),
		seeder("dev", ["development"]),
	);
	await fsp.writeFile(
		path.join(dir, "2_prod_reference.mjs"),
		seeder("prod", ["production"]),
	);
	await fsp.writeFile(
		path.join(dir, "3_test_fixtures.mjs"),
		seeder("test", ["testing"]),
	);
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute("CREATE TABLE seed_gate_log (marker TEXT)");
	setDb(conn);
});

beforeEach(async () => {
	await conn.execute("DELETE FROM seed_gate_log");
});

afterAll(async () => {
	setEnv(saved);
	clearDb(conn);
	await conn.close();
	await fsp.rm(dir, { recursive: true, force: true });
});

describe("db:seed environment gate", () => {
	it("REAM_ENV=production alone is production", async () => {
		setEnv({ REAM_ENV: "production" });
		expect(await seed()).toEqual(["prod"]);
	});

	it("no environment at all runs no restricted seeder", async () => {
		setEnv({});
		expect(currentEnvironment()).toBe("unknown");
		expect(await seed()).toEqual([]);
	});

	it("reads the aliases on both sides: prod is production, test is testing", async () => {
		setEnv({ NODE_ENV: "prod" });
		expect(await seed()).toEqual(["prod"]);
		await conn.execute("DELETE FROM seed_gate_log");
		setEnv({ NODE_ENV: "test" });
		expect(await seed()).toEqual(["test"]);
	});

	it("production wins when either variable says so", async () => {
		setEnv({ NODE_ENV: "development", REAM_ENV: "production" });
		expect(await seed()).toEqual(["prod"]);
	});

	it("takes the application's nodeEnvironment when the kernel hands it over", async () => {
		setEnv({ NODE_ENV: "development" });
		expect(await seed({ nodeEnvironment: "production" })).toEqual(["prod"]);
	});

	it("--environment still picks the environment explicitly", async () => {
		setEnv({ NODE_ENV: "production" });
		const Command = dbSeedCommand({ seedersDir: dir });
		const command = new Command();
		Reflect.set(command, "environment", "dev");
		await command.run();
		expect(await markers()).toEqual(["dev"]);
	});

	it("runSeederDirectory() without an environment still runs everything", async () => {
		await runSeederDirectory(dir, conn);
		expect(await markers()).toEqual(["dev", "prod", "test"]);
	});
});
