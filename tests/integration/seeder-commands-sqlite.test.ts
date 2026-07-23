/**
 * Seeder console commands (Adonis Lucid `make:seeder` / `db:seed`): scaffold a
 * seeder file, then run seeders — all, a `--files` subset, and via `--connection`.
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
import { makeFactoryCommand } from "../../src/console/factoryCommands.js";
import {
	dbSeedCommand,
	makeSeederCommand,
} from "../../src/console/seederCommands.js";
import { runSeederDirectory } from "../../src/schema/Seeder.js";
import { clearDb, registerConnection, setDb } from "../../src/services/db.js";

let conn: AsyncDatabaseConnection;
let dir: string;

/**
 * Write a seeder file that inserts one marker row. A seeder only needs a default
 * class with `constructor(db)` + `run()` (what `runSeederDirectory` invokes), so
 * the tmp file stays self-contained — no cross-directory import of BaseSeeder.
 */
async function writeSeeder(file: string, marker: string): Promise<void> {
	await fsp.writeFile(
		path.join(dir, file),
		`export default class {
  constructor(db) { this.db = db; }
  async run() {
    await this.db.execute("INSERT INTO seed_log (marker) VALUES (?)", ["${marker}"]);
  }
}
`,
	);
}

const SEEDER_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Seeder.js"),
).href;

/** A seeder that extends BaseSeeder and uses `this.client`, gated by environment. */
async function writeClassSeeder(
	file: string,
	marker: string,
	environment?: string[],
): Promise<void> {
	const envLine = environment
		? `  static environment = ${JSON.stringify(environment)};\n`
		: "";
	await fsp.writeFile(
		path.join(dir, file),
		`import { BaseSeeder } from "${SEEDER_SRC}";
export default class extends BaseSeeder {
${envLine}  async run() {
    await this.client.execute("INSERT INTO seed_log (marker) VALUES (?)", ["${marker}"]);
  }
}
`,
	);
}

async function markers(): Promise<string[]> {
	const rows = await conn.query<{ marker: string }>(
		"SELECT marker FROM seed_log ORDER BY marker",
	);
	return rows.map((r) => r.marker);
}

beforeEach(async () => {
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	await conn.execute(
		"CREATE TABLE seed_log (id INTEGER PRIMARY KEY AUTOINCREMENT, marker TEXT)",
	);
	setDb(conn);
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-seeders-"));
});

afterEach(async () => {
	clearDb(conn);
	await conn?.close();
	await fsp.rm(dir, { recursive: true, force: true });
});

describe("atlas > seeder commands (Lucid)", () => {
	it("make:seeder scaffolds a file; refuses an invalid name", async () => {
		await makeSeederCommand({ seedersDir: dir }).run(["UserSeeder"], {});
		const files = await fsp.readdir(dir);
		expect(files.some((f) => f.endsWith("_UserSeeder.ts"))).toBe(true);

		// Path-traversal name is rejected (no file written).
		await makeSeederCommand({ seedersDir: dir }).run(["../evil"], {});
		expect(process.exitCode).toBe(1);
		process.exitCode = 0;
	});

	it("db:seed runs every seeder; --files runs only the subset", async () => {
		await writeSeeder("A_Seeder.ts", "a");
		await writeSeeder("B_Seeder.ts", "b");

		// --files runs only B_Seeder.
		await dbSeedCommand({ seedersDir: dir }).run([], { files: "B_Seeder" });
		expect(await markers()).toEqual(["b"]);

		// A bare run executes both (a already there → a, b).
		await dbSeedCommand({ seedersDir: dir }).run([], {});
		expect(await markers()).toEqual(["a", "b", "b"]);
	});

	it("db:seed --connection targets a registered connection", async () => {
		const other = await createNapiConnection("sqlite::memory:", 1, 1);
		await other.execute(
			"CREATE TABLE seed_log (id INTEGER PRIMARY KEY AUTOINCREMENT, marker TEXT)",
		);
		registerConnection("other", other);
		try {
			await writeSeeder("A_Seeder.ts", "x");
			await dbSeedCommand({ seedersDir: dir }).run([], {
				connection: "other",
			});
			// Ran against 'other', not the default connection.
			const rows = await other.query<{ marker: string }>(
				"SELECT marker FROM seed_log",
			);
			expect(rows.map((r) => r.marker)).toEqual(["x"]);
			expect(await markers()).toEqual([]); // default untouched
		} finally {
			await other.close();
		}
	});

	it("this.client works; static environment gates a seeder (Lucid)", async () => {
		await writeClassSeeder("Always_Seeder.ts", "always");
		await writeClassSeeder("ProdOnly_Seeder.ts", "prod", ["production"]);

		// In 'testing', the production-only seeder is skipped; `this.client` ran the other.
		const executed = await runSeederDirectory(dir, conn, {
			environment: "testing",
		});
		expect(executed).toEqual(["Always_Seeder"]);
		expect(await markers()).toEqual(["always"]);

		// In 'production', both run.
		await runSeederDirectory(dir, conn, { environment: "production" });
		expect((await markers()).sort()).toEqual(["always", "always", "prod"]);
	});

	it("naturalSort orders 2 before 10 (Lucid)", async () => {
		await writeSeeder("2_Seeder.ts", "two");
		await writeSeeder("10_Seeder.ts", "ten");
		const executed = await runSeederDirectory(dir, conn, { naturalSort: true });
		expect(executed).toEqual(["2_Seeder", "10_Seeder"]);
	});

	it("make:factory scaffolds a <Model>Factory.ts file", async () => {
		await makeFactoryCommand({ factoriesDir: dir }).run(["User"], {});
		const files = await fsp.readdir(dir);
		expect(files).toContain("UserFactory.ts");
		expect(
			await fsp.readFile(path.join(dir, "UserFactory.ts"), "utf8"),
		).toContain("export const UserFactory = factory(User");
	});
});
