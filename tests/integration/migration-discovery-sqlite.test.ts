/**
 * Which files the MigrationRunner takes for migrations.
 *
 * Every `.ts`/`.js` entry used to count: a `.d.ts` beside compiled migrations
 * became a migration named `x.d`, and `x.ts` beside its compiled `x.js` was
 * discovered twice under one name and applied twice in one batch. Discovery
 * now follows the script-file rule (js/mjs/cjs/ts, never .d.ts) and refuses
 * two files with one name.
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
import { setAtlasDialect } from "../../src/query/native.js";
import { MigrationRunner } from "../../src/schema/MigrationRunner.js";

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

let conn: AsyncDatabaseConnection;
let dir: string;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-discovery-"));
});

afterEach(async () => {
	await conn.close();
	await fsp.rm(dir, { recursive: true, force: true });
});

function source(table: string): string {
	return `import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('${table}', (t) => { t.increments('id') }) }
  async down() { this.schema.dropTable('${table}') }
}`;
}

async function write(file: string, body: string): Promise<void> {
	await fsp.writeFile(path.join(dir, file), body);
}

function runner(): MigrationRunner {
	return new MigrationRunner(conn, { migrationsDir: dir, dialect: "sqlite" });
}

describe("atlas > migration discovery", () => {
	it("ignores declaration files and finds .mjs/.cjs migrations", async () => {
		await write("001_users.ts", source("users"));
		await write("001_users.d.ts", "export {}\n");
		await write("002_posts.mjs", source("posts"));
		await write("notes.md", "# not a migration\n");

		const status = await runner().status();
		expect(status.map((s) => s.name)).toEqual(["001_users", "002_posts"]);

		expect(await runner().migrate()).toEqual(["001_users", "002_posts"]);
		const tables = await conn.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users','posts') ORDER BY name",
		);
		expect(tables.map((t) => t.name)).toEqual(["posts", "users"]);
	});

	it("refuses two files with the same name instead of running one twice", async () => {
		await write("001_users.ts", source("users"));
		await write("001_users.js", source("users"));

		await expect(runner().status()).rejects.toThrow(/001_users/);
		await expect(runner().migrate()).rejects.toMatchObject({
			code: "E_ATLAS_MIGRATION_DUPLICATE",
		});
		const tables = await conn.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name = 'users'",
		);
		expect(tables).toEqual([]);
	});
});
