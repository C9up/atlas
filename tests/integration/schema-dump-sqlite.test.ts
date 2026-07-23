/**
 * Schema dumps (Adonis Lucid `schema:dump` / `SchemaDumper` + `migration:run
 * --schema-path`): dump a migrated database, then rebuild a fresh one from the
 * dump instead of replaying every migration, running only migrations that
 * postdate the dump. Plus `--prune` (squash the migration files).
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
import { schemaDumpCommand } from "../../src/console/schemaDumpCommand.js";
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";
import {
	renderPgCreateTable,
	renderPgForeignKeyDdl,
	SchemaDumper,
	type SchemaDumpResult,
} from "../../src/schema/SchemaDumper.js";
import { clearDb, setDb } from "../../src/services/db.js";

function toAdapter(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: <T>(sql: string, params?: unknown[]) => conn.query<T>(sql, params),
		close: () => conn.close(),
		runInTransaction: (batch) => conn.runInTransaction(batch),
	};
}

/** Run a dumper and return its result, failing the test if it errored. */
async function dump(
	conn: AsyncDatabaseConnection,
	options: ConstructorParameters<typeof SchemaDumper>[1],
): Promise<SchemaDumpResult> {
	const dumper = new SchemaDumper(conn, options);
	await dumper.run();
	if (dumper.error) throw dumper.error;
	const result = dumper.result;
	if (!result) throw new Error("dumper produced no result");
	return result;
}

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

const CREATE_USERS = `
import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('users', (t) => { t.increments('id'); t.string('email', 120) }) }
  async down() { this.schema.dropTable('users') }
}
`;

const CREATE_POSTS = `
import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() { this.schema.createTable('posts', (t) => { t.increments('id'); t.string('title', 120) }) }
  async down() { this.schema.dropTable('posts') }
}
`;

let src: AsyncDatabaseConnection;
let migDir: string;
let dumpDir: string;

beforeEach(async () => {
	src = await createNapiConnection("sqlite::memory:", 1, 1);
	migDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-dump-migs-"));
	dumpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-dump-out-"));
	// A source database migrated to just the users table.
	await fsp.writeFile(path.join(migDir, "001_users.ts"), CREATE_USERS);
	const runner = new MigrationRunner(toAdapter(src), { migrationsDir: migDir });
	expect(await runner.migrate()).toEqual(["001_users"]);
});

afterEach(async () => {
	await src?.close();
	await fsp.rm(migDir, { recursive: true, force: true });
	await fsp.rm(dumpDir, { recursive: true, force: true });
});

async function tableNames(conn: AsyncDatabaseConnection): Promise<string[]> {
	const rows = await conn.query<{ name: string }>(
		"SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
	);
	return rows.map((r) => r.name);
}

describe("atlas > schema dumps", () => {
	it("SchemaDumper writes a .sql dump + a .meta.json manifest", async () => {
		const result = await dump(src, {
			connectionName: "primary",
			outputDir: dumpDir,
			generatedAt: "2026-07-23T00:00:00.000Z",
		});
		expect(result.dumpPath).toBe(path.join(dumpDir, "primary-schema.sql"));

		const sql = await fsp.readFile(result.dumpPath, "utf8");
		expect(sql).toContain('CREATE TABLE "users"');
		// The applied migration bookkeeping row is embedded as an INSERT.
		expect(sql).toContain('INSERT INTO "ream_migrations"');
		expect(sql).toContain("'001_users'");

		expect(result.metaPath).toBe(
			path.join(dumpDir, "primary-schema.meta.json"),
		);
		const meta = JSON.parse(await fsp.readFile(result.metaPath, "utf8"));
		expect(meta).toMatchObject({
			version: 1,
			connection: "primary",
			dialect: "sqlite",
			dumpPath: result.dumpPath,
			generatedAt: "2026-07-23T00:00:00.000Z",
			schemaTableName: "ream_migrations",
			schemaVersionsTableName: null,
			squashedMigrationNames: [],
		});
	});

	it("--path is a FILE path; the manifest sits beside it (Lucid)", async () => {
		const file = path.join(dumpDir, "nested", "custom.sql");
		const result = await dump(src, { dumpPath: file });
		expect(result.dumpPath).toBe(file);
		expect(result.metaPath).toBe(
			path.join(dumpDir, "nested", "custom.meta.json"),
		);
		expect(await fsp.readFile(file, "utf8")).toContain('CREATE TABLE "users"');
	});

	it("a corrupt manifest is rejected on load (validation)", async () => {
		const result = await dump(src, { outputDir: dumpDir });
		// Corrupt the manifest sidecar.
		await fsp.writeFile(result.metaPath, JSON.stringify({ version: 99 }));
		const dst = await createNapiConnection("sqlite::memory:", 1, 1);
		try {
			await expect(
				new MigrationRunner(toAdapter(dst), {
					migrationsDir: migDir,
				}).migrate({ schemaPath: result.dumpPath }),
			).rejects.toThrow(/Invalid schema-dump manifest/);
		} finally {
			await dst.close();
		}
	});

	it("loadDump rebuilds the schema + bookkeeping on a fresh database", async () => {
		const result = await dump(src, { outputDir: dumpDir });

		const dst = await createNapiConnection("sqlite::memory:", 1, 1);
		try {
			await new MigrationRunner(toAdapter(dst)).loadDump(result.dumpPath);
			expect(await tableNames(dst)).toContain("users");
			// The applied-migration row is restored, so the runner sees it as applied.
			const rows = await dst.query<{ name: string }>(
				"SELECT name FROM ream_migrations",
			);
			expect(rows.map((r) => r.name)).toEqual(["001_users"]);
		} finally {
			await dst.close();
		}
	});

	it("migration:run --schema-path bootstraps from the dump, then runs only newer migrations", async () => {
		const result = await dump(src, { outputDir: dumpDir });

		// A fresh database whose migrations dir now ALSO has a newer migration.
		const dst = await createNapiConnection("sqlite::memory:", 1, 1);
		const dstMigDir = await fsp.mkdtemp(
			path.join(os.tmpdir(), "atlas-dump-dst-"),
		);
		await fsp.writeFile(path.join(dstMigDir, "001_users.ts"), CREATE_USERS);
		await fsp.writeFile(path.join(dstMigDir, "002_posts.ts"), CREATE_POSTS);
		try {
			const runner = new MigrationRunner(toAdapter(dst), {
				migrationsDir: dstMigDir,
			});
			// 001_users comes from the dump (skipped); only 002_posts is replayed.
			const executed = await runner.migrate({ schemaPath: result.dumpPath });
			expect(executed).toEqual(["002_posts"]);

			const tables = await tableNames(dst);
			expect(tables).toContain("users"); // from the dump
			expect(tables).toContain("posts"); // from the newer migration
		} finally {
			await dst.close();
			await fsp.rm(dstMigDir, { recursive: true, force: true });
		}
	});

	it("schema:dump command writes to the --path FILE (via the default connection)", async () => {
		setDb(src);
		try {
			const file = path.join(dumpDir, "dump.sql");
			await schemaDumpCommand({ migrationsDir: migDir }).run([], {
				path: file,
			});
			expect(await fsp.readFile(file, "utf8")).toContain(
				'CREATE TABLE "users"',
			);
		} finally {
			clearDb(src);
		}
	});

	it("migration:run auto-loads the default schemaPath when nothing is applied", async () => {
		const result = await dump(src, { outputDir: dumpDir });

		// A fresh DB + the migration command configured with a default schemaPath
		// (no --schema-path flag) → it bootstraps from the dump automatically.
		const dst = await createNapiConnection("sqlite::memory:", 1, 1);
		setDb(dst);
		try {
			await migrationRunCommand({
				migrationsDir: migDir,
				schemaPath: result.dumpPath,
			}).run([], {});
			expect(await tableNames(dst)).toContain("users");
			const rows = await dst.query<{ name: string }>(
				"SELECT name FROM ream_migrations",
			);
			expect(rows.map((r) => r.name)).toEqual(["001_users"]);
		} finally {
			clearDb(dst);
			await dst.close();
		}
	});

	it("Postgres DDL renderers (pure): CREATE TABLE + FK ALTER (coller à Lucid introspection)", () => {
		// CREATE TABLE from introspected columns + PK.
		const create = renderPgCreateTable("posts", [
			{ name: "id", type: "integer", nullable: false, primaryKey: true },
			{
				name: "author_id",
				type: "integer",
				nullable: false,
				primaryKey: false,
			},
			{ name: "title", type: "text", nullable: true, primaryKey: false },
		]);
		expect(create).toBe(
			'CREATE TABLE "posts" (\n' +
				'  "id" integer NOT NULL,\n' +
				'  "author_id" integer NOT NULL,\n' +
				'  "title" text,\n' +
				'  PRIMARY KEY ("id")\n);',
		);

		// FK → ALTER TABLE; ON DELETE emitted, default ON UPDATE (NO ACTION) dropped.
		const fk = renderPgForeignKeyDdl("posts", {
			name: "posts_author_id_fkey",
			columns: ["author_id"],
			foreignTable: "authors",
			foreignColumns: ["id"],
			onUpdate: "NO ACTION",
			onDelete: "CASCADE",
		});
		expect(fk).toBe(
			'ALTER TABLE "posts" ADD CONSTRAINT "posts_author_id_fkey" ' +
				'FOREIGN KEY ("author_id") REFERENCES "authors" ("id") ON DELETE CASCADE;',
		);
	});

	it("--prune deletes the migration files and records them in the manifest", async () => {
		const result = await dump(src, {
			outputDir: dumpDir,
			migrationsDir: migDir,
			prune: true,
		});

		// The migration file is gone…
		expect(await fsp.readdir(migDir)).toEqual([]);
		// …and recorded as squashed in the manifest.
		const meta = JSON.parse(await fsp.readFile(result.metaPath, "utf8"));
		expect(meta.squashedMigrationNames).toEqual(["001_users"]);
	});
});
