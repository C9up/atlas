/**
 * Postgres schema-dump validation against a REAL PostgreSQL (podman), gated on
 * ATLAS_TEST_PG_URL. Proves the catalog-introspection queries the dumper writes
 * for foreign keys (information_schema) and indexes (pg_indexes) actually work,
 * and that a dump round-trips (loadDump) on Postgres.
 *
 *   podman run --rm -d -p 55432:5432 -e POSTGRES_PASSWORD=atlas_test \
 *     -e POSTGRES_DB=atlas_test postgres:16
 *   ATLAS_TEST_PG_URL=postgres://postgres:atlas_test@localhost:55432/atlas_test \
 *     pnpm test tests/integration/schema-dump-pg.test.ts
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { MigrationRunner } from "../../src/schema/MigrationRunner.js";
import { SchemaDumper } from "../../src/schema/SchemaDumper.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

/**
 * A schema of its own, because a dump is a WHOLE-SCHEMA operation.
 *
 * `SchemaDumper.run()` dumps everything in `current_schema()` and `loadDump`
 * replays all of it, so sharing `public` with the other integration suites
 * meant the dump captured whatever they had created at that moment and the
 * replay then tried to CREATE a table that still existed. The MySQL twin of
 * this test failed exactly that way; this one had the same race and had simply
 * not lost the coin toss yet.
 *
 * The pool is capped at ONE connection on purpose: `search_path` is per
 * session, so a second connection would land back in `public` and the
 * isolation would be silently undone.
 */
const DUMP_SCHEMA = "atlas_dump_pg";

describePg("atlas > schema dump against real PostgreSQL", () => {
	let db: AsyncDatabaseConnection;
	let dumpDir: string;

	beforeAll(async () => {
		db = await createNapiConnection(PG_URL, 1, 1);
		await db.execute(`DROP SCHEMA IF EXISTS ${DUMP_SCHEMA} CASCADE`);
		await db.execute(`CREATE SCHEMA ${DUMP_SCHEMA}`);
		await db.execute(`SET search_path TO ${DUMP_SCHEMA}`);
		// Clean slate.
		await db.execute("DROP TABLE IF EXISTS dump_posts CASCADE");
		await db.execute("DROP TABLE IF EXISTS dump_authors CASCADE");
		await db.execute(
			"CREATE TABLE dump_authors (id serial PRIMARY KEY, email varchar(120) NOT NULL)",
		);
		// FK to authors + a secondary index on title.
		await db.execute(
			"CREATE TABLE dump_posts (" +
				"id serial PRIMARY KEY, " +
				"author_id integer NOT NULL REFERENCES dump_authors (id) ON DELETE CASCADE, " +
				"title text)",
		);
		await db.execute("CREATE INDEX dump_posts_title_idx ON dump_posts (title)");
		dumpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-pgdump-"));
	});

	afterAll(async () => {
		// The schema goes with the suite, so nothing it created can outlive it
		// and reach another file.
		await db?.execute(`DROP SCHEMA IF EXISTS ${DUMP_SCHEMA} CASCADE`);
		await db?.close();
		// Guarded because a failed `beforeAll` leaves it unset, and an unguarded
		// `rm(undefined)` throws from cleanup — replacing the real failure in the
		// report with a TypeError that says nothing about it.
		if (dumpDir !== undefined) {
			await fsp.rm(dumpDir, { recursive: true, force: true });
		}
	});

	it("introspects FKs (information_schema) + indexes (pg_indexes) into the dump", async () => {
		const dumper = new SchemaDumper(db, {
			outputDir: dumpDir,
			generatedAt: "2026-07-23T00:00:00.000Z",
		});
		await dumper.run();
		expect(dumper.error).toBeUndefined();
		const sql = await fsp.readFile(dumper.result?.dumpPath ?? "", "utf8");

		// CREATE TABLE for both, the FK ALTER, and the secondary index.
		expect(sql).toContain('CREATE TABLE "dump_authors"');
		expect(sql).toContain('CREATE TABLE "dump_posts"');
		expect(sql).toMatch(
			/ALTER TABLE "dump_posts" ADD CONSTRAINT ".*" FOREIGN KEY \("author_id"\) REFERENCES "dump_authors" \("id"\) ON DELETE CASCADE;/,
		);
		expect(sql).toContain("CREATE INDEX dump_posts_title_idx ON");
	});

	it("composite foreign keys map columns by position (not cartesian)", async () => {
		await db.execute("DROP TABLE IF EXISTS cfk_child CASCADE");
		await db.execute("DROP TABLE IF EXISTS cfk_parent CASCADE");
		await db.execute(
			"CREATE TABLE cfk_parent (a int, b int, PRIMARY KEY (a, b))",
		);
		await db.execute(
			"CREATE TABLE cfk_child (x int, y int, FOREIGN KEY (x, y) REFERENCES cfk_parent (a, b))",
		);
		try {
			const dumper = new SchemaDumper(db, { outputDir: dumpDir });
			await dumper.run();
			expect(dumper.error).toBeUndefined();
			const sql = await fsp.readFile(dumper.result?.dumpPath ?? "", "utf8");
			// Correct pairing: (x,y) → (a,b), NOT (x,x,y,y) → (a,b,a,b).
			expect(sql).toContain(
				'FOREIGN KEY ("x", "y") REFERENCES "cfk_parent" ("a", "b")',
			);
		} finally {
			await db.execute("DROP TABLE IF EXISTS cfk_child CASCADE");
			await db.execute("DROP TABLE IF EXISTS cfk_parent CASCADE");
		}
	});

	it("dumps CHECK constraints + views (Postgres introspection)", async () => {
		await db.execute("DROP VIEW IF EXISTS dump_active_authors");
		await db.execute(
			"ALTER TABLE dump_authors ADD CONSTRAINT dump_authors_email_chk CHECK (char_length(email) > 3)",
		);
		await db.execute(
			"CREATE VIEW dump_active_authors AS SELECT id, email FROM dump_authors",
		);
		try {
			const dumper = new SchemaDumper(db, { outputDir: dumpDir });
			await dumper.run();
			expect(dumper.error).toBeUndefined();
			const sql = await fsp.readFile(dumper.result?.dumpPath ?? "", "utf8");
			expect(sql).toMatch(
				/ALTER TABLE "dump_authors" ADD CONSTRAINT "dump_authors_email_chk" CHECK/,
			);
			expect(sql).toContain('CREATE VIEW "dump_active_authors" AS');
		} finally {
			await db.execute("DROP VIEW IF EXISTS dump_active_authors");
			await db.execute(
				"ALTER TABLE dump_authors DROP CONSTRAINT IF EXISTS dump_authors_email_chk",
			);
		}
	});

	it("loadDump round-trips the schema on a fresh Postgres database state", async () => {
		const dumper = new SchemaDumper(db, { outputDir: dumpDir });
		await dumper.run();
		expect(dumper.error).toBeUndefined();

		// Drop the tables, then rebuild purely from the dump.
		await db.execute("DROP TABLE IF EXISTS dump_posts CASCADE");
		await db.execute("DROP TABLE IF EXISTS dump_authors CASCADE");
		await new MigrationRunner(
			{
				execute: async (sql, params) => {
					await db.execute(sql, params);
				},
				query: (sql, params) => db.query(sql, params),
				close: async () => {},
			},
			{ dialect: "postgres" },
		).loadDump(dumper.result?.dumpPath ?? "");

		// Both tables exist again and the FK is enforced.
		const tables = await db.query<{ tablename: string }>(
			"SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'dump_%' ORDER BY tablename",
		);
		expect(tables.map((t) => t.tablename)).toEqual([
			"dump_authors",
			"dump_posts",
		]);
		// The FK rejects an orphan insert — proof the constraint was restored.
		await expect(
			db.execute("INSERT INTO dump_posts (author_id, title) VALUES (999, 'x')"),
		).rejects.toThrow();
	});
});
