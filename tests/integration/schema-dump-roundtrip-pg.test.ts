/**
 * A Postgres schema dump loads back into the same schema — names, defaults
 * and all — and never runs SQL hidden in a name.
 *
 * Names were wrapped in double quotes without doubling an embedded `"`, and
 * the loader split the file on every `;`: a constraint or view named
 * `c" CHECK (true); CREATE TABLE pwned (x int); --` created a table wherever
 * the dump was loaded, and a legitimate `';'` inside a CHECK made the dump
 * unloadable. Column defaults and sequences were dropped, so a database
 * bootstrapped from a dump refused every insert relying on a serial id.
 *
 * Gated on ATLAS_TEST_PG_URL. Source and destination are two schemas of one
 * single-connection pool (`search_path` is per session).
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

const SRC = "atlas_rt_src";
const DST = "atlas_rt_dst";

describePg("atlas > Postgres schema dump round-trip", () => {
	let db: AsyncDatabaseConnection;
	let dir: string;
	let dumpPath: string;

	/** Column shape of a schema, for comparing source and destination. */
	async function columns(schema: string): Promise<unknown[]> {
		const rows = await db.query<Record<string, unknown>>(
			`SELECT c.relname::text AS t, a.attname::text AS c,
			        format_type(a.atttypid, a.atttypmod)::text AS type, a.attnotnull AS nn,
			        a.attidentity::text AS ident, a.attgenerated::text AS gen,
			        pg_get_expr(d.adbin, d.adrelid)::text AS def
			 FROM pg_attribute a
			 JOIN pg_class c ON c.oid = a.attrelid
			 JOIN pg_namespace n ON n.oid = c.relnamespace
			 LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			 WHERE n.nspname = $1 AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
			 ORDER BY c.relname, a.attnum`,
			[schema],
		);
		// A default naming a sequence outside search_path is schema-qualified.
		return rows.map((r) => ({
			...r,
			def: typeof r.def === "string" ? r.def.replace(`${schema}.`, "") : r.def,
		}));
	}

	beforeAll(async () => {
		db = await createNapiConnection(PG_URL, 1, 1);
		for (const schema of [SRC, DST]) {
			await db.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
			await db.execute(`CREATE SCHEMA ${schema}`);
		}
		await db.execute(`SET search_path TO ${SRC}`);
		await db.execute(
			`CREATE TABLE probe_defaults (
			   id serial PRIMARY KEY,
			   status text NOT NULL DEFAULT 'new',
			   created_at timestamptz DEFAULT now(),
			   code varchar(12),
			   amount numeric(10,2) DEFAULT 1.5,
			   ident integer GENERATED ALWAYS AS IDENTITY,
			   doubled integer GENERATED ALWAYS AS (id * 2) STORED,
			   "we""ird;col" text DEFAULT 'a;b')`,
		);
		await db.execute(
			`CREATE TABLE probe_chk (id int PRIMARY KEY, note text,
			   CONSTRAINT "c"" CHECK (true); CREATE TABLE pwned_c (x int); --" CHECK (note <> 'a;b'))`,
		);
		await db.execute(
			`CREATE VIEW "v"" AS SELECT 1; CREATE TABLE pwned_v (x int); --" AS SELECT 1 AS one`,
		);
		dir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-pg-rt-"));
		dumpPath = path.join(dir, "pg-schema.sql");
		const dumper = new SchemaDumper(db, { dumpPath });
		await dumper.run();
		expect(dumper.error).toBeUndefined();

		await db.execute(`SET search_path TO ${DST}`);
		await new MigrationRunner(
			{
				execute: async (sql, params) => {
					await db.execute(sql, params);
				},
				query: (sql, params) => db.query(sql, params),
				close: async () => {},
			},
			{ dialect: "postgres" },
		).loadDump(dumpPath);
	});

	afterAll(async () => {
		for (const schema of [SRC, DST]) {
			await db?.execute(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		}
		await db?.close();
		if (dir !== undefined) await fsp.rm(dir, { recursive: true, force: true });
	});

	it("runs no SQL hidden in a constraint or view name", async () => {
		const tables = await db.query<{ t: string }>(
			"SELECT tablename::text AS t FROM pg_tables WHERE schemaname = $1 ORDER BY 1",
			[DST],
		);
		expect(tables.map((r) => r.t)).toEqual(["probe_chk", "probe_defaults"]);
		const names = await db.query<{ n: string }>(
			`SELECT conname::text AS n FROM pg_constraint con
			 JOIN pg_namespace ns ON ns.oid = con.connamespace
			 WHERE ns.nspname = $1 AND con.contype = 'c'`,
			[DST],
		);
		expect(names.map((r) => r.n)).toContain(
			'c" CHECK (true); CREATE TABLE pwned_c (x int); --',
		);
		const views = await db.query<{ n: string }>(
			"SELECT viewname::text AS n FROM pg_views WHERE schemaname = $1",
			[DST],
		);
		expect(views.map((r) => r.n)).toEqual([
			'v" AS SELECT 1; CREATE TABLE pwned_v (x int); --',
		]);
	});

	it("keeps a ';' inside a CHECK literal", async () => {
		await expect(
			db.execute("INSERT INTO probe_chk (id, note) VALUES (1, 'a;b')"),
		).rejects.toThrow();
		await db.execute("INSERT INTO probe_chk (id, note) VALUES (2, 'ok')");
	});

	it("restores defaults, sequences, identity and generated columns", async () => {
		expect(await columns(DST)).toEqual(await columns(SRC));
		const rows = await db.query<Record<string, unknown>>(
			"INSERT INTO probe_defaults DEFAULT VALUES RETURNING id, status, doubled, ident, code",
		);
		expect(rows).toEqual([
			{ id: 1, status: "new", doubled: 2, ident: 1, code: null },
		]);
	});
});
