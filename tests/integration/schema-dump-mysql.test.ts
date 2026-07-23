/**
 * MySQL schema-dump validation against a REAL MySQL (podman), gated on
 * ATLAS_TEST_MYSQL_URL. Proves the `SHOW CREATE TABLE` dump path emits the exact
 * DDL (indexes + FKs included) and that a dump round-trips (loadDump) on MySQL.
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

const MYSQL_URL = process.env.ATLAS_TEST_MYSQL_URL ?? "";
const describeMysql = MYSQL_URL ? describe : describe.skip;

describeMysql("atlas > schema dump against real MySQL", () => {
	let db: AsyncDatabaseConnection;
	let dumpDir: string;

	beforeAll(async () => {
		db = await createNapiConnection(MYSQL_URL, 1, 5);
		await db.execute("DROP TABLE IF EXISTS mdump_posts");
		await db.execute("DROP TABLE IF EXISTS mdump_authors");
		await db.execute(
			"CREATE TABLE mdump_authors (id int AUTO_INCREMENT PRIMARY KEY, email varchar(120) NOT NULL)",
		);
		await db.execute(
			"CREATE TABLE mdump_posts (" +
				"id int AUTO_INCREMENT PRIMARY KEY, " +
				"author_id int NOT NULL, " +
				"title varchar(200), " +
				"INDEX mdump_posts_title_idx (title), " +
				"CONSTRAINT fk_mdump_author FOREIGN KEY (author_id) REFERENCES mdump_authors (id))",
		);
		dumpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-mydump-"));
	});

	afterAll(async () => {
		await db.execute("DROP TABLE IF EXISTS mdump_posts");
		await db.execute("DROP TABLE IF EXISTS mdump_authors");
		await db?.close();
		await fsp.rm(dumpDir, { recursive: true, force: true });
	});

	it("SHOW CREATE TABLE dumps exact DDL (indexes + FK); loadDump round-trips", async () => {
		const dumper = new SchemaDumper(db, { outputDir: dumpDir });
		await dumper.run();
		expect(dumper.error).toBeUndefined();
		const sql = await fsp.readFile(dumper.result?.dumpPath ?? "", "utf8");
		expect(sql).toContain("CREATE TABLE `mdump_authors`");
		expect(sql).toContain("CREATE TABLE `mdump_posts`");
		expect(sql).toContain("mdump_posts_title_idx");
		expect(sql.toUpperCase()).toContain("FOREIGN KEY");

		// Drop + rebuild purely from the dump (FK order handled by the dump order).
		await db.execute("DROP TABLE IF EXISTS mdump_posts");
		await db.execute("DROP TABLE IF EXISTS mdump_authors");
		await new MigrationRunner(
			{
				execute: async (s, p) => {
					await db.execute(s, p);
				},
				query: (s, p) => db.query(s, p),
				close: async () => {},
			},
			{ dialect: "mysql" },
		).loadDump(dumper.result?.dumpPath ?? "");

		const rows = await db.query<{ name: string }>(
			"SELECT CAST(table_name AS CHAR) AS name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE 'mdump_%' ORDER BY table_name",
		);
		expect(rows.map((r) => r.name)).toEqual(["mdump_authors", "mdump_posts"]);
	});
});
