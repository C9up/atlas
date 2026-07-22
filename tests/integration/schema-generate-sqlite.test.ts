/**
 * `schema:generate` (Adonis Lucid parity): introspects the database and writes
 * a `schema.ts` with one `BaseModel` subclass per table.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { migrationRunCommand } from "../../src/console/migrationCommands.js";
import {
	renderSchemaFile,
	schemaGenerateCommand,
} from "../../src/console/schemaGenerateCommand.js";
import { clearDb, registerConnection, setDb } from "../../src/services/db.js";

describe("atlas > schema:generate — renderSchemaFile (pure)", () => {
	it("generates a BaseModel class per table with mapped columns", () => {
		const src = renderSchemaFile([
			{
				table: "blog_posts",
				columns: [
					{
						name: "id",
						type: "INTEGER",
						nullable: false,
						hasDefault: false,
						primaryKey: true,
					},
					{
						name: "title",
						type: "varchar",
						nullable: false,
						hasDefault: false,
						primaryKey: false,
					},
					{
						name: "created_at",
						type: "timestamp",
						nullable: true,
						hasDefault: true,
						primaryKey: false,
					},
				],
			},
		]);
		expect(src).toContain("export class BlogPostsSchema extends BaseModel");
		expect(src).toContain('static table = "blog_posts"');
		expect(src).toContain(
			'static $columns = ["id", "title", "createdAt"] as const;',
		);
		expect(src).toContain("@PrimaryKey() declare id: number");
		expect(src).toContain("@Column() declare title: string");
		// Date columns → @column.dateTime + a Chronos DateTime (Lucid-style).
		expect(src).toContain(
			"@column.dateTime() declare createdAt: DateTime | null",
		);
		expect(src).toContain('import { DateTime } from "@c9up/chronos";');
	});

	it("applies rulesPaths rules: custom tsType/decorator + imports (Lucid)", () => {
		const src = renderSchemaFile(
			[
				{
					table: "users",
					columns: [
						{
							name: "id",
							type: "INTEGER",
							nullable: false,
							hasDefault: false,
							primaryKey: true,
						},
						{
							name: "status",
							type: "varchar",
							nullable: false,
							hasDefault: false,
							primaryKey: false,
						},
					],
				},
			],
			{
				columns: {
					status: {
						tsType: "UserStatus",
						decorator: "@Column()",
						imports: [{ source: "#types/enums", namedImports: ["UserStatus"] }],
					},
				},
			},
		);
		// The rule overrides the type and contributes its import to the header.
		expect(src).toContain("@Column() declare status: UserStatus;");
		expect(src).toContain('import { UserStatus } from "#types/enums";');
	});

	it("compact output drops the blank line inside each class (--compact-output)", () => {
		const tables = [
			{
				table: "users",
				columns: [
					{
						name: "id",
						type: "INTEGER",
						nullable: false,
						hasDefault: false,
						primaryKey: true,
					},
				],
			},
		];
		const normal = renderSchemaFile(tables, {}, false);
		const compact = renderSchemaFile(tables, {}, true);
		// Normal keeps a blank line before the first column; compact removes it.
		expect(normal).toContain("$columns;\n\n\t@PrimaryKey()");
		expect(compact).toContain("$columns;\n\t@PrimaryKey()");
		expect(compact).not.toContain("$columns;\n\n");
	});
});

describe("atlas > schema:generate — command (sqlite)", () => {
	let conn: AsyncDatabaseConnection;
	let tmpDir: string;

	beforeEach(async () => {
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		await conn.execute(
			"CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER)",
		);
		await conn.execute("CREATE TABLE ream_migrations (id INTEGER PRIMARY KEY)");
		setDb(conn);
		tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-schemagen-"));
	});

	afterEach(async () => {
		clearDb(conn);
		await conn?.close();
		await fsp.rm(tmpDir, { recursive: true, force: true });
	});

	it("introspects tables and writes the schema file (skipping framework tables)", async () => {
		const out = path.join(tmpDir, "schema.ts");
		await schemaGenerateCommand({ outputPath: out }).run([], {});

		const content = await fsp.readFile(out, "utf8");
		expect(content).toContain("export class WidgetsSchema extends BaseModel");
		expect(content).toContain('static table = "widgets"');
		expect(content).toContain("@PrimaryKey() declare id: number");
		expect(content).toContain("@Column() declare name: string");
		// ream_migrations is a framework table — excluded.
		expect(content).not.toContain("ReamMigrations");
	});

	it("--connection targets a registered connection; --compact-output compacts", async () => {
		// A second connection with its OWN table, registered under a name.
		const other = await createNapiConnection("sqlite::memory:", 1, 1);
		await other.execute(
			"CREATE TABLE gadgets (id INTEGER PRIMARY KEY, sku TEXT)",
		);
		registerConnection("reporting", other);
		try {
			const out = path.join(tmpDir, "schema.ts");
			await schemaGenerateCommand({ outputPath: out }).run([], {
				connection: "reporting",
				"compact-output": true,
			});
			const content = await fsp.readFile(out, "utf8");
			// The 'reporting' connection's table, not the default 'widgets'.
			expect(content).toContain("export class GadgetsSchema extends BaseModel");
			expect(content).not.toContain("WidgetsSchema");
			// Compact: no blank line inside the class.
			expect(content).not.toContain("$columns;\n\n");
		} finally {
			await other.close();
		}
	});

	it("enabled: false disables the direct schema:generate command (Lucid)", async () => {
		const out = path.join(tmpDir, "schema.ts");
		await schemaGenerateCommand({ outputPath: out, enabled: false }).run(
			[],
			{},
		);
		await expect(fsp.access(out)).rejects.toThrow();
	});

	it("migration:run auto-regenerates the schema file; --no-schema-generate skips it", async () => {
		const out = path.join(tmpDir, "schema.ts");
		const cmd = migrationRunCommand({
			migrationsDir: path.join(tmpDir, "migs"),
			schemaGeneration: { enabled: true, outputPath: out },
		});
		const exists = () =>
			fsp.access(out).then(
				() => true,
				() => false,
			);

		await cmd.run([], {});
		expect(await exists()).toBe(true);
		expect(await fsp.readFile(out, "utf8")).toContain("WidgetsSchema");

		// --no-schema-generate suppresses the regeneration.
		await fsp.rm(out);
		await cmd.run([], { "no-schema-generate": true });
		expect(await exists()).toBe(false);
	});
});
