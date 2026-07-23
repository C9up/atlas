/**
 * schemaGeneration.schemas validation against a REAL PostgreSQL (podman), gated
 * on ATLAS_TEST_PG_URL. Proves that restricting generation to a named Postgres
 * schema actually lists + introspects the tables in THAT schema.
 */
import * as fsp from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { generateSchemaFile } from "../../src/console/schemaGenerateCommand.js";

const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";
const describePg = PG_URL ? describe : describe.skip;

describePg("atlas > schema:generate schemas against real PostgreSQL", () => {
	let db: AsyncDatabaseConnection;
	let outDir: string;

	beforeAll(async () => {
		db = await createNapiConnection(PG_URL, 1, 5);
		await db.execute("DROP SCHEMA IF EXISTS reporting CASCADE");
		await db.execute("CREATE SCHEMA reporting");
		await db.execute(
			"CREATE TABLE reporting.widgets (id serial PRIMARY KEY, label text)",
		);
		outDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-pggen-"));
	});

	afterAll(async () => {
		await db.execute("DROP SCHEMA IF EXISTS reporting CASCADE");
		await db?.close();
		await fsp.rm(outDir, { recursive: true, force: true });
	});

	it("schemas: ['reporting'] lists + introspects the reporting-schema table", async () => {
		const out = path.join(outDir, "schema.ts");
		const n = await generateSchemaFile(db, {
			outputPath: out,
			schemas: ["reporting"],
		});
		expect(n).toBeGreaterThanOrEqual(1);
		const src = await fsp.readFile(out, "utf8");
		expect(src).toContain("export class WidgetsSchema extends BaseModel");
		expect(src).toContain("@PrimaryKey() declare id: number");
		expect(src).toContain("declare label");
	});
});
