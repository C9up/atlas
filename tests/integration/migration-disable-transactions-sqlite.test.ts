/**
 * Adonis Lucid parity for the transaction-control migration surface:
 *  - `static disableTransactions = true` and the global `disableTransactions`
 *    option run a migration OUTSIDE a transaction (deliberate opt-out) — so
 *    `this.defer()` runs even on an adapter with no `transaction()`, no throw;
 *  - `naturalSort` orders `2_x` before `10_x`;
 *  - `this.dryRun` is exposed to `up()` during a dry run.
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
import {
	type DatabaseAdapter,
	MigrationRunner,
} from "../../src/schema/MigrationRunner.js";

/** Adapter WITHOUT transaction() — proves the opt-out doesn't require one. */
function toAdapterNoTx(conn: AsyncDatabaseConnection): DatabaseAdapter {
	return {
		execute: async (sql, params) => {
			await conn.execute(sql, params);
		},
		query: (sql, params) => conn.query(sql, params),
		close: () => conn.close(),
		runInTransaction: (batch) => conn.runInTransaction(batch),
		// transaction intentionally omitted.
	};
}

const MIGRATION_SRC = pathToFileURL(
	path.resolve(__dirname, "../../src/schema/Migration.ts"),
).href;

let conn: AsyncDatabaseConnection;
let tmpDir: string;

beforeEach(async () => {
	setAtlasDialect("sqlite");
	conn = await createNapiConnection("sqlite::memory:", 1, 1);
	tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "atlas-disabletx-"));
});

afterEach(async () => {
	await conn?.close();
	await fsp.rm(tmpDir, { recursive: true, force: true });
});

describe("atlas > migrations > disableTransactions / naturalSort / dryRun", () => {
	it("runs a `static disableTransactions` migration + defer WITHOUT a transaction()", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_seed.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  static disableTransactions = true
  async up() {
    this.schema.createTable('roles', (t) => { t.increments('id'); t.string('name') })
    this.defer(async (db) => { await db.execute("INSERT INTO roles (name) VALUES ('admin')") })
  }
  async down() { this.schema.dropTable('roles') }
}`,
		);
		// No transaction() on the adapter, yet the deliberate opt-out means defer is
		// allowed to run loose instead of throwing E_DEFER_REQUIRES_TRANSACTION.
		const runner = new MigrationRunner(toAdapterNoTx(conn), {
			migrationsDir: tmpDir,
		});
		const ran = await runner.migrate();
		expect(ran).toEqual(["001_seed"]);

		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM roles",
		);
		expect(rows[0]?.n).toBe(1);
	});

	it("honours the global disableTransactions option the same way", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_seed.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {
    this.schema.createTable('widgets', (t) => { t.increments('id') })
    this.defer(async (db) => { await db.execute("INSERT INTO widgets DEFAULT VALUES") })
  }
  async down() { this.schema.dropTable('widgets') }
}`,
		);
		const runner = new MigrationRunner(toAdapterNoTx(conn), {
			migrationsDir: tmpDir,
			disableTransactions: true,
		});
		await expect(runner.migrate()).resolves.toEqual(["001_seed"]);
		const rows = await conn.query<{ n: number }>(
			"SELECT COUNT(*) AS n FROM widgets",
		);
		expect(rows[0]?.n).toBe(1);
	});

	it("orders files numerically when naturalSort is on (2 before 10)", async () => {
		for (const name of ["2_a", "10_b"]) {
			await fsp.writeFile(
				path.join(tmpDir, `${name}.ts`),
				`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {}
  async down() {}
}`,
			);
		}
		const natural = new MigrationRunner(toAdapterNoTx(conn), {
			migrationsDir: tmpDir,
			naturalSort: true,
		});
		const naturalOrder = (await natural.dryRun()).map((m) => m.name);
		expect(naturalOrder).toEqual(["2_a", "10_b"]);

		// Default (lexicographic): "10_b" sorts before "2_a".
		const lexical = new MigrationRunner(toAdapterNoTx(conn), {
			migrationsDir: tmpDir,
		});
		const lexicalOrder = (await lexical.dryRun()).map((m) => m.name);
		expect(lexicalOrder).toEqual(["10_b", "2_a"]);
	});

	it("exposes this.dryRun to up() during a dry run", async () => {
		await fsp.writeFile(
			path.join(tmpDir, "001_branch.ts"),
			`import { Migration } from '${MIGRATION_SRC}'
export default class extends Migration {
  async up() {
    if (this.dryRun) this.schema.createTable('dry_marker', (t) => { t.increments('id') })
    else this.schema.createTable('real_table', (t) => { t.increments('id') })
  }
  async down() {}
}`,
		);
		const runner = new MigrationRunner(toAdapterNoTx(conn), {
			migrationsDir: tmpDir,
		});
		const plan = await runner.dryRun();
		expect(plan[0]?.sql.join("\n")).toContain("dry_marker");
	});
});
