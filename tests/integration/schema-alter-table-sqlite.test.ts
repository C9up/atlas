/**
 * `schema.alterTable()` / `renameTable()` / `createTableIfNotExists()` —
 * the TS side of the DDL compiler, which had no caller despite the Rust
 * (`compile_alter_table` / `compile_rename_table`) being complete.
 *
 * The ADD/DROP/RENAME cases run against a real SQLite database, so a statement
 * that compiles but doesn't execute still fails here. The type-change and
 * nullability cases are asserted at the SQL level per dialect: SQLite cannot
 * ALTER COLUMN in place at all, and Postgres/MySQL disagree on the syntax.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { Schema } from "../../src/schema/Schema.js";

/** Column names of a live SQLite table, in declaration order. */
async function columnsOf(
	conn: AsyncDatabaseConnection,
	table: string,
): Promise<string[]> {
	const rows = await conn.query<{ name: string }>(
		`PRAGMA table_info(${table})`,
	);
	return rows.map((r) => r.name);
}

async function runAll(
	conn: AsyncDatabaseConnection,
	schema: Schema,
): Promise<void> {
	for (const sql of schema.toSQL()) await conn.execute(sql, []);
	schema.reset();
}

describe("schema.alterTable (sqlite, executed)", () => {
	let conn: AsyncDatabaseConnection;
	let schema: Schema;

	beforeEach(async () => {
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
		schema = new Schema("sqlite");
		schema.createTable("users", (t) => {
			t.increments("id");
			t.string("email").notNullable();
		});
		await runAll(conn, schema);
	});

	it("adds a column that the database actually accepts", async () => {
		schema.alterTable("users", (t) => {
			t.string("nickname");
		});
		await runAll(conn, schema);

		expect(await columnsOf(conn, "users")).toContain("nickname");
		await conn.execute("INSERT INTO users (email, nickname) VALUES (?, ?)", [
			"a@b.co",
			"ada",
		]);
		const rows = await conn.query<{ nickname: string }>(
			"SELECT nickname FROM users",
		);
		expect(rows[0]?.nickname).toBe("ada");
	});

	it("renames and drops columns, in call order", async () => {
		schema.alterTable("users", (t) => {
			t.string("scratch");
			t.renameColumn("email", "email_address");
			t.dropColumn("scratch");
		});
		await runAll(conn, schema);

		const cols = await columnsOf(conn, "users");
		expect(cols).toContain("email_address");
		expect(cols).not.toContain("email");
		expect(cols).not.toContain("scratch");
	});

	it("drops several columns via dropColumns", async () => {
		schema.alterTable("users", (t) => {
			t.string("a");
			t.string("b");
		});
		await runAll(conn, schema);
		schema.alterTable("users", (t) => {
			t.dropColumns("a", "b");
		});
		await runAll(conn, schema);

		const cols = await columnsOf(conn, "users");
		expect(cols).not.toContain("a");
		expect(cols).not.toContain("b");
	});

	it("renames a table", async () => {
		schema.renameTable("users", "accounts");
		await runAll(conn, schema);

		const rows = await conn.query<{ name: string }>(
			"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users','accounts')",
		);
		expect(rows.map((r) => r.name)).toEqual(["accounts"]);
	});

	it("creates an index declared alongside an alter", async () => {
		schema.alterTable("users", (t) => {
			t.string("slug");
			t.uniqueIndex(["slug"], "idx_users_slug");
		});
		await runAll(conn, schema);

		await conn.execute("INSERT INTO users (email, slug) VALUES (?, ?)", [
			"a@b.co",
			"dup",
		]);
		await expect(
			conn.execute("INSERT INTO users (email, slug) VALUES (?, ?)", [
				"c@d.co",
				"dup",
			]),
		).rejects.toThrow();
	});

	it("createTableIfNotExists is idempotent", async () => {
		for (let i = 0; i < 2; i++) {
			schema.createTableIfNotExists("logs", (t) => {
				t.increments("id");
			});
			await runAll(conn, schema);
		}
		expect(await columnsOf(conn, "logs")).toEqual(["id"]);
	});

	it("refuses to alter a column in place (SQLite cannot)", () => {
		expect(() =>
			schema.alterTable("users", (t) => {
				t.text("email").alter();
			}),
		).toThrow(/E_UNSUPPORTED/);
	});

	it("refuses setNullable on SQLite", () => {
		expect(() =>
			schema.alterTable("users", (t) => {
				t.setNullable("email");
			}),
		).toThrow(/E_UNSUPPORTED/);
	});
});

describe("schema.alterTable (SQL per dialect)", () => {
	it("changes a type without touching NOT NULL unless asked", () => {
		const schema = new Schema("postgres");
		schema.alterTable("users", (t) => {
			t.string("email", 120).alter();
		});
		const sql = schema.toSQL();

		expect(sql).toEqual([
			'ALTER TABLE "users" ALTER COLUMN "email" TYPE VARCHAR(120);',
		]);
	});

	it("moves nullability only on an explicit notNullable().alter()", () => {
		const schema = new Schema("postgres");
		schema.alterTable("users", (t) => {
			t.string("email", 120).notNullable().alter();
		});

		expect(schema.toSQL()).toEqual([
			'ALTER TABLE "users" ALTER COLUMN "email" TYPE VARCHAR(120);',
			'ALTER TABLE "users" ALTER COLUMN "email" SET NOT NULL;',
		]);
	});

	it("compiles setNullable/dropNullable on postgres", () => {
		const schema = new Schema("postgres");
		schema.alterTable("users", (t) => {
			t.setNullable("email");
			t.dropNullable("name");
		});

		expect(schema.toSQL()).toEqual([
			'ALTER TABLE "users" ALTER COLUMN "email" DROP NOT NULL;',
			'ALTER TABLE "users" ALTER COLUMN "name" SET NOT NULL;',
		]);
	});

	it("restates the type on MySQL, where MODIFY COLUMN needs it", () => {
		const schema = new Schema("mysql");
		schema.alterTable("users", (t) => {
			t.string("email", 120).nullable().alter();
		});

		expect(schema.toSQL()).toEqual([
			"ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR(120);",
		]);
	});

	it("points MySQL callers at .alter() instead of setNullable", () => {
		const schema = new Schema("mysql");
		expect(() =>
			schema.alterTable("users", (t) => {
				t.setNullable("email");
			}),
		).toThrow(/E_UNSUPPORTED[\s\S]*\.alter\(\)/);
	});

	it("rejects an alter-only method inside createTable", () => {
		const schema = new Schema("sqlite");
		expect(() =>
			schema.createTable("users", (t) => {
				t.dropColumn("nope");
			}),
		).toThrow(/E_ALTER_MISUSE/);
	});

	it("rejects alter() with no column in front of it", () => {
		const schema = new Schema("sqlite");
		expect(() =>
			schema.alterTable("users", (t) => {
				t.alter();
			}),
		).toThrow(/E_ALTER_MISUSE/);
	});

	it("rejects an alterTable that declares nothing", () => {
		const schema = new Schema("sqlite");
		expect(() => schema.alterTable("users", () => {})).toThrow(/E_ALTER_EMPTY/);
	});

	it("rejects an injected identifier", () => {
		const schema = new Schema("postgres");
		expect(() =>
			schema.alterTable("users", (t) => {
				t.dropColumn('x"; DROP TABLE users; --');
			}),
		).toThrow();
	});

	it("table() is an alias of alterTable()", () => {
		const alter = new Schema("postgres");
		alter.alterTable("users", (t) => {
			t.dropColumn("x");
		});
		const aliased = new Schema("postgres");
		aliased.table("users", (t) => {
			t.dropColumn("x");
		});

		expect(aliased.toSQL()).toEqual(alter.toSQL());
	});
});
