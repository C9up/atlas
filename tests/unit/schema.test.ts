import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Migration, Schema, TableBuilder } from "../../src/index.js";
import type { DatabaseAdapter } from "../../src/schema/MigrationRunner.js";

const pg = "postgres" as const;
const sqlite = "sqlite" as const;

import { Database } from "../../src/testing/TestDatabase.js";

let db: Database;
let adapter: DatabaseAdapter;

beforeEach(async () => {
	db = await Database.memory();
	adapter = db.asAdapter();
});

afterEach(async () => {
	await db.close();
});

describe("atlas > SchemaBuilder > SQL generation", () => {
	it("generates valid Postgres CREATE TABLE", () => {
		const builder = new TableBuilder("orders");
		builder.uuid("id").primary().defaultTo("gen_random_uuid()");
		builder.string("status", 50).notNullable().defaultTo("'pending'");
		builder.decimal("total", 10, 2).notNullable();
		builder.timestamp("created_at").notNullable().defaultTo("NOW()");

		const sql = builder.toStatements(pg).join("\n");
		expect(sql).toContain('CREATE TABLE "orders"');
		expect(sql).toContain('"id" UUID PRIMARY KEY DEFAULT gen_random_uuid()');
		expect(sql).toContain('"status" VARCHAR(50) NOT NULL');
		expect(sql).toContain('"total" DECIMAL(10, 2) NOT NULL');
	});

	it("generates valid SQLite CREATE TABLE", () => {
		const builder = new TableBuilder("users");
		builder.uuid("id").primary();
		builder.string("name").notNullable();
		builder.boolean("active");

		const sql = builder.toStatements(sqlite).join("\n");
		expect(sql).toContain('"id" TEXT PRIMARY KEY');
		expect(sql).toContain('"name" TEXT NOT NULL');
		expect(sql).toContain('"active" INTEGER');
	});

	it("supports id() and timestamps() shortcuts", () => {
		const builder = new TableBuilder("products");
		builder.id();
		builder.timestamps();
		const sql = builder.toStatements(pg).join("\n");
		expect(sql).toContain('"id" UUID PRIMARY KEY DEFAULT gen_random_uuid()');
		expect(sql).toContain('"created_at" TIMESTAMP NOT NULL DEFAULT NOW()');
		expect(sql).toContain('"updated_at" TIMESTAMP NOT NULL DEFAULT NOW()');
	});

	it("timestamptz() emits TIMESTAMPTZ on Postgres, degrades elsewhere", () => {
		const mysql = "mysql" as const;
		const make = () => {
			const b = new TableBuilder("events");
			b.timestamptz("occurred_at").notNullable();
			return b;
		};
		expect(make().toStatements(pg).join("\n")).toContain(
			'"occurred_at" TIMESTAMPTZ NOT NULL',
		);
		expect(make().toStatements(mysql).join("\n")).toContain(
			"`occurred_at` TIMESTAMP NOT NULL",
		);
		expect(make().toStatements(sqlite).join("\n")).toContain(
			'"occurred_at" TEXT NOT NULL',
		);
	});

	// ─── Portability pinning tests (Story 48.2 AC5) ─────────────────────────
	//
	// The id() and timestamps() helpers emit Postgres-only DDL — see their
	// JSDoc warnings in TableBuilder.ts. These tests pin the BROKEN output on
	// SQLite and MySQL so that if a future contributor makes the helper
	// dialect-aware (rewrites the Rust compiler to emit CURRENT_TIMESTAMP /
	// randomblob() etc.), THESE tests fail loudly and the contributor MUST
	// also update:
	//   1. The JSDoc warnings on id() / timestamps()
	//   2. The grep ban in tests/unit/no-non-portable-helpers-in-templates.test.ts
	//   3. The cerebrum entry `## 2026-05-05: Atlas migration template portability`
	//   4. The AUDIT-migration-templates.md doc
	//
	// See packages/atlas/AUDIT-migration-templates.md → "Escape hatch".

	it("PIN: timestamps() emits Postgres-only NOW() on sqlite (broken — Story 48.2 AC5)", () => {
		const builder = new TableBuilder("products");
		builder.timestamps();
		const sql = builder.toStatements(sqlite).join("\n");
		// SQLite has no NOW() function — the migration crashes at runtime.
		// We pin the broken literal so a silent fix to the helper trips this test.
		expect(sql).toMatch(/"created_at" TEXT NOT NULL DEFAULT \(?NOW\(\)\)?/);
		expect(sql).toMatch(/"updated_at" TEXT NOT NULL DEFAULT \(?NOW\(\)\)?/);
	});

	it("PIN: id() emits Postgres-only gen_random_uuid() on sqlite (broken — Story 48.2 AC5)", () => {
		const builder = new TableBuilder("products");
		builder.id();
		const sql = builder.toStatements(sqlite).join("\n");
		// SQLite has no gen_random_uuid() — the migration crashes at runtime.
		expect(sql).toMatch(
			/"id" TEXT PRIMARY KEY DEFAULT \(?gen_random_uuid\(\)\)?/,
		);
	});

	it("PIN: id() emits Postgres-only gen_random_uuid() on mysql (broken — Story 48.2 AC5)", () => {
		const mysql = "mysql" as const;
		const builder = new TableBuilder("products");
		builder.id();
		const sql = builder.toStatements(mysql).join("\n");
		// MySQL has no gen_random_uuid() — UUID() exists but the helper hard-codes
		// gen_random_uuid(), so the migration fails at runtime on MySQL too.
		expect(sql).toMatch(
			/`id` (?:CHAR\(36\)|VARCHAR\(36\)) PRIMARY KEY DEFAULT \(?gen_random_uuid\(\)\)?/,
		);
	});

	it("supports foreign key references", () => {
		const builder = new TableBuilder("order_items");
		builder.uuid("order_id").notNullable().references("orders", "id");
		const sql = builder.toStatements(pg).join("\n");
		expect(sql).toContain('REFERENCES "orders"("id")');
	});

	it("supports unique constraint", () => {
		const builder = new TableBuilder("users");
		builder.string("email").unique().notNullable();
		const sql = builder.toStatements(pg).join("\n");
		expect(sql).toContain("NOT NULL UNIQUE");
	});

	it("supports all column types", () => {
		const builder = new TableBuilder("test");
		builder.uuid("a").string("b").text("c").integer("d").bigInteger("e");
		builder
			.decimal("f")
			.boolean("g")
			.date("h")
			.timestamp("i")
			.json("j")
			.binary("k");
		expect(builder.getColumns()).toHaveLength(11);
	});
});

describe("atlas > Schema > executes against real SQLite", () => {
	it("creates a table that actually exists in SQLite", async () => {
		const schema = new Schema("sqlite");
		schema.createTable("orders", (table) => {
			table.integer("id").primary();
			table.string("status").notNullable();
			table.decimal("total");
		});

		for (const sql of schema.toSQL()) {
			await adapter.execute(sql);
		}

		const tables = await db.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='orders'",
		);
		expect(tables).toHaveLength(1);

		const info = await db.query("PRAGMA table_info('orders')");
		const colNames = info.map((c) => c.name);
		expect(colNames).toContain("id");
		expect(colNames).toContain("status");
		expect(colNames).toContain("total");
	});

	it("drops a table from SQLite", async () => {
		await db.execute("CREATE TABLE old_table (id INTEGER)");
		let tables = await db.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'",
		);
		expect(tables).toHaveLength(1);

		const schema = new Schema("sqlite");
		schema.dropTable("old_table");
		for (const sql of schema.toSQL()) {
			await adapter.execute(sql);
		}

		tables = await db.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='old_table'",
		);
		expect(tables).toHaveLength(0);
	});

	it("inserts and queries data after CREATE TABLE", async () => {
		const schema = new Schema("sqlite");
		schema.createTable("users", (table) => {
			table.integer("id").primary();
			table.string("name").notNullable();
			table.string("email").unique().notNullable();
		});

		for (const sql of schema.toSQL()) {
			await adapter.execute(sql);
		}

		await db.execute(
			"INSERT INTO users (id, name, email) VALUES (1, 'Kaen', 'kaen@c9up.com')",
		);
		await db.execute(
			"INSERT INTO users (id, name, email) VALUES (2, 'Alice', 'alice@test.com')",
		);

		const rows = await db.query("SELECT * FROM users ORDER BY id");
		expect(rows).toHaveLength(2);
		expect(rows[0].name).toBe("Kaen");
		expect(rows[1].email).toBe("alice@test.com");
	});
});

describe("atlas > Migration > real SQLite execution", () => {
	it("migration creates and drops table", async () => {
		class CreateOrders extends Migration {
			up() {
				this.schema.createTable("orders", (table) => {
					table.integer("id").primary();
					table.string("status", 50).notNullable();
					table.decimal("total", 10, 2).notNullable();
				});
			}
			down() {
				this.schema.dropTable("orders");
			}
		}

		const migration = new CreateOrders("sqlite");

		for (const sql of await migration.getUpSQL()) {
			await adapter.execute(sql);
		}

		let tables = await db.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='orders'",
		);
		expect(tables).toHaveLength(1);

		for (const sql of await migration.getDownSQL()) {
			await adapter.execute(sql);
		}

		tables = await db.query(
			"SELECT name FROM sqlite_master WHERE type='table' AND name='orders'",
		);
		expect(tables).toHaveLength(0);
	});
});

describe("atlas > QueryBuilder > executes against real SQLite", () => {
	it("generated SQL is valid and executable", async () => {
		await db.execute(
			"CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL, created_at TEXT)",
		);
		await db.execute(
			"INSERT INTO orders VALUES (1, 'active', 42.50, '2026-03-31')",
		);
		await db.execute(
			"INSERT INTO orders VALUES (2, 'pending', 10.00, '2026-03-30')",
		);
		await db.execute(
			"INSERT INTO orders VALUES (3, 'active', 100.00, '2026-03-29')",
		);

		const { QueryBuilder } = await import("../../src/query/QueryBuilder.js");
		const { sql, params } = new QueryBuilder("orders")
			.where("status", "active")
			.orderBy("total", "desc")
			.toSQL();

		// Replace $N with ? for SQLite
		let sqliteSQL = sql;
		for (let i = params.length; i >= 1; i--) {
			sqliteSQL = sqliteSQL.replace(`$${i}`, "?");
		}

		const rows = await db.queryWithParams(sqliteSQL, params);
		expect(rows).toHaveLength(2);
		expect(rows[0].total).toBe(100.0);
		expect(rows[1].total).toBe(42.5);
	});
});
