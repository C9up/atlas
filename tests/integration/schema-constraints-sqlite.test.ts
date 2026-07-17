/**
 * Table-level constraints the builder was missing against Lucid/Knex: CHECK
 * helpers, composite primary/unique/foreign, the drop* family, and the
 * comment/collate/first/after modifiers.
 *
 * CHECK values are interpolated (DDL cannot bind parameters), so the quoting is
 * load-bearing and gets its own test. The SQLite cases execute for real, which
 * is what proves a CHECK actually rejects a bad row rather than merely parsing.
 *
 * The operator allow-list behind `checkLength` is covered by the Rust suite
 * (`check_rejects_non_scalar_values_and_bad_operators`) — `CheckOperator` makes
 * a bad operator unreachable from typed code, so there is nothing to assert here.
 */
import { beforeEach, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { Schema } from "../../src/schema/Schema.js";
import type { TableBuilder } from "../../src/schema/TableBuilder.js";

function createSql(
	dialect: "sqlite" | "postgres" | "mysql",
	callback: (t: TableBuilder) => void,
): string[] {
	const schema = new Schema(dialect);
	schema.createTable("t", callback);
	return schema.toSQL();
}

describe("check constraints", () => {
	it("renders the Knex predicates", () => {
		expect(
			createSql("postgres", (t) => t.integer("qty").checkPositive())[0],
		).toContain('CHECK ("qty" > 0)');
		expect(
			createSql("postgres", (t) => t.integer("qty").checkNegative())[0],
		).toContain('CHECK ("qty" < 0)');
		expect(
			createSql("postgres", (t) => t.string("role").checkIn(["a", "b"]))[0],
		).toContain(`CHECK ("role" IN ('a', 'b'))`);
		expect(
			createSql("postgres", (t) => t.string("role").checkNotIn(["x"]))[0],
		).toContain(`CHECK ("role" NOT IN ('x'))`);
		expect(
			createSql("postgres", (t) => t.string("code").checkLength("<=", 8))[0],
		).toContain('CHECK (LENGTH("code") <= 8)');
	});

	it("accepts one interval or several, OR'ing them like Knex", () => {
		expect(
			createSql("postgres", (t) => t.integer("n").checkBetween([1, 10]))[0],
		).toContain('CHECK ("n" BETWEEN 1 AND 10)');
		expect(
			createSql("postgres", (t) =>
				t.integer("n").checkBetween([
					[1, 10],
					[20, 30],
				]),
			)[0],
		).toContain('CHECK ("n" BETWEEN 1 AND 10 OR "n" BETWEEN 20 AND 30)');
	});

	it("spells regex per dialect", () => {
		expect(
			createSql("postgres", (t) => t.string("sku").checkRegex("^[A-Z]+$"))[0],
		).toContain(`"sku" ~ '^[A-Z]+$'`);
		expect(
			createSql("mysql", (t) => t.string("sku").checkRegex("^[A-Z]+$"))[0],
		).toContain("`sku` REGEXP '^[A-Z]+$'");
	});

	it("names a constraint when asked", () => {
		expect(
			createSql("postgres", (t) =>
				t.integer("qty").checkPositive("qty_pos"),
			)[0],
		).toContain('CONSTRAINT "qty_pos" CHECK ("qty" > 0)');
	});

	it("quotes check values instead of interpolating them raw", () => {
		expect(
			createSql("postgres", (t) =>
				t.string("role").checkIn(["a') OR 1=1 --"]),
			)[0],
		).toContain(`CHECK ("role" IN ('a'') OR 1=1 --'))`);
	});

	it("rejects a check helper with no column in front of it", () => {
		expect(() => createSql("postgres", (t) => t.checkPositive())).toThrow(
			/E_CHECK_MISUSE/,
		);
	});

	it("emits a raw check verbatim", () => {
		expect(
			createSql("postgres", (t) => {
				t.integer("a");
				t.check("a <> 42", "not_42");
			})[0],
		).toContain('CONSTRAINT "not_42" CHECK (a <> 42)');
	});
});

describe("composite constraints", () => {
	it("declares composite primary/unique/foreign", () => {
		const sql = createSql("postgres", (t) => {
			t.integer("a");
			t.integer("b");
			t.primary(["a", "b"]);
			t.unique(["a", "b"]);
			t.foreign(["a"]).references(["id"]).inTable("other").onDelete("cascade");
		})[0];

		expect(sql).toContain('PRIMARY KEY ("a", "b")');
		expect(sql).toContain('CONSTRAINT "t_a_b_unique" UNIQUE ("a", "b")');
		expect(sql).toContain(
			'FOREIGN KEY ("a") REFERENCES "other" ("id") ON DELETE CASCADE',
		);
	});

	it("keeps the no-arg column modifiers working", () => {
		const sql = createSql("postgres", (t) => {
			t.uuid("id").primary();
			t.string("email").unique();
		})[0];

		expect(sql).toContain('"id" UUID PRIMARY KEY');
		expect(sql).toContain('"email" VARCHAR(255) UNIQUE');
	});
});

describe("comments, collation and placement", () => {
	it("puts a column comment inline on MySQL and apart on Postgres", () => {
		expect(
			createSql("mysql", (t) => t.integer("id").comment("the id"))[0],
		).toContain("COMMENT 'the id'");

		const pg = createSql("postgres", (t) => t.integer("id").comment("the id"));
		expect(pg[1]).toBe(`COMMENT ON COLUMN "t"."id" IS 'the id';`);
		// SQLite has no column comments — it must not leak into the DDL.
		expect(
			createSql("sqlite", (t) => t.integer("id").comment("the id"))[0],
		).not.toContain("COMMENT");
	});

	it("applies table options on MySQL, and the comment on Postgres", () => {
		const my = createSql("mysql", (t) => {
			t.increments("id");
			t.engine("InnoDB").charset("utf8mb4").tableComment("people");
		})[0];
		expect(my).toContain("ENGINE = `InnoDB`");
		expect(my).toContain("COMMENT = 'people'");

		const pg = createSql("postgres", (t) => {
			t.increments("id");
			t.engine("InnoDB").tableComment("people");
		});
		expect(pg[0]).not.toContain("ENGINE");
		expect(pg).toContain(`COMMENT ON TABLE "t" IS 'people';`);
	});

	it("collates a column", () => {
		expect(
			createSql("mysql", (t) =>
				t.string("name").collate("utf8mb4_unicode_ci"),
			)[0],
		).toContain("COLLATE `utf8mb4_unicode_ci`");
	});

	it("refuses first()/after() outside MySQL rather than ignoring it", () => {
		const schema = new Schema("postgres");
		expect(() =>
			schema.alterTable("t", (t) => {
				t.string("x").first();
			}),
		).toThrow(/E_UNSUPPORTED/);

		const mysql = new Schema("mysql");
		mysql.alterTable("t", (t) => {
			t.string("x").after("id");
		});
		expect(mysql.toSQL()[0]).toContain("AFTER `id`");
	});
});

describe("dropping constraints", () => {
	const drop = (
		dialect: "postgres" | "mysql",
		cb: (t: TableBuilder) => void,
	): string[] => {
		const schema = new Schema(dialect);
		schema.alterTable("t", cb);
		return schema.toSQL();
	};

	it("drops per dialect", () => {
		expect(drop("mysql", (t) => t.dropPrimary())[0]).toBe(
			"ALTER TABLE `t` DROP PRIMARY KEY;",
		);
		expect(drop("postgres", (t) => t.dropPrimary())[0]).toBe(
			'ALTER TABLE "t" DROP CONSTRAINT "t_pkey";',
		);
		// A unique constraint is an index on MySQL.
		expect(drop("mysql", (t) => t.dropUnique(["a", "b"]))[0]).toBe(
			"ALTER TABLE `t` DROP INDEX `t_a_b_unique`;",
		);
		expect(drop("postgres", (t) => t.dropForeign(["a"]))[0]).toBe(
			'ALTER TABLE "t" DROP CONSTRAINT "t_a_foreign";',
		);
		expect(drop("postgres", (t) => t.dropChecks("c1", "c2"))).toEqual([
			'ALTER TABLE "t" DROP CONSTRAINT "c1";',
			'ALTER TABLE "t" DROP CONSTRAINT "c2";',
		]);
	});

	it("drops the timestamps pair", () => {
		expect(drop("postgres", (t) => t.dropTimestamps())).toEqual([
			'ALTER TABLE "t" DROP COLUMN "created_at";',
			'ALTER TABLE "t" DROP COLUMN "updated_at";',
		]);
	});

	it("names unique()/dropUnique() the same way, so they pair up", () => {
		const created = createSql("postgres", (t) => {
			t.integer("a");
			t.unique(["a"]);
		})[0];

		expect(created).toContain('CONSTRAINT "t_a_unique"');
		expect(drop("postgres", (t) => t.dropUnique(["a"]))[0]).toContain(
			'"t_a_unique"',
		);
	});
});

describe("constraints (sqlite, executed)", () => {
	let conn: AsyncDatabaseConnection;

	beforeEach(async () => {
		conn = await createNapiConnection("sqlite::memory:", 1, 1);
	});

	it("enforces a CHECK against a real database", async () => {
		const schema = new Schema("sqlite");
		schema.createTable("items", (t) => {
			t.increments("id");
			t.integer("qty").checkPositive();
			t.string("role").checkIn(["admin", "user"]);
		});
		for (const sql of schema.toSQL()) await conn.execute(sql, []);

		await conn.execute("INSERT INTO items (qty, role) VALUES (?, ?)", [
			5,
			"admin",
		]);
		// The CHECK must actually reject, not merely parse.
		await expect(
			conn.execute("INSERT INTO items (qty, role) VALUES (?, ?)", [
				-1,
				"admin",
			]),
		).rejects.toThrow();
		await expect(
			conn.execute("INSERT INTO items (qty, role) VALUES (?, ?)", [1, "root"]),
		).rejects.toThrow();
	});

	it("enforces a composite primary key against a real database", async () => {
		const schema = new Schema("sqlite");
		schema.createTable("memberships", (t) => {
			t.integer("user_id");
			t.integer("team_id");
			t.primary(["user_id", "team_id"]);
		});
		for (const sql of schema.toSQL()) await conn.execute(sql, []);

		await conn.execute(
			"INSERT INTO memberships (user_id, team_id) VALUES (?, ?)",
			[1, 1],
		);
		await expect(
			conn.execute(
				"INSERT INTO memberships (user_id, team_id) VALUES (?, ?)",
				[1, 1],
			),
		).rejects.toThrow();
	});

	it("refuses to add or drop a constraint on SQLite, as Knex does", () => {
		const schema = new Schema("sqlite");
		expect(() =>
			schema.alterTable("items", (t) => t.integer("q").checkPositive()),
		).toThrow(/E_UNSUPPORTED/);
		expect(() => schema.alterTable("items", (t) => t.dropPrimary())).toThrow(
			/E_UNSUPPORTED/,
		);
	});
});
