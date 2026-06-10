/**
 * Postgres parameter casts (sqlx text-bind fix). sqlx binds JS strings as
 * `text`, which Postgres won't coerce to timestamp/uuid/date. The compiler
 * emits `$N::<type>` for flagged columns on Postgres only — verified end-to-end
 * through the NAPI binding (proves the `casts` field crosses the serde boundary).
 */
import "reflect-metadata";
import { describe, expect, it } from "vitest";
import { BaseRepository, computeCastTypes } from "../../src/BaseRepository.js";
import {
	BaseEntity,
	Column,
	column,
	Entity,
	HasMany,
	PrimaryKey,
} from "../../src/index.js";
import { compileStatementNative } from "../../src/query/native.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

const insertSpec = {
	kind: "insert",
	table: "users",
	rows: [
		[
			["id", "0191-uuid"],
			["created_at", "2026-06-09T00:00:00Z"],
			["name", "Ada"],
		],
	],
	casts: { id: "uuid", created_at: "timestamp" },
	returning: [],
};

describe("atlas > Postgres parameter casts", () => {
	it("emits $N::type for flagged columns on Postgres, untyped stay plain", () => {
		const pg = compileStatementNative(insertSpec, "postgres");
		expect(pg.statements[0]).toContain("$1::uuid");
		expect(pg.statements[0]).toContain("$2::timestamp");
		expect(pg.statements[0]).toContain("$3");
		expect(pg.statements[0]).not.toContain("$3::");
	});

	it("never casts on SQLite (driver coerces)", () => {
		const sqlite = compileStatementNative(insertSpec, "sqlite");
		expect(sqlite.statements[0]).not.toContain("::");
	});

	it("never casts on MySQL", () => {
		const mysql = compileStatementNative(insertSpec, "mysql");
		expect(mysql.statements[0]).not.toContain("::");
	});

	it("derives a ::timestamp / ::date cast for @column.dateTime / @column.date columns with no explicit type", () => {
		@Entity("users")
		class User extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			@column.dateTime() declare emailVerifiedAt: Date | null;
			@column.date() declare birthday: Date | null;
			@Column() declare name: string;
		}

		// Regression: a date column tracked via DATE_COLUMNS_KEY (not `col.type`)
		// must still be flagged — otherwise its ISO-string bind reaches Postgres
		// as `text` with no `::timestamp` cast and the INSERT is rejected.
		expect(computeCastTypes(User)).toEqual({
			id: "uuid",
			email_verified_at: "timestamp",
			birthday: "date",
		});
		// `name` is a plain text column — never flagged.
		expect(computeCastTypes(User)).not.toHaveProperty("name");
	});

	it("a plain @Column custom-adapter column needs an explicit `type` to get a cast", () => {
		// The footgun: a hand-rolled date helper built on `@Column({ prepare })`
		// is NOT registered as a date column, so without `type` it gets no cast
		// and its ISO/Date value binds as text → Postgres rejects it.
		const isoAdapter = {
			prepare: (d: unknown) => (d instanceof Date ? d.toISOString() : d),
			consume: (r: unknown) => (r == null ? r : new Date(String(r))),
		};

		@Entity("members_untyped")
		class Untyped extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			@Column(isoAdapter) declare emailVerifiedAt: Date | null;
		}
		// No `type`, not a date column → only the uuid PK is flagged.
		expect(computeCastTypes(Untyped)).toEqual({ id: "uuid" });

		@Entity("members_typed")
		class Typed extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			@Column({ type: "timestamptz", ...isoAdapter })
			declare emailVerifiedAt: Date | null;
		}
		// Declaring `type` (timestamp / timestamptz / date) emits the cast.
		expect(computeCastTypes(Typed)).toEqual({
			id: "uuid",
			email_verified_at: "timestamptz",
		});
	});

	it("a @PrimaryKey propagates its options so a typed PK (no generator) registers its cast", () => {
		@Entity("tokens")
		class Token extends BaseEntity {
			// No `generated` — the uuid is caller-supplied. Before option
			// propagation, @PrimaryKey called Column() with no type, so the cast
			// was never registered and `WHERE id = $1` failed on Postgres.
			@PrimaryKey({ type: "uuid" }) declare id: string;
			@Column() declare value: string;
		}
		expect(computeCastTypes(Token)).toEqual({ id: "uuid" });

		// And `@PrimaryKey({ generated: "uuid" })` infers `type: 'uuid'`.
		@Entity("sessions2")
		class Session2 extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
		}
		expect(computeCastTypes(Session2)).toEqual({ id: "uuid" });
	});

	it("end-to-end: the derived dateTime cast reaches the compiled Postgres INSERT", () => {
		@Entity("sessions")
		class Session extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			@column.dateTime() declare expiresAt: Date | null;
		}
		const casts = computeCastTypes(Session);
		const pg = compileStatementNative(
			{
				kind: "insert",
				table: "sessions",
				rows: [
					[
						["id", "0191-uuid"],
						["expires_at", "2026-06-09T00:00:00Z"],
					],
				],
				casts,
				returning: [],
			},
			"postgres",
		);
		expect(pg.statements[0]).toContain("$1::uuid");
		expect(pg.statements[0]).toContain("$2::timestamp");
	});

	it("casts an UPDATE SET against a typed column on Postgres", () => {
		const updateSpec = {
			kind: "update",
			table: "users",
			set: [
				["updated_at", "2026-06-09T00:00:00Z"],
				["name", "Ada"],
			],
			wheres: [],
			casts: { updated_at: "timestamp" },
		};
		const pg = compileStatementNative(updateSpec, "postgres");
		expect(pg.statements[0]).toContain('"updated_at" = $1::timestamp');
		expect(pg.statements[0]).toContain('"name" = $2');
		expect(pg.statements[0]).not.toContain("$2::");
	});

	it("registers a relation FK cast so an UNTYPED uuid FK gets ::uuid in relation queries", () => {
		@Entity("a3_children")
		class A3Child extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			// FK is NOT typed — relies on the relation registering its cast.
			@Column() declare parentId: string;
		}
		@Entity("a3_parents")
		class A3Parent extends BaseEntity {
			@PrimaryKey({ generated: "uuid" }) declare id: string;
			@HasMany(() => A3Child, { foreignKey: "parent_id" })
			declare children: A3Child[];
		}
		// Constructing the parent repo publishes `a3_children.parent_id → uuid`.
		const conn = wrapPrepareMock({
			prepare: () => ({ run: () => ({ changes: 0 }), all: () => [] }),
		});
		new BaseRepository(A3Parent, conn);

		// An eager-preload-shaped select on the child table (WHERE fk IN (uuids))
		// now gets `::uuid` even though `parent_id` was never typed.
		const pg = compileStatementNative(
			{
				kind: "select",
				table: "a3_children",
				select: ["*"],
				wheres: [
					{ column: "parent_id", operator: "IN", value: ["x", "y"], type: "and" },
				],
				orderBy: [],
				groupBy: [],
				having: [],
				limit: null,
				offset: null,
				distinct: false,
				ctes: [],
				unions: [],
				joins: [],
				lockMode: null,
				selectSubqueries: [],
			},
			"postgres",
		);
		expect(pg.statements[0]).toContain("IN ($1::uuid, $2::uuid)");
	});
});
