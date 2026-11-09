/**
 * A transaction client carries the dialect of the connection it runs on.
 *
 * A repository resolves its dialect from the connection it is built on, then
 * from the process-wide default. The transaction client used to carry none, so
 * a repository built directly on `trx` compiled for the process default —
 * `sqlite` until a provider set it — and Postgres refused the statement
 * (`syntax error at or near ","`). SQLite always runs; Postgres and MySQL run
 * when their URL is set.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseEntity } from "../../src/BaseEntity.js";
import { BaseRepository } from "../../src/BaseRepository.js";
import { Column, Entity, PrimaryKey } from "../../src/decorators/entity.js";
import { type AtlasDialect, setAtlasDialect } from "../../src/query/native.js";
import { transaction } from "../../src/Transaction.js";

@Entity("trx_dialect_rows")
class Row extends BaseEntity {
	@PrimaryKey() declare id: number;
	@Column() declare name: string;
}

function begin(conn: AsyncDatabaseConnection) {
	if (!conn.transaction) throw new Error("connection lacks transaction()");
	return conn.transaction();
}

/** A process default that is NOT the connection's dialect. */
function otherDialect(dialect: AtlasDialect): AtlasDialect {
	return dialect === "mysql" ? "postgres" : "mysql";
}

afterEach(() => {
	setAtlasDialect("sqlite");
});

function suite(
	label: string,
	url: string,
	dialect: AtlasDialect,
	pk: string,
): void {
	(url ? describe : describe.skip)(`transaction dialect ${label}`, () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			conn = await createNapiConnection(url, 1, 2);
			await conn.execute("DROP TABLE IF EXISTS trx_dialect_rows");
			await conn.execute(
				`CREATE TABLE trx_dialect_rows (id ${pk}, name VARCHAR(50) NOT NULL)`,
			);
		});

		afterAll(async () => {
			await conn?.execute("DROP TABLE IF EXISTS trx_dialect_rows");
			await conn?.close();
		});

		it("the client and its savepoints announce the connection's dialect", async () => {
			const trx = await begin(conn);
			try {
				expect(trx.dialect).toBe(dialect);
				const sp = await trx.transaction();
				expect(sp.dialect).toBe(dialect);
				await sp.rollback();
			} finally {
				await trx.rollback();
			}
		});

		it("a repository built on the transaction compiles for the connection, not the process default", async () => {
			setAtlasDialect(otherDialect(dialect));
			const quote = dialect === "mysql" ? "`" : '"';
			await conn.transaction?.(async (trx) => {
				const sql = new BaseRepository(Row, trx).query().toSQL().sql;
				expect(sql).toContain(`${quote}trx_dialect_rows${quote}`);
				await trx.transaction(async (sp) => {
					const nested = new BaseRepository(Row, sp).query().toSQL().sql;
					expect(nested).toContain(`${quote}trx_dialect_rows${quote}`);
				});
			});
		});

		it("writes through a repository built on the transaction", async () => {
			setAtlasDialect(otherDialect(dialect));
			await conn.execute("DELETE FROM trx_dialect_rows");
			await conn.transaction?.(async (trx) => {
				await new BaseRepository(Row, trx).create({ name: "outer" });
				await trx.transaction(async (sp) => {
					await new BaseRepository(Row, sp).create({ name: "inner" });
				});
			});
			const rows = await conn.query<{ name: string }>(
				"SELECT name FROM trx_dialect_rows ORDER BY name",
			);
			expect(rows.map((r) => r.name)).toEqual(["inner", "outer"]);
		});
	});
}

suite("SQLite", "sqlite::memory:", "sqlite", "INTEGER PRIMARY KEY");
suite(
	"PG",
	process.env.ATLAS_TEST_PG_URL ?? "",
	"postgres",
	"SERIAL PRIMARY KEY",
);
suite(
	"MySQL",
	process.env.ATLAS_TEST_MYSQL_URL ?? "",
	"mysql",
	"INT AUTO_INCREMENT PRIMARY KEY",
);

describe("transaction dialect without a pinned connection", () => {
	it("the BEGIN/COMMIT fallback carries the connection's dialect", async () => {
		const seen: AtlasDialect[] = [];
		await transaction(
			{
				dialect: "mysql",
				execute: () => Promise.resolve({ rowsAffected: 0 }),
				query: () => Promise.resolve([]),
			},
			async (trx) => {
				seen.push(trx.dialect);
				await trx.transaction(async (sp) => {
					seen.push(sp.dialect);
				});
			},
		);
		expect(seen).toEqual(["mysql", "mysql"]);
	});
});
