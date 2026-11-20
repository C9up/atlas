/**
 * whereExpr()'s extra expression calls allow-listed functions only.
 *
 * The 4-arg form is the strict-mode-exempt alternative to whereRaw, and it
 * checked a charset and a keyword DENY-list: every server function whose
 * arguments could be built from numbers and nested calls ran, with
 * ATLAS_STRICT on. `+ length(concat(pg_sleep(30)))` held a connection for 30
 * seconds, `ascii(substr(version(), 1, 1))` read the server one character at
 * a time.
 */
import "reflect-metadata";
import { afterEach, describe, expect, it } from "vitest";
import {
	BaseEntity,
	BaseRepository,
	Column,
	Entity,
	PrimaryKey,
	setAtlasStrictMode,
} from "../../src/index.js";
import { wrapPrepareMock } from "../_support/sync-mock-adapter.js";

@Entity("orders_wx")
class Order extends BaseEntity {
	@PrimaryKey() declare id: string;
	@Column({ type: "decimal" }) declare total: number;
	@Column() declare tax: number;
	@Column() declare label: string;
}

function query() {
	const db = wrapPrepareMock({
		prepare() {
			return { run: () => ({ changes: 0 }), all: () => [] };
		},
	});
	return new BaseRepository(Order, db, { dialect: "postgres" }).query();
}

afterEach(() => setAtlasStrictMode(false));

describe("atlas > whereExpr() extra expression", () => {
	it.each([
		"+ length(concat(pg_sleep(30)))",
		"* 0 + ascii(substr(version(), 1, 1))",
		"+ length(current_setting(chr(105)))",
		"+ pg_terminate_backend(1)",
		"+ sleep(5)",
		"+ abs(pg_sleep(1))",
		"+ length(current_user)",
		"+ length(session_user)",
	])("refuses %s, strict mode on or off", (extra) => {
		for (const strict of [false, true]) {
			setAtlasStrictMode(strict);
			expect(() => query().whereExpr("total", extra, "=", 0)).toThrow(
				/whereExpr: extraExpression/,
			);
		}
	});

	it("still accepts arithmetic and the allow-listed scalar functions", () => {
		setAtlasStrictMode(true);
		expect(
			query().whereExpr("total", "+ tax * 2", ">=", 100).toSQL().sql,
		).toContain('"total" + tax * 2 >=');
		expect(
			query().whereExpr("total", "+ coalesce(tax, 0)", ">", 1).toSQL().sql,
		).toContain('"total" + coalesce(tax, 0) >');
		expect(
			query().whereExpr("total", "- round(abs(tax), 2)", "<", 5).toSQL().sql,
		).toContain('"total" - round(abs(tax), 2) <');
		expect(
			query().whereExpr("total", "+ LENGTH (label)", ">", 5).toSQL().sql,
		).toContain('"total" + LENGTH (label) >');
	});
});
