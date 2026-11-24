/**
 * whereExpr()'s extra expression calls allow-listed functions only.
 *
 * The 4-arg form is the strict-mode-exempt alternative to whereRaw, and it
 * checked a charset and a keyword DENY-list: every server function whose
 * arguments could be built from numbers and nested calls ran, with
 * ATLAS_STRICT on. `+ length(concat(pg_sleep(30)))` held a connection for 30
 * seconds, `ascii(substr(version(), 1, 1))` read the server one character at
 * a time.
 *
 * The allow-list that replaced it looked for a name directly followed by `(`,
 * while the charset admitted `/` and `*`: with a comment between `pg_sleep`
 * and its parenthesis, the name was a bare word for the check and a call for
 * the server. The fragment is now tokenised whole by the compiler.
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

	it.each([
		"+ pg_sleep/**/(1)",
		"+ pg_sleep /* x */ (1)",
		"+ length(version/**/())",
		"+ sleep/**/(1)",
		"+ length(concat/**/(pg_sleep/**/(2)))",
		"* 0 + length(randomblob/**/(1048576))",
		"+ length(current_user/**/)",
		"+ tax -- trailing",
		"+ tax */",
	])("refuses a comment wherever it sits: %s", (extra) => {
		for (const strict of [false, true]) {
			setAtlasStrictMode(strict);
			expect(() => query().whereExpr("total", extra, "=", 0)).toThrow(
				expect.objectContaining({
					code: "E_ATLAS_INVALID_EXPRESSION",
					message: expect.stringMatching(
						/whereExpr: extraExpression .* a comment/,
					),
				}),
			);
		}
	});

	it.each([
		// A tail that opens with a parenthesis would turn the quoted column
		// before it into a function name.
		"(tax)",
		"(1)",
		// Quoted and qualified names, literals, other operators.
		'+ "pg_sleep"(1)',
		"+ pg_catalog.pg_sleep(1)",
		"+ orders.tax",
		"+ 'a'",
		"+ tax = 1",
		"+ tax || tax",
		"+ tax % 2",
		// Characters no token starts with.
		"+ tax\u000c",
		"+ tax\u00a0",
		"+ $1",
		"+ @@version",
		// An operand or an operator left over.
		"+",
		"+ 1 2",
		"tax",
		"",
	])("refuses what is not a tail of arithmetic: %j", (extra) => {
		expect(() => query().whereExpr("total", extra, "=", 0)).toThrow(
			expect.objectContaining({ code: "E_ATLAS_INVALID_EXPRESSION" }),
		);
	});

	it("names the function it refuses and the ones it allows", () => {
		expect(() =>
			query().whereExpr("total", "+ length(sqlite_version())", ">", 0),
		).toThrow(/calls 'sqlite_version', which is not one of abs ceil floor/);
	});

	it("reads a value that is not a string as its text, once", () => {
		const forged: unknown = { toString: () => "+ pg_sleep(1)" };
		const whereExpr: unknown = Reflect.get(query(), "whereExpr");
		expect(() =>
			typeof whereExpr === "function"
				? Reflect.apply(whereExpr, query(), ["total", forged, "=", 0])
				: undefined,
		).toThrow(expect.objectContaining({ code: "E_ATLAS_INVALID_EXPRESSION" }));
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
		expect(query().whereExpr("total", "* 2 / 3", ">", 5).toSQL().sql).toContain(
			'"total" * 2 / 3 >',
		);
		expect(
			query().whereExpr("total", "* 1.5 - -1", ">", 5).toSQL().sql,
		).toContain('"total" * 1.5 - -1 >');
	});
});
