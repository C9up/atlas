/**
 * No builder argument compiles an object taken from a request as raw SQL.
 *
 * A JSON body or a bracketed query string (`?groupBy[raw]=…`) yields a plain
 * object. `groupBy()` on the connection-level builder and on the exported
 * `QueryBuilder` handed it to the compiler, whose GROUP BY term was read by
 * shape: an object with a `raw` key was a verbatim fragment. Strict mode did
 * not see it, because the caller never named a raw method.
 *
 * Every argument position of the three builders is swept here with the object
 * shapes a request can produce, strict mode on. The marker `pwn(1)` is no
 * allow-listed function, so it can only reach the SQL through a raw arm.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { BaseModel, Column, PrimaryKey } from "../../src/index.js";
import { setAtlasStrictMode } from "../../src/ModelQuery.js";
import { QueryBuilder } from "../../src/query/QueryBuilder.js";
import {
	clearDb,
	createDbService,
	type DbService,
	setDb,
} from "../../src/services/db.js";

const MARK = "pwn(1)";

/** The objects a request body or a bracketed query string can yield. */
const SHAPES: Array<[string, string]> = [
	["{raw}", '{"raw":"pwn(1)"}'],
	["{raw,bindings}", '{"raw":"pwn(1)","bindings":["1"]}'],
	["{sql,params}", '{"sql":"pwn(1)","params":["1"]}'],
	["{kind:raw,sql}", '{"kind":"raw","sql":"pwn(1)","type":"and"}'],
	["{kind:raw,sql,bindings}", '{"kind":"raw","sql":"pwn(1)","bindings":[]}'],
	["{column,raw}", '{"column":"id","raw":"pwn(1)"}'],
	["{column,order,raw}", '{"column":"id","order":"asc","raw":"pwn(1)"}'],
	["{kind:column,name,raw}", '{"kind":"column","name":"id","raw":"pwn(1)"}'],
	["{rawValue}", '{"rawValue":{"sql":"pwn(1)"}}'],
	["[{raw}]", '[{"raw":"pwn(1)"}]'],
	// The marker as a KEY: an object whose property name is the payload.
	["{pwn(1):1}", '{"pwn(1)":"1"}'],
];

/** The value a handler reads from the request. */
function input(json: string): unknown {
	return JSON.parse(json);
}

class Order extends BaseModel {
	static override table = "sw_orders";
	@PrimaryKey() declare id: number;
	@Column() declare status: string;
	@Column() declare total: number;
}

let conn: AsyncDatabaseConnection;
let db: DbService;
const seen: string[] = [];

/** Call `target[method](...args)` with arguments TypeScript would refuse. */
function call(target: object, method: string, ...args: unknown[]): unknown {
	const fn: unknown = Reflect.get(target, method);
	if (typeof fn !== "function") throw new Error(`no method ${method}`);
	return Reflect.apply(fn, target, args);
}

function sqlOf(built: unknown): string {
	if (typeof built !== "object" || built === null) return "";
	const toSQL: unknown = Reflect.get(built, "toSQL");
	if (typeof toSQL !== "function") return "";
	const out: unknown = Reflect.apply(toSQL, built, []);
	const sql: unknown =
		typeof out === "object" && out !== null ? Reflect.get(out, "sql") : "";
	return typeof sql === "string" ? sql : "";
}

type Case = [label: string, build: (p: unknown) => unknown];

const dbq = (): object => db.from("sw_orders");
const CASES: Case[] = [
	// --- connection-level builder: db.from(t) ---
	["db.groupBy(p)", (p) => call(dbq(), "groupBy", p)],
	["db.groupBy('status', p)", (p) => call(dbq(), "groupBy", "status", p)],
	["db.orderBy(p)", (p) => call(dbq(), "orderBy", p)],
	["db.orderBy(p, 'desc')", (p) => call(dbq(), "orderBy", p, "desc")],
	["db.orderBy('id', p)", (p) => call(dbq(), "orderBy", "id", p)],
	["db.orderBy([p])", (p) => call(dbq(), "orderBy", [p])],
	["db.select(p)", (p) => call(dbq(), "select", p)],
	["db.select([p])", (p) => call(dbq(), "select", [p])],
	["db.select('id', p)", (p) => call(dbq(), "select", "id", p)],
	["db.distinct(p)", (p) => call(dbq(), "distinct", p)],
	["db.distinctOn(p)", (p) => call(dbq(), "distinctOn", p)],
	["db.count(p)", (p) => call(dbq(), "count", p)],
	["db.sum(p)", (p) => call(dbq(), "sum", p)],
	["db.sum('total', p)", (p) => call(dbq(), "sum", "total", p)],
	["db.where(p)", (p) => call(dbq(), "where", p)],
	["db.where(p, 1)", (p) => call(dbq(), "where", p, 1)],
	["db.where('id', p)", (p) => call(dbq(), "where", "id", p)],
	["db.where('id', p, 1)", (p) => call(dbq(), "where", "id", p, 1)],
	["db.where('id', '=', p)", (p) => call(dbq(), "where", "id", "=", p)],
	["db.whereNot(p)", (p) => call(dbq(), "whereNot", p)],
	["db.whereIn('id', p)", (p) => call(dbq(), "whereIn", "id", p)],
	["db.whereIn('id', [p])", (p) => call(dbq(), "whereIn", "id", [p])],
	["db.whereIn(p, [1])", (p) => call(dbq(), "whereIn", p, [1])],
	[
		"db.whereBetween('id', [p, p])",
		(p) => call(dbq(), "whereBetween", "id", [p, p]),
	],
	["db.whereNull(p)", (p) => call(dbq(), "whereNull", p)],
	["db.whereLike('status', p)", (p) => call(dbq(), "whereLike", "status", p)],
	[
		"db.whereColumn(p, '=', 'id')",
		(p) => call(dbq(), "whereColumn", p, "=", "id"),
	],
	[
		"db.whereColumn('id', p, 'id')",
		(p) => call(dbq(), "whereColumn", "id", p, "id"),
	],
	[
		"db.whereJsonPath('status', p, 1)",
		(p) => call(dbq(), "whereJsonPath", "status", p, 1),
	],
	["db.whereExists(p)", (p) => call(dbq(), "whereExists", p)],
	[
		"db.having(p, 1)",
		(p) => call(call(dbq(), "groupBy", "status") ?? {}, "having", p, 1),
	],
	[
		"db.having(p)",
		(p) => call(call(dbq(), "groupBy", "status") ?? {}, "having", p),
	],
	[
		"db.having('total', p)",
		(p) => call(call(dbq(), "groupBy", "status") ?? {}, "having", "total", p),
	],
	[
		"db.having('total', p, 1)",
		(p) =>
			call(call(dbq(), "groupBy", "status") ?? {}, "having", "total", p, 1),
	],
	[
		"db.havingIn('total', p)",
		(p) => call(call(dbq(), "groupBy", "status") ?? {}, "havingIn", "total", p),
	],
	["db.from(p)", (p) => call(dbq(), "from", p)],
	["db.table(p)", (p) => call(dbq(), "table", p)],
	["db.withSchema(p)", (p) => call(dbq(), "withSchema", p)],
	[
		"db.join(p, 'a.id', 'b.id')",
		(p) => call(dbq(), "join", p, "sw_orders.id", "sw_users.id"),
	],
	[
		"db.join('sw_users', p, 'b.id')",
		(p) => call(dbq(), "join", "sw_users", p, "sw_users.id"),
	],
	[
		"db.join('sw_users', 'a.id', p, 'b.id')",
		(p) => call(dbq(), "join", "sw_users", "sw_orders.id", p, "sw_users.id"),
	],
	["db.crossJoin(p)", (p) => call(dbq(), "crossJoin", p)],
	["db.union(p)", (p) => call(dbq(), "union", p)],
	["db.with('c', p)", (p) => call(dbq(), "with", "c", p)],
	["db.limit(p)", (p) => call(dbq(), "limit", p)],
	["db.offset(p)", (p) => call(dbq(), "offset", p)],
	["db.forPage(p)", (p) => call(dbq(), "forPage", p)],
	["db.comment(p)", (p) => call(dbq(), "comment", p)],
	[
		"db.as(p) in select",
		(p) =>
			call(dbq(), "select", call(db.from("sw_users").select("id"), "as", p)),
	],
	["db.update({status: p})", (p) => call(dbq(), "update", { status: p })],
	["db.update('status', p)", (p) => call(dbq(), "update", "status", p)],
	["db.update(p)", (p) => call(dbq(), "update", p)],
	[
		"db.update().returning(p)",
		(p) => call(call(dbq(), "returning", p) ?? {}, "update", { status: "x" }),
	],
	[
		"db.update().returning([p])",
		(p) => call(call(dbq(), "returning", [p]) ?? {}, "update", { status: "x" }),
	],
	["db.insert({status: p})", (p) => call(dbq(), "insert", { status: p })],
	["db.insert(p)", (p) => call(dbq(), "insert", p)],
	["db.multiInsert([p])", (p) => call(dbq(), "multiInsert", [p])],
	[
		"db.insert().onConflict(p).merge()",
		(p) =>
			call(call(call(dbq(), "onConflict", p) ?? {}, "merge") ?? {}, "insert", {
				id: 1,
				status: "x",
			}),
	],
	[
		"db.insert().merge(p)",
		(p) =>
			call(
				call(call(dbq(), "onConflict", "id") ?? {}, "merge", p) ?? {},
				"insert",
				{ id: 1, status: "x" },
			),
	],
	[
		"db.insert().merge({status: p})",
		(p) =>
			call(
				call(call(dbq(), "onConflict", "id") ?? {}, "merge", { status: p }) ??
					{},
				"insert",
				{ id: 1, status: "x" },
			),
	],
	[
		"db.insert().merge([p])",
		(p) =>
			call(
				call(call(dbq(), "onConflict", "id") ?? {}, "merge", [p]) ?? {},
				"insert",
				{ id: 1, status: "x" },
			),
	],
	["db.increment('total', p)", (p) => call(dbq(), "increment", "total", p)],
	["db.increment({total: p})", (p) => call(dbq(), "increment", { total: p })],
	["db.increment(p)", (p) => call(dbq(), "increment", p)],
	// --- the exported QueryBuilder ---
	[
		"QueryBuilder.groupBy(p)",
		(p) => call(new QueryBuilder("sw_orders"), "groupBy", p),
	],
	[
		"QueryBuilder.orderBy(p)",
		(p) => call(new QueryBuilder("sw_orders"), "orderBy", p),
	],
	[
		"QueryBuilder.select(p)",
		(p) => call(new QueryBuilder("sw_orders"), "select", p),
	],
	[
		"QueryBuilder.where(p, 1)",
		(p) => call(new QueryBuilder("sw_orders"), "where", p, 1),
	],
	[
		"QueryBuilder.where('id', p)",
		(p) => call(new QueryBuilder("sw_orders"), "where", "id", p),
	],
	[
		"QueryBuilder.having(p, 1)",
		(p) => call(new QueryBuilder("sw_orders"), "having", p, 1),
	],
	[
		"QueryBuilder.having('total', p)",
		(p) => call(new QueryBuilder("sw_orders"), "having", "total", p),
	],
	// --- model builder: Order.query() ---
	["model.groupBy(p)", (p) => call(Order.query(), "groupBy", p)],
	["model.orderBy(p)", (p) => call(Order.query(), "orderBy", p)],
	["model.orderBy('id', p)", (p) => call(Order.query(), "orderBy", "id", p)],
	["model.orderBy([p])", (p) => call(Order.query(), "orderBy", [p])],
	["model.select(p)", (p) => call(Order.query(), "select", p)],
	["model.select([p])", (p) => call(Order.query(), "select", [p])],
	["model.distinct(p)", (p) => call(Order.query(), "distinct", p)],
	["model.distinctOn(p)", (p) => call(Order.query(), "distinctOn", p)],
	["model.sum(p)", (p) => call(Order.query(), "sum", p)],
	["model.where(p)", (p) => call(Order.query(), "where", p)],
	["model.where(p, 1)", (p) => call(Order.query(), "where", p, 1)],
	["model.where('id', p)", (p) => call(Order.query(), "where", "id", p)],
	["model.where('id', p, 1)", (p) => call(Order.query(), "where", "id", p, 1)],
	["model.whereIn('id', p)", (p) => call(Order.query(), "whereIn", "id", p)],
	[
		"model.whereIn('id', [p])",
		(p) => call(Order.query(), "whereIn", "id", [p]),
	],
	[
		"model.whereColumn('id', p, 'total')",
		(p) => call(Order.query(), "whereColumn", "id", p, "total"),
	],
	[
		"model.whereExpr('total', p, 1)",
		(p) => call(Order.query(), "whereExpr", "total", p, 1),
	],
	[
		"model.whereExpr('total', '>', p)",
		(p) => call(Order.query(), "whereExpr", "total", ">", p),
	],
	[
		"model.whereJsonPath('status', p, 1)",
		(p) => call(Order.query(), "whereJsonPath", "status", p, 1),
	],
	[
		"model.having(p, '>', 1)",
		(p) =>
			call(call(Order.query(), "groupBy", "status") ?? {}, "having", p, ">", 1),
	],
	[
		"model.having('total', p, 1)",
		(p) =>
			call(
				call(Order.query(), "groupBy", "status") ?? {},
				"having",
				"total",
				p,
				1,
			),
	],
	[
		"model.having('total', '>', p)",
		(p) =>
			call(
				call(Order.query(), "groupBy", "status") ?? {},
				"having",
				"total",
				">",
				p,
			),
	],
	[
		"model.join(p, 'a', 'b')",
		(p) => call(Order.query(), "join", p, "sw_orders.id", "sw_users.id"),
	],
	[
		"model.join('sw_users', p, 'b')",
		(p) => call(Order.query(), "join", "sw_users", p, "sw_users.id"),
	],
	[
		"model.update({status: p})",
		(p) => call(Order.query(), "update", { status: p }),
	],
	[
		"model.update('status', p)",
		(p) => call(Order.query(), "update", "status", p),
	],
	["model.update(p)", (p) => call(Order.query(), "update", p)],
	[
		"model.update({..}, p) returning",
		(p) => call(Order.query(), "update", { status: "x" }, p),
	],
	[
		"model.update({..}, [p]) returning",
		(p) => call(Order.query(), "update", { status: "x" }, [p]),
	],
	["model.delete(p) returning", (p) => call(Order.query(), "delete", p)],
	[
		"model.increment('total', p)",
		(p) => call(Order.query(), "increment", "total", p),
	],
	[
		"model.increment({total: p})",
		(p) => call(Order.query(), "increment", { total: p }),
	],
	["model.increment(p)", (p) => call(Order.query(), "increment", p)],
	["model.limit(p)", (p) => call(Order.query(), "limit", p)],
	["model.comment(p)", (p) => call(Order.query(), "comment", p)],
];

beforeAll(async () => {
	const real = await createNapiConnection("sqlite::memory:", 1, 1);
	await real.execute(
		"CREATE TABLE sw_orders (id INTEGER PRIMARY KEY, status TEXT, total INTEGER)",
	);
	await real.execute(
		"CREATE TABLE sw_users (id INTEGER PRIMARY KEY, password TEXT)",
	);
	// Record every statement that reaches the driver (increment() runs at once).
	conn = new Proxy(real, {
		get(target, prop, receiver) {
			const value: unknown = Reflect.get(target, prop, receiver);
			if (
				(prop === "query" || prop === "execute") &&
				typeof value === "function"
			) {
				return (sql: string, ...rest: unknown[]): unknown => {
					seen.push(sql);
					return Reflect.apply(value, target, [sql, ...rest]);
				};
			}
			return typeof value === "function" ? value.bind(target) : value;
		},
	});
	db = createDbService(() => conn);
	setDb(conn);
	setAtlasStrictMode(true);
});

afterAll(async () => {
	setAtlasStrictMode(false);
	clearDb(conn);
	await conn?.close();
});

describe("atlas > request objects as builder arguments (strict mode on)", () => {
	it("compiles none of them as raw SQL, in any argument position", async () => {
		const raw: string[] = [];
		for (const [label, build] of CASES) {
			for (const [shape, json] of SHAPES) {
				seen.length = 0;
				let sql = "";
				try {
					const built = build(input(json));
					sql = sqlOf(built);
					// A promise (increment) or a thenable that already ran: wait, read the log.
					if (sql === "" && built instanceof Promise) await built;
					if (sql === "") sql = seen.join(" ;; ");
				} catch {
					sql = seen.join(" ;; ");
				}
				if (sql.includes(MARK)) raw.push(`${label} ${shape} => ${sql}`);
			}
		}
		expect(raw).toEqual([]);
	});

	it("covers the three builders", () => {
		expect(CASES.length).toBeGreaterThan(100);
	});
});
