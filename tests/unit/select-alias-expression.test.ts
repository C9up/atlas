/**
 * An aliased selection is `column AS alias` or it is refused.
 *
 * The compiler took the first and last words around the AS, so
 * `select('price * 2 AS double')` compiled to `SELECT "price" AS "double"`:
 * a different expression, returned without a word.
 */
import { describe, expect, it } from "vitest";
import {
	DatabaseQueryBuilder,
	type QueryExecutor,
} from "../../src/query/DatabaseQueryBuilder.js";

const exec: QueryExecutor = {
	query: async () => [],
	execute: async () => ({ rowsAffected: 0 }),
};
const products = () => new DatabaseQueryBuilder(exec, "postgres", "products");

describe("atlas > select() aliases", () => {
	it.each(["price * 2 AS double", "price + tax as total", "a AS b AS c"])(
		"refuses %s instead of compiling another expression",
		(expression) => {
			expect(() => products().select(expression).toSQL()).toThrow(
				/E_UNSAFE_EXPRESSION/,
			);
		},
	);

	it("refuses the object form of an expression too", () => {
		expect(() => products().select({ double: "price * 2" }).toSQL()).toThrow(
			/E_UNSAFE_EXPRESSION/,
		);
	});

	it("still quotes a bare column alias", () => {
		expect(products().select("price as total").toSQL().sql).toBe(
			'SELECT "price" AS "total" FROM "products"',
		);
	});
});
