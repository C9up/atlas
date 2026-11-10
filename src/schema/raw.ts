/**
 * Default-value rendering for the schema builder — Lucid/Knex semantics.
 *
 * `defaultTo` quotes JS literals and emits raw SQL only when given a `RawSql`
 * (the single raw-expression type atlas uses everywhere, incl. queries),
 * produced in a migration by `this.now()` / `this.raw(...)`:
 *
 *   t.text('status').defaultTo('new')             // → DEFAULT 'new'   (quoted literal)
 *   t.boolean('active').defaultTo(false)          // → DEFAULT false
 *   t.integer('count').defaultTo(0)               // → DEFAULT 0
 *   t.uuid('id').defaultTo(this.raw('gen_random_uuid()'))  // → DEFAULT gen_random_uuid()
 *   t.timestamp('created_at').defaultTo(this.now())        // → DEFAULT NOW()/CURRENT_TIMESTAMP
 *
 * Before this, `defaultTo` wrote its argument verbatim, so a bare
 * `defaultTo('new')` produced the invalid `DEFAULT new` and string defaults had
 * to be hand-quoted (`defaultTo("'new'")`) — a footgun Lucid/Knex avoid.
 */

import { AtlasError } from "../errors.js";
import { RawSql } from "../query/QueryBuilder.js";

/** Accepted `defaultTo` argument: a JS literal or a raw SQL expression. */
export type DefaultValue = string | number | boolean | RawSql;

/**
 * Render a non-string `defaultTo` argument to the SQL fragment stored as the
 * column default. A {@link RawSql} passes through verbatim; a string default is
 * not rendered here but quoted by the compiler for the target dialect.
 */
export function renderDefaultValue(
	value: Exclude<DefaultValue, string>,
): string {
	if (value instanceof RawSql) {
		// DDL cannot bind parameters, so a fragment's bindings would be dropped.
		if (value.namedBindings !== undefined || value.params.length > 0) {
			throw new AtlasError(
				"E_ATLAS_RAW_VALUE",
				`defaultTo(db.raw()) cannot carry bindings: ${value.sql}`,
			);
		}
		return value.sql;
	}
	if (typeof value === "number") {
		if (!Number.isFinite(value)) {
			throw new AtlasError(
				"E_ATLAS_NON_FINITE_NUMBER",
				`defaultTo(${value}) is not a SQL number.`,
			);
		}
		return String(value);
	}
	return value ? "true" : "false";
}
