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

import { RawSql } from "../query/QueryBuilder.js";

/** Accepted `defaultTo` argument: a JS literal or a raw SQL expression. */
export type DefaultValue = string | number | boolean | RawSql;

/**
 * Render a `defaultTo` argument to the SQL fragment stored as the column
 * default. Literals are quoted/escaped; a {@link RawSql} passes through verbatim.
 */
export function renderDefaultValue(value: DefaultValue): string {
	if (value instanceof RawSql) return value.sql;
	if (typeof value === "number") return String(value);
	if (typeof value === "boolean") return value ? "true" : "false";
	// String literal — single-quote and escape embedded quotes (SQL standard).
	return `'${value.replace(/'/g, "''")}'`;
}
