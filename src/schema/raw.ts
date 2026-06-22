/**
 * Default-value handling for the schema builder — Lucid/Knex semantics.
 *
 * `defaultTo` quotes JS literals and emits raw SQL only when wrapped in
 * {@link raw} (or produced by `Migration.now()`):
 *
 *   t.text('status').defaultTo('new')          // → DEFAULT 'new'   (quoted literal)
 *   t.boolean('active').defaultTo(false)        // → DEFAULT false
 *   t.integer('count').defaultTo(0)             // → DEFAULT 0
 *   t.uuid('id').defaultTo(raw('gen_random_uuid()'))  // → DEFAULT gen_random_uuid()
 *   t.timestamp('created_at').defaultTo(this.now())    // → DEFAULT NOW()/CURRENT_TIMESTAMP
 *
 * Before this, `defaultTo` wrote its argument verbatim, so a bare
 * `defaultTo('new')` produced the invalid `DEFAULT new` and string defaults had
 * to be hand-quoted (`defaultTo("'new'")`) — a footgun Lucid/Knex avoid.
 */

/** A raw SQL expression for a column default — emitted verbatim, never quoted. */
export class RawValue {
	constructor(readonly sql: string) {}
}

/** Wrap a SQL expression so {@link TableBuilder.defaultTo} emits it verbatim. */
export function raw(sql: string): RawValue {
	return new RawValue(sql);
}

/** Accepted `defaultTo` argument: a JS literal or a {@link RawValue}. */
export type DefaultValue = string | number | boolean | RawValue;

/**
 * Render a `defaultTo` argument to the SQL fragment stored as the column
 * default. Literals are quoted/escaped; {@link RawValue} passes through.
 */
export function renderDefaultValue(value: DefaultValue): string {
	if (value instanceof RawValue) return value.sql;
	if (typeof value === "number") return String(value);
	if (typeof value === "boolean") return value ? "true" : "false";
	// String literal — single-quote and escape embedded quotes (SQL standard).
	return `'${value.replace(/'/g, "''")}'`;
}
