/**
 * The one shape every aggregate takes.
 *
 * Lucid types all eight — `count`, `countDistinct`, `min`, `max`, `sum`,
 * `sumDistinct`, `avg`, `avgDistinct` — with a single `Aggregate<Builder>`
 * signature, and every one of them returns the BUILDER. There is no terminal
 * scalar form: the value comes back as a column of a row, in `$extras` on the
 * model builder.
 *
 * atlas used to split them by argument: an aliased expression projected and
 * chained, a bare column ran the query and answered a number. That read well
 * one method at a time and was inconsistent across the set — `count('appid')`
 * chained while `countDistinct('appid')` resolved to a number, so code written
 * from one shape broke on the other. This module is the shared normalisation,
 * so the two builders cannot drift again.
 */

/** One column, or several — Lucid's `OneOrMany<string>`. */
export type AggregateColumns = string | readonly string[];

/** `{ alias: column }`, for several aggregates in one call. */
export type AggregateColumnsMap = Record<string, AggregateColumns>;

/** A single `FN(column) AS alias` to project. */
export interface AggregateTarget {
	/** The column the function applies to. `*` is passed through untouched. */
	column: string;
	/** The output alias, when the call named one. */
	alias?: string;
}

/** Matches a trailing ` as <alias>`, the inline way to name the output. */
const ALIASED = /^(.*?)\s+as\s+(.+)$/i;

function isColumnsMap(
	value: AggregateColumns | AggregateColumnsMap,
): value is AggregateColumnsMap {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function targetsFor(
	columns: AggregateColumns,
	alias?: string,
): AggregateTarget[] {
	const list = typeof columns === "string" ? [columns] : [...columns];
	return list.map((entry) => {
		// An explicit `alias` argument wins over an inline one: it is the more
		// specific of the two, and Lucid resolves the column through its key
		// mapper in that branch for the same reason.
		if (alias !== undefined) return { column: entry.trim(), alias };
		const match = ALIASED.exec(entry.trim());
		if (match?.[1] !== undefined && match[2] !== undefined) {
			return { column: match[1].trim(), alias: match[2].trim() };
		}
		return { column: entry.trim() };
	});
}

/**
 * Normalise the argument forms Lucid accepts into a flat list of projections.
 *
 *   count('*')                    → [{ column: '*' }]
 *   count('* as total')           → [{ column: '*', alias: 'total' }]
 *   count('id', 'total')          → [{ column: 'id', alias: 'total' }]
 *   count({ total: 'id' })        → [{ column: 'id', alias: 'total' }]
 *   count(['a as x', 'b as y'])   → two targets
 */
export function aggregateTargets(
	columns: AggregateColumns | AggregateColumnsMap,
	alias?: string,
): AggregateTarget[] {
	if (isColumnsMap(columns)) {
		return Object.entries(columns).flatMap(([key, value]) =>
			targetsFor(value, key),
		);
	}
	return targetsFor(columns, alias);
}
