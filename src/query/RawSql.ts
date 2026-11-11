import { AtlasError } from "../errors.js";

/**
 * Typed raw SQL with parameterized values.
 *
 * The fragment is written in one notation: `?` binds the next value, `??` the
 * next identifier and `\?` is a literal `?` (Knex); or `$1`, `$2`… address
 * the params by position (what {@link RawSql.sql} emits). Wherever the
 * fragment is embedded, the compiler renumbers its placeholders into the
 * enclosing statement and refuses a fragment whose placeholders and params
 * disagree.
 */
export class RawSql {
	readonly sql: string;
	readonly #params: unknown[];
	readonly #named: Readonly<Record<string, unknown>> | undefined;

	/**
	 * Named bindings (`{ id: 7 }` for `:id` / `:id:`) are kept as given: where a
	 * string or a comment ends depends on the dialect, so they are resolved by
	 * the query that compiles the fragment, never when it is built.
	 */
	constructor(sql: string, params: unknown[] | Record<string, unknown> = []) {
		this.sql = sql;
		if (Array.isArray(params)) {
			this.#params = params;
			this.#named = undefined;
		} else {
			this.#params = [];
			this.#named = params;
		}
	}

	/**
	 * The positional params. A fragment built with named bindings has none
	 * until a query resolves it for its dialect, so reading them throws rather
	 * than answering an empty list that would drop every binding.
	 */
	get params(): unknown[] {
		if (this.#named !== undefined) {
			throw new AtlasError(
				"E_ATLAS_RAW_BINDINGS",
				`The named bindings of db.raw(${this.sql}) are resolved for the dialect of the query that uses it, not before.`,
			);
		}
		return this.#params;
	}

	/** The named bindings as given, or `undefined` for a positional fragment. */
	get namedBindings(): Readonly<Record<string, unknown>> | undefined {
		return this.#named;
	}

	/**
	 * Wrap the fragment with a prefix/suffix — e.g. parentheses (Lucid/Knex raw
	 * `.wrap('(', ')')`). Immutable: returns a new `RawSql` with the same params.
	 */
	wrap(before: string, after: string): RawSql {
		return new RawSql(
			`${before}${this.sql}${after}`,
			this.#named === undefined ? this.#params : { ...this.#named },
		);
	}

	/** Tagged template literal for raw SQL with automatic parameterization. */
	static sql(strings: TemplateStringsArray, ...values: unknown[]): RawSql {
		let sql = "";
		const params: unknown[] = [];
		let paramIndex = 1;

		for (let i = 0; i < strings.length; i++) {
			sql += strings[i];
			if (i < values.length) {
				const raw = values[i];
				if (raw instanceof RawSql) {
					// Inline raw SQL fragments — re-index their $N placeholders
					let remapped = raw.sql;
					for (let j = raw.params.length; j >= 1; j--) {
						remapped = remapped.replace(
							new RegExp(`\\$${j}(?!\\d)`, "g"),
							`$${paramIndex + j - 1}`,
						);
					}
					sql += remapped;
					params.push(...raw.params);
					paramIndex += raw.params.length;
				} else {
					sql += `$${paramIndex++}`;
					params.push(values[i]);
				}
			}
		}

		return new RawSql(sql, params);
	}
}

/**
 * A validated, dialect-quoted identifier (`db.ref('posts.created_at')`). It
 * carries no SQL a caller wrote, so atlas strict mode lets it through where it
 * refuses a `db.raw()` fragment.
 */
export class RefSql extends RawSql {
	constructor(quotedIdentifier: string) {
		super(quotedIdentifier, []);
	}
}
