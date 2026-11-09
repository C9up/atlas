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
	readonly params: unknown[];

	constructor(sql: string, params: unknown[] = []) {
		this.sql = sql;
		this.params = params;
	}

	/**
	 * Wrap the fragment with a prefix/suffix — e.g. parentheses (Lucid/Knex raw
	 * `.wrap('(', ')')`). Immutable: returns a new `RawSql` with the same params.
	 */
	wrap(before: string, after: string): RawSql {
		return new RawSql(`${before}${this.sql}${after}`, this.params);
	}

	/** Tagged template literal for raw SQL with automatic parameterization. */
	static sql(strings: TemplateStringsArray, ...values: unknown[]): RawSql {
		let sql = "";
		const params: unknown[] = [];
		let paramIndex = 1;

		for (let i = 0; i < strings.length; i++) {
			sql += strings[i];
			if (i < values.length) {
				if (values[i] instanceof RawSql) {
					// Inline raw SQL fragments — re-index their $N placeholders
					const raw = values[i] as RawSql;
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
