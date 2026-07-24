/**
 * Lazy, chainable, inspectable DML builder — Lucid `insert()`/`update()`/
 * `delete()` return a builder that only runs on `await`/`.exec()`. This lets the
 * Lucid-documented order work:
 *
 *     db.table('users').insert(data).onConflict('email').merge()
 *     db.from('users').where(...).update(data).returning('id')
 *     db.table('users').insert(data).toSQL()
 *     db.from('users').where(...).delete().timeout(1000)
 *
 * The clause methods delegate back to the owning {@link DatabaseQueryBuilder}
 * (via `hooks`), so the reverse (atlas) order `onConflict(...).insert(...)` keeps
 * working too — both mutate the same state, read when the query finally compiles.
 */

/** The chainable DML clauses the builder forwards to its owner. */
export interface DmlChainHooks {
	onConflict(...columns: Array<string | string[]>): void;
	merge(...args: Array<string | string[] | Record<string, unknown>>): void;
	ignore(): void;
	returning(...columns: Array<string | string[]>): void;
	timeout(ms?: number): void;
	comment(text: string): void;
}

export class DmlBuilder<R> implements PromiseLike<R> {
	readonly #run: () => Promise<R>;
	readonly #compile: () => {
		sql: string;
		bindings: unknown[];
		params: unknown[];
	};
	readonly #hooks: DmlChainHooks;

	constructor(
		run: () => Promise<R>,
		compile: () => { sql: string; bindings: unknown[]; params: unknown[] },
		hooks: DmlChainHooks,
	) {
		this.#run = run;
		this.#compile = compile;
		this.#hooks = hooks;
	}

	/** The compiled statement without executing (Lucid `toSQL`). */
	toSQL(): { sql: string; bindings: unknown[]; params: unknown[] } {
		return this.#compile();
	}

	/** The parameterized SQL string (Lucid `toQuery`). */
	toQuery(): string {
		return this.#compile().sql;
	}

	/** Conflict target for an upsert (Lucid `insert(...).onConflict(...)`). */
	onConflict(...columns: Array<string | string[]>): this {
		this.#hooks.onConflict(...columns);
		return this;
	}

	/** On conflict, UPDATE columns / custom values (Lucid `merge`). */
	merge(...args: Array<string | string[] | Record<string, unknown>>): this {
		this.#hooks.merge(...args);
		return this;
	}

	/** On conflict, do nothing (Lucid `ignore`). */
	ignore(): this {
		this.#hooks.ignore();
		return this;
	}

	/** Columns to return (Lucid `insert(...).returning(...)`). */
	returning(...columns: Array<string | string[]>): this {
		this.#hooks.returning(...columns);
		return this;
	}

	/** Caller-facing statement timeout (Lucid `delete().timeout(ms)`). */
	timeout(ms?: number): this {
		this.#hooks.timeout(ms);
		return this;
	}

	/** Prefix a `/* … *​/` SQL comment (Lucid `comment`). */
	comment(text: string): this {
		this.#hooks.comment(text);
		return this;
	}

	/** Run the statement and resolve to its result. */
	exec(): Promise<R> {
		return this.#run();
	}

	// biome-ignore lint/suspicious/noThenProperty: Lucid DML builders are awaitable by design — `await db.table(t).insert(...)` must resolve to the result; the thenable is the intended public API.
	then<TResult1 = R, TResult2 = never>(
		onfulfilled?:
			| ((value: R) => TResult1 | PromiseLike<TResult1>)
			| undefined
			| null,
		onrejected?:
			| ((reason: unknown) => TResult2 | PromiseLike<TResult2>)
			| undefined
			| null,
	): PromiseLike<TResult1 | TResult2> {
		return this.#run().then(onfulfilled, onrejected);
	}
}
