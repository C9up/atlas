/**
 * `scope()` — typed identity helper for declaring entity query scopes.
 *
 * Entity classes expose reusable filter/sort logic via `static scopes = {...}`.
 * TypeScript can't infer the scope type on the naked object form, so this
 * helper lets callers pin the argument types explicitly:
 *
 *     class User extends BaseEntity {
 *       static scopes = {
 *         active:  scope((q: ModelQuery<User>) => q.where('status', 'active')),
 *         forOrg:  scope((q: ModelQuery<User>, org: Org) => q.where('org_id', org.id)),
 *       }
 *     }
 *
 * The runtime is an identity pass-through — `scope` returns its argument
 * unchanged. Its only purpose is to give TS enough information to infer the
 * scope's parameter types (and, by extension, the return type of
 * `repo.query().apply(s => s.forOrg(...))` call sites).
 *
 * @implements Story 29.8 (scope helper)
 */

import type { BaseEntity } from "../BaseEntity.js";
import type { ModelQuery } from "../ModelQuery.js";

/**
 * A query scope function — takes the current query as first argument and
 * an arbitrary list of extra arguments. Returns the query for chaining
 * (or `void` — the chain is applied in place).
 */
export type ScopeFn<TEntity extends BaseEntity, Args extends unknown[] = []> = (
	query: ModelQuery<TEntity>,
	...args: Args
) => ModelQuery<TEntity> | undefined;

/**
 * Typed identity helper. Pass any scope function through `scope()` to get
 * TS inference without changing the runtime behaviour.
 */
export function scope<TEntity extends BaseEntity, Args extends unknown[] = []>(
	fn: ScopeFn<TEntity, Args>,
): ScopeFn<TEntity, Args> {
	return fn;
}
