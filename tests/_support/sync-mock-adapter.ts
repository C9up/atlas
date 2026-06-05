/**
 * Test-only adapter — wraps a better-sqlite3-shaped mock (`{prepare(sql) => {run, get, all}}`)
 * into the async `DatabaseConnection` shape that atlas now consumes in production.
 *
 * Lives under `tests/_support/` because it has no place in the published runtime —
 * the production binding (`@c9up/atom`) speaks `execute()`/`query()` directly via napi.
 */

import type { DatabaseConnection } from "../../src/BaseRepository.js";

interface PrepareStatement {
	run(...params: unknown[]): { changes?: number; lastInsertRowid?: number };
	get?(...params: unknown[]): Record<string, unknown> | undefined;
	all?(...params: unknown[]): Record<string, unknown>[];
}

export interface PrepareMock {
	prepare(sql: string): PrepareStatement;
}

export function wrapPrepareMock(mock: PrepareMock): DatabaseConnection {
	return {
		execute(sql: string, params: unknown[] = []) {
			const stmt = mock.prepare(sql);
			const r = stmt.run(...params);
			return Promise.resolve({ rowsAffected: r.changes ?? 0 });
		},
		query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
			const stmt = mock.prepare(sql);
			if (stmt.all) {
				return Promise.resolve(stmt.all(...params) as T[]);
			}
			if (stmt.get) {
				const row = stmt.get(...params);
				return Promise.resolve(row === undefined ? [] : ([row] as T[]));
			}
			return Promise.resolve([] as T[]);
		},
	};
}
