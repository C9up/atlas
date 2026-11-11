/**
 * TestDatabase — in-memory SQLite database for testing via the ream-db Rust driver.
 */

import type { DatabaseAdapter } from "../schema/MigrationRunner.js";

/** Simple async database interface for tests. */
export class Database {
	#impl: DatabaseImpl;

	constructor(impl: DatabaseImpl) {
		this.#impl = impl;
	}

	/** Create an in-memory SQLite database backed by the Rust ream-db driver. */
	static async memory(): Promise<Database> {
		const { createNapiConnection } = await import(
			"../adapters/NapiDbAdapter.js"
		);
		const conn = await createNapiConnection("sqlite::memory:", 1, 1);
		return new Database({
			async query<T>(sql: string): Promise<T[]> {
				return conn.query<T>(sql);
			},
			async queryWithParams<T>(sql: string, params: unknown[]): Promise<T[]> {
				return conn.query<T>(sql, params);
			},
			async execute(sql: string, params?: unknown[]): Promise<void> {
				await conn.execute(sql, params);
			},
			async close(): Promise<void> {
				await conn.close();
			},
		});
	}

	async query(sql: string): Promise<Record<string, unknown>[]> {
		return this.#impl.query(sql);
	}

	async queryWithParams(
		sql: string,
		params: unknown[],
	): Promise<Record<string, unknown>[]> {
		return this.#impl.queryWithParams(sql, params);
	}

	async execute(sql: string, params?: unknown[]): Promise<void> {
		return this.#impl.execute(sql, params);
	}

	async close(): Promise<void> {
		return this.#impl.close();
	}

	/** Get a DatabaseAdapter for MigrationRunner compatibility. */
	asAdapter(): DatabaseAdapter {
		return {
			execute: (sql: string, params?: unknown[]) =>
				this.#impl.execute(sql, params),
			query: <T>(sql: string, params?: unknown[]) =>
				params
					? this.#impl.queryWithParams<T>(sql, params)
					: this.#impl.query<T>(sql),
			close: () => this.#impl.close(),
		};
	}
}

interface DatabaseImpl {
	query<T = Record<string, unknown>>(sql: string): Promise<T[]>;
	queryWithParams<T = Record<string, unknown>>(
		sql: string,
		params: unknown[],
	): Promise<T[]>;
	execute(sql: string, params?: unknown[]): Promise<void>;
	close(): Promise<void>;
}
