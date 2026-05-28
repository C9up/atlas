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
			async query(sql: string): Promise<Record<string, unknown>[]> {
				return conn.query(sql);
			},
			async queryWithParams(
				sql: string,
				params: unknown[],
			): Promise<Record<string, unknown>[]> {
				return conn.query(sql, params);
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
					? (this.#impl.queryWithParams(sql, params) as Promise<T[]>)
					: (this.#impl.query(sql) as Promise<T[]>),
			close: () => this.#impl.close(),
		};
	}
}

interface DatabaseImpl {
	query(sql: string): Promise<Record<string, unknown>[]>;
	queryWithParams(
		sql: string,
		params: unknown[],
	): Promise<Record<string, unknown>[]>;
	execute(sql: string, params?: unknown[]): Promise<void>;
	close(): Promise<void>;
}
