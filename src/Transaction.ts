/**
 * Transaction — wraps database operations in BEGIN/COMMIT/ROLLBACK.
 *
 * @implements MISS-1
 */

import { randomBytes } from "node:crypto";
import type { DatabaseConnection } from "./BaseRepository.js";
import {
	isTransactionClient,
	TRANSACTION_BRAND,
} from "./utils/transactionBrand.js";

export interface TransactionClient extends DatabaseConnection {
	commit(): Promise<void>;
	rollback(): Promise<void>;
	readonly isNested: boolean;
	readonly [TRANSACTION_BRAND]: true;
}

export async function transaction<T>(
	db: DatabaseConnection,
	callback: (trx: TransactionClient) => Promise<T> | T,
): Promise<T> {
	if (isTransactionClient(db)) {
		const name = `sp_${randomBytes(6).toString("hex")}`;
		await db.execute(`SAVEPOINT ${name}`, []);

		const trx: TransactionClient = {
			execute: db.execute.bind(db),
			query: db.query.bind(db),
			async commit() {
				await db.execute(`RELEASE SAVEPOINT ${name}`, []);
			},
			async rollback() {
				await db.execute(`ROLLBACK TO SAVEPOINT ${name}`, []);
			},
			isNested: true,
			[TRANSACTION_BRAND]: true,
		};

		try {
			const result = await callback(trx);
			await trx.commit();
			return result;
		} catch (err) {
			try {
				await trx.rollback();
			} catch {
				/* best-effort */
			}
			throw err;
		}
	}

	// Pinned interactive transaction (napi-backed): db.begin() acquires ONE
	// connection and issues BEGIN on it; the returned client routes every
	// statement to that same connection until commit/rollback hands it back to
	// the pool. This is the CORRECT path. The BEGIN/COMMIT-over-the-pool
	// fallback below is broken on a pool (each db.execute() picks a different
	// connection, so BEGIN/COMMIT and the statements scatter — no atomicity);
	// it's kept only for minimal single-connection connections that lack begin().
	if (typeof db.begin === "function") {
		const trx = await db.begin();
		try {
			const result = await callback(trx);
			await trx.commit();
			return result;
		} catch (err) {
			try {
				await trx.rollback();
			} catch {
				/* best-effort */
			}
			throw err;
		}
	}

	await db.execute("BEGIN", []);

	const trx: TransactionClient = {
		execute: db.execute.bind(db),
		query: db.query.bind(db),
		async commit() {
			await db.execute("COMMIT", []);
		},
		async rollback() {
			await db.execute("ROLLBACK", []);
		},
		isNested: false,
		[TRANSACTION_BRAND]: true,
	};

	try {
		const result = await callback(trx);
		await trx.commit();
		return result;
	} catch (err) {
		try {
			await trx.rollback();
		} catch {
			/* best-effort */
		}
		throw err;
	}
}
