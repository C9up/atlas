import type { DatabaseConnection } from "../BaseRepository.js";
import type { TransactionClient } from "../Transaction.js";

export const TRANSACTION_BRAND = Symbol.for("atlas:transaction");

export function isTransactionClient(
	db: DatabaseConnection,
): db is TransactionClient {
	return TRANSACTION_BRAND in db;
}
