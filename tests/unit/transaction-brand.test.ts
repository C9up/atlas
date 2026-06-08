import { describe, expect, it } from "vitest";
import {
	isTransactionClient,
	TRANSACTION_BRAND,
} from "../../src/utils/transactionBrand.js";

describe("atlas > transactionBrand", () => {
	it("returns false for a plain DatabaseConnection without the brand", () => {
		const db = {
			execute() {
				return Promise.resolve({ rowsAffected: 0 });
			},
			query() {
				return Promise.resolve([]);
			},
		};
		expect(isTransactionClient(db)).toBe(false);
	});

	it("returns true when the brand symbol is present", () => {
		const trx = {
			execute() {
				return Promise.resolve({ rowsAffected: 0 });
			},
			query() {
				return Promise.resolve([]);
			},
			[TRANSACTION_BRAND]: true as const,
		};
		expect(isTransactionClient(trx)).toBe(true);
	});

	it("uses Symbol.for so cross-module identity holds", () => {
		expect(TRANSACTION_BRAND).toBe(Symbol.for("atlas:transaction"));
	});
});
