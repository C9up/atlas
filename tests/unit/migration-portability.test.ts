/**
 * Coverage for `assertInnodbPkBudget` (AC3 of Story 48.2). The helper itself
 * lives in `migration-portability.ts` (non-test sibling) so cross-package
 * test imports do not double-load this test file.
 */

import { describe, expect, it } from "vitest";
import {
	assertInnodbPkBudget,
	InnodbBudgetError,
} from "./migration-portability.js";

describe("atlas > assertInnodbPkBudget", () => {
	it("accepts a VARCHAR PK at exactly the 3072-byte budget (768 chars × 4)", () => {
		const ddl = 'CREATE TABLE "t" ( "endpoint" VARCHAR(768) PRIMARY KEY )';
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).not.toThrow();
	});

	it("throws when an inline PRIMARY KEY VARCHAR exceeds 3072 bytes", () => {
		const ddl = "CREATE TABLE `t` ( `endpoint` VARCHAR(1024) PRIMARY KEY )";
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).toThrowError(InnodbBudgetError);
	});

	it("reports the offending column with chars + bytes + reason", () => {
		const ddl = "CREATE TABLE `t` ( `endpoint` VARCHAR(2048) PRIMARY KEY )";
		try {
			assertInnodbPkBudget(ddl);
			throw new Error("expected assertInnodbPkBudget to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(InnodbBudgetError);
			if (!(err instanceof InnodbBudgetError)) return;
			expect(err.violations).toHaveLength(1);
			const v = err.violations[0];
			expect(v).toBeDefined();
			if (v === undefined) return;
			expect(v.column).toBe("endpoint");
			expect(v.length).toBe(2048);
			expect(v.bytes).toBe(8192);
			expect(v.reason).toBe("PRIMARY KEY");
		}
	});

	it("catches a separate-clause PRIMARY KEY (col)", () => {
		const ddl =
			"CREATE TABLE `t` ( `endpoint` VARCHAR(900), PRIMARY KEY (`endpoint`) )";
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).toThrowError(InnodbBudgetError);
	});

	it("catches a UNIQUE inline VARCHAR over budget", () => {
		const ddl = "CREATE TABLE `t` ( `slug` VARCHAR(800) NOT NULL UNIQUE )";
		try {
			assertInnodbPkBudget(ddl);
			throw new Error("expected assertInnodbPkBudget to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(InnodbBudgetError);
			if (!(err instanceof InnodbBudgetError)) return;
			expect(err.violations[0]?.reason).toBe("UNIQUE");
		}
	});

	it("catches a UNIQUE (col) trailing-constraint clause", () => {
		const ddl =
			"CREATE TABLE `t` ( `slug` VARCHAR(900) NOT NULL, UNIQUE (`slug`) )";
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).toThrowError(InnodbBudgetError);
	});

	it("ignores non-indexed VARCHAR columns even when they exceed 3072 bytes", () => {
		// A free-form `description VARCHAR(2000)` is fine — only PK / UNIQUE
		// columns hit the InnoDB index limit.
		const ddl =
			"CREATE TABLE `t` ( `id` VARCHAR(36) PRIMARY KEY, `description` VARCHAR(2000) NOT NULL )";
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).not.toThrow();
	});

	it("ignores non-VARCHAR PRIMARY KEYs (UUID, INTEGER, etc.)", () => {
		const ddl = "CREATE TABLE `t` ( `id` UUID PRIMARY KEY )";
		expect(() => {
			assertInnodbPkBudget(ddl);
		}).not.toThrow();
	});

	it("reports every offending column in a multi-violation DDL", () => {
		const ddl = [
			"CREATE TABLE `t` (",
			"  `a` VARCHAR(1024) PRIMARY KEY,",
			"  `b` VARCHAR(900) NOT NULL UNIQUE",
			")",
		].join("\n");
		try {
			assertInnodbPkBudget(ddl);
			throw new Error("expected assertInnodbPkBudget to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(InnodbBudgetError);
			if (!(err instanceof InnodbBudgetError)) return;
			expect(err.violations.map((v) => v.column).sort()).toEqual(["a", "b"]);
		}
	});

	it("throws when a composite PRIMARY KEY sums over the 3072-byte budget", () => {
		// 500 + 500 chars × 4 = 4000 bytes — each member fits alone (2000 bytes
		// individually) but the composite key blows the InnoDB index limit.
		const ddl = [
			"CREATE TABLE `t` (",
			"  `a` VARCHAR(500) NOT NULL,",
			"  `b` VARCHAR(500) NOT NULL,",
			"  PRIMARY KEY (`a`, `b`)",
			")",
		].join("\n");
		try {
			assertInnodbPkBudget(ddl);
			throw new Error("expected assertInnodbPkBudget to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(InnodbBudgetError);
			if (!(err instanceof InnodbBudgetError)) return;
			expect(err.violations).toHaveLength(1);
			const violation = err.violations[0];
			expect(violation).toBeDefined();
			if (!violation) return;
			expect(violation.column).toBe("a, b");
			expect(violation.length).toBe(1000);
			expect(violation.bytes).toBe(4000);
			expect(violation.reason).toBe("PRIMARY KEY");
		}
	});

	it("throws when a composite UNIQUE KEY sums over the 3072-byte budget", () => {
		const ddl = [
			"CREATE TABLE `t` (",
			"  `a` VARCHAR(400) NOT NULL,",
			"  `b` VARCHAR(400) NOT NULL,",
			"  UNIQUE KEY `t_a_b_unique` (`a`, `b`)",
			")",
		].join("\n");
		try {
			assertInnodbPkBudget(ddl);
			throw new Error("expected assertInnodbPkBudget to throw");
		} catch (err) {
			expect(err).toBeInstanceOf(InnodbBudgetError);
			if (!(err instanceof InnodbBudgetError)) return;
			expect(err.violations).toHaveLength(1);
			const violation = err.violations[0];
			expect(violation).toBeDefined();
			if (!violation) return;
			expect(violation.column).toBe("a, b");
			expect(violation.bytes).toBe(3200);
			expect(violation.reason).toBe("UNIQUE");
		}
	});

	it("accepts a composite PRIMARY KEY whose summed bytes stay at-or-under the budget", () => {
		// 384 + 384 chars × 4 = 3072 bytes — exactly at the InnoDB limit.
		const ddl = [
			"CREATE TABLE `t` (",
			"  `a` VARCHAR(384) NOT NULL,",
			"  `b` VARCHAR(384) NOT NULL,",
			"  PRIMARY KEY (`a`, `b`)",
			")",
		].join("\n");
		expect(() => assertInnodbPkBudget(ddl)).not.toThrow();
	});
});
