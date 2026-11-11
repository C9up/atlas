/**
 * `@c9up/atom`'s `decimalColumn` against the real Atlas pipeline.
 *
 * Atlas has no atom dependency, and this file does not add one: it reads the
 * sibling package's source, so it only runs in the monorepo. vitest.config.ts
 * leaves it out of a standalone checkout of atlas instead of letting it skip,
 * so it never passes for coverage on a runner that cannot execute it. What it proves is the contract between the
 * two — that the `type` and `nullable` `decimalColumn` hands `@Column` are the
 * fields Atlas reads, so a Postgres bind is cast to `numeric`, and that a value
 * wider than a double survives a write and a read digit for digit.
 *
 * Postgres runs when `ATLAS_TEST_PG_URL` is set, as in the other PG suites.
 */
import "reflect-metadata";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "../../src/adapters/NapiDbAdapter.js";
import { computeCastTypes } from "../../src/BaseRepository.js";
import { getColumnMetadata } from "../../src/decorators/entity.js";
import {
	BaseModel,
	Column,
	type ColumnOptions,
	PrimaryKey,
} from "../../src/index.js";
import { clearDb, setDb } from "../../src/services/db.js";

const ATOM_ATLAS = new URL("../../../atom/src/atlas.ts", import.meta.url);
const ATOM_INDEX = new URL("../../../atom/src/index.ts", import.meta.url);
const PG_URL = process.env.ATLAS_TEST_PG_URL ?? "";

/** Wider than a double: `Number()` of it loses the cents. */
const WIDE = "12345678901234567890123.45";

type DecimalColumn = (options: {
	scale?: number;
	nullable?: boolean;
	columnType?: "decimal" | "numeric";
}) => unknown;

interface Atom {
	decimalColumn: DecimalColumn;
	/** `new Decimal(value)`, reached without naming atom's types. */
	decimal: (value: string) => unknown;
}

function isFunction(value: unknown): value is (...args: never[]) => unknown {
	return typeof value === "function";
}

function isColumn(value: unknown): value is ColumnOptions & { type: string } {
	return (
		typeof value === "object" &&
		value !== null &&
		"type" in value &&
		typeof value.type === "string" &&
		"prepare" in value &&
		isFunction(value.prepare) &&
		"consume" in value &&
		isFunction(value.consume)
	);
}

async function loadAtom(): Promise<Atom> {
	const atlasSide: unknown = await import(ATOM_ATLAS.href);
	const main: unknown = await import(ATOM_INDEX.href);
	if (
		typeof atlasSide !== "object" ||
		atlasSide === null ||
		!("decimalColumn" in atlasSide) ||
		typeof atlasSide.decimalColumn !== "function" ||
		typeof main !== "object" ||
		main === null ||
		!("decimal" in main) ||
		typeof main.decimal !== "function"
	) {
		throw new Error("@c9up/atom no longer exports decimalColumn / decimal");
	}
	const { decimalColumn } = atlasSide;
	const { decimal } = main;
	return {
		decimalColumn: (options) =>
			Reflect.apply(decimalColumn, undefined, [options]),
		decimal: (value) => Reflect.apply(decimal, undefined, [value]),
	};
}

/** The column `decimalColumn` returns, checked to be one `@Column` takes. */
function columnOf(atom: Atom, options: Parameters<DecimalColumn>[0]) {
	const column = atom.decimalColumn(options);
	if (!isColumn(column)) {
		throw new Error("decimalColumn did not return @Column options");
	}
	return column;
}

/** An Invoice model whose `total` is declared with atom's column. */
function invoiceModel(atom: Atom, columnType: "decimal" | "numeric") {
	const total = columnOf(atom, { scale: 2, nullable: false, columnType });
	class Invoice extends BaseModel {
		static override table = "invoices";
		@PrimaryKey() declare id: string;
		@Column(total) declare total: unknown;
	}
	return Invoice;
}

async function roundTrip(
	atom: Atom,
	conn: AsyncDatabaseConnection,
	columnType: "decimal" | "numeric",
): Promise<void> {
	const Invoice = invoiceModel(atom, columnType);
	setDb(conn);
	try {
		await Invoice.create({ id: "a", total: atom.decimal(WIDE) });
		const found = await Invoice.find("a");
		expect(String(found?.total)).toBe(WIDE);

		if (found === null) throw new Error("expected the invoice back");
		found.total = atom.decimal("0.10");
		await found.save();
		const updated = await Invoice.find("a");
		expect(String(updated?.total)).toBe("0.1");
	} finally {
		clearDb(conn);
	}
}

describe("atom decimalColumn → @Column", () => {
	let atom: Atom;

	beforeAll(async () => {
		atom = await loadAtom();
	});

	it("registers the type and nullable it hands over", () => {
		const Invoice = invoiceModel(atom, "numeric");
		const total = getColumnMetadata(Invoice).find(
			(column) => column.propertyKey === "total",
		);
		expect(total?.type).toBe("numeric");
		expect(total?.nullable).toBe(false);
	});

	it("makes a Postgres bind cast to the column's numeric type", () => {
		expect(computeCastTypes(invoiceModel(atom, "numeric"))).toEqual({
			total: "numeric",
		});
		expect(computeCastTypes(invoiceModel(atom, "decimal"))).toEqual({
			total: "decimal",
		});
	});

	describe("on SQLite", () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			conn = await createNapiConnection("sqlite::memory:", 1, 1);
			// TEXT, not NUMERIC: SQLite's numeric affinity would store the
			// value as a double and drop its last digits before atlas sees it.
			await conn.execute(
				"CREATE TABLE invoices (id TEXT PRIMARY KEY, total TEXT NOT NULL)",
			);
		});

		afterAll(async () => {
			await conn?.close();
		});

		it("writes and reads back a value wider than a double, digit for digit", async () => {
			await roundTrip(atom, conn, "decimal");
			const [raw] = await conn.query<Record<string, unknown>>(
				"SELECT total FROM invoices WHERE id = 'a'",
			);
			// Stored as the canonical text `prepare` returns.
			expect(raw?.total).toBe("0.1");
		});
	});

	describe.skipIf(!PG_URL)("on Postgres", () => {
		let conn: AsyncDatabaseConnection;

		beforeAll(async () => {
			conn = await createNapiConnection(PG_URL, 1, 1);
			await conn.execute("DROP TABLE IF EXISTS invoices");
			await conn.execute(
				"CREATE TABLE invoices (id TEXT PRIMARY KEY, total NUMERIC(40, 2) NOT NULL)",
			);
		});

		afterAll(async () => {
			await conn?.execute("DROP TABLE IF EXISTS invoices");
			await conn?.close();
		});

		it("writes and reads back a NUMERIC(40, 2) digit for digit", async () => {
			await roundTrip(atom, conn, "numeric");
			const [raw] = await conn.query<Record<string, unknown>>(
				"SELECT total::text AS total FROM invoices WHERE id = 'a'",
			);
			expect(raw?.total).toBe("0.10");
		});
	});
});
