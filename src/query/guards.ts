/**
 * Input guards shared by the model query, the repository and the db builder.
 */
import { AtlasError } from "../errors.js";

/**
 * The amount of an `increment()`/`decrement()`. A non-finite number (an
 * unparsable `Number(body.amount)`) was written as `col + NULL`, which sets the
 * column to NULL; a string or an object was bound as text. Knex runs the
 * amount through `parseFloat` and sends a NaN on to the driver; atlas refuses
 * anything but a finite number instead.
 */
export function assertCounterAmount(
	op: "increment" | "decrement",
	column: string,
	amount: unknown,
): number {
	if (typeof amount === "number" && Number.isFinite(amount)) return amount;
	throw new AtlasError(
		"E_ATLAS_INVALID_INCREMENT",
		`${op}('${column}') needs a finite number, got ${typeof amount === "number" ? amount : typeof amount}.`,
		{ hint: "Validate the amount (Number.isFinite) before calling it." },
	);
}

/**
 * A LIMIT/OFFSET row count. NaN and Infinity were written as `null`, which the
 * compiler reads as "no LIMIT" — `limit(Number('abc'))` returned the table.
 */
export function assertRowCount(method: string, n: unknown): number {
	if (typeof n === "number" && Number.isSafeInteger(n) && n >= 0) return n;
	throw new AtlasError(
		"E_ATLAS_INVALID_PAGINATION",
		`${method} must be a non-negative integer, got ${String(n)}`,
	);
}

/**
 * A 1-based page and its size, as `paginate()`/`forPage()` take them. Cast
 * with `Number()` like Lucid, floored and clamped to 1; a value that is still
 * not a finite integer is refused. Lucid hands NaN to Knex, whose `limit()`
 * ignores it and returns every row: a `?perPage=abc` dumped the table.
 */
export function normalizePage(
	page: unknown,
	perPage: unknown,
): { page: number; perPage: number; offset: number } {
	const p = Math.max(1, Math.floor(Number(page)));
	const pp = Math.max(1, Math.floor(Number(perPage)));
	const offset = (p - 1) * pp;
	if (
		!Number.isSafeInteger(p) ||
		!Number.isSafeInteger(pp) ||
		!Number.isSafeInteger(offset)
	) {
		throw new AtlasError(
			"E_ATLAS_INVALID_PAGINATION",
			`Invalid pagination: page ${String(page)}, perPage ${String(perPage)} — both must be finite integers.`,
			{ hint: "Validate page and perPage from the request before paginating." },
		);
	}
	return { page: p, perPage: pp, offset };
}
