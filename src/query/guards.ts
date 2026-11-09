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
