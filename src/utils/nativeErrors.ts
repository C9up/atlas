/**
 * The NAPI boundary for errors.
 *
 * The Rust query compiler and the Rust driver can only hand a string across
 * the boundary, so a refusal reaches JavaScript as a plain `Error` whose
 * message carries the code: `E_UNSAFE_IDENTIFIER: …`, or, when the compiler
 * wraps a nested refusal, `Invalid EXISTS subquery: E_UNSAFE_IDENTIFIER: …`.
 * An application branches on `error.code`, not on the wording of a message,
 * so each one is rethrown here as the class its code names, the native error
 * kept as `cause`.
 */

import type { Exception } from "@c9up/flare";
import {
	E_ALTER_EMPTY,
	E_ATLAS_EMPTY_INSERT,
	E_ATLAS_EMPTY_TABLE,
	E_ATLAS_EMPTY_UPDATE,
	E_ATLAS_INSECURE_SSLMODE,
	E_ATLAS_INTERNAL,
	E_ATLAS_INVALID_EXPRESSION,
	E_ATLAS_INVALID_OPERATOR,
	E_ATLAS_INVALID_SPEC,
	E_ATLAS_LISTEN_CHANNEL,
	E_ATLAS_LISTEN_CLOSED,
	E_ATLAS_LISTEN_CONNECT,
	E_ATLAS_LISTEN_UNSUPPORTED,
	E_ATLAS_RAW_BINDINGS,
	E_ATLAS_RAW_VALUE,
	E_ATLAS_UNDEFINED_BINDING,
	E_CHECK_EMPTY,
	E_CHECK_RANGE,
	E_CONSTRAINT_EMPTY,
	E_EMPTY_VIEW_SELECT,
	E_ENUM_EMPTY,
	E_INJECTION_PATTERN,
	E_SPECIFIC_TYPE_EMPTY,
	E_SQLITE_DECIMAL_PRECISION,
	E_UNSAFE_EXPRESSION,
	E_UNSAFE_IDENTIFIER,
	E_UNSAFE_LITERAL,
	E_UNSAFE_SQL,
	E_UNSUPPORTED,
	E_UNSUPPORTED_CLIENT,
} from "../errors.js";

type NativeErrorFactory = (message: string, options: ErrorOptions) => Exception;

/**
 * An error whose class has a fixed message template, given the native
 * message instead: the compiler's wording names what it refused.
 */
function withMessage(error: Exception, message: string): Exception {
	error.message = message;
	return error;
}

/** Every code the native side sends, with the class it becomes. */
const NATIVE_ERRORS = new Map<string, NativeErrorFactory>([
	["E_UNSUPPORTED", (m, o) => new E_UNSUPPORTED([m], o)],
	["E_UNSAFE_SQL", (m, o) => new E_UNSAFE_SQL([m], o)],
	["E_UNSAFE_IDENTIFIER", (m, o) => new E_UNSAFE_IDENTIFIER([m], o)],
	["E_UNSAFE_EXPRESSION", (m, o) => new E_UNSAFE_EXPRESSION([m], o)],
	["E_UNSAFE_LITERAL", (m, o) => new E_UNSAFE_LITERAL([m], o)],
	["E_INJECTION_PATTERN", (m, o) => new E_INJECTION_PATTERN([m], o)],
	["E_SPECIFIC_TYPE_EMPTY", (m, o) => new E_SPECIFIC_TYPE_EMPTY([m], o)],
	["E_CHECK_EMPTY", (m, o) => new E_CHECK_EMPTY([m], o)],
	["E_CHECK_RANGE", (m, o) => new E_CHECK_RANGE([m], o)],
	["E_CONSTRAINT_EMPTY", (m, o) => new E_CONSTRAINT_EMPTY([m], o)],
	["E_ENUM_EMPTY", (m, o) => new E_ENUM_EMPTY([m], o)],
	["E_EMPTY_VIEW_SELECT", (m, o) => new E_EMPTY_VIEW_SELECT([m], o)],
	[
		"E_SQLITE_DECIMAL_PRECISION",
		(m, o) => new E_SQLITE_DECIMAL_PRECISION([m], o),
	],
	[
		"E_ATLAS_UNDEFINED_BINDING",
		(m, o) => new E_ATLAS_UNDEFINED_BINDING(m, undefined, o),
	],
	["E_ATLAS_INSECURE_SSLMODE", (m, o) => new E_ATLAS_INSECURE_SSLMODE([m], o)],
	["E_ATLAS_LISTEN_CONNECT", (m, o) => new E_ATLAS_LISTEN_CONNECT([m], o)],
	["E_ATLAS_LISTEN_CHANNEL", (m, o) => new E_ATLAS_LISTEN_CHANNEL([m], o)],
	["E_ATLAS_LISTEN_CLOSED", (m, o) => new E_ATLAS_LISTEN_CLOSED([m], o)],
	[
		"E_ATLAS_LISTEN_UNSUPPORTED",
		(m, o) => new E_ATLAS_LISTEN_UNSUPPORTED([m], o),
	],
	["E_ATLAS_INVALID_SPEC", (m, o) => new E_ATLAS_INVALID_SPEC([m], o)],
	["E_ATLAS_INVALID_OPERATOR", (m, o) => new E_ATLAS_INVALID_OPERATOR([m], o)],
	[
		"E_ATLAS_INVALID_EXPRESSION",
		(m, o) => new E_ATLAS_INVALID_EXPRESSION([m], o),
	],
	["E_ATLAS_RAW_BINDINGS", (m, o) => new E_ATLAS_RAW_BINDINGS([m], o)],
	["E_ATLAS_RAW_VALUE", (m, o) => new E_ATLAS_RAW_VALUE(m, undefined, o)],
	["E_ATLAS_EMPTY_INSERT", (m, o) => new E_ATLAS_EMPTY_INSERT([m], o)],
	[
		"E_ATLAS_EMPTY_UPDATE",
		(m, o) => withMessage(new E_ATLAS_EMPTY_UPDATE(undefined, o), m),
	],
	["E_ATLAS_EMPTY_TABLE", (m, o) => new E_ATLAS_EMPTY_TABLE([m], o)],
	["E_ALTER_EMPTY", (m, o) => withMessage(new E_ALTER_EMPTY([""], o), m)],
	["E_UNSUPPORTED_CLIENT", (m, o) => new E_UNSUPPORTED_CLIENT([m], o)],
	["E_ATLAS_INTERNAL", (m, o) => new E_ATLAS_INTERNAL([m], o)],
]);

/** A code at the start of the message, or right after a wrapping `…: `. */
const NATIVE_CODE = /(^|: )(E_[A-Z0-9_]+): /;

/**
 * A native error as the class its message names, with the native error as
 * its `cause`. Anything else (a driver error from the database server, a
 * message with no code) is returned unchanged.
 */
export function fromNativeError(error: unknown): unknown {
	if (!(error instanceof Error)) return error;
	const coded = NATIVE_CODE.exec(error.message);
	const lead = coded?.[1];
	const code = coded?.[2];
	if (coded === null || lead === undefined || code === undefined) return error;
	const factory = NATIVE_ERRORS.get(code);
	if (factory === undefined) return error;
	const message =
		error.message.slice(0, coded.index + lead.length) +
		error.message.slice(coded.index + coded[0].length);
	return factory(message, { cause: error });
}
