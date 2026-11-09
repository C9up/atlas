/**
 * Split a schema dump into the statements it holds.
 *
 * A `;` ends a statement only outside string literals, quoted identifiers,
 * comments, Postgres dollar-quoted bodies and a trigger's `BEGIN … END` body.
 * Splitting on every `;` cut a `CHECK (x <> 'a;b')` in two, made a dump with
 * a SQLite trigger unloadable, and let a `;` inside a quoted name end the
 * statement early so the rest of the name ran as SQL.
 */
import type { AtlasDialect } from "../query/native.js";

const WORD_CHAR = /[A-Za-z0-9_$]/;
const WORD_START = /[A-Za-z_]/;
const TRIGGER_HEAD = /^CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TRIGGER\b/i;

/** Index just past a quoted region opened at `start`. */
function skipQuoted(
	sql: string,
	start: number,
	quote: string,
	backslash: boolean,
): number {
	let i = start + 1;
	while (i < sql.length) {
		const c = sql[i];
		if (backslash && c === "\\") {
			i += 2;
			continue;
		}
		if (c === quote) {
			if (sql[i + 1] === quote) {
				i += 2;
				continue;
			}
			return i + 1;
		}
		i += 1;
	}
	return sql.length;
}

function isWordAt(sql: string, index: number): boolean {
	const c = sql[index];
	return c !== undefined && WORD_CHAR.test(c);
}

/** The statement text with its comments removed, trimmed. */
function withoutComments(sql: string): string {
	return sql
		.replace(/\/\*[\s\S]*?\*\//g, "")
		.replace(/--.*$/gm, "")
		.trim();
}

export function splitSqlStatements(
	sql: string,
	dialect: AtlasDialect,
): string[] {
	const statements: string[] = [];
	let start = 0;
	let depth = 0;
	let i = 0;
	const push = (end: number) => {
		const chunk = sql.slice(start, end).trim();
		if (withoutComments(chunk).length > 0) statements.push(chunk);
		start = end + 1;
		depth = 0;
	};
	while (i < sql.length) {
		const c = sql[i] ?? "";
		const next = sql[i + 1];
		if (c === "'") {
			// MySQL strings, and Postgres E'' strings, take backslash escapes.
			const escapeString =
				(sql[i - 1] === "E" || sql[i - 1] === "e") && !isWordAt(sql, i - 2);
			i = skipQuoted(sql, i, "'", dialect === "mysql" || escapeString);
			continue;
		}
		if (c === '"') {
			i = skipQuoted(sql, i, '"', dialect === "mysql");
			continue;
		}
		if (c === "`" && dialect === "mysql") {
			i = skipQuoted(sql, i, "`", false);
			continue;
		}
		if (c === "-" && next === "-") {
			const eol = sql.indexOf("\n", i);
			i = eol === -1 ? sql.length : eol + 1;
			continue;
		}
		if (c === "/" && next === "*") {
			const end = sql.indexOf("*/", i + 2);
			i = end === -1 ? sql.length : end + 2;
			continue;
		}
		if (c === "$" && dialect === "postgres" && !isWordAt(sql, i - 1)) {
			const tag = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i))?.[0];
			if (tag !== undefined) {
				const end = sql.indexOf(tag, i + tag.length);
				i = end === -1 ? sql.length : end + tag.length;
				continue;
			}
		}
		if (WORD_START.test(c) && !isWordAt(sql, i - 1)) {
			let end = i + 1;
			while (isWordAt(sql, end)) end += 1;
			const word = sql.slice(i, end).toUpperCase();
			if (word === "BEGIN" || word === "CASE" || word === "END") {
				// A trigger body holds `;`-terminated statements between BEGIN and
				// END; CASE … END nests inside it.
				if (TRIGGER_HEAD.test(withoutComments(sql.slice(start, i)))) {
					depth = word === "END" ? Math.max(0, depth - 1) : depth + 1;
				}
			}
			i = end;
			continue;
		}
		if (c === ";" && depth === 0) push(i);
		i += 1;
	}
	push(sql.length);
	return statements;
}
