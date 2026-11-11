/**
 * `src/` carries no type assertion (`x as T`, `<T>x`); `as const` is fine.
 *
 * An assertion tells the compiler a shape nothing checked, which is how a
 * relation target, a stored document or a config export slipped through as
 * whatever the line claimed. A guard or a typed accessor checks it instead.
 * Read from the syntax tree, so a word "as" in a comment or a string is not
 * counted and an assertion split over lines is.
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const SRC = join(import.meta.dirname, "../../src");

function sourceFiles(dir: string): string[] {
	return readdirSync(dir).flatMap((name) => {
		const file = join(dir, name);
		if (statSync(file).isDirectory()) return sourceFiles(file);
		return file.endsWith(".ts") && !file.endsWith(".d.ts") ? [file] : [];
	});
}

function assertionsIn(file: string): string[] {
	const text = readFileSync(file, "utf8");
	const source = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
	const found: string[] = [];
	const visit = (node: ts.Node): void => {
		if (
			(ts.isAsExpression(node) || ts.isTypeAssertionExpression(node)) &&
			node.type.getText(source) !== "const"
		) {
			const { line } = source.getLineAndCharacterOfPosition(node.getStart());
			found.push(`${relative(SRC, file)}:${line + 1} ${node.getText(source)}`);
		}
		ts.forEachChild(node, visit);
	};
	visit(source);
	return found;
}

describe("atlas > no type assertions in src", () => {
	it("finds none", () => {
		// The generated binding types are emitted by a script, not written here.
		const files = sourceFiles(SRC).filter(
			(file) => !file.includes(join("native", "generated")),
		);
		expect(files.length).toBeGreaterThan(50);
		expect(files.flatMap(assertionsIn)).toEqual([]);
	});
});
