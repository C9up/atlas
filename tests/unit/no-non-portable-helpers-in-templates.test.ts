/**
 * Grep ban on Atlas's non-portable helpers (`t.timestamps()`, `t.id()`) inside
 * any framework-shipped migration template (AC6 of Story 48.2).
 *
 * Both helpers emit Postgres-only DDL — see TableBuilder.ts JSDoc and
 * AUDIT-migration-templates.md for the full rationale. They are still
 * available for user-app migrations where the target dialect is known, but
 * MUST NOT appear inside a migration the framework ships to user apps.
 *
 * Inventory source: `packages/atlas/AUDIT-migration-templates.md`. At write
 * time the inventory is exactly one file. If a future package starts
 * shipping a migration template, add its path to TEMPLATE_PATHS below AND
 * to the audit doc — the test fails closed (file-not-found is a real failure,
 * not silent skip).
 *
 * Escape hatch: if a helper becomes genuinely dialect-aware in a future
 * story, drop it from the BANNED_PATTERNS array AND update both the JSDoc
 * warnings in TableBuilder.ts and the audit doc per the procedure in
 * `packages/atlas/AUDIT-migration-templates.md` → "Escape hatch — making a
 * helper dialect-aware". Never silently bypass.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const HERE = path.dirname(fileURLToPath(import.meta.url));

/**
 * Resolved relative to atlas's own tests/ dir. Atlas is a git submodule of
 * ream-dev, so `../../..` lands in `packages/`; the sibling-package path
 * walks down into nova's migrations folder.
 */
const TEMPLATE_PATHS = [
	path.resolve(
		HERE,
		"..",
		"..",
		"..",
		"nova",
		"migrations",
		"create_push_subscriptions.ts",
	),
] as const;

interface BannedPattern {
	readonly helper: string;
	readonly regex: RegExp;
	readonly reason: string;
}

const BANNED_PATTERNS: readonly BannedPattern[] = [
	{
		helper: "t.timestamps()",
		regex: /\bt\.timestamps\s*\(/,
		reason:
			"emits Postgres-only `DEFAULT NOW()` — invalid SQLite syntax. Write explicit created_at/updated_at columns.",
	},
	{
		helper: "t.id()",
		regex: /\bt\.id\s*\(/,
		reason:
			"emits Postgres-only `DEFAULT gen_random_uuid()` — invalid SQLite + MySQL syntax. Use `t.uuid('id').primary()` and supply the UUID at INSERT time.",
	},
] as const;

// Strip JS line and block comments so the grep targets actual call sites,
// not references inside comments / JSDoc. Naive but sufficient for migration
// templates (which never embed comment markers inside strings).
function stripComments(source: string): string {
	let out = source.replace(/\/\*[\s\S]*?\*\//g, "");
	out = out.replace(/^\s*\/\/.*$/gm, "");
	return out;
}

describe("atlas > no non-portable helpers in framework-shipped migration templates", () => {
	for (const templatePath of TEMPLATE_PATHS) {
		describe(path.basename(templatePath), () => {
			for (const banned of BANNED_PATTERNS) {
				it(`does NOT use ${banned.helper}`, async () => {
					const content = await readFile(templatePath, "utf8");
					const code = stripComments(content);
					const matches = code.match(banned.regex);
					expect(
						matches,
						`${path.relative(process.cwd(), templatePath)} uses banned helper \`${banned.helper}\` — ${banned.reason}`,
					).toBeNull();
				});
			}
		});
	}

	it("covers every shipped template (file existence check — fails closed)", async () => {
		for (const templatePath of TEMPLATE_PATHS) {
			await expect(readFile(templatePath, "utf8")).resolves.toBeTypeOf(
				"string",
			);
		}
	});
});
