/**
 * Grep ban on Atlas's non-portable helpers inside every migration template a
 * package ships to user apps (the `stubs/database/migrations/*.stub` files).
 *
 * A shipped template runs on whatever dialect the app picked, so it may only
 * use helpers whose DDL is valid on sqlite, postgres and mysql alike. Each ban
 * below is backed by the DDL the schema builder emits today: if a helper
 * becomes portable, its proof fails and the ban must go with it.
 *
 * `t.timestamps()` is not banned: it emits `DEFAULT CURRENT_TIMESTAMP`, which
 * every dialect understands (checked below).
 *
 * The templates live in sibling packages, so this suite only runs in the
 * monorepo (see vitest.config.ts). There, a template listed here that no
 * longer exists is a failure, not a skip.
 */

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { TableBuilder } from "../../src/index.js";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PACKAGES = path.resolve(HERE, "..", "..", "..");

const TEMPLATE_PATHS = [
	"nova/stubs/database/migrations/0048_create_push_subscriptions.stub",
	"visa/stubs/database/migrations/9000_create_visa_tables.stub",
	"visa/stubs/database/migrations/9001_add_visa_client_secret_expiry.stub",
	"visa/stubs/database/migrations/9002_add_visa_code_redirect_uri_omitted.stub",
].map((relative) => path.join(PACKAGES, relative));

const DIALECTS = ["sqlite", "postgres", "mysql"] as const;
type Dialect = (typeof DIALECTS)[number];

interface BannedPattern {
	readonly helper: string;
	readonly regex: RegExp;
	readonly reason: string;
	/** Applies the helper to a fresh table. */
	readonly apply: (table: TableBuilder) => void;
	/** The non-portable fragment the reason names. */
	readonly fragment: string;
	/** The dialects that reject that fragment. */
	readonly rejectedBy: readonly Dialect[];
}

const BANNED_PATTERNS: readonly BannedPattern[] = [
	{
		helper: "t.id()",
		regex: /\bt\.id\s*\(/,
		reason:
			"emits Postgres-only `DEFAULT gen_random_uuid()` — invalid SQLite + MySQL syntax. Use `t.uuid('id').primary()` and supply the UUID at INSERT time.",
		apply: (table) => {
			table.id();
		},
		fragment: "gen_random_uuid()",
		rejectedBy: ["sqlite", "mysql"],
	},
];

function ddl(apply: (table: TableBuilder) => void, dialect: Dialect): string {
	const table = new TableBuilder("probe");
	apply(table);
	return table.toStatements(dialect).join("\n");
}

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
						`${path.relative(PACKAGES, templatePath)} uses banned helper \`${banned.helper}\` — ${banned.reason}`,
					).toBeNull();
				});
			}
		});
	}

	for (const banned of BANNED_PATTERNS) {
		it(`${banned.helper} still emits ${banned.fragment} on ${banned.rejectedBy.join(" and ")} (the ban's reason)`, () => {
			for (const dialect of banned.rejectedBy) {
				expect(ddl(banned.apply, dialect)).toContain(banned.fragment);
			}
		});
	}

	it("t.timestamps() is allowed: it emits CURRENT_TIMESTAMP, never NOW(), on every dialect", () => {
		for (const dialect of DIALECTS) {
			const sql = ddl((table) => {
				table.timestamps();
			}, dialect);
			expect(sql).toContain("CURRENT_TIMESTAMP");
			expect(sql).not.toMatch(/\bNOW\s*\(/i);
		}
	});

	it("covers every shipped template (file existence check — fails closed)", async () => {
		for (const templatePath of TEMPLATE_PATHS) {
			await expect(readFile(templatePath, "utf8")).resolves.toBeTypeOf(
				"string",
			);
		}
	});
});
