/**
 * Schema dumps (Adonis Lucid `schema:dump` / `SchemaDumper`). Serialise the live
 * database's structure — every table's DDL plus the migration bookkeeping rows —
 * into a `.sql` file and a `.meta.json` manifest, so a fresh database can be
 * rebuilt from the dump instead of replaying the whole migration history.
 *
 * The migration runner consumes this via `migration:run --schema-path` /
 * `migration:fresh --schema-path`: when a database has no applied migrations and
 * a dump exists, it loads the dump, then runs only the migrations that postdate
 * it (see {@link MigrationRunner.loadDump}).
 *
 *     const dumper = new SchemaDumper(db, { migrationsDir: 'database/migrations' })
 *     await dumper.run()
 *     if (dumper.error) throw dumper.error
 *     console.log(dumper.result?.dumpPath, dumper.result?.metaPath)
 */

import * as fsp from "node:fs/promises";
import * as path from "node:path";
import type { AsyncDatabaseConnection } from "../adapters/NapiDbAdapter.js";
import type { AtlasDialect } from "../query/native.js";
import { listUserTables } from "./catalog.js";
import { introspectTable } from "./introspect.js";

/** Current time as an ISO string (extracted so tests can stub it). */
function nowIso(): string {
	return new Date().toISOString();
}

/** The manifest path for a dump `.sql` path (extension swapped for `.meta.json`). */
export function schemaDumpManifestPath(dumpPath: string): string {
	return `${dumpPath.replace(/\.sql$/, "")}.meta.json`;
}

/** Structural guard for a manifest — no cast, `in`-narrowed field checks. */
function isSchemaDumpManifest(v: unknown): v is SchemaDumpManifest {
	return (
		typeof v === "object" &&
		v !== null &&
		"version" in v &&
		v.version === 1 &&
		"schemaTableName" in v &&
		typeof v.schemaTableName === "string"
	);
}

/**
 * Read + validate a dump's manifest, or return `undefined` when it is missing.
 * Throws on a present-but-malformed manifest (wrong `version`, missing fields)
 * so a corrupt dump is caught rather than silently mis-loaded.
 */
export async function readSchemaDumpManifest(
	dumpPath: string,
): Promise<SchemaDumpManifest | undefined> {
	const metaPath = schemaDumpManifestPath(dumpPath);
	let raw: string;
	try {
		raw = await fsp.readFile(metaPath, "utf8");
	} catch {
		return undefined;
	}
	const parsed: unknown = JSON.parse(raw);
	if (!isSchemaDumpManifest(parsed)) {
		throw new Error(
			`Invalid schema-dump manifest at ${metaPath}: expected version 1 with a schemaTableName.`,
		);
	}
	return parsed;
}

/** The manifest sidecar written next to the SQL dump (Adonis Lucid `.meta.json`). */
export interface SchemaDumpManifest {
	version: 1;
	connection: string;
	dialect: AtlasDialect;
	/** Path to the SQL dump this manifest describes (Adonis Lucid `dumpPath`). */
	dumpPath: string;
	generatedAt: string;
	schemaTableName: string;
	/**
	 * Lucid tracks migration file versions in a second table; atlas's single
	 * `ream_migrations` (name+batch) has no equivalent, so this is always `null`.
	 * Kept for manifest-shape parity with Lucid.
	 */
	schemaVersionsTableName: string | null;
	/**
	 * Migration files collapsed into this dump (only when `--prune` deleted them).
	 * Lets the runner tell a deliberately-squashed migration from a missing file.
	 */
	squashedMigrationNames: string[];
}

export interface SchemaDumperOptions {
	/** Logical connection name — used in the default dump/manifest file names. Default `"default"`. */
	connectionName?: string;
	/**
	 * Explicit SQL dump FILE path (Adonis Lucid `--path`). Wins over
	 * `outputDir`/`connectionName`; the manifest is written beside it as
	 * `<name>.meta.json`.
	 */
	dumpPath?: string;
	/** Directory for the default `{connection}-schema.sql` when `dumpPath` is unset. Default `"database/schema"`. */
	outputDir?: string;
	/** Migration directory, required for `--prune` (the files it deletes). */
	migrationsDir?: string;
	/** Migration bookkeeping table name. Default `"ream_migrations"`. */
	schemaTableName?: string;
	/**
	 * Collapse the migration history into this dump: after a successful dump,
	 * delete every file in `migrationsDir` and record their names in the manifest.
	 */
	prune?: boolean;
	/** Manifest timestamp; defaults to the current time. Pass one for deterministic tests. */
	generatedAt?: string;
}

export interface SchemaDumpResult {
	dumpPath: string;
	metaPath: string;
	tableCount: number;
}

/**
 * Dumps a database's schema to `{outputDir}/{connection}-schema.sql` and a
 * `.meta.json` manifest. Errors are captured on `.error` (Lucid parity) rather
 * than thrown, so a CLI can report cleanly; `.result` holds the output paths on
 * success.
 */
export class SchemaDumper {
	readonly #db: AsyncDatabaseConnection;
	readonly #options: Required<
		Omit<SchemaDumperOptions, "generatedAt" | "dumpPath">
	> & { generatedAt?: string; dumpPath?: string };
	#result?: SchemaDumpResult;
	#error?: Error;

	constructor(db: AsyncDatabaseConnection, options: SchemaDumperOptions = {}) {
		this.#db = db;
		this.#options = {
			connectionName: options.connectionName ?? "default",
			dumpPath: options.dumpPath,
			outputDir: options.outputDir ?? "database/schema",
			migrationsDir: options.migrationsDir ?? "",
			schemaTableName: options.schemaTableName ?? "ream_migrations",
			prune: options.prune ?? false,
			generatedAt: options.generatedAt,
		};
	}

	get result(): SchemaDumpResult | undefined {
		return this.#result;
	}

	get error(): Error | undefined {
		return this.#error;
	}

	/** Resolved SQL dump file path — the explicit `dumpPath` or the default per-connection name. */
	get dumpPath(): string {
		return (
			this.#options.dumpPath ??
			path.join(
				this.#options.outputDir,
				`${this.#options.connectionName}-schema.sql`,
			)
		);
	}

	/** Manifest path — the dump path with its extension swapped for `.meta.json`. */
	get metaPath(): string {
		return this.dumpPath.replace(/\.sql$/, "") + ".meta.json";
	}

	async run(): Promise<void> {
		try {
			const sql = await this.#buildDump();
			const dumpPath = this.dumpPath;
			const metaPath = this.metaPath;
			await fsp.mkdir(path.dirname(dumpPath), { recursive: true });
			await fsp.writeFile(dumpPath, sql);

			const squashed = this.#options.prune ? await this.#prune() : [];
			const manifest: SchemaDumpManifest = {
				version: 1,
				connection: this.#options.connectionName,
				dialect: this.#db.dialect,
				dumpPath,
				generatedAt: this.#options.generatedAt ?? nowIso(),
				schemaTableName: this.#options.schemaTableName,
				schemaVersionsTableName: null,
				squashedMigrationNames: squashed,
			};
			await fsp.writeFile(metaPath, `${JSON.stringify(manifest, null, 2)}\n`);
			this.#result = {
				dumpPath,
				metaPath,
				tableCount: (sql.match(/CREATE TABLE/gi) ?? []).length,
			};
		} catch (err) {
			this.#error = err instanceof Error ? err : new Error(String(err));
		}
	}

	/** Serialise DDL for every table (incl. the migrations table) + its bookkeeping rows. */
	async #buildDump(): Promise<string> {
		const dialect = this.#db.dialect;
		const header =
			"-- Generated by @c9up/atlas schema:dump — DO NOT EDIT.\n" +
			`-- connection=${this.#options.connectionName} dialect=${dialect}\n\n`;
		const ddl = await this.#dumpDdl(dialect);
		const data = await this.#dumpMigrationRows();
		return header + ddl + data;
	}

	/** Dialect-specific DDL for all tables (framework tables included, for the dump). */
	async #dumpDdl(dialect: AtlasDialect): Promise<string> {
		// The migration LOCK table is transient infrastructure, never schema — its
		// presence in the dump would collide with the runner re-creating it.
		const lockTable = `${this.#options.schemaTableName}_lock`;

		if (dialect === "sqlite") {
			// sqlite_master carries the exact CREATE statements (tables + indexes).
			const rows = await this.#db.query<{
				name: string;
				sql: string | null;
			}>(
				"SELECT name, sql FROM sqlite_master " +
					"WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' " +
					"ORDER BY (type = 'table') DESC, name",
			);
			return `${rows
				.filter((r) => r.sql && r.name !== lockTable)
				.map((r) => `${r.sql};`)
				.join("\n\n")}\n\n`;
		}

		const tables = (
			await listUserTables(this.#db, dialect, {
				includeFrameworkTables: true,
			})
		).filter((t) => t !== lockTable);
		if (dialect === "mysql") {
			// SHOW CREATE TABLE gives the exact DDL, indexes and constraints included.
			const out: string[] = [];
			for (const table of tables.sort()) {
				const rows = await this.#db.query<Record<string, string>>(
					`SHOW CREATE TABLE \`${table}\``,
				);
				const create = rows[0]?.["Create Table"];
				if (create) out.push(`${create};`);
			}
			return `${out.join("\n\n")}\n\n`;
		}

		// postgres: no single "show create table" — reconstruct CREATE TABLE from
		// the introspected columns + primary key. Secondary indexes / non-PK
		// constraints are NOT captured (documented limitation for the pg dump).
		const out: string[] = [];
		for (const table of tables.sort()) {
			const columns = await introspectTable(this.#db, dialect, table);
			if (columns) out.push(this.#reconstructCreateTable(table, columns));
		}
		return `${out.join("\n\n")}\n\n`;
	}

	/** Best-effort `CREATE TABLE` from introspected columns (postgres dump path). */
	#reconstructCreateTable(
		table: string,
		columns: Array<{
			name: string;
			type: string;
			nullable: boolean;
			primaryKey: boolean;
		}>,
	): string {
		const q = (id: string) => `"${id}"`;
		const cols = columns.map((c) => {
			const notNull = c.nullable ? "" : " NOT NULL";
			return `  ${q(c.name)} ${c.type}${notNull}`;
		});
		const pks = columns.filter((c) => c.primaryKey).map((c) => q(c.name));
		if (pks.length > 0) cols.push(`  PRIMARY KEY (${pks.join(", ")})`);
		return `CREATE TABLE ${q(table)} (\n${cols.join(",\n")}\n);`;
	}

	/** `INSERT` the applied-migration rows so the runner sees them as applied. */
	async #dumpMigrationRows(): Promise<string> {
		const table = this.#options.schemaTableName;
		let rows: Array<{ name: string; batch: number }>;
		try {
			rows = await this.#db.query<{ name: string; batch: number }>(
				`SELECT name, batch FROM ${this.#quote(table)} ORDER BY id`,
			);
		} catch {
			// No bookkeeping table yet (nothing migrated) — nothing to record.
			return "";
		}
		if (rows.length === 0) return "";
		const values = rows
			.map((r) => `(${this.#literal(r.name)}, ${Number(r.batch)})`)
			.join(",\n  ");
		return `INSERT INTO ${this.#quote(table)} (name, batch) VALUES\n  ${values};\n`;
	}

	/** Delete every migration file and return the pruned names (`--prune`). */
	async #prune(): Promise<string[]> {
		if (!this.#options.migrationsDir) {
			throw new Error("schema:dump --prune requires a migrationsDir");
		}
		const entries = (await fsp.readdir(this.#options.migrationsDir))
			.filter(
				(f) => (f.endsWith(".ts") || f.endsWith(".js")) && !f.endsWith(".d.ts"),
			)
			.sort();
		for (const file of entries) {
			await fsp.rm(path.join(this.#options.migrationsDir, file));
		}
		return entries.map((f) => f.replace(/\.(ts|js)$/, ""));
	}

	#quote(id: string): string {
		return this.#db.dialect === "mysql" ? `\`${id}\`` : `"${id}"`;
	}

	/** Single-quote a string literal for the dump (doubling embedded quotes). */
	#literal(value: string): string {
		return `'${value.replace(/'/g, "''")}'`;
	}
}
