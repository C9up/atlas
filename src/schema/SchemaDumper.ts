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

/** A foreign-key constraint introspected from Postgres, for the dump. */
export interface PgForeignKey {
	name: string;
	columns: string[];
	foreignTable: string;
	foreignColumns: string[];
	onUpdate?: string;
	onDelete?: string;
}

/** A column shape the pure Postgres renderers accept (subset of IntrospectedColumn). */
interface DumpColumn {
	name: string;
	type: string;
	nullable: boolean;
	primaryKey: boolean;
}

const pgQuote = (id: string): string => `"${id}"`;

/** Map a `pg_constraint.confupdtype`/`confdeltype` char to its SQL action. */
function pgActionCode(code: string | null): string | undefined {
	switch (code) {
		case "c":
			return "CASCADE";
		case "n":
			return "SET NULL";
		case "d":
			return "SET DEFAULT";
		case "r":
			return "RESTRICT";
		default:
			return undefined; // 'a' NO ACTION (default) — omit
	}
}

/**
 * Render `CREATE TABLE` from introspected columns + primary key (Postgres dump).
 * Pure — no DB access — so the assembly is unit-testable with fixture columns.
 */
export function renderPgCreateTable(
	table: string,
	columns: DumpColumn[],
): string {
	const cols = columns.map((c) => {
		const notNull = c.nullable ? "" : " NOT NULL";
		return `  ${pgQuote(c.name)} ${c.type}${notNull}`;
	});
	const pks = columns.filter((c) => c.primaryKey).map((c) => pgQuote(c.name));
	if (pks.length > 0) cols.push(`  PRIMARY KEY (${pks.join(", ")})`);
	return `CREATE TABLE ${pgQuote(table)} (\n${cols.join(",\n")}\n);`;
}

/**
 * Render an `ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` from an introspected
 * {@link PgForeignKey} (Postgres dump). Pure — unit-testable. `ON UPDATE` /
 * `ON DELETE` are emitted only when the rule is not the default `NO ACTION`.
 */
export function renderPgForeignKeyDdl(table: string, fk: PgForeignKey): string {
	const cols = fk.columns.map(pgQuote).join(", ");
	const fcols = fk.foreignColumns.map(pgQuote).join(", ");
	const nonDefault = (rule?: string) =>
		rule && rule.toUpperCase() !== "NO ACTION" ? rule.toUpperCase() : undefined;
	const onUpdate = nonDefault(fk.onUpdate);
	const onDelete = nonDefault(fk.onDelete);
	return (
		`ALTER TABLE ${pgQuote(table)} ADD CONSTRAINT ${pgQuote(fk.name)} ` +
		`FOREIGN KEY (${cols}) REFERENCES ${pgQuote(fk.foreignTable)} (${fcols})` +
		(onUpdate ? ` ON UPDATE ${onUpdate}` : "") +
		(onDelete ? ` ON DELETE ${onDelete}` : "") +
		";"
	);
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
				let rows: Array<Record<string, string>>;
				try {
					rows = await this.#db.query<Record<string, string>>(
						`SHOW CREATE TABLE \`${table}\``,
					);
				} catch {
					// The table vanished between listing and dumping (concurrent DDL) —
					// skip it, mirroring the postgres path (introspectTable → null).
					continue;
				}
				const create = rows[0]?.["Create Table"];
				if (create) out.push(`${create};`);
			}
			return `${out.join("\n\n")}\n\n`;
		}

		// postgres: reconstruct from catalog introspection (Adonis Lucid introspects
		// the connection — it does NOT shell out to pg_dump). Per table: CREATE
		// TABLE (columns + PK) + foreign keys + CHECK constraints + indexes; then
		// the schema's views. Proven against real PostgreSQL (podman) in
		// schema-dump-pg.test.ts.
		const out: string[] = [];
		for (const table of tables.sort()) {
			const columns = await introspectTable(this.#db, dialect, table);
			if (!columns) continue;
			out.push(renderPgCreateTable(table, columns));
			for (const fk of await this.#introspectPgForeignKeys(table)) {
				out.push(renderPgForeignKeyDdl(table, fk));
			}
			// CHECK constraints — pg_get_constraintdef gives the exact `CHECK (...)`.
			for (const chk of await this.#introspectPgChecks(table)) {
				out.push(
					`ALTER TABLE ${pgQuote(table)} ADD CONSTRAINT ${pgQuote(chk.name)} ${chk.def};`,
				);
			}
			// `indexdef` is the exact CREATE INDEX statement; pass it through.
			for (const indexDef of await this.#introspectPgIndexes(table)) {
				out.push(`${indexDef};`);
			}
		}
		// Views + materialized views (schema-level, not in the base-table list).
		for (const view of await this.#introspectPgViews()) {
			const kind = view.materialized ? "MATERIALIZED VIEW" : "VIEW";
			out.push(
				`CREATE ${kind} ${pgQuote(view.name)} AS\n${view.definition.replace(/;\s*$/, "")};`,
			);
		}
		return `${out.join("\n\n")}\n\n`;
	}

	/**
	 * FK constraints for `table`, grouped per constraint (Postgres). Uses
	 * `pg_constraint` with `unnest(conkey, confkey) WITH ORDINALITY` so the local
	 * and foreign columns of a COMPOSITE FK are paired by position — the
	 * information_schema kcu×ccu join produces a cartesian product and mismaps
	 * multi-column keys.
	 */
	async #introspectPgForeignKeys(table: string): Promise<PgForeignKey[]> {
		const rows = await this.#db.query<{
			constraint_name: string;
			column_name: string;
			foreign_table: string;
			foreign_column: string;
			update_type: string | null;
			delete_type: string | null;
		}>(
			// ::text on every column — pg_catalog uses the `name` and `"char"` types
			// which atlas's strict sqlx decode rejects as Option<String>.
			`SELECT con.conname::text AS constraint_name,
			        att.attname::text AS column_name,
			        fatt.attname::text AS foreign_column,
			        fcl.relname::text AS foreign_table,
			        con.confupdtype::text AS update_type,
			        con.confdeltype::text AS delete_type
			 FROM pg_constraint con
			 JOIN pg_class cl ON cl.oid = con.conrelid
			 JOIN pg_class fcl ON fcl.oid = con.confrelid
			 JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY
			   AS keys(conkey, confkey, ord) ON true
			 JOIN pg_attribute att
			   ON att.attrelid = con.conrelid AND att.attnum = keys.conkey
			 JOIN pg_attribute fatt
			   ON fatt.attrelid = con.confrelid AND fatt.attnum = keys.confkey
			 WHERE con.contype = 'f' AND cl.relname = $1
			   AND cl.relnamespace = current_schema()::regnamespace
			 ORDER BY con.conname, keys.ord`,
			[table],
		);
		const byName = new Map<string, PgForeignKey>();
		for (const r of rows) {
			const fk = byName.get(r.constraint_name) ?? {
				name: r.constraint_name,
				columns: [],
				foreignTable: r.foreign_table,
				foreignColumns: [],
				onUpdate: pgActionCode(r.update_type),
				onDelete: pgActionCode(r.delete_type),
			};
			fk.columns.push(r.column_name);
			fk.foreignColumns.push(r.foreign_column);
			byName.set(r.constraint_name, fk);
		}
		return [...byName.values()];
	}

	/** Non-primary index definitions for `table` (Postgres `pg_indexes.indexdef`). */
	async #introspectPgIndexes(table: string): Promise<string[]> {
		const rows = await this.#db.query<{ indexdef: string }>(
			`SELECT i.indexdef FROM pg_indexes i
			 WHERE i.schemaname = current_schema() AND i.tablename = $1
			   AND NOT EXISTS (
			     SELECT 1 FROM pg_constraint c
			     WHERE c.conname = i.indexname AND c.contype = 'p'
			   )
			 ORDER BY i.indexname`,
			[table],
		);
		return rows.map((r) => r.indexdef).filter((d): d is string => Boolean(d));
	}

	/** CHECK constraints for `table` (Postgres) — `pg_get_constraintdef` is exact. */
	async #introspectPgChecks(
		table: string,
	): Promise<Array<{ name: string; def: string }>> {
		const rows = await this.#db.query<{ name: string; def: string }>(
			`SELECT con.conname::text AS name,
			        pg_get_constraintdef(con.oid)::text AS def
			 FROM pg_constraint con
			 JOIN pg_class cl ON cl.oid = con.conrelid
			 WHERE con.contype = 'c' AND cl.relname = $1
			   AND cl.relnamespace = current_schema()::regnamespace
			 ORDER BY con.conname`,
			[table],
		);
		return rows.filter((r) => r.name && r.def);
	}

	/** Views + materialized views in the current schema (Postgres). */
	async #introspectPgViews(): Promise<
		Array<{ name: string; definition: string; materialized: boolean }>
	> {
		const views = await this.#db.query<{ name: string; definition: string }>(
			`SELECT viewname::text AS name, definition::text AS definition
			 FROM pg_views WHERE schemaname = current_schema() ORDER BY viewname`,
		);
		const matviews = await this.#db.query<{ name: string; definition: string }>(
			`SELECT matviewname::text AS name, definition::text AS definition
			 FROM pg_matviews WHERE schemaname = current_schema() ORDER BY matviewname`,
		);
		return [
			...views.map((v) => ({ ...v, materialized: false })),
			...matviews.map((v) => ({ ...v, materialized: true })),
		].filter((v) => v.name && v.definition);
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
