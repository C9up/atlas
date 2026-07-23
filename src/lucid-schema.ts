/**
 * `@c9up/atlas/schema` — Adonis Lucid `@adonisjs/lucid/schema` parity subpath.
 *
 * The migration base class ({@link Migration}, Lucid's `BaseSchema`), the schema
 * builder and the table builder — everything for authoring migrations. (The file
 * is named `lucid-schema` to avoid colliding with the `schema/` source folder;
 * the public subpath is `@c9up/atlas/schema`.)
 */
export { RawSql } from "./query/QueryBuilder.js";
export * from "./schema/Migration.js";
export * from "./schema/Schema.js";
export {
	readSchemaDumpManifest,
	SchemaDumper,
	type SchemaDumperOptions,
	type SchemaDumpManifest,
	type SchemaDumpResult,
	schemaDumpManifestPath,
} from "./schema/SchemaDumper.js";
export * from "./schema/TableBuilder.js";
