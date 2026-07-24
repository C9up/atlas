/**
 * @module @c9up/atlas
 * @description Atlas — Data Mapper ORM for the Ream framework
 * @implements FR29, FR30, FR31, FR34, FR35, FR37
 */

import "reflect-metadata";

export { SQLITE_PROD_PRAGMAS } from "./AtlasProvider.js";
export type {
	AsyncDatabaseConnection,
	ConnectRetryOptions,
	ObservabilityOptions,
	QueryMeta,
} from "./adapters/NapiDbAdapter.js";
export { createNapiConnection } from "./adapters/NapiDbAdapter.js";
export type { DomainEvent } from "./BaseEntity.js";
export { BaseEntity } from "./BaseEntity.js";
export { BaseModel } from "./BaseModel.js";
export type { DatabaseConnection } from "./BaseRepository.js";
export { BaseRepository } from "./BaseRepository.js";
export { defineConfig } from "./config.js";
export { configure } from "./configure.js";
export {
	type FactoryCommandOptions,
	makeFactoryCommand,
} from "./console/factoryCommands.js";
export {
	dbWipeCommand,
	type MigrationCommandOptions,
	makeMigrationCommand,
	migrationFreshCommand,
	migrationRefreshCommand,
	migrationResetCommand,
	migrationRollbackCommand,
	migrationRunCommand,
	migrationStatusCommand,
	migrationUnlockCommand,
} from "./console/migrationCommands.js";
export {
	type AtlasCommand,
	schemaCheckCommand,
} from "./console/schemaCheckCommand.js";
export {
	type SchemaDumpCommandOptions,
	schemaDumpCommand,
} from "./console/schemaDumpCommand.js";
export {
	generateSchemaFile,
	renderSchemaFile,
	type SchemaGenerateOptions,
	schemaGenerateCommand,
} from "./console/schemaGenerateCommand.js";
export {
	dbSeedCommand,
	makeSeederCommand,
	type SeederCommandOptions,
} from "./console/seederCommands.js";
export type {
	ColumnAdapter,
	ColumnMetadata,
	ColumnOptions,
	DateColumnConfig,
	DateTimeColumnOptions,
	EntityMetadata,
	ManyToManyOptions,
	RelationMetadata,
} from "./decorators/entity.js";
export {
	BelongsTo,
	belongsTo,
	Column,
	column,
	computed,
	Entity,
	getColumnMetadata,
	getDateColumnConfig,
	getEntityMetadata,
	getPrimaryKey,
	getRelationMetadata,
	HasMany,
	HasManyThrough,
	HasOne,
	HasOneThrough,
	hasMany,
	hasManyThrough,
	hasOne,
	hasOneThrough,
	hasSoftDeletes,
	ManyToMany,
	manyToMany,
	PrimaryKey,
	SoftDeletes,
} from "./decorators/entity.js";
export {
	afterCreate,
	afterDelete,
	afterFetch,
	afterFind,
	afterPaginate,
	afterSave,
	afterUpdate,
	beforeCreate,
	beforeDelete,
	beforeFetch,
	beforeFind,
	beforePaginate,
	beforeSave,
	beforeUpdate,
} from "./decorators/hooks.js";
export type { ScopeFn } from "./decorators/scope.js";
export { scope } from "./decorators/scope.js";
export {
	AtlasError,
	EntityNotFoundError,
	MassAssignmentError,
	OptimisticLockError,
	RelationNotLoadedError,
} from "./errors.js";
export type { DbQueryEvent, DbQueryListener } from "./events.js";
export {
	clearDbQueryListeners,
	onDbQuery,
	prettyPrintQuery,
} from "./events.js";
export {
	isAtlasStrictMode,
	ModelQuery,
	setAtlasStrictMode,
} from "./ModelQuery.js";
export type { NamingStrategy } from "./naming/NamingStrategy.js";
export {
	CamelCaseNamingStrategy,
	defaultNamingStrategy,
	getNamingStrategy,
} from "./naming/NamingStrategy.js";
export type { AtlasDialect } from "./query/native.js";
export { getAtlasDialect, setAtlasDialect } from "./query/native.js";
export type {
	CteDefinition,
	ExistsClause,
	OrderByClause,
	QueryResult,
	WhereClause,
	WhereOperator,
} from "./query/QueryBuilder.js";
export { QueryBuilder, RawSql } from "./query/QueryBuilder.js";
export {
	type IntrospectedColumn,
	introspectTable,
} from "./schema/introspect.js";
// `BaseSchema` is Lucid's name for the migration base class; `Migration` is the
// atlas alias kept for back-compat. Prefer `extends BaseSchema` (AdonisJS parity).
export { Migration, Migration as BaseSchema } from "./schema/Migration.js";
export type {
	DatabaseAdapter,
	MigrationRecord,
	MigrationState,
	MigrationStatus,
} from "./schema/MigrationRunner.js";
export { MigrationRunner } from "./schema/MigrationRunner.js";
export type { DefaultValue } from "./schema/raw.js";
export type { ColumnDefinition, ColumnType } from "./schema/SchemaBuilder.js";
export { Schema, TableBuilder } from "./schema/SchemaBuilder.js";
export {
	checkSchema,
	formatSchemaFindings,
	runSchemaCheck,
	type SchemaFinding,
	type SchemaFindingKind,
	suggestColumn,
	typesCompatible,
	verifySchema,
} from "./schema/SchemaCheck.js";
export {
	type PgForeignKey,
	readSchemaDumpManifest,
	renderPgCreateTable,
	renderPgForeignKeyDdl,
	SchemaDumper,
	type SchemaDumperOptions,
	type SchemaDumpManifest,
	type SchemaDumpResult,
	schemaDumpManifestPath,
} from "./schema/SchemaDumper.js";
export {
	BaseSeeder,
	runSeederDirectory,
	runSeeders,
	Seeder,
} from "./schema/Seeder.js";
export type { TransactionClient } from "./Transaction.js";
export { transaction } from "./Transaction.js";
export { truncateAll, useTransaction } from "./testing/DatabaseCleanup.js";
export { Factory, factory } from "./testing/Factory.js";
