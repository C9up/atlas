/**
 * @module @c9up/atlas
 * @description Atlas — Data Mapper ORM for the Ream framework
 * @implements FR29, FR30, FR31, FR34, FR35, FR37
 */

import "reflect-metadata";

export { SQLITE_PROD_PRAGMAS } from "./AtlasProvider.js";
export type { AsyncDatabaseConnection } from "./adapters/NapiDbAdapter.js";
export { createNapiConnection } from "./adapters/NapiDbAdapter.js";
export type { DomainEvent } from "./BaseEntity.js";
export { BaseEntity } from "./BaseEntity.js";
export type { DatabaseConnection } from "./BaseRepository.js";
export { BaseRepository } from "./BaseRepository.js";
export { defineConfig } from "./config.js";
export { configure } from "./configure.js";
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
	hasSoftDeletes,
	ManyToMany,
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
export { Migration } from "./schema/Migration.js";
export type {
	DatabaseAdapter,
	MigrationRecord,
	MigrationState,
	MigrationStatus,
} from "./schema/MigrationRunner.js";
export { MigrationRunner } from "./schema/MigrationRunner.js";
export type { ColumnDefinition, ColumnType } from "./schema/SchemaBuilder.js";
export { Schema, TableBuilder } from "./schema/SchemaBuilder.js";
export {
	BaseSeeder,
	runSeederDirectory,
	runSeeders,
	Seeder,
} from "./schema/Seeder.js";
export type { TransactionClient } from "./Transaction.js";
export { transaction } from "./Transaction.js";
export { truncateAll, useTransaction } from "./testing/DatabaseCleanup.js";
export { factory } from "./testing/Factory.js";
