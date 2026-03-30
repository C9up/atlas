/**
 * @module @c9up/atlas
 * @description Atlas — Data Mapper ORM for the Ream framework
 * @implements FR29, FR30, FR31, FR34, FR35, FR37
 */

import 'reflect-metadata'

export { BaseEntity } from './BaseEntity.js'
export type { DomainEvent } from './BaseEntity.js'
export { BaseRepository } from './BaseRepository.js'
export {
  BelongsTo,
  Column,
  Entity,
  HasMany,
  ManyToMany,
  PrimaryKey,
  getColumnMetadata,
  getEntityMetadata,
  getPrimaryKey,
  getRelationMetadata,
} from './decorators/entity.js'
export type { ColumnMetadata, EntityMetadata, RelationMetadata } from './decorators/entity.js'
export { QueryBuilder, RawSql } from './query/QueryBuilder.js'
export type { CteDefinition, ExistsClause, OrderByClause, QueryResult, WhereClause, WhereOperator } from './query/QueryBuilder.js'
