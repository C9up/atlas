/**
 * BaseRepository — Data Mapper pattern base.
 *
 * @implements FR29, FR31, FR35
 */

import type { BaseEntity, DomainEvent } from './BaseEntity.js'
import { getEntityMetadata, getPrimaryKey } from './decorators/entity.js'
import { QueryBuilder } from './query/QueryBuilder.js'

// biome-ignore lint/suspicious/noExplicitAny: Repository works with any entity
type EntityConstructor<T extends BaseEntity> = new (...args: any[]) => T

/**
 * Base repository — provides CRUD operations and query builder access.
 *
 * Domain events are flushed after save() — caller can dispatch them to Pulsar.
 */
export class BaseRepository<T extends BaseEntity> {
  protected entityClass: EntityConstructor<T>
  protected tableName: string
  protected primaryKey: string

  /** Callback to dispatch domain events (set by framework integration). */
  onDomainEvents?: (events: DomainEvent[]) => Promise<void>

  constructor(entityClass: EntityConstructor<T>) {
    this.entityClass = entityClass
    const meta = getEntityMetadata(entityClass)
    if (!meta) {
      throw new Error(`[ATLAS_NOT_ENTITY] Class '${entityClass.name}' is not decorated with @Entity()`)
    }
    this.tableName = meta.tableName
    this.primaryKey = getPrimaryKey(entityClass) ?? 'id'
  }

  /** Create a query builder for this entity's table. */
  query(): QueryBuilder<T> {
    return new QueryBuilder<T>(this.tableName)
  }

  /**
   * Save an entity — dispatches domain events after save.
   * In a real implementation, this would execute an INSERT or UPDATE.
   * For now, it flushes domain events for Pulsar dispatch.
   */
  async save(entity: T): Promise<void> {
    // TODO: Execute actual INSERT/UPDATE via DB driver

    // Dispatch domain events after "commit" — clear only on success
    const events = entity.getDomainEvents()
    if (events.length > 0 && this.onDomainEvents) {
      await this.onDomainEvents([...events])
      entity.clearDomainEvents()
    } else if (events.length > 0) {
      entity.clearDomainEvents()
    }
  }

  /**
   * Delete an entity.
   * In a real implementation, this would execute a DELETE.
   */
  async delete(_entity: T): Promise<void> {
    // TODO: Execute actual DELETE via DB driver
  }

  /** Get the table name. */
  getTableName(): string {
    return this.tableName
  }

  /** Get the primary key column. */
  getPrimaryKeyColumn(): string {
    return this.primaryKey
  }
}
