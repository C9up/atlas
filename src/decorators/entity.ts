/**
 * Entity decorators — @Entity, @Column, @PrimaryKey, @BelongsTo, @HasMany
 *
 * @implements FR29, FR30
 */

import 'reflect-metadata'

const ENTITY_KEY = Symbol('atlas:entity')
const COLUMNS_KEY = Symbol('atlas:columns')
const PRIMARY_KEY = Symbol('atlas:primary')
const RELATIONS_KEY = Symbol('atlas:relations')

export interface EntityMetadata {
  tableName: string
}

export interface ColumnMetadata {
  propertyKey: string
  type?: string
  nullable?: boolean
  default?: unknown
}

export interface RelationMetadata {
  propertyKey: string
  type: 'belongsTo' | 'hasMany' | 'manyToMany'
  target: () => Function
}

// biome-ignore lint/suspicious/noExplicitAny: Decorator target
type Constructor = new (...args: any[]) => any

/** @Entity('table_name') — marks a class as a database entity. */
export function Entity(tableName: string): ClassDecorator {
  return (target) => {
    Reflect.defineMetadata(ENTITY_KEY, { tableName }, target)
  }
}

/** @Column() — marks a property as a database column. */
export function Column(options?: { type?: string; nullable?: boolean; default?: unknown }): PropertyDecorator {
  return (target, propertyKey) => {
    const columns: ColumnMetadata[] = Reflect.getOwnMetadata(COLUMNS_KEY, target.constructor) ?? []
    const key = String(propertyKey)
    // Deduplicate — @PrimaryKey also calls Column()
    if (!columns.some((c) => c.propertyKey === key)) {
      columns.push({
        propertyKey: key,
        type: options?.type,
        nullable: options?.nullable,
        default: options?.default,
      })
      Reflect.defineMetadata(COLUMNS_KEY, columns, target.constructor)
    }
  }
}

/** @PrimaryKey() — marks a property as the primary key. */
export function PrimaryKey(): PropertyDecorator {
  return (target, propertyKey) => {
    Reflect.defineMetadata(PRIMARY_KEY, String(propertyKey), target.constructor)
    // Also register as a column
    Column()(target, propertyKey)
  }
}

/** @BelongsTo(() => Related) */
export function BelongsTo(target: () => Function): PropertyDecorator {
  return (proto, propertyKey) => {
    addRelation(proto.constructor, {
      propertyKey: String(propertyKey),
      type: 'belongsTo',
      target,
    })
  }
}

/** @HasMany(() => Related) */
export function HasMany(target: () => Function): PropertyDecorator {
  return (proto, propertyKey) => {
    addRelation(proto.constructor, {
      propertyKey: String(propertyKey),
      type: 'hasMany',
      target,
    })
  }
}

/** @ManyToMany(() => Related) */
export function ManyToMany(target: () => Function): PropertyDecorator {
  return (proto, propertyKey) => {
    addRelation(proto.constructor, {
      propertyKey: String(propertyKey),
      type: 'manyToMany',
      target,
    })
  }
}

function addRelation(constructor: Function, relation: RelationMetadata): void {
  const relations: RelationMetadata[] = Reflect.getOwnMetadata(RELATIONS_KEY, constructor) ?? []
  relations.push(relation)
  Reflect.defineMetadata(RELATIONS_KEY, relations, constructor)
}

/** Get entity metadata for a class. */
export function getEntityMetadata(target: Constructor): EntityMetadata | undefined {
  return Reflect.getMetadata(ENTITY_KEY, target)
}

/** Get column metadata for a class (returns a copy). */
export function getColumnMetadata(target: Constructor): ColumnMetadata[] {
  return [...(Reflect.getMetadata(COLUMNS_KEY, target) ?? [])]
}

/** Get primary key property name. */
export function getPrimaryKey(target: Constructor): string | undefined {
  return Reflect.getMetadata(PRIMARY_KEY, target)
}

/** Get relation metadata for a class (returns a copy). */
export function getRelationMetadata(target: Constructor): RelationMetadata[] {
  return [...(Reflect.getMetadata(RELATIONS_KEY, target) ?? [])]
}
