/**
 * `@c9up/atlas/types/relations` — Adonis Lucid `@adonisjs/lucid/types/relations`
 * parity subpath.
 *
 * Lucid declares relation properties as `declare posts: HasMany<typeof Post>`.
 * Atlas declares them as the plain related shape (`declare posts: Post[]`), so
 * these helpers simply RESOLVE to that shape — a Lucid-style declaration
 * type-checks against atlas's decorators and hydration unchanged:
 *
 *     import type { HasMany, BelongsTo } from '@c9up/atlas/types/relations'
 *
 *     class Post extends BaseModel {
 *       @belongsTo(() => User) declare author: BelongsTo<typeof User>
 *     }
 *     class User extends BaseModel {
 *       @hasMany(() => Post) declare posts: HasMany<typeof Post>
 *     }
 */

/** A `hasOne` related row, or `null` when absent (resolves to `InstanceType<Related>`). */
export type HasOne<Related extends new (...args: never[]) => unknown> =
	InstanceType<Related> | null;

/** A `belongsTo` owner row, or `null` when absent. */
export type BelongsTo<Related extends new (...args: never[]) => unknown> =
	InstanceType<Related> | null;

/** A `hasMany` collection. */
export type HasMany<Related extends new (...args: never[]) => unknown> = Array<
	InstanceType<Related>
>;

/** A `manyToMany` collection. */
export type ManyToMany<Related extends new (...args: never[]) => unknown> =
	Array<InstanceType<Related>>;

/** A `hasOneThrough` related row, or `null` when absent. */
export type HasOneThrough<Related extends new (...args: never[]) => unknown> =
	InstanceType<Related> | null;

/** A `hasManyThrough` collection. */
export type HasManyThrough<Related extends new (...args: never[]) => unknown> =
	Array<InstanceType<Related>>;
