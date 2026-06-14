/**
 * Atlas error hierarchy — structured errors for the ORM layer.
 *
 * Every error extends {@link AtlasError} with a stable code (prefixed `ATLAS_`),
 * a human-readable message, and an optional hint. Specialised subclasses allow
 * callers to catch by type (`catch (e) { if (e instanceof OptimisticLockError) ... }`)
 * without string-matching on codes.
 *
 * @implements Story 32.10
 */

export class AtlasError extends Error {
	readonly code: string;
	readonly hint?: string;

	constructor(code: string, message: string, options?: { hint?: string }) {
		super(message);
		this.name = "AtlasError";
		this.code = code.startsWith("ATLAS_") ? code : `ATLAS_${code}`;
		this.hint = options?.hint;
	}
}

/**
 * Thrown when an entity cannot be found (find, findOrFail, firstOrFail).
 */
export class EntityNotFoundError extends AtlasError {
	readonly entityClass: string;
	readonly criteria: unknown;

	constructor(entityClass: string, criteria: unknown, hint?: string) {
		super(
			"E_ENTITY_NOT_FOUND",
			`${entityClass} not found with ${JSON.stringify(criteria)}`,
			{ hint },
		);
		this.name = "EntityNotFoundError";
		this.entityClass = entityClass;
		this.criteria = criteria;
	}
}

/**
 * Thrown when an optimistic lock check fails on save — the row was modified
 * by another transaction since we read it. Requires `@Version()` column on the entity.
 */
export class OptimisticLockError extends AtlasError {
	readonly entityClass: string;
	readonly primaryKey: unknown;
	readonly expectedVersion: number;

	constructor(
		entityClass: string,
		primaryKey: unknown,
		expectedVersion: number,
	) {
		super(
			"E_OPTIMISTIC_LOCK",
			`Optimistic lock failure saving ${entityClass}#${String(primaryKey)}: the row was modified by another transaction (expected version ${expectedVersion}).`,
			{ hint: "Reload the entity, reapply your changes, and try again." },
		);
		this.name = "OptimisticLockError";
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
		this.expectedVersion = expectedVersion;
	}
}

/**
 * Thrown when accessing a relation that was not eager-loaded AND has no lazy
 * loader available on the entity. Forces callers to be explicit about loading.
 */
export class RelationNotLoadedError extends AtlasError {
	readonly entityClass: string;
	readonly relationName: string;

	constructor(entityClass: string, relationName: string) {
		super(
			"E_RELATION_NOT_LOADED",
			`Relation '${relationName}' on ${entityClass} was not loaded.`,
			{
				hint: `Call .preload('${relationName}') on the query or .load('${relationName}') on the instance.`,
			},
		);
		this.name = "RelationNotLoadedError";
		this.entityClass = entityClass;
		this.relationName = relationName;
	}
}

/**
 * Thrown when mass-assignment (`fill` / `merge` / `create`) tries to set a
 * field that is not in the `fillable` list or is in the `guarded` list.
 */
export class MassAssignmentError extends AtlasError {
	readonly entityClass: string;
	readonly attribute: string;

	constructor(entityClass: string, attribute: string) {
		super(
			"E_MASS_ASSIGNMENT",
			`Attribute '${attribute}' on ${entityClass} is not mass-assignable.`,
			{
				hint: `Add '${attribute}' to the static 'fillable' array or remove it from 'guarded'.`,
			},
		);
		this.name = "MassAssignmentError";
		this.entityClass = entityClass;
		this.attribute = attribute;
	}
}
