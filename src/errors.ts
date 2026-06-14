/**
 * Atlas error hierarchy — structured errors for the ORM layer.
 *
 * Every error extends {@link AtlasError} with a stable code (prefixed `ATLAS_`),
 * a human-readable message, and an optional hint. Specialised subclasses allow
 * callers to catch by type (`catch (e) { if (e instanceof EntityNotFoundError) ... }`)
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
