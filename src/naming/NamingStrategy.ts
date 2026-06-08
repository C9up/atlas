/**
 * Naming Strategy — override the snake_case ↔ camelCase convention per-entity.
 *
 * Each entity class can declare `static namingStrategy = new MyStrategy()` to
 * replace the default camelCase ↔ snake_case conversion with its own rules.
 * Used to migrate legacy databases (e.g. PascalCase columns, prefixed tables,
 * custom pivot table names).
 *
 *     class LegacyUser extends BaseEntity {
 *       static namingStrategy = new SnakeCaseSingularStrategy()
 *     }
 *
 * @implements Story 32.7
 */

import { camelToSnake, snakeToCamel } from "../utils/casing.js";

/** Constructor-level hook for a naming override. Defaults to camelCase ↔ snake_case. */
export interface NamingStrategy {
	/** Table name for an entity class, given its constructor name. */
	tableName(className: string): string;
	/** Database column name for a TS property (e.g. `userId` → `user_id`). */
	columnName(propertyName: string): string;
	/** Reverse mapping — DB column back to the TS property. Used by hydrate. */
	propertyName(columnName: string): string;
	/** Serialized field name in `toJSON()`. Defaults to the property name. */
	serializedName(propertyName: string): string;
	/** Local key for a belongsTo/hasMany relation (usually the parent PK). */
	relationLocalKey(
		kind: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
		parentPk: string,
	): string;
	/** Foreign key column name on the owning side of a relation. */
	relationForeignKey(
		kind: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
		parentClass: string,
		parentPk: string,
	): string;
	/** Default pivot table name for a manyToMany relation. */
	relationPivotTable(aClass: string, bClass: string): string;
}

/**
 * Default strategy — camelCase TS properties, snake_case DB columns, plural
 * snake_case table names, `<parent_name>_<parent_pk>` foreign keys.
 */
export class CamelCaseNamingStrategy implements NamingStrategy {
	tableName(className: string): string {
		// Default: snake_case + plural-s. Entities wanting non-default (e.g. irregular
		// plurals like "people") should override via static `namingStrategy`.
		const snake = camelToSnake(className);
		return snake.endsWith("s") ? snake : `${snake}s`;
	}

	columnName(propertyName: string): string {
		return camelToSnake(propertyName);
	}

	propertyName(columnName: string): string {
		return snakeToCamel(columnName);
	}

	serializedName(propertyName: string): string {
		return propertyName;
	}

	relationLocalKey(
		_kind: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
		parentPk: string,
	): string {
		return parentPk;
	}

	relationForeignKey(
		_kind: "belongsTo" | "hasMany" | "hasOne" | "manyToMany",
		parentClass: string,
		parentPk: string,
	): string {
		return `${camelToSnake(parentClass)}_${parentPk}`;
	}

	relationPivotTable(aClass: string, bClass: string): string {
		// Sort alphabetically so `UserSkill` and `SkillUser` collapse to the same name.
		const [x, y] = [camelToSnake(aClass), camelToSnake(bClass)].sort();
		return `${x}_${y}`;
	}
}

/** The default singleton — used when an entity doesn't override `static namingStrategy`. */
export const defaultNamingStrategy: NamingStrategy =
	new CamelCaseNamingStrategy();

/**
 * Resolve the naming strategy for an entity class. Walks the prototype chain
 * so subclasses inherit their parent's strategy unless they override.
 */
export function getNamingStrategy(entityClass: object): NamingStrategy {
	let current: object | null = entityClass;
	while (current && current !== Function.prototype) {
		const explicit = (current as { namingStrategy?: NamingStrategy })
			.namingStrategy;
		if (explicit) return explicit;
		current = Object.getPrototypeOf(current);
	}
	return defaultNamingStrategy;
}
