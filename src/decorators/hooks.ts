/**
 * Lifecycle hook decorators — Lucid-compatible hook surface.
 *
 * Decorate a static method on an entity with one of the hooks below; Atlas
 * fires it at the matching point in the BaseRepository CRUD pipeline. Hooks
 * are inherited from the prototype chain — a hook on a parent entity also
 * fires for subclasses.
 *
 *     class User extends BaseEntity {
 *       @beforeSave()
 *       static async hashPassword(user: User) {
 *         if (user.isDirty('password')) {
 *           user.password = await hash(user.password)
 *         }
 *       }
 *     }
 *
 * @implements Story 32.1
 */

import "reflect-metadata";
import type { BaseEntity } from "../BaseEntity.js";
import type { ModelQuery } from "../ModelQuery.js";

/** All hook kinds Atlas supports — mirrors Lucid's surface. */
export type HookKind =
	| "beforeSave"
	| "afterSave"
	| "beforeCreate"
	| "afterCreate"
	| "beforeUpdate"
	| "afterUpdate"
	| "beforeDelete"
	| "afterDelete"
	| "beforeFind"
	| "afterFind"
	| "beforeFetch"
	| "afterFetch"
	| "beforePaginate"
	| "afterPaginate";

/** Argument shape for each hook kind. */
export interface HookArgs {
	beforeSave: BaseEntity;
	afterSave: BaseEntity;
	beforeCreate: BaseEntity;
	afterCreate: BaseEntity;
	beforeUpdate: BaseEntity;
	afterUpdate: BaseEntity;
	beforeDelete: BaseEntity;
	afterDelete: BaseEntity;
	beforeFind: ModelQuery<BaseEntity>;
	afterFind: BaseEntity | null;
	beforeFetch: ModelQuery<BaseEntity>;
	afterFetch: BaseEntity[];
	beforePaginate: ModelQuery<BaseEntity>;
	afterPaginate: BaseEntity[];
}

/** A hook handler is a static function that receives the kind-specific arg. */
export type HookHandler<K extends HookKind = HookKind> = (
	arg: HookArgs[K],
) => void | Promise<void>;

const HOOKS_KEY = Symbol.for("atlas:hooks");

/** Per-entity-class store: kind → handlers. Walked at fire time across the prototype chain. */
type HookRegistry = Partial<Record<HookKind, HookHandler[]>>;

function getOwnRegistry(target: object): HookRegistry {
	const existing: unknown = Reflect.getOwnMetadata(HOOKS_KEY, target);
	if (isHookRegistry(existing)) return existing;
	const registry: HookRegistry = {};
	Reflect.defineMetadata(HOOKS_KEY, registry, target);
	return registry;
}

function isHookRegistry(value: unknown): value is HookRegistry {
	if (typeof value !== "object" || value === null) return false;
	for (const v of Object.values(value)) {
		if (v !== undefined && !Array.isArray(v)) return false;
	}
	return true;
}

function isHookHandler(value: unknown): value is HookHandler {
	return typeof value === "function";
}

/** Register a static method on the class as a hook handler. */
function register(kind: HookKind): MethodDecorator {
	return (target, propertyKey) => {
		// target is the constructor for static methods, the prototype for instance methods
		const ctor = typeof target === "function" ? target : target.constructor;
		const handler: unknown = Reflect.get(ctor, propertyKey);
		if (!isHookHandler(handler)) {
			throw new Error(
				`@${kind} must decorate a static method, got ${String(propertyKey)}`,
			);
		}
		const registry = getOwnRegistry(ctor);
		if (!registry[kind]) registry[kind] = [];
		registry[kind]?.push(handler);
	};
}

// ─── Decorators (one per hook kind) ───────────────────────────

export const beforeSave = (): MethodDecorator => register("beforeSave");
export const afterSave = (): MethodDecorator => register("afterSave");
export const beforeCreate = (): MethodDecorator => register("beforeCreate");
export const afterCreate = (): MethodDecorator => register("afterCreate");
export const beforeUpdate = (): MethodDecorator => register("beforeUpdate");
export const afterUpdate = (): MethodDecorator => register("afterUpdate");
export const beforeDelete = (): MethodDecorator => register("beforeDelete");
export const afterDelete = (): MethodDecorator => register("afterDelete");
export const beforeFind = (): MethodDecorator => register("beforeFind");
export const afterFind = (): MethodDecorator => register("afterFind");
export const beforeFetch = (): MethodDecorator => register("beforeFetch");
export const afterFetch = (): MethodDecorator => register("afterFetch");
export const beforePaginate = (): MethodDecorator => register("beforePaginate");
export const afterPaginate = (): MethodDecorator => register("afterPaginate");

/**
 * Walk the prototype chain and collect every handler registered for `kind`.
 * Parent-class hooks fire BEFORE child-class hooks (so a base entity can set
 * up scoping that the child then refines).
 */
export function collectHooks<K extends HookKind>(
	entityClass: new (...args: unknown[]) => BaseEntity,
	kind: K,
): HookHandler<K>[] {
	const handlers: HookHandler<K>[] = [];
	let ctor: object | null = entityClass;
	const chain: object[] = [];
	while (
		ctor &&
		ctor !== Function.prototype &&
		(ctor as { name?: string }).name
	) {
		chain.push(ctor);
		ctor = Object.getPrototypeOf(ctor);
	}
	// Walk parent → child so the most-base hook fires first
	for (const c of chain.reverse()) {
		const registry = Reflect.getOwnMetadata(HOOKS_KEY, c) as
			| HookRegistry
			| undefined;
		const list = registry?.[kind] as HookHandler<K>[] | undefined;
		if (list) handlers.push(...list);
	}
	return handlers;
}

/**
 * Fire every handler for `kind` sequentially. Awaits each one so a hook can
 * mutate the entity before the next hook (or the persistence step) sees it.
 * If any hook throws, the whole operation aborts and the error propagates.
 */
export async function fireHooks<K extends HookKind>(
	entityClass: new (...args: unknown[]) => BaseEntity,
	kind: K,
	arg: HookArgs[K],
): Promise<void> {
	const handlers = collectHooks(entityClass, kind);
	for (const handler of handlers) {
		await handler(arg);
	}
}
