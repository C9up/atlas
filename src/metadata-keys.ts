/**
 * Shared entity metadata keys + serialize config.
 *
 * Extracted from `BaseEntity` so the decorator layer (`decorators/entity.ts`)
 * can read them without importing `BaseEntity` — that import-back formed a
 * runtime cycle `BaseEntity` ↔ `entity` (fallow 2026-06-14). Agnostic: pure
 * `Symbol.for` keys + a plain interface, zero runtime dependencies.
 */

/** Symbol metadata key for the computed-property registry on an entity class. */
export const COMPUTED_KEY = Symbol.for("atlas:computed");

/** Symbol metadata key for the serialize-as / serializer overrides on columns. */
export const COLUMN_SERIALIZE_KEY = Symbol.for("atlas:columnSerialize");

/** Per-column serialization config (populated by @Column options). */
export interface ColumnSerializeConfig {
	/** Rename this column at toJSON time (e.g. `password` → `passwordHash`). Null = hidden. */
	serializeAs?: string | null;
	/** Transform function applied to the value at toJSON time. Signature mirrors
	 * Adonis Lucid: `(value, attribute, model)`; extra args optional. */
	serialize?: (value: unknown, attribute?: string, model?: unknown) => unknown;
}
