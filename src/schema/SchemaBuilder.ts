/**
 * SchemaBuilder — barrel re-export for backward compatibility.
 *
 * The classes were split into one-class-per-file:
 *   - {@link Schema}        → ./Schema.ts
 *   - {@link TableBuilder}  → ./TableBuilder.ts
 *   - shared types          → ./types.ts
 *
 * @implements FR34
 */

export { Schema } from "./Schema.js";
export { TableBuilder } from "./TableBuilder.js";
export type { ColumnDefinition, ColumnType, IndexDefinition } from "./types.js";
