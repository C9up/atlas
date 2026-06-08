/** Convert snake_case to camelCase. */
export function snakeToCamel(s: string): string {
	return s.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

/** Convert camelCase or PascalCase to snake_case (no leading underscore). */
export function camelToSnake(s: string): string {
	return s.replace(/[A-Z]/g, (c, i) =>
		i === 0 ? c.toLowerCase() : `_${c.toLowerCase()}`,
	);
}
