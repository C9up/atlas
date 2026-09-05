/**
 * Reading an aggregate the way Lucid returns one.
 *
 * Every aggregate is a projection: the builder is returned, and the value comes
 * back as a column of the result row — in `$extras` on the model builder. That
 * is two lines at every call site, and these tests have twenty-nine of them, so
 * the reading is factored out. The QUERY is still written in full at each site,
 * which is the part worth seeing.
 */

/** The value of a projected aggregate on a db-builder row. */
export async function aggregateOf(
	query: PromiseLike<Record<string, unknown>[]>,
	alias: string,
): Promise<number> {
	const [row] = await query;
	return Number(row?.[alias] ?? 0);
}

/** The same, from a model builder — the value lands on `$extras`. */
export async function modelAggregateOf(
	query: PromiseLike<{ $extras: Record<string, unknown> }[]>,
	alias: string,
): Promise<number> {
	const [row] = await query;
	return Number(row?.$extras[alias] ?? 0);
}
