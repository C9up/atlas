/**
 * Connection manager — the AdonisJS Lucid `db.manager` surface. Owns a map of
 * named {@link ConnectionNode}s (config + live connection + lifecycle state) and
 * emits `connect`/`disconnect` events. `AtlasProvider` registers the connections
 * it opens at boot; user code can also `add` + `connect` (or `patch`/`release`)
 * connections at runtime.
 */

import { EventEmitter } from "node:events";
import type { ConnectionConfig } from "./AtlasProvider.js";
import {
	type AsyncDatabaseConnection,
	createNapiConnection,
} from "./adapters/NapiDbAdapter.js";

/** Lifecycle state of a {@link ConnectionNode} (Lucid parity). */
export type ConnectionState = "registered" | "open" | "closed";

/** A managed connection: its config, the live handle (once open), and its state. */
export interface ConnectionNode {
	readonly name: string;
	config: ConnectionConfig;
	connection?: AsyncDatabaseConnection;
	state: ConnectionState;
}

/** Opens a live connection from a config (Lucid's dialect client — here, napi). */
export type ConnectionFactory = (
	name: string,
	config: ConnectionConfig,
) => Promise<AsyncDatabaseConnection>;

/** Default factory — opens via the Rust/NAPI driver from a {@link ConnectionConfig}. */
export async function openFromConfig(
	name: string,
	config: ConnectionConfig,
): Promise<AsyncDatabaseConnection> {
	const url = config.url ?? config.connection;
	if (!url) {
		throw new Error(
			`[atlas] connection '${name}' has no URL — set 'url' (or Lucid's 'connection').`,
		);
	}
	return createNapiConnection(
		url,
		config.pool?.min ?? config.poolMin ?? 1,
		config.pool?.max ?? config.poolMax ?? 10,
		config.pragmas,
		{
			retries: config.connectRetries,
			backoffMs: config.connectBackoffMs,
			timeoutMs: config.connectTimeoutMs,
		},
		{ debug: config.debug ?? false, connectionName: name },
	);
}

/** The Lucid `db.manager` connection manager. */
export class ConnectionManager {
	readonly #nodes = new Map<string, ConnectionNode>();
	readonly #events = new EventEmitter();
	readonly #factory: ConnectionFactory;

	constructor(factory: ConnectionFactory = openFromConfig) {
		this.#factory = factory;
	}

	/** The node map, keyed by connection name (Lucid `manager.connections`). */
	get connections(): ReadonlyMap<string, ConnectionNode> {
		return this.#nodes;
	}

	/** Register a connection config WITHOUT opening it (Lucid `manager.add`). */
	add(name: string, config: ConnectionConfig): this {
		const existing = this.#nodes.get(name);
		if (existing && existing.state === "open") return this; // don't clobber a live node
		this.#nodes.set(name, { name, config, state: "registered" });
		return this;
	}

	/** Replace a registered connection's config (Lucid `manager.patch`). Applies on the next connect. */
	patch(name: string, config: ConnectionConfig): this {
		const node = this.#nodes.get(name);
		if (node) node.config = config;
		else this.#nodes.set(name, { name, config, state: "registered" });
		return this;
	}

	/**
	 * Open a connection (Lucid `manager.connect`). Idempotent — returns the live
	 * handle if already open. The connection must have been {@link add}ed first,
	 * or pass its config here.
	 */
	async connect(
		name: string,
		config?: ConnectionConfig,
	): Promise<AsyncDatabaseConnection> {
		let node = this.#nodes.get(name);
		if (!node) {
			if (!config) {
				throw new Error(
					`[atlas] no connection '${name}' registered — call manager.add(name, config) first.`,
				);
			}
			node = { name, config, state: "registered" };
			this.#nodes.set(name, node);
		}
		if (node.state === "open" && node.connection) return node.connection;
		const connection = await this.#factory(name, config ?? node.config);
		node.connection = connection;
		node.state = "open";
		this.#events.emit("connect", node);
		return connection;
	}

	/**
	 * @internal Register an ALREADY-OPEN connection (used by `AtlasProvider`, which
	 * opens connections itself with its retry/rollback logic). Node state = open.
	 */
	register(
		name: string,
		config: ConnectionConfig,
		connection: AsyncDatabaseConnection,
	): void {
		this.#nodes.set(name, { name, config, connection, state: "open" });
		this.#events.emit("connect", this.#nodes.get(name));
	}

	/** Whether a connection is registered (Lucid `manager.has`). */
	has(name: string): boolean {
		return this.#nodes.has(name);
	}

	/** The connection node, or `undefined` (Lucid `manager.get`). */
	get(name: string): ConnectionNode | undefined {
		return this.#nodes.get(name);
	}

	/** Whether the connection is open (Lucid `manager.isConnected`). */
	isConnected(name: string): boolean {
		return this.#nodes.get(name)?.state === "open";
	}

	/** The live connection handle for `name`, or `undefined` if not open. */
	connection(name: string): AsyncDatabaseConnection | undefined {
		const node = this.#nodes.get(name);
		return node?.state === "open" ? node.connection : undefined;
	}

	/** Close the connection but keep its node (state → closed) (Lucid `manager.close`). */
	async close(name: string): Promise<void> {
		const node = this.#nodes.get(name);
		if (!node?.connection || node.state !== "open") return;
		const conn = node.connection;
		node.state = "closed";
		node.connection = undefined;
		await conn.close();
		this.#events.emit("disconnect", node);
	}

	/** Close every open connection (Lucid `manager.closeAll`). */
	async closeAll(): Promise<void> {
		for (const name of [...this.#nodes.keys()]) {
			await this.close(name);
		}
	}

	/** Close and REMOVE a connection node entirely (Lucid `manager.release`). */
	async release(name: string): Promise<void> {
		await this.close(name);
		this.#nodes.delete(name);
	}

	/**
	 * @internal Remove a node WITHOUT closing it — the caller already closed the
	 * connection (e.g. `AtlasProvider.shutdown`). Ownership-guarded: only removes
	 * when the node still points at `connection`, so a re-registered node from a
	 * newer provider isn't dropped by an older one's teardown.
	 */
	deregister(name: string, connection?: AsyncDatabaseConnection): void {
		const node = this.#nodes.get(name);
		if (!node) return;
		if (connection && node.connection !== connection) return;
		this.#nodes.delete(name);
	}

	/** Subscribe to `connect`/`disconnect` lifecycle events (Node EventEmitter). */
	on(
		event: "connect" | "disconnect",
		listener: (node: ConnectionNode) => void,
	): this {
		this.#events.on(event, listener);
		return this;
	}
	/** One-shot {@link on}. */
	once(
		event: "connect" | "disconnect",
		listener: (node: ConnectionNode) => void,
	): this {
		this.#events.once(event, listener);
		return this;
	}
	/** Remove a lifecycle listener. */
	off(
		event: "connect" | "disconnect",
		listener: (node: ConnectionNode) => void,
	): this {
		this.#events.off(event, listener);
		return this;
	}
}
