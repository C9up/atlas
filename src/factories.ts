/**
 * `@c9up/atlas/factories` — Adonis Lucid `@adonisjs/lucid/factories` parity
 * subpath. The model factory: `Factory.define(Model, cb).build()` (or the
 * `factory()` shorthand) for generating and persisting test data.
 */
export {
	type FactoryCommandOptions,
	makeFactoryCommand,
} from "./console/factoryCommands.js";
export { Factory, type FactoryBuilder, factory } from "./testing/Factory.js";
