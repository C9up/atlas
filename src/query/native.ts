/**
 * Native query compiler loader — loads the Rust NAPI binary.
 *
 * @implements FR36
 */

import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import { arch, platform } from 'node:process'
import { fileURLToPath } from 'node:url'

const require2 = createRequire(import.meta.url)
const __dirname2 = dirname(fileURLToPath(import.meta.url))

const platformMap: Record<string, string> = {
  'linux-x64': 'linux-x64-gnu',
  'darwin-x64': 'darwin-x64',
  'darwin-arm64': 'darwin-arm64',
  'win32-x64': 'win32-x64-msvc',
  'linux-arm64': 'linux-arm64-gnu',
}

let native: { compileQuery: (json: string) => string; quoteIdent: (name: string) => string } | undefined

let loadError: unknown

try {
  const suffix = platformMap[`${platform}-${arch}`]
  if (suffix) {
    native = require2(join(__dirname2, `../../index.${suffix}.node`))
  }
} catch (e) {
  loadError = e
}

/**
 * Compile a query via the Rust NAPI compiler.
 * Throws if the native binary is not available.
 */
export function compileQueryNative(queryJson: string): { sql: string; params: unknown[] } {
  if (!native) {
    throw new Error(`[ATLAS_NAPI_NOT_FOUND] Rust query compiler not available: ${loadError ?? 'binary not found'}`)
  }
  const resultJson = native.compileQuery(queryJson)
  return JSON.parse(resultJson)
}

/**
 * Check if the native compiler is available.
 */
export function isNativeAvailable(): boolean {
  return native !== undefined
}
