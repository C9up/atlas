// Atlas ships TWO NAPI artefacts:
//   - atlas-query-napi → index.<suffix>.node  (default)
//   - atlas-db-napi    → db.<suffix>.node     (--basename db)
//
// Usage:
//   node scripts/copy-napi.mjs                  # query → index.<suffix>.node
//   node scripts/copy-napi.mjs --basename db    # db    → db.<suffix>.node

import { copyFileSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { argv, arch, env, platform } from 'node:process'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const root = join(here, '..')

// Cross-compile aware: set CARGO_BUILD_TARGET (e.g. x86_64-apple-darwin on an
// arm64 runner so we don't depend on the scarce macos-13 Intel runners) and we
// read target/<triple>/release. Unset = host platform / target/release.
const tripleMap = {
  'x86_64-unknown-linux-gnu': { suffix: 'linux-x64-gnu', os: 'linux' },
  'aarch64-unknown-linux-gnu': { suffix: 'linux-arm64-gnu', os: 'linux' },
  'x86_64-apple-darwin': { suffix: 'darwin-x64', os: 'darwin' },
  'aarch64-apple-darwin': { suffix: 'darwin-arm64', os: 'darwin' },
  'x86_64-pc-windows-msvc': { suffix: 'win32-x64-msvc', os: 'win32' },
}
const hostSuffixMap = {
  'linux-x64': 'linux-x64-gnu',
  'linux-arm64': 'linux-arm64-gnu',
  'darwin-x64': 'darwin-x64',
  'darwin-arm64': 'darwin-arm64',
  'win32-x64': 'win32-x64-msvc',
}

const basenameIdx = argv.indexOf('--basename')
const basename = basenameIdx >= 0 ? argv[basenameIdx + 1] : 'index'
if (!basename || basename.startsWith('-')) {
  throw new Error(`[atlas:napi] --basename requires a value (got: ${basename ?? '<missing>'})`)
}

const crateMap = {
  index: 'atlas_query_napi',
  db: 'atlas_db_napi',
}
const crate = crateMap[basename]
if (!crate) {
  throw new Error(`[atlas:napi] unknown basename '${basename}'. Expected one of: ${Object.keys(crateMap).join(', ')}`)
}

const triple = env.CARGO_BUILD_TARGET ?? ''
let suffix
let os
let releaseDir
if (triple) {
  const entry = tripleMap[triple]
  if (!entry) {
    throw new Error(`[atlas:napi] unsupported CARGO_BUILD_TARGET: ${triple}`)
  }
  suffix = entry.suffix
  os = entry.os
  releaseDir = join(root, 'target', triple, 'release')
} else {
  suffix = hostSuffixMap[`${platform}-${arch}`]
  os = platform
  releaseDir = join(root, 'target', 'release')
  if (!suffix) {
    throw new Error(`[atlas:napi] unsupported platform/arch: ${platform}-${arch}`)
  }
}

const candidates = os === 'win32'
  ? [join(releaseDir, `${crate}.dll`), join(releaseDir, `lib${crate}.dll`)]
  : os === 'darwin'
  ? [join(releaseDir, `lib${crate}.dylib`)]
  : [join(releaseDir, `lib${crate}.so`)]

const source = candidates.find((candidate) => existsSync(candidate))
if (!source) {
  throw new Error(
    `[atlas:napi] native library not found. Looked for:\n${candidates.map((p) => `- ${p}`).join('\n')}`,
  )
}

const target = join(root, `${basename}.${suffix}.node`)
copyFileSync(source, target)
console.log(`[atlas:napi] copied ${source} -> ${target}`)
