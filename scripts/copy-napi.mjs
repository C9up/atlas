// Atlas ships TWO NAPI artefacts:
//   - atlas-query-napi → index.<suffix>.node  (default)
//   - atlas-db-napi    → db.<suffix>.node     (--basename db)
//
// Usage:
//   node scripts/copy-napi.mjs                  # query → index.<suffix>.node
//   node scripts/copy-napi.mjs --basename db    # db    → db.<suffix>.node

import { copyFileSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { argv, arch, platform } from 'node:process'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const root = join(here, '..')

const suffixMap = {
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

const suffix = suffixMap[`${platform}-${arch}`]
if (!suffix) {
  throw new Error(`[atlas:napi] unsupported platform/arch: ${platform}-${arch}`)
}

const candidates = platform === 'win32'
  ? [
      join(root, 'target', 'release', `${crate}.dll`),
      join(root, 'target', 'release', `lib${crate}.dll`),
    ]
  : platform === 'darwin'
  ? [join(root, 'target', 'release', `lib${crate}.dylib`)]
  : [join(root, 'target', 'release', `lib${crate}.so`)]

const source = candidates.find((candidate) => existsSync(candidate))
if (!source) {
  throw new Error(
    `[atlas:napi] native library not found. Looked for:\n${candidates.map((p) => `- ${p}`).join('\n')}`,
  )
}

const target = join(root, `${basename}.${suffix}.node`)
copyFileSync(source, target)
console.log(`[atlas:napi] copied ${source} -> ${target}`)
