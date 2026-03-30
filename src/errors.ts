/**
 * AtlasError — structured error for Atlas ORM.
 */
export class AtlasError extends Error {
  readonly code: string
  readonly hint?: string

  constructor(code: string, message: string, options?: { hint?: string }) {
    super(message)
    this.name = 'AtlasError'
    this.code = `ATLAS_${code}`
    this.hint = options?.hint
  }
}
