/**
 * BaseEntity — base class for all Atlas entities.
 *
 * Provides domain event accumulation and common fields.
 *
 * @implements FR29, FR35
 */

export interface DomainEvent {
  name: string
  data: Record<string, unknown>
}

export class BaseEntity {
  /** Accumulated domain events — dispatched on Pulsar after DB commit. */
  private _domainEvents: DomainEvent[] = []

  /** Add a domain event to be dispatched after save. */
  protected addDomainEvent(name: string, data: Record<string, unknown>): void {
    this._domainEvents.push({ name, data })
  }

  /** Get accumulated domain events (non-destructive read). */
  getDomainEvents(): readonly DomainEvent[] {
    return [...this._domainEvents]
  }

  /** Clear accumulated domain events. */
  clearDomainEvents(): void {
    this._domainEvents = []
  }

  /** Get and clear accumulated domain events atomically. */
  flushDomainEvents(): DomainEvent[] {
    const events = [...this._domainEvents]
    this._domainEvents = []
    return events
  }

  /** Check if entity has pending domain events. */
  hasDomainEvents(): boolean {
    return this._domainEvents.length > 0
  }

  /** Exclude internal fields from JSON serialization. */
  toJSON(): Record<string, unknown> {
    const result: Record<string, unknown> = {}
    for (const [key, value] of Object.entries(this)) {
      if (!key.startsWith('_')) {
        result[key] = value
      }
    }
    return result
  }
}
