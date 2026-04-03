import type { ColumnType } from '../schema/SchemaBuilder.js'

export interface Dialect {
  name: string
  mapColumnType(type: ColumnType, options?: { length?: number; precision?: number; scale?: number }): string
  wrapIdentifier(name: string): string
  wrapDefaultValue(value: string): string
  autoIncrementPrimaryKey(): string
  currentTimestamp(): string
  supportsReturning: boolean
}

export class PostgresDialect implements Dialect {
  name = 'postgres'
  supportsReturning = true

  mapColumnType(type: ColumnType, opts?: { length?: number; precision?: number; scale?: number }): string {
    switch (type) {
      case 'uuid': return 'UUID'
      case 'string': return `VARCHAR(${opts?.length ?? 255})`
      case 'text': return 'TEXT'
      case 'integer': return 'INTEGER'
      case 'bigInteger': return 'BIGINT'
      case 'decimal': return `DECIMAL(${opts?.precision ?? 10}, ${opts?.scale ?? 2})`
      case 'boolean': return 'BOOLEAN'
      case 'date': return 'DATE'
      case 'timestamp': return 'TIMESTAMP'
      case 'json': return 'JSONB'
      case 'binary': return 'BYTEA'
    }
  }

  wrapIdentifier(name: string): string { return `"${name}"` }
  wrapDefaultValue(value: string): string { return `DEFAULT ${value}` }
  autoIncrementPrimaryKey(): string { return 'SERIAL PRIMARY KEY' }
  currentTimestamp(): string { return 'NOW()' }
}

export class SqliteDialect implements Dialect {
  name = 'sqlite'
  supportsReturning = false

  mapColumnType(type: ColumnType): string {
    switch (type) {
      case 'uuid': return 'TEXT'
      case 'string': return 'TEXT'
      case 'text': return 'TEXT'
      case 'integer': return 'INTEGER'
      case 'bigInteger': return 'INTEGER'
      case 'decimal': return 'REAL'
      case 'boolean': return 'INTEGER'
      case 'date': return 'TEXT'
      case 'timestamp': return 'TEXT'
      case 'json': return 'TEXT'
      case 'binary': return 'BLOB'
    }
  }

  wrapIdentifier(name: string): string { return `"${name}"` }
  wrapDefaultValue(value: string): string { return `DEFAULT (${value})` }
  autoIncrementPrimaryKey(): string { return 'INTEGER PRIMARY KEY AUTOINCREMENT' }
  currentTimestamp(): string { return "datetime('now')" }
}

export class MysqlDialect implements Dialect {
  name = 'mysql'
  supportsReturning = false

  mapColumnType(type: ColumnType, opts?: { length?: number; precision?: number; scale?: number }): string {
    switch (type) {
      case 'uuid': return 'CHAR(36)'
      case 'string': return `VARCHAR(${opts?.length ?? 255})`
      case 'text': return 'TEXT'
      case 'integer': return 'INT'
      case 'bigInteger': return 'BIGINT'
      case 'decimal': return `DECIMAL(${opts?.precision ?? 10}, ${opts?.scale ?? 2})`
      case 'boolean': return 'TINYINT(1)'
      case 'date': return 'DATE'
      case 'timestamp': return 'TIMESTAMP'
      case 'json': return 'JSON'
      case 'binary': return 'BLOB'
    }
  }

  wrapIdentifier(name: string): string { return `\`${name}\`` }
  wrapDefaultValue(value: string): string { return `DEFAULT ${value}` }
  autoIncrementPrimaryKey(): string { return 'INT AUTO_INCREMENT PRIMARY KEY' }
  currentTimestamp(): string { return 'NOW()' }
}

const dialects: Record<string, Dialect> = {
  postgres: new PostgresDialect(),
  sqlite: new SqliteDialect(),
  mysql: new MysqlDialect(),
  mariadb: new MysqlDialect(),
}

export function getDialect(name: string): Dialect {
  const dialect = dialects[name]
  if (!dialect) throw new Error(`Unknown dialect '${name}'. Available: ${Object.keys(dialects).join(', ')}`)
  return dialect
}
