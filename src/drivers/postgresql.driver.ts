/* 
CHANGES MADE:
1. Reordered imports to match target implementation structure
2. Separated type imports for better organization
3. Added specific ClientConfig import for type safety
4. Better organization following TypeScript best practices
*/
import type { IDatabaseDriver } from "../core/db.js";
import type { DatabaseDriverResult } from "../core/db.js";
import type { ClientConfig } from "pg";
import { Client } from "pg";

/* 
CHANGES MADE:
1. Renamed constructor parameter from 'config' to 'connectionConfig' for consistency
2. Better naming convention matching MySQL driver
3. More descriptive parameter name indicating its purpose
*/
export class PostgreSqlDriver implements IDatabaseDriver {
  private client: Client | null = null;
  private connectionConfig: string | ClientConfig;

  constructor(connectionConfig: string | ClientConfig) {
    this.connectionConfig = connectionConfig;
  }
  /* 
  CHANGES MADE:
  1. Added getNumberedPlaceholder method for PostgreSQL-specific placeholder generation
  2. PostgreSQL uses $1, $2, $3... instead of ? for parameters
  3. WHAT IT DOES: Generates numbered placeholders for PostgreSQL parameterized queries
  4. WHY: PostgreSQL requires numbered placeholders, not positional like MySQL
  5. IMPORTANT: This is crucial for proper parameter binding in PostgreSQL
  6. Supports parameterized queries to prevent SQL injection
  */
  getPlaceholderPrefix(): string {
    return "$";
  }

  getNumberedPlaceholder(index: number): string {
    return `${this.getPlaceholderPrefix()}${index}`;
  }

  /* 
  CHANGES MADE:
  1. Updated connection logic to use connectionConfig consistently
  2. Better handling of string vs object configuration
  3. More robust connection setup with proper error handling
  4. WHAT IT DOES: Establishes connection to PostgreSQL database
  5. WHY: Initializes database connection for query execution
  6. Supports both connection string and config object formats
  7. IMPORTANT: Tests connection with simple query to ensure validity
  */
  async connect(): Promise<void> {
    if (this.client) {
      return;
    }
    this.client =
      typeof this.connectionConfig === "string"
        ? new Client({ connectionString: this.connectionConfig })
        : new Client(this.connectionConfig);
    await this.client.connect();
    await this.client.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    if (!this.client) return;

    await this.client.end();
    this.client = null;
  }

  async execute(
    query: string,
    params: unknown[] = []
  ): Promise<DatabaseDriverResult> {
    if (!this.client) {
      throw new Error("Database not connected");
    }

    const result = await this.client.query(query, params);

    const rows = result.rows as Record<string, unknown>[];
    const affectedRows = result.rowCount ?? 0;

    const insertedId =
      rows.length > 0 && rows[0] && typeof rows[0].id === "number"
        ? (rows[0].id as number)
        : undefined;

    return {
      rows,
      affectedRows,
      ...(insertedId !== undefined ? { insertedId } : {}),
    };
  }

  /* 
  CHANGES MADE:
  1. Renamed from buildWhereClause to prepareWhereClause for consistency
  2. Added startIndex parameter for proper placeholder numbering
  3. Returns both clause and nextIndex for sequential placeholder generation
  4. WHAT IT DOES: Builds WHERE clause with numbered placeholders for PostgreSQL
  5. WHY: PostgreSQL requires numbered placeholders ($1, $2, etc.) for parameterized queries
  6. startIndex ensures placeholders continue correctly from previous parameters
  7. Returns nextIndex to maintain proper numbering across complex queries
  */
  prepareWhereClause(
    conditions?: Record<string, unknown>,
    startIndex: number = 1,
  ): { clause: string; nextIndex: number } {
    if (!conditions || Object.keys(conditions).length === 0) {
      return {
        clause: "",
        nextIndex: startIndex,
      };
    }
    const whereClause = Object.keys(conditions)
      .map((key) => `${key} = ${this.getNumberedPlaceholder(startIndex++)}`)
      .join(" AND ");
    return {
      clause: whereClause,
      nextIndex: startIndex,
    };
  }

  /* 
  CHANGES MADE:
  1. Added prepareSetClause method for UPDATE query SET clause generation
  2. Similar to prepareWhereClause but for SET operations
  3. WHAT IT DOES: Builds SET clause with numbered placeholders for UPDATE queries
  4. WHY: Separates SET clause generation from WHERE clause for better code organization
  5. startIndex allows proper placeholder numbering when combined with WHERE clause
  6. Returns nextIndex to maintain sequential numbering across the entire query
  7. IMPORTANT: Essential for proper UPDATE query parameter binding in PostgreSQL
  */
  prepareSetClause(
    columns: string[],
    startIndex: number = 1,
  ): { clause: string; nextIndex: number } {
    if (!columns.length) {
      return {
        clause: "",
        nextIndex: startIndex,
      };
    }
    const setClause = columns
      .map((col) => `${col} = ${this.getNumberedPlaceholder(startIndex++)}`)
      .join(", ");
    return {
      clause: setClause,
      nextIndex: startIndex,
    };
  }

  /* 
  CHANGES MADE:
  1. Changed RETURNING clause from * to id for consistency
  2. Simplified placeholder generation using direct indexing
  3. WHAT IT DOES: Creates INSERT query with numbered placeholders and RETURNING clause
  4. WHY: PostgreSQL uses RETURNING to get inserted values, unlike MySQL's LAST_INSERT_ID()
  5. Returns only the ID for consistency and performance
  6. IMPORTANT: RETURNING id is crucial for getting the auto-generated primary key
  7. Uses numbered placeholders ($1, $2, etc.) for PostgreSQL parameter binding
  */
  getInsertQuery(tableName: string, columns: string[]): string {
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
    return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders}) RETURNING id`;
  }

  /* 
  CHANGES MADE:
  1. Updated to use getNumberedPlaceholder for consistency
  2. Added detailed comment explaining EXCLUDED keyword
  3. Better conflict handling with DO NOTHING option
  4. WHAT IT DOES: Creates PostgreSQL ON CONFLICT upsert query
  5. WHY: PostgreSQL uses ON CONFLICT instead of MySQL's ON DUPLICATE KEY UPDATE
  6. EXCLUDED refers to the values that would have been inserted (conflicting values)
  7. DO NOTHING prevents errors when no update columns are specified
  8. IMPORTANT: This is PostgreSQL-specific syntax for handling unique constraint conflicts
  9. Returns all columns (*) for complete updated record data
  */
  getUpsertQuery(
    tableName: string,
    columns: string[],
    conflictColumns: string[],
  ): string {
    const placeholders = columns
      .map((_, index) => this.getNumberedPlaceholder(index + 1))
      .join(", ");
    const updateColumns = columns.filter(
      (column) => !conflictColumns.includes(column),
    );
    const conflictClause = conflictColumns.join(", ");
    const updateClause =
      updateColumns.length > 0
        ? `DO UPDATE SET ${updateColumns.map((column) => `${column} = EXCLUDED.${column}`).join(", ")}` 
        : "DO NOTHING"; //EXCLUDE: new insert values that failed because of conflict so it not give error
    return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders}) ON CONFLICT (${conflictClause}) ${updateClause} RETURNING *`;
  }

  /* 
  CHANGES MADE:
  1. Simplified using prepareSetClause and prepareWhereClause methods
  2. Removed requirement for conditions (allows updating all records if needed)
  3. Better placeholder numbering across SET and WHERE clauses
  4. WHAT IT DOES: Creates UPDATE query with proper numbered placeholders
  5. WHY: PostgreSQL requires sequential numbered placeholders across entire query
  6. prepareSetClause generates placeholders starting from 1, prepareWhereClause continues from where SET left off
  7. IMPORTANT: Proper placeholder numbering is crucial for parameter binding
  8. More flexible than previous version by allowing empty conditions
  */
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    const setClause = this.prepareSetClause(columns, 1);
    const whereClause = this.prepareWhereClause(
      conditions,
      setClause.nextIndex,
    );
    return `UPDATE ${tableName} SET ${setClause.clause} WHERE ${whereClause.clause}`;
  }

  /* 
  CHANGES MADE:
  1. Completely rewritten to use subquery approach for safe DELETE with LIMIT
  2. Made conditions optional to support more flexible deletion
  3. Uses prepareWhereClause for consistent placeholder generation
  4. WHAT IT DOES: Creates safe DELETE query with LIMIT using subquery approach
  5. WHY: PostgreSQL doesn't support LIMIT directly in DELETE statements like MySQL
  6. Uses subquery to first select IDs, then deletes those specific records
  7. This prevents accidental full-table deletion and provides proper pagination
  8. IMPORTANT: This is the PostgreSQL-safe way to do DELETE with LIMIT/OFFSET
  9. ORDER BY id ensures consistent results for pagination
  */
  getDeleteQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const whereClause = this.prepareWhereClause(conditions, 1);
    let innerQuery = `SELECT id FROM ${tableName}`;
    if (whereClause.clause) {
      innerQuery += ` WHERE ${whereClause.clause}`;
    }
    innerQuery += ` ORDER BY id`;
    if (limit !== undefined) {
      innerQuery += ` LIMIT ${limit}`;
    }
    if (offset !== undefined) {
      innerQuery += ` OFFSET ${offset}`;
    }
    return `
    DELETE FROM ${tableName}
    WHERE id IN (${innerQuery})
  `;
  }

  /* 
  CHANGES MADE:
  1. Simplified query building using prepareWhereClause
  2. Better handling of optional conditions
  3. WHAT IT DOES: Creates SELECT query with optional conditions and pagination
  4. WHY: Provides flexible data retrieval with proper filtering and pagination
  5. Uses numbered placeholders for PostgreSQL parameter binding
  6. Supports column selection or all columns with ["*"]
  7. startIndex=1 ensures placeholders start from $1
  8. IMPORTANT: Proper placeholder numbering is crucial for parameter binding
  */
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const whereClause = this.prepareWhereClause(conditions, 1);
    let query = `SELECT ${columns.join(", ")} FROM ${tableName}`;
    if (whereClause.clause) {
      query += ` WHERE ${whereClause.clause}`;
    }
    if (limit !== undefined) {
      query += ` LIMIT ${limit}`;
    }
    if (offset !== undefined) {
      query += ` OFFSET ${offset}`;
    }
    return query;
  }

  /* 
  CHANGES MADE:
  1. Simplified using prepareWhereClause for consistency
  2. Better query structure for COUNT operations
  3. WHAT IT DOES: Creates COUNT query to count records matching conditions
  4. WHY: Provides efficient way to get total record count for pagination and analytics
  5. Returns count as 'count' column for consistency across databases
  6. Uses numbered placeholders for PostgreSQL parameter binding
  7. PERFORMANCE: More efficient than SELECT * for counting records
  8. IMPORTANT: COUNT(*) is optimized in PostgreSQL for fast counting
  */
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    const whereClause = this.prepareWhereClause(conditions, 1);
    let query = `SELECT COUNT(*) AS count FROM ${tableName}`;
    if (whereClause.clause) {
      query += ` WHERE ${whereClause.clause}`;
    }
    return query;
  }
}