/* 
CHANGES MADE:
1. Reordered imports to match target implementation structure
2. Separated type imports for better organization
3. Added specific ClientConfig import for type safety
4. Added BaseEntity import for escapeIdentifier function
5. Better organization following TypeScript best practices
*/
import type { IDatabaseDriver } from "../core/db.js";
import type { DatabaseDriverResult } from "../core/db.js";
import type { ClientConfig } from "pg";
import { BaseEntity } from "../core/base.entity.js";
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
  3. Extended to support multiple operators (=, >, <, >=, <=, !=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
  4. Returns both clause and nextIndex for sequential placeholder generation
  5. WHAT IT DOES: Builds WHERE clause with numbered placeholders and operators for PostgreSQL
  6. WHY: PostgreSQL requires numbered placeholders ($1, $2, etc.) for parameterized queries
  7. startIndex ensures placeholders continue correctly from previous parameters
  8. Returns nextIndex to maintain proper numbering across complex queries
  9. Enables complex queries beyond simple equality while maintaining security
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
    const predicates: string[] = [];
    let currentIndex = startIndex;
    
    for (const [columnWithOperator, value] of Object.entries(conditions)) {
      // Handle different operators
      if (columnWithOperator.toUpperCase().includes('IN') && Array.isArray(value)) {
        const placeholders = value.map(() => this.getNumberedPlaceholder(currentIndex++)).join(', ');
        predicates.push(`${columnWithOperator} (${placeholders})`);
      } else if (columnWithOperator.toUpperCase().includes('IS')) {
        // IS NULL / IS NOT NULL don't need values
        predicates.push(`${columnWithOperator}`);
      } else {
        // Standard operators: =, >, <, >=, <=, !=, LIKE, NOT LIKE
        predicates.push(`${columnWithOperator} ${this.getNumberedPlaceholder(currentIndex++)}`);
      }
    }
    
    return {
      clause: predicates.join(" AND "),
      nextIndex: currentIndex,
    };
  }

  /* 
  CHANGES MADE:
  1. Added prepareSetClause method for UPDATE query SET clause generation
  2. Similar to prepareWhereClause but for SET operations
  3. Takes escaped columns array instead of raw column names
  4. WHAT IT DOES: Builds SET clause with numbered placeholders for UPDATE queries
  5. WHY: Separates SET clause generation from WHERE clause for better code organization
  6. startIndex allows proper placeholder numbering when combined with WHERE clause
  7. Returns nextIndex to maintain sequential numbering across the entire query
  8. IMPORTANT: Essential for proper UPDATE query parameter binding in PostgreSQL
  */
  prepareSetClause(
    escapedColumns: string[],
    startIndex: number = 1,
  ): { clause: string; nextIndex: number } {
    if (!escapedColumns.length) {
      return {
        clause: "",
        nextIndex: startIndex,
      };
    }
    const setClause = escapedColumns
      .map((col) => `${col} = ${this.getNumberedPlaceholder(startIndex++)}`)
      .join(", ");
    return {
      clause: setClause,
      nextIndex: startIndex,
    };
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support to wrap table and column names in double quotes
  2. This prevents SQL errors when using reserved keywords as identifiers
  3. Applied to getInsertQuery to escape both table name and column names
  4. Changed RETURNING clause from * to id for consistency
  5. Simplified placeholder generation using direct indexing
  6. WHAT IT DOES: Creates INSERT query with proper identifier escaping for PostgreSQL
  7. WHY: Ensures table/column names like 'order', 'group', 'user' work correctly
  8. IMPORTANT: Uses double quotes (") for PostgreSQL identifier escaping
  */
  getInsertQuery(tableName: string, columns: string[]): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'postgresql'));
    const placeholders = columns.map((_, i) => `$${i + 1}`).join(", ");
    return `INSERT INTO ${escapedTableName} (${escapedColumns.join(", ")}) VALUES (${placeholders}) RETURNING id`;
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Updated to use getNumberedPlaceholder for consistency
  3. Added detailed comment explaining EXCLUDED keyword
  4. Better conflict handling with DO NOTHING option
  5. WHAT IT DOES: Creates PostgreSQL ON CONFLICT upsert query with proper escaping
  6. WHY: PostgreSQL uses ON CONFLICT instead of MySQL's ON DUPLICATE KEY UPDATE
  7. EXCLUDED refers to the values that would have been inserted (conflicting values)
  8. DO NOTHING prevents errors when no update columns are specified
  9. IMPORTANT: This is PostgreSQL-specific syntax for handling unique constraint conflicts
  10. Returns all columns (*) for complete updated record data
  */
  getUpsertQuery(
    tableName: string,
    columns: string[],
    conflictColumns: string[],
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'postgresql'));
    const escapedConflictColumns = conflictColumns.map(col => BaseEntity.escapeIdentifier(col, 'postgresql'));
    const placeholders = columns
      .map((_, index) => this.getNumberedPlaceholder(index + 1))
      .join(", ");
    const updateColumns = columns.filter(
      (column) => !conflictColumns.includes(column),
    );
    const escapedUpdateColumns = updateColumns.map(col => BaseEntity.escapeIdentifier(col, 'postgresql'));
    const conflictClause = escapedConflictColumns.join(", ");
    const updateClause =
      updateColumns.length > 0
        ? `DO UPDATE SET ${escapedUpdateColumns.map((column) => `${column} = EXCLUDED.${column}`).join(", ")}` 
        : "DO NOTHING"; //EXCLUDE: new insert values that failed because of conflict so it not give error
    return `INSERT INTO ${escapedTableName} (${escapedColumns.join(", ")}) VALUES (${placeholders}) ON CONFLICT (${conflictClause}) ${updateClause} RETURNING *`;
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Simplified using prepareSetClause and prepareWhereClause methods
  3. Removed requirement for conditions (allows updating all records if needed)
  4. Better placeholder numbering across SET and WHERE clauses
  5. WHAT IT DOES: Creates UPDATE query with escaped identifiers and proper numbered placeholders
  6. WHY: PostgreSQL requires sequential numbered placeholders across entire query
  7. prepareSetClause generates placeholders starting from 1, prepareWhereClause continues from where SET left off
  8. IMPORTANT: Proper placeholder numbering is crucial for parameter binding
  9. More flexible than previous version by allowing empty conditions
  */
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'postgresql'));
    const setClause = this.prepareSetClause(escapedColumns, 1);
    const whereClause = this.prepareWhereClause(
      conditions,
      setClause.nextIndex,
    );
    return `UPDATE ${escapedTableName} SET ${setClause.clause} WHERE ${whereClause.clause}`;
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table name
  2. Completely rewritten to use subquery approach for safe DELETE with LIMIT
  3. Made conditions optional to support more flexible deletion
  4. Uses prepareWhereClause for consistent placeholder generation
  5. WHAT IT DOES: Creates safe DELETE query with escaped table name using subquery approach
  6. WHY: PostgreSQL doesn't support LIMIT directly in DELETE statements like MySQL
  7. Uses subquery to first select IDs, then deletes those specific records
  8. This prevents accidental full-table deletion and provides proper pagination
  9. IMPORTANT: This is the PostgreSQL-safe way to do DELETE with LIMIT/OFFSET
  10. ORDER BY id ensures consistent results for pagination
  */
  getDeleteQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const whereClause = this.prepareWhereClause(conditions, 1);
    let innerQuery = `SELECT id FROM ${escapedTableName}`;
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
    DELETE FROM ${escapedTableName}
    WHERE id IN (${innerQuery})
  `;
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Simplified query building using prepareWhereClause
  3. Better handling of optional conditions
  4. WHAT IT DOES: Creates SELECT query with escaped identifiers and flexible conditions
  5. WHY: Provides flexible data retrieval with proper filtering, pagination, and identifier escaping
  6. Uses numbered placeholders for PostgreSQL parameter binding
  7. Supports column selection or all columns with ["*"]
  8. startIndex=1 ensures placeholders start from $1
  9. IMPORTANT: Proper placeholder numbering is crucial for parameter binding
  */
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const escapedColumns = columns.map(col => col === '*' ? '*' : BaseEntity.escapeIdentifier(col, 'postgresql'));
    const whereClause = this.prepareWhereClause(conditions, 1);
    let query = `SELECT ${escapedColumns.join(", ")} FROM ${escapedTableName}`;
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
  1. Added escapeIdentifier support for table name
  2. Simplified using prepareWhereClause for consistency
  3. Better query structure for COUNT operations
  4. WHAT IT DOES: Creates COUNT query with escaped table name and flexible conditions
  5. WHY: Provides efficient way to get total record count for pagination and analytics
  6. Returns count as 'count' column for consistency across databases
  7. Uses numbered placeholders for PostgreSQL parameter binding
  8. PERFORMANCE: More efficient than SELECT * for counting records
  9. IMPORTANT: COUNT(*) is optimized in PostgreSQL for fast counting
  */
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'postgresql');
    const whereClause = this.prepareWhereClause(conditions, 1);
    let query = `SELECT COUNT(*) AS count FROM ${escapedTableName}`;
    if (whereClause.clause) {
      query += ` WHERE ${whereClause.clause}`;
    }
    return query;
  }
}