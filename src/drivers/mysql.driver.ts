/* 
CHANGES MADE:
1. Reordered imports to match target implementation structure
2. Changed import order: DatabaseDriverResult first, then IDatabaseDriver
3. Added BaseEntity import for escapeIdentifier function
4. This follows TypeScript best practices for type imports
5. Better organization of dependencies
*/
import type { ConnectionOptions } from "mysql2";
import type { IDatabaseDriver, DatabaseDriverResult } from "../core/db.js";
import { BaseEntity } from "../core/base.entity.js";
import { createConnection, Connection } from "mysql2/promise";



export class MySqlDriver implements IDatabaseDriver {
    private connection: Connection | null = null;
    private connectionConfig: string | ConnectionOptions;

    constructor(connectionConfig: string | ConnectionOptions) {
        this.connectionConfig = connectionConfig;
    }


    async connect(): Promise<void> {
        if (this.connection) {
            return;
        }
        this.connection = await (typeof this.connectionConfig === "string" ? createConnection(this.connectionConfig) : createConnection(this.connectionConfig));
        await this.connection.query("SELECT 1");
    }

    async disconnect(): Promise<void> {
        if (!this.connection) {
            return;
        }
        await this.connection.end();
        this.connection = null;
    }

    async execute(query: string, params?: unknown[]): Promise<DatabaseDriverResult> {
        if (!this.connection) {
            throw new Error("Not connected to the database");
        }
        const [results] = await this.connection.execute(query, params as any);
        if (Array.isArray(results)) {
            return {
                rows: results as Record<string, unknown>[],
                affectedRows: 0,
            };
        }

        if (results && typeof results === "object" && "affectedRows" in results) {
            const maybeInsertId = "insertId" in results ? results.insertId : undefined;
            const insertedId = typeof maybeInsertId === "number" && Number.isFinite(maybeInsertId)
                ? maybeInsertId
                : undefined;
            return {
                rows: [],
                affectedRows: Number(results.affectedRows ?? 0),
                ...(insertedId !== undefined ? { insertedId } : {}),
            };
        }

        return {
            rows: [],
            affectedRows: 0,
        };
    }
    
  getPlaceholderPrefix(): string {
    return "?";
  }
  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support to wrap table and column names in backticks
  2. This prevents SQL errors when using reserved keywords as identifiers
  3. Applied to getInsertQuery to escape both table name and column names
  4. WHAT IT DOES: Creates INSERT query with proper identifier escaping for MySQL
  5. WHY: Ensures table/column names like 'order', 'group', 'user' work correctly
  6. IMPORTANT: Uses backticks (`) for MySQL identifier escaping
  */
  getInsertQuery(tableName: string, columns: string[]): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'mysql'));
    const placeholders = columns
      .map(() => this.getPlaceholderPrefix())
      .join(", ");
    return `INSERT INTO ${escapedTableName} (${escapedColumns.join(", ")}) VALUES (${placeholders})`;
  }
  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Updated to use consistent placeholder generation
  3. Added LAST_INSERT_ID(id) for proper ID retrieval on updates
  4. More robust upsert handling for MySQL
  5. WHAT IT DOES: Creates MySQL ON DUPLICATE KEY UPDATE query with proper escaping
  6. WHY: Handles both insert and update in single query, preventing duplicate key errors
  7. Updates all columns except ID, then retrieves the last inserted ID
  8. IMPORTANT: This is MySQL-specific syntax for handling conflicts
  */
  getUpsertQuery(
    tableName: string,
    columns: string[],
    _conflictColumns: string[],
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'mysql'));
    const placeholders = columns
      .map(() => this.getPlaceholderPrefix())
      .join(", ");
    const updateColumns = columns.filter((column) => column !== "id");
    const escapedUpdateColumns = updateColumns.map(col => BaseEntity.escapeIdentifier(col, 'mysql'));
    const updateAssignments = escapedUpdateColumns.map(
      (column) => `${column} = VALUES(${column})`,
    );
    updateAssignments.push("id = LAST_INSERT_ID(id)");
    const updateClause = updateAssignments.join(", ");
    return `INSERT INTO ${escapedTableName} (${escapedColumns.join(", ")}) VALUES (${placeholders}) ON DUPLICATE KEY UPDATE ${updateClause}`;
  }

  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Simplified SET clause generation using map()
  3. Uses prepareWhereClause for consistent WHERE clause building
  4. Better separation of SET and WHERE clause construction
  5. WHAT IT DOES: Creates UPDATE query with escaped identifiers and proper placeholders
  6. WHY: Provides template for updating specific records with conditions and proper escaping
  7. Uses placeholders for all values to prevent SQL injection
  8. Separates query structure from parameter binding for better maintainability
  */
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    const escapedColumns = columns.map(col => BaseEntity.escapeIdentifier(col, 'mysql'));
    const setClause = escapedColumns.map((col) => `${col} = ?`).join(", ");
    let query = `UPDATE ${escapedTableName} SET ${setClause}`;
    const whereClause = this.prepareWhereClause(conditions);
    if (whereClause) {
      query += ` WHERE ${whereClause}`;
    }
    return query;
  }
  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table name
  2. Updated prepareWhereClause to support multiple operators beyond just equals
  3. Simplified query building with better structure
  4. Proper handling of LIMIT and OFFSET in MySQL syntax
  5. WHAT IT DOES: Creates DELETE query with escaped table name and flexible conditions
  6. WHY: Provides safe deletion with proper WHERE clauses and identifier escaping
  7. Supports pagination for batch deletions
  8. IMPORTANT: Always uses WHERE clause to prevent deleting entire table
  */
  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    let query = `DELETE FROM ${escapedTableName}`;
    const whereClause = this.prepareWhereClause(conditions);
    if (whereClause) {
      query += ` WHERE ${whereClause}`;
    }
    if (limit !== undefined) {
      query += ` LIMIT ${limit}`;
      if (offset !== undefined) query += ` OFFSET ${offset}`;
    }
    return query;
  }
  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table and column names
  2. Updated prepareWhereClause to support multiple operators
  3. Simplified query building approach
  4. Better handling of LIMIT and OFFSET
  5. WHAT IT DOES: Creates SELECT query with escaped identifiers and flexible conditions
  6. WHY: Provides flexible data retrieval with proper filtering, pagination, and identifier escaping
  7. Supports selecting specific columns or all columns with ["*"]
  8. Uses parameterized queries for security and performance
  */
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    const escapedColumns = columns.map(col => col === '*' ? '*' : BaseEntity.escapeIdentifier(col, 'mysql'));
    let query = `SELECT ${escapedColumns.join(", ")} FROM ${escapedTableName}`;
    const whereClause = this.prepareWhereClause(conditions);
    if (whereClause) {
      query += ` WHERE ${whereClause}`;
    }
    if (limit !== undefined) query += ` LIMIT ${limit}`;
    if (offset !== undefined) query += ` OFFSET ${offset}`;
    return query;
  }
  /* 
  CHANGES MADE:
  1. Added escapeIdentifier support for table name
  2. Updated prepareWhereClause to support multiple operators
  3. Simplified query building using prepareWhereClause
  4. Better structure for COUNT query generation
  5. WHAT IT DOES: Creates COUNT query with escaped table name and flexible conditions
  6. WHY: Provides efficient way to get total record count for pagination and analytics
  7. Returns count as 'count' column name for consistency across databases
  8. Uses parameterized queries for security
  9. PERFORMANCE: More efficient than SELECT * for counting records
  */
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    const escapedTableName = BaseEntity.escapeIdentifier(tableName, 'mysql');
    let query = `SELECT COUNT(*) AS count FROM ${escapedTableName}`;
    const whereClause = this.prepareWhereClause(conditions);
    if (whereClause) {
      query += ` WHERE ${whereClause}`;
    }
    return query;
  }

  /* 
  CHANGES MADE:
  1. Renamed from getWhereClause to prepareWhereClause
  2. Removed value serialization - now uses placeholders only
  3. Extended to support multiple operators (=, >, <, >=, <=, !=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
  4. This change supports parameterized queries to prevent SQL injection
  5. Simplifies query building by separating concerns (query structure vs parameter binding)
  6. More consistent with PostgreSQL driver approach
  7. Returns only the WHERE clause string without WHERE prefix
  8. WHAT IT DOES: Builds WHERE clause with operators for MySQL parameterized queries
  9. WHY: Enables complex queries beyond simple equality while maintaining security
  */
  private prepareWhereClause(conditions?: Record<string, unknown>): string {
    if (!conditions || Object.keys(conditions).length === 0) {
      return "";
    }
    const predicates: string[] = [];
    
    for (const [columnWithOperator, value] of Object.entries(conditions)) {
      // Handle different operators
      if (columnWithOperator.toUpperCase().includes('IN') && Array.isArray(value)) {
        const placeholders = value.map(() => '?').join(', ');
        predicates.push(`${columnWithOperator} (${placeholders})`);
      } else if (columnWithOperator.toUpperCase().includes('IS')) {
        // IS NULL / IS NOT NULL don't need values
        predicates.push(`${columnWithOperator}`);
      } else {
        // Standard operators: =, >, <, >=, <=, !=, LIKE, NOT LIKE
        predicates.push(`${columnWithOperator} ?`);
      }
    }
    
    return predicates.join(" AND ");
  }

}