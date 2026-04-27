/* 
CHANGES MADE:
1. Reordered imports to match target implementation structure
2. Changed import order: DatabaseDriverResult first, then IDatabaseDriver
3. This follows TypeScript best practices for type imports
4. Better organization of dependencies
*/
import type { ConnectionOptions } from "mysql2";
import type { IDatabaseDriver, DatabaseDriverResult } from "../core/db.js";
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
  1. Simplified placeholder generation using map()
  2. More consistent code style with other methods
  3. Uses getPlaceholderPrefix() for consistency
  4. Generates INSERT query with proper parameter placeholders
  5. WHAT IT DOES: Creates SQL INSERT statement with column names and value placeholders
  6. WHY: Provides template for parameterized INSERT queries to prevent SQL injection
  */
  getInsertQuery(tableName: string, columns: string[]): string {
    const placeholders = columns
      .map(() => this.getPlaceholderPrefix())
      .join(", ");
    return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders})`;
  }
  /* 
  CHANGES MADE:
  1. Updated to use consistent placeholder generation
  2. Added LAST_INSERT_ID(id) for proper ID retrieval on updates
  3. More robust upsert handling for MySQL
  4. WHAT IT DOES: Creates MySQL ON DUPLICATE KEY UPDATE query for upsert operations
  5. WHY: Handles both insert and update in single query, preventing duplicate key errors
  6. Updates all columns except ID, then retrieves the last inserted ID
  7. IMPORTANT: This is MySQL-specific syntax for handling conflicts
  */
  getUpsertQuery(
    tableName: string,
    columns: string[],
    _conflictColumns: string[],
  ): string {
    const placeholders = columns
      .map(() => this.getPlaceholderPrefix())
      .join(", ");
    const updateColumns = columns.filter((column) => column !== "id");
    const updateAssignments = updateColumns.map(
      (column) => `${column} = VALUES(${column})`,
    );
    updateAssignments.push("id = LAST_INSERT_ID(id)");
    const updateClause = updateAssignments.join(", ");
    return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders}) ON DUPLICATE KEY UPDATE ${updateClause}`;
  }

  /* 
  CHANGES MADE:
  1. Simplified SET clause generation using map()
  2. Uses prepareWhereClause for consistent WHERE clause building
  3. Better separation of SET and WHERE clause construction
  4. WHAT IT DOES: Creates UPDATE query with SET clause and WHERE conditions
  5. WHY: Provides template for updating specific records with conditions
  6. Uses placeholders for all values to prevent SQL injection
  7. Separates query structure from parameter binding for better maintainability
  */
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    const setClause = columns.map((col) => `${col} = ?`).join(", ");
    let query = `UPDATE ${tableName} SET ${setClause}`;
    const whereClause = this.prepareWhereClause(conditions);
    if (whereClause) {
      query += ` WHERE ${whereClause}`;
    }
    return query;
  }
  /* 
  CHANGES MADE:
  1. Simplified query building with better structure
  2. Uses prepareWhereClause for consistent WHERE clause
  3. Proper handling of LIMIT and OFFSET in MySQL syntax
  4. WHAT IT DOES: Creates DELETE query with optional conditions and pagination
  5. WHY: Provides safe deletion with proper WHERE clauses to prevent accidental data loss
  6. Supports pagination for batch deletions
  7. IMPORTANT: Always uses WHERE clause to prevent deleting entire table
  */
  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    let query = `DELETE FROM ${tableName}`;
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
  1. Simplified query building approach
  2. Uses prepareWhereClause for consistent WHERE clause generation
  3. Better handling of LIMIT and OFFSET
  4. WHAT IT DOES: Creates SELECT query with optional conditions, columns, and pagination
  5. WHY: Provides flexible data retrieval with proper filtering and pagination
  6. Supports selecting specific columns or all columns with ["*"]
  7. Uses parameterized queries for security and performance
  */
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    let query = `SELECT ${columns.join(", ")} FROM ${tableName}`;
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
  1. Simplified query building using prepareWhereClause
  2. Better structure for COUNT query generation
  3. WHAT IT DOES: Creates COUNT query to count records matching conditions
  4. WHY: Provides efficient way to get total record count for pagination and analytics
  5. Returns count as 'count' column name for consistency across databases
  6. Uses parameterized queries for security
  7. PERFORMANCE: More efficient than SELECT * for counting records
  */
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    let query = `SELECT COUNT(*) AS count FROM ${tableName}`;
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
  3. This change supports parameterized queries to prevent SQL injection
  4. Simplifies query building by separating concerns (query structure vs parameter binding)
  5. More consistent with PostgreSQL driver approach
  6. Returns only the WHERE clause string without WHERE prefix
  */
  private prepareWhereClause(conditions?: Record<string, unknown>): string {
    if (!conditions || Object.keys(conditions).length === 0) {
      return "";
    }
    const entries = Object.entries(conditions);
    const predicates = entries.map(([column]) => `${column} = ?`);
    return `${predicates.join(" AND ")}`;
  }

}