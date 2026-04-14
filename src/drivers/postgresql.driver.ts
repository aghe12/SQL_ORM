import type { IDatabaseDriver } from "../core/db.js";

export class PostgreSqlDriver implements IDatabaseDriver {
  connect(): Promise<void> {
    console.log("[SIMULATING]: Connecting to MySQL database...");
    return Promise.resolve();
  }
  disconnect(): Promise<void> {
    console.log("[SIMULATING]: Disconnecting from MySQL database...");
    return Promise.resolve();
  }
  execute(query: string, params?: any[]): Promise<any> {
    console.log("[SIMULATING]: Executing query...", query, params);
    return Promise.resolve();
  }

  getPlaceholderPrefix(): string {
    return "$";
  }
  getNumberedPlaceholder(index: number): string {
    return `${this.getPlaceholderPrefix()}${index}`;
  }
  getInsertQuery(tableName: string, columns: string[]): string {
    const placeholders = columns
      .map((_, i) => this.getNumberedPlaceholder(i + 1))
      .join(", ");
    return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders})`;
  }
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    console.log(
      "[SIMULATING]: Updating query...",
      tableName,
      columns,
      conditions,
    );
    return "";
  }
  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    console.log(
      "[SIMULATING]: Deleting query...",
      tableName,
      conditions,
      limit,
      offset,
    );
    return "";
  }
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    console.log(
      "[SIMULATING]: Selecting query...",
      tableName,
      columns,
      conditions,
      limit,
      offset,
    );
    return "";
  }
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    console.log("[SIMULATING]: Counting query...", tableName, conditions);
    return "";
  }
}
