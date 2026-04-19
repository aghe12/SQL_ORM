import type { IDatabaseDriver } from "../core/db.js";
import { Client, Connection, type ConnectionConfig } from "pg";

export class PostgreSqlDriver implements IDatabaseDriver {
  client: Client | null = null;
  connectionConfig: string | ConnectionConfig;

  constructor(connectionConfig: string | ConnectionConfig) {
    this.connectionConfig = connectionConfig;
  }
  getUpsertQuery(tableName: string, columns: string[], conflictColumns: string[]): string {
    throw new Error("Method not implemented.");
  }

  async connect(): Promise<void> {
    if (this.client) {
      return;
    }
    this.client = new Client(this.connectionConfig);
    await this.client.connect();
  }
  async disconnect(): Promise<void> {
    if (!this.client) {
      return;
    }
    await this.client.end();
  }

  async execute(query: string, params?: any[]): Promise<any> {
    if (!this.client) {
      return;
    }
    return await this.client.query(query, params);
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

  getWhereStatement(
    conditions: Record<string, unknown> | undefined,
    index: number,
  ): string {
    const conditionPlaceHolder = conditions
      ? Object.keys(conditions)
          .map((condition) => {
            return `${condition} = ${this.getNumberedPlaceholder(index++)}`;
          })
          .join(" AND ")
      : "";
    return conditions && Object.keys(conditions).length > 0
      ? ` WHERE ${conditionPlaceHolder}`
      : "";
  }

  getLimitStatement(limit?: number): string {
    return limit ? ` LIMIT ${limit}` : "";
  }

  getOffsetStatement(offset?: number): string {
    return offset ? `OFFSET ${offset}` : "";
  }

  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    let index = 1;
    const columnPlaceholders = columns
      .map((column) => `${column} = ${this.getNumberedPlaceholder(index++)}`)
      .join(", ");
    return `UPDATE ${tableName} SET ${columnPlaceholders}${this.getWhereStatement(conditions, index)}`;
  }
  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
  ): string {
    return `DELETE FROM ${tableName}${this.getWhereStatement(conditions, 1)}`;
  }
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    return `SELECT ${columns.join(", ")}
FROM ${tableName}${this.getWhereStatement(conditions, 1)}${this.getLimitStatement(limit)}${this.getOffsetStatement(offset)}`;
  }

  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string {
    return `SELECT COUNT(*) 
FROM ${tableName}${this.getWhereStatement(conditions, 1)}`;
  }
}