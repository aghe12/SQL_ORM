import type { DatabaseDriverResult, IDatabaseDriver } from "../core/db.js";
import { Client, type ClientConfig } from "pg";

export class PostgreSqlDriver implements IDatabaseDriver {
  private client: Client | null = null;
  private config: string | ClientConfig;

  constructor(config: string | ClientConfig) {
    this.config = config;
  }
  getPlaceholderPrefix(): string {
    return "$";
  }

  async connect(): Promise<void> {
    if (this.client) return;

    this.client = new Client(this.config);
    await this.client.connect();

    // check connection
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

  private getPlaceholder(index: number): string {
    return `$${index}`;
  }

  private buildWhereClause(
    conditions?: Record<string, unknown>,
    startIndex: number = 1
  ): { clause: string; nextIndex: number } {
    if (!conditions || Object.keys(conditions).length === 0) {
      return { clause: "", nextIndex: startIndex };
    }

    let index = startIndex;

    const clause =
      " WHERE " +
      Object.keys(conditions)
        .map((key) => `${key} = ${this.getPlaceholder(index++)}`)
        .join(" AND ");

    return { clause, nextIndex: index };
  }

  getInsertQuery(tableName: string, columns: string[]): string {
    const placeholders = columns
      .map((_, i) => this.getPlaceholder(i + 1))
      .join(", ");

    return `INSERT INTO ${tableName} (${columns.join(
      ", "
    )}) VALUES (${placeholders}) RETURNING *`;
  }

  getUpsertQuery(
    tableName: string,
    columns: string[],
    conflictColumns: string[]
  ): string {
    const placeholders = columns
      .map((_, i) => this.getPlaceholder(i + 1))
      .join(", ");

    const updateColumns = columns.filter(
      (col) => !conflictColumns.includes(col)
    );

    const updateClause =
      updateColumns.length > 0
        ? `DO UPDATE SET ${updateColumns
            .map((col) => `${col} = EXCLUDED.${col}`)
            .join(", ")}`
        : "DO NOTHING";

    return `INSERT INTO ${tableName} (${columns.join(
      ", "
    )}) VALUES (${placeholders}) ON CONFLICT (${conflictColumns.join(
      ", "
    )}) ${updateClause} RETURNING *`;
  }

  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>
  ): string {
    if (!conditions || Object.keys(conditions).length === 0) {
      throw new Error("Update requires conditions");
    }

    let index = 1;

    const setClause = columns
      .map((col) => `${col} = ${this.getPlaceholder(index++)}`)
      .join(", ");

    const { clause } = this.buildWhereClause(conditions, index);

    return `UPDATE ${tableName} SET ${setClause}${clause} RETURNING *`;
  }

  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    offset?: number
  ): string {
    if (!conditions || Object.keys(conditions).length === 0) {
      throw new Error("Delete requires conditions");
    }

    const where = Object.keys(conditions)
      .map((key, i) => `${key} = ${this.getPlaceholder(i + 1)}`)
      .join(" AND ");

    // PostgreSQL safe delete with limit
    if (limit !== undefined) {
      const offsetClause = offset !== undefined ? ` OFFSET ${offset}` : "";

      return `DELETE FROM ${tableName}
WHERE ctid IN (
  SELECT ctid FROM ${tableName} WHERE ${where} LIMIT ${limit}${offsetClause}
)
RETURNING *`;
    }

    return `DELETE FROM ${tableName} WHERE ${where} RETURNING *`;
  }

  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number
  ): string {
    const { clause } = this.buildWhereClause(conditions);

    let query = `SELECT ${columns.join(", ")} FROM ${tableName}${clause}`;

    if (limit !== undefined) {
      query += ` LIMIT ${limit}`;
    }

    if (offset !== undefined) {
      query += ` OFFSET ${offset}`;
    }

    return query;
  }

  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>
  ): string {
    const { clause } = this.buildWhereClause(conditions);

    return `SELECT COUNT(*) AS count FROM ${tableName}${clause}`;
  }
}