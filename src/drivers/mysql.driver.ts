import type { ConnectionOptions } from "mysql2";
import type { ResultSetHeader } from "mysql2";
import type { DatabaseDriverResult, IDatabaseDriver } from "../core/db.js";
import { createConnection, type Connection } from "mysql2/promise";

export class MySqlDriver implements IDatabaseDriver {
  private connection: Connection | null = null;
  private connectionConfig: string | ConnectionOptions;

  constructor(connectionConfig: string | ConnectionOptions) {
    this.connectionConfig = connectionConfig;
  }

  getUpsertQuery(tableName: string, columns: string[], conflictColumns: string[]): string {
    const placeholders = columns.map(() => "?").join(", ");
    const updateCols = columns.filter((c) => !conflictColumns.includes(c));
    const assignments =
      updateCols.length > 0
        ? updateCols.map((c) => `\`${this.escapeIdentifier(c)}\` = VALUES(\`${this.escapeIdentifier(c)}\`)`).join(", ")
        : conflictColumns.map((c) => `\`${this.escapeIdentifier(c)}\` = VALUES(\`${this.escapeIdentifier(c)}\`)`).join(", ");
    const colList = columns.map((c) => `\`${this.escapeIdentifier(c)}\``).join(", ");
    return `INSERT INTO ${tableName} (${colList}) VALUES (${placeholders}) ON DUPLICATE KEY UPDATE ${assignments}`;
  }

  /** Backtick reserved / mixed-case columns (e.g. createdAt). */
  private escapeIdentifier(name: string): string {
    return name.replace(/`/g, "``");
  }

  private getWhereFragment(
    conditions: Record<string, unknown> | undefined,
  ): { sql: string; values: unknown[] } {
    if (!conditions || Object.keys(conditions).length === 0) {
      return { sql: "", values: [] };
    }
    const keys = Object.keys(conditions);
    const fragments = keys.map((k) => `\`${this.escapeIdentifier(k)}\` = ?`);
    const values = keys.map((k) => conditions[k]);
    return { sql: ` WHERE ${fragments.join(" AND ")}`, values };
  }

  async connect(): Promise<void> {
    if (this.connection) {
      return;
    }
    this.connection = await (typeof this.connectionConfig === "string"
      ? createConnection(this.connectionConfig)
      : createConnection(this.connectionConfig));
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
    const [results] = await this.connection.execute(query, params as never[] | undefined);
    if (Array.isArray(results)) {
      const out: DatabaseDriverResult = {
        rows: results as Record<string, unknown>[],
        affectedRows: results.length,
      };
      return out;
    }
    const header = results as ResultSetHeader;
    const out: DatabaseDriverResult = {
      rows: [],
      affectedRows: header.affectedRows ?? 0,
    };
    if (header.insertId !== undefined && header.insertId !== null) {
      out.insertedId = Number(header.insertId);
    }
    return out;
  }

  getPlaceholderPrefix(): string {
    return "?";
  }
  getInsertQuery(tableName: string, columns: string[]): string {
    const quoted = columns.map((c) => `\`${this.escapeIdentifier(c)}\``).join(", ");
    const placeholders = columns.map(() => "?").join(", ");
    return `INSERT INTO ${tableName} (${quoted}) VALUES (${placeholders})`;
  }

  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string {
    const setClause = columns
      .map((c) => `\`${this.escapeIdentifier(c)}\` = ?`)
      .join(", ");
    const { sql: whereSql } = this.getWhereFragment(conditions);
    return `UPDATE ${tableName} SET ${setClause}${whereSql}`;
  }

  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    _offset?: number,
  ): string {
    const { sql: whereSql } = this.getWhereFragment(conditions);
    let sql = `DELETE FROM ${tableName}${whereSql}`;
    if (limit !== undefined) {
      sql += ` LIMIT ${Number(limit)}`;
    }
    return sql;
  }

  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string {
    const colList = columns.map((c) => (c === "*" ? "*" : `\`${this.escapeIdentifier(c)}\``)).join(", ");
    const { sql: whereSql } = this.getWhereFragment(conditions);
    let sql = `SELECT ${colList} FROM ${tableName}${whereSql}`;
    if (limit !== undefined) {
      sql += ` LIMIT ${Number(limit)}`;
    }
    if (offset !== undefined) {
      sql += ` OFFSET ${Number(offset)}`;
    }
    return sql;
  }

  getCountQuery(tableName: string, conditions?: Record<string, unknown>): string {
    const { sql: whereSql } = this.getWhereFragment(conditions);
    return `SELECT COUNT(*) AS \`count\` FROM ${tableName}${whereSql}`;
  }
}
