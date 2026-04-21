import type { ConnectionOptions } from "mysql2";
import type { DatabaseDriverResult, IDatabaseDriver } from "../core/db.js";
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
        return '?';
    }
    getInsertQuery(tableName: string, columns: string[]): string {
        const placeholders = columns.map(() => this.getPlaceholderPrefix()).join(', ');
        return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${placeholders})`;
    }
    getUpsertQuery(tableName: string, columns: string[], _conflictColumns: string[]): string {
        const placeholders = columns.map(() => this.getPlaceholderPrefix()).join(", ");
        const updateColumns = columns.filter((column) => column !== "id");
        const updateAssignments = updateColumns.map((column) => `${column} = VALUES(${column})`);
        updateAssignments.push("id = LAST_INSERT_ID(id)");
        const updateClause = updateAssignments.join(", ");
        return `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders}) ON DUPLICATE KEY UPDATE ${updateClause}`;
    }

    getUpdateQuery(tableName: string, columns: string[], conditions: Record<string, unknown>): string {
        const setClause = columns.map((column) => `${column} = ${this.getPlaceholderPrefix()}`).join(", ");
        const conditionKeys = Object.keys(conditions);
        const whereClause = conditionKeys.length > 0
            ? ` WHERE ${conditionKeys.map((column) => `${column} = ${this.getPlaceholderPrefix()}`).join(" AND ")}`
            : "";

        return `UPDATE ${tableName} SET ${setClause}${whereClause}`;
    }
    getDeleteQuery(tableName: string, conditions: Record<string, unknown>, limit?: number, offset?: number): string {
        const whereClause = this.getWhereClause(conditions);
        const query = [`DELETE FROM ${tableName}${whereClause}`];

        if (limit !== undefined && offset !== undefined) {
            query.push(`LIMIT ${offset}, ${limit}`);
        } else if (limit !== undefined) {
            query.push(`LIMIT ${limit}`);
        }

        return query.join(" ");
    }
    getSelectQuery(tableName: string, columns: string[], conditions?: Record<string, unknown>, limit?: number, offset?: number): string {
        const whereClause = this.getWhereClause(conditions);
        const query = [`SELECT ${columns.join(", ")} FROM ${tableName}${whereClause}`];

        if (limit !== undefined) {
            query.push(`LIMIT ${limit}`);
        } else if (offset !== undefined) {
            query.push("LIMIT 18446744073709551615");
        }

        if (offset !== undefined) {
            query.push(`OFFSET ${offset}`);
        }

        return query.join(" ");
    }
    getCountQuery(tableName: string, conditions?: Record<string, unknown>): string {
        return `SELECT COUNT(*) AS count FROM ${tableName}${this.getWhereClause(conditions)}`;
    }

    private getWhereClause(conditions?: Record<string, unknown>): string {
        if (!conditions || Object.keys(conditions).length === 0) {
            return "";
        }

        const entries = Object.entries(conditions);
        const predicates = entries.map(([column, value]) => `${column} = ${this.serializeValue(value)}`);
        return ` WHERE ${predicates.join(" AND ")}`;
    }

    private serializeValue(value: unknown): string {
        if (value === null) {
            return "NULL";
        }

        if (typeof value === "number") {
            if (!Number.isFinite(value)) {
                throw new Error(`Invalid numeric value: ${value}`);
            }
            return value.toString();
        }

        if (typeof value === "boolean") {
            return value ? "1" : "0";
        }

        if (value instanceof Date) {
            return `'${value.toISOString().replace("T", " ").replace("Z", "")}'`;
        }

        if (typeof value === "bigint") {
            return value.toString();
        }

        const serialized = typeof value === "string" ? value : JSON.stringify(value);
        return `'${serialized.replace(/'/g, "''")}'`;
    }
}