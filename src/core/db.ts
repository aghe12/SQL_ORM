export interface IDatabaseDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  execute(query: string, params?: any[]): Promise<any>;

  getPlaceholderPrefix(): string;
  getInsertQuery(tableName: string, columns: string[]): string;
  getUpdateQuery(
    tableName: string,
    columns: string[],
    conditions: Record<string, unknown>,
  ): string;
  getDeleteQuery(
    tableName: string,
    conditions: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string;
  getSelectQuery(
    tableName: string,
    columns: string[],
    conditions?: Record<string, unknown>,
    limit?: number,
    offset?: number,
  ): string;
  getCountQuery(
    tableName: string,
    conditions?: Record<string, unknown>,
  ): string;
}

export class DB {
  private static instance: IDatabaseDriver;
  static setDriver(driver: IDatabaseDriver) {
    this.instance = driver;
  }
  static get driver(): IDatabaseDriver {
    if (!this.instance) {
      throw new Error("Database driver not set");
    }
    return this.instance;
  }
}
