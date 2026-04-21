import { DB } from "./db.js";
import { Column, getColumnSqlName } from "./column.decorator.js";
import { TABLE_METADATA_KEY } from "./table.decorator.js";


export interface IBaseEntity {
    id?: number | undefined;

    createdAt: Date;
    createdBy: number;
    updatedAt: Date;
    updatedBy: number;
}

export abstract class BaseEntity implements IBaseEntity {

    @Column()
    id?: number | undefined;

    @Column()
    createdAt: Date;
    @Column()
    createdBy: number;
    @Column()
    updatedAt: Date;
    @Column()
    updatedBy: number;

    constructor(entity: IBaseEntity) {
        this.id = entity.id;
        this.createdAt = entity.createdAt;
        this.createdBy = entity.createdBy;
        this.updatedAt = entity.updatedAt;
        this.updatedBy = entity.updatedBy;
    }

    async save(): Promise<void> {
        const ctor = this.constructor;
        const proto = Object.getPrototypeOf(this) as object;
        const tableName = Reflect.getMetadata(TABLE_METADATA_KEY, ctor) as string;
        const propertyValues = Object.keys(this).reduce<Record<string, unknown>>((acc, key) => {
            acc[key] = (this as any)[key];
            return acc;
        }, {});
        const persistableValues = Object.entries(propertyValues).reduce<Record<string, unknown>>((acc, [key, value]) => {
            if (value !== undefined) {
                acc[key] = value;
            }
            return acc;
        }, {});
        const mappedValues = BaseEntity.mapPropertyKeysToDbColumns(proto, persistableValues);
        const columns = Object.keys(mappedValues);
        if (columns.length === 0) {
            throw new Error("Cannot save entity without any mapped columns");
        }

        const values = Object.values(mappedValues);
        const query = DB.driver.getUpsertQuery(tableName, columns, ["id"]);
        const result = await DB.driver.execute(query, values);
        const resolvedId = BaseEntity.resolveNumericId(result.insertedId ?? result.rows[0]?.id);
        if (resolvedId !== undefined) {
            this.id = resolvedId;
        }

        const returnedRow = result.rows[0];
        if (returnedRow) {
            this.hydrateFromRow(proto, returnedRow);
            return;
        }

        await this.reloadCurrentState(tableName, proto);
    }
    static async findAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions?: Record<string, unknown>, limit?: number, offset?: number): Promise<T[]> {
        const dbConditions = conditions ? BaseEntity.mapPropertyKeysToDbColumns(this.prototype, conditions) : undefined;
        const query = DB.driver.getSelectQuery(Reflect.getMetadata(TABLE_METADATA_KEY, this), ['*'], dbConditions, limit, offset);
        const params =
            dbConditions !== undefined
                ? Object.keys(dbConditions).map((k) => dbConditions[k]!)
                : undefined;
        const result = await DB.driver.execute(query, params);
        return result.rows.map((row) => new this(row as I));
    }
    static async findOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Record<string, unknown>): Promise<T | null> {
        const results = await (this as any).findAll(conditions);
        return results.length > 0 ? results[0] : null;
    }
    static async findById<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, id: number): Promise<T | null> {
        return await (this as any).findOne({ id });
    }
    static async deleteAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Record<string, unknown>, limit?: number, offset?: number): Promise<number> {
        const dbConditions = BaseEntity.mapPropertyKeysToDbColumns(this.prototype, conditions);
        const query = DB.driver.getDeleteQuery(Reflect.getMetadata(TABLE_METADATA_KEY, this), dbConditions, limit, offset);
        const params = Object.keys(dbConditions).map((k) => dbConditions[k]!);
        const result = await DB.driver.execute(query, params);
        return result.affectedRows;
    }
    static async deleteOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Record<string, unknown>): Promise<boolean> {
        const affectedRows = await (this as any).deleteAll(conditions, 1);
        return affectedRows > 0;
    }
    static async deleteById<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, id: number): Promise<boolean> {
        return await (this as any).deleteOne({ id });
    }
    static async count<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions?: Record<string, unknown>): Promise<number> {
        const dbConditions = conditions ? BaseEntity.mapPropertyKeysToDbColumns(this.prototype, conditions) : undefined;
        const query = DB.driver.getCountQuery(Reflect.getMetadata(TABLE_METADATA_KEY, this), dbConditions);
        const params =
            dbConditions !== undefined
                ? Object.keys(dbConditions).map((k) => dbConditions[k]!)
                : undefined;
        const result = await DB.driver.execute(query, params);
        return Number(result.rows[0]?.count ?? 0);
    }
    static async updateAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, updates: Record<string, unknown>, conditions: Record<string, unknown>): Promise<number> {
        const dbUpdates = BaseEntity.mapPropertyKeysToDbColumns(this.prototype, updates);
        const dbConditions = BaseEntity.mapPropertyKeysToDbColumns(this.prototype, conditions);
        const query = DB.driver.getUpdateQuery(Reflect.getMetadata(TABLE_METADATA_KEY, this), Object.keys(dbUpdates), dbConditions);
        const params = [
            ...Object.keys(dbUpdates).map((k) => dbUpdates[k]!),
            ...Object.keys(dbConditions).map((k) => dbConditions[k]!),
        ];
        const result = await DB.driver.execute(query, params);
        return result.affectedRows;
    }
    static async updateById<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, id: number, updates: Record<string, unknown>): Promise<boolean> {
        const affectedRows = await (this as any).updateAll(updates, { id });
        return affectedRows > 0;
    }

    private static mapPropertyKeysToDbColumns(
        prototype: object,
        values: Record<string, unknown>
    ): Record<string, unknown> {
        const mapped: Record<string, unknown> = {};
        for (const [propertyName, value] of Object.entries(values)) {
            const metadata = getColumnSqlName(prototype, propertyName);
            const dbColumnName = metadata.dbColumnName;
            if (!dbColumnName) {
                continue;
            }
            mapped[dbColumnName] = value;
        }

        return mapped;
    }

    private async reloadCurrentState(tableName: string, prototype: object): Promise<void> {
        const entityId = BaseEntity.resolveNumericId(this.id);
        if (entityId === undefined) {
            throw new Error("Cannot reload entity after save without an id");
        }

        const mappedId = BaseEntity.mapPropertyKeysToDbColumns(prototype, { id: entityId });
        const query = DB.driver.getSelectQuery(tableName, ["*"], mappedId, 1);
        const result = await DB.driver.execute(query, Object.keys(mappedId).map((k) => mappedId[k]!));
        const row = result.rows[0];
        if (!row) {
            throw new Error(`Unable to reload entity with id ${entityId} after save`);
        }

        this.hydrateFromRow(prototype, row);
    }

    private static resolveNumericId(value: unknown): number | undefined {
        if (typeof value === "number" && Number.isFinite(value)) {
            return value;
        }
        if (typeof value === "string" && value.trim() !== "" && !Number.isNaN(Number(value))) {
            return Number(value);
        }
        return undefined;
    }

    private hydrateFromRow(prototype: object, row: Record<string, unknown>): void {
        const propertyToColumn = Object.keys(this).reduce<Record<string, string>>((acc, propertyName) => {
            const metadata = getColumnSqlName(prototype, propertyName);
            if (metadata.dbColumnName) {
                acc[metadata.dbColumnName] = propertyName;
            }
            return acc;
        }, {});

        for (const [columnName, value] of Object.entries(row)) {
            const propertyName = propertyToColumn[columnName] ?? columnName;
            if (propertyName in this) {
                (this as Record<string, unknown>)[propertyName] = value;
            }
        }
    }
}