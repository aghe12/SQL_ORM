import { TABLE_METADATA_KEY } from "./table.decorator.js";

export interface IBaseEntity {
  id: number;

  createdAt: Date;
  createdBy: number;
  updatedAt: Date;
  updatedBy: number;
}

export interface PaginationOptions {
  limit?: number;
  offset?: number;
}

export abstract class BaseEntity implements IBaseEntity {
  id: number;

  createdAt: Date;
  createdBy: number;
  updatedAt: Date;
  updatedBy: number;

  constructor(entity: IBaseEntity) {
    this.id = entity.id;
    this.createdAt = entity.createdAt;
    this.createdBy = entity.createdBy;
    this.updatedAt = entity.updatedAt;
    this.updatedBy = entity.updatedBy;
  }

  static getTableName(): string {
    return Reflect.getMetadata(TABLE_METADATA_KEY, this);
  }

  async save(): Promise<void> {
    if (!this.id) {
      const keys = Object.keys(this);
      const columns = keys.join(", ");
      const values_placeholder = "?, ".repeat(keys.length).slice(0, -2);
      const query = `INSERT INTO ${(this.constructor as typeof BaseEntity).getTableName()} (${columns}) VALUES (${values_placeholder})`;
      await db.execute(query, Object.values(this));
    } else {
      const keys = Object.keys(this).filter((key) => key !== "id");
      const setClause = keys.map((key) => `${key} = ?`).join(", ");
      const query = `UPDATE ${(this.constructor as typeof BaseEntity).getTableName()} SET ${setClause} WHERE id = ?`;
      await db.execute(query, [
        ...Object.values(this).filter((_, index) => index !== 0),
        this.id,
      ]);
    }
  }

  static async count<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions?: Partial<I>,
  ): Promise<number> {
    const tableName = Reflect.getMetadata(TABLE_METADATA_KEY, this);
    const values: unknown[] = [];

    let query = `SELECT COUNT(*) as count FROM ${tableName}`;

    if (conditions && Object.keys(conditions).length > 0) {
      const whereClause = Object.keys(conditions)
        .map((key) => `${key} = ?`)
        .join(" AND ");
      query += ` WHERE ${whereClause}`;
      values.push(...Object.values(conditions));
    }

    const result = await db.execute(query, values);
    return result[0].count;
  }

  // base for findOne and findAll
  static async findAll<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions?: Partial<I>,
    pagination?: PaginationOptions,
  ): Promise<T[]> {
    const tableName = Reflect.getMetadata(TABLE_METADATA_KEY, this);
    const values: unknown[] = [];

    let query = `SELECT * FROM ${tableName}`;

    if (conditions && Object.keys(conditions).length > 0) {
      const whereClause = Object.keys(conditions)
        .map((key) => `${key} = ?`)
        .join(" AND ");
      query += ` WHERE ${whereClause}`;
      values.push(...Object.values(conditions));
    }

    if (pagination?.limit !== undefined) {
      query += ` LIMIT ?`;
      values.push(pagination.limit);

      if (pagination?.offset !== undefined) {
        query += ` OFFSET ?`;
        values.push(pagination.offset);
      }
    }

    const result = await db.execute(query, values);
    return result.map((row: I) => new this(row));
  }

  // reuses findAll with LIMIT 1
  static async findOne<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions: Partial<I>,
  ): Promise<T | null> {
    const result = await (this as any).findAll(conditions, { limit: 1 });
    return result.length > 0 ? result[0] : null;
  }

  // reuses findOne with id condition
  static async findById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
  ): Promise<T | null> {
    return (this as any).findOne({ id } as Partial<I>);
  }

  // base for deleteOne and deleteAll
  static async deleteAll<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions?: Partial<I>,
    limit?: number,
  ): Promise<number> {
    const tableName = Reflect.getMetadata(TABLE_METADATA_KEY, this);
    const values: unknown[] = [];

    let query = `DELETE FROM ${tableName}`;

    if (conditions && Object.keys(conditions).length > 0) {
      const whereClause = Object.keys(conditions)
        .map((key) => `${key} = ?`)
        .join(" AND ");
      query += ` WHERE ${whereClause}`;
      values.push(...Object.values(conditions));
    }

    if (limit !== undefined) {
      query += ` LIMIT ?`;
      values.push(limit);
    }

    const result = await db.execute(query, values);
    return result.affectedRows;
  }

  // reuses deleteAll with LIMIT 1
  static async deleteOne<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions: Partial<I>,
  ): Promise<number> {
    return (this as any).deleteAll(conditions, 1);
  }

  // reuses deleteAll with id condition and LIMIT 1
  static async deleteById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
  ): Promise<boolean> {
    const deleted = await (this as any).deleteAll({ id } as Partial<I>, 1);
    return deleted > 0;
  }
}
