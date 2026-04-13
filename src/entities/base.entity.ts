import { TABLE_METADATA_KEY } from "./table.decorator.js";

export interface IBaseEntity {
  id: number;

  createdAt: Date;
  createdBy: number;
  updatedAt: Date;
  updatedBy: number;
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

  // TASK: update this to insert / update
  async save(): Promise<void> {
    if (!this.id) {
      const keys = Object.keys(this);
      const columns = keys.join(", ");
      const values_placeholder = "?, ".repeat(keys.length).slice(0, -2);
      const query = `INSERT INTO ${(this.constructor as typeof BaseEntity).getTableName()} (${columns}) VALUES (${values_placeholder})`;
      await db.execute(query, Object.values(this));
    
    } else {
      const keys = Object.keys(this).filter(key => key !== 'id');
      const setClause = keys.map(key => `${key} = ?`).join(", ");
      const query = `UPDATE ${(this.constructor as typeof BaseEntity).getTableName()} SET ${setClause} WHERE id = ?`;
      await db.execute(query, [...Object.values(this).filter((_, index) => index !== 0), this.id]);

    }
  }

  static async findById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
  ): Promise<T | null> {
    const query = `SELECT * FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)} WHERE id = ?`;
    const result = await db.execute(query, [id]);
    const instance = new this(result[0]);
    return instance;

  }
  // TASKS:
  // static async findAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T): Promise<T[]>
  // static async findOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Partial<I>): Promise<T | null>

  // static async deleteById<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T): Promise<T[]>
  // static async deleteAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T): Promise<T[]>
  // static async deleteOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Partial<I>): Promise<T | null>

  static async findAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T): Promise<T[]> {
    const query = `SELECT * FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)}`;
    const result = await db.execute(query);
    return result.map((row: I) => new this(row));
  }

  static async findOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Partial<I>): Promise<T | null> {
    const whereClause = Object.keys(conditions).map(key => `${key} = ?`).join(' AND ');
    const query = `SELECT * FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)} WHERE ${whereClause}`;
    const result = await db.execute(query, Object.values(conditions));
    return result.length > 0 ? new this(result[0]) : null;
  }
    static async deleteById<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, id: number): Promise<boolean> {
    const query = `DELETE FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)} WHERE id = ?`;
    const result = await db.execute(query, [id]);
    return result.affectedRows > 0;
  }

  static async deleteAll<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T): Promise<boolean> {
    const query = `DELETE FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)}`;
    const result = await db.execute(query);
    return result.affectedRows > 0;
  }

  static async deleteOne<T extends BaseEntity, I extends IBaseEntity>(this: new (entity: I) => T, conditions: Partial<I>): Promise<boolean> {
    const whereClause = Object.keys(conditions).map(key => `${key} = ?`).join(' AND ');
    const query = `DELETE FROM ${Reflect.getMetadata(TABLE_METADATA_KEY, this)} WHERE ${whereClause}`;
    const result = await db.execute(query, Object.values(conditions));
    return result.affectedRows > 0;
  }

}
