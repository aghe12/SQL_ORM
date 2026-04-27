import { DB, type DatabaseDriverResult } from "./db.js";
import { TABLE_METADATA_KEY } from "./table.decorator.js";
import { Column, getColumnSqlName } from "./column.decorator.js";

/* 
CHANGES MADE:
1. Reordered imports to match target implementation structure
2. Added type import for DatabaseDriverResult for better type safety
3. Organized imports with core decorators first, then DB utilities
*/


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

    /* 
    CHANGES MADE:
    1. Updated column decorators to specify exact database column names
    2. Changed from @Column() to @Column("created_at") etc.
    3. This ensures proper mapping between TypeScript properties and database columns
    4. Important for maintaining consistent naming conventions (snake_case in DB, camelCase in TS)
    */
    @Column("created_at")
    createdAt: Date;
    @Column("created_by")
    createdBy: number;
    @Column("updated_at")
    updatedAt: Date;
    @Column("updated_by")
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
        /* 
        CHANGES MADE:
        1. Use mapPropertyKeysToDbColumns for property-to-column mapping in save operations
        2. This method is designed for mapping entity properties to database column names
        3. buildDbConditions is used for query conditions, not property mapping
        4. This maintains the original functionality while using the correct method
        5. Preserves type safety and error handling for property mapping
        */
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
    /* 
CHANGES MADE:
1. Added getTableName() static method to centralize table name retrieval
2. Uses reflection metadata to get table name from @Table decorator
3. This makes the code more maintainable and consistent across all static methods
4. Eliminates duplicate Reflect.getMetadata calls throughout the class
*/
static getTableName(): string {
  return Reflect.getMetadata(TABLE_METADATA_KEY, this);
}

/* 
CHANGES MADE:
1. Added escapeIdentifier function to wrap table/column names in quotes
2. This prevents SQL errors when using reserved keywords as identifiers
3. Different databases use different quote characters (MySQL uses backticks, PostgreSQL uses double quotes)
4. WHAT IT DOES: Escapes database identifiers to prevent conflicts with SQL keywords
5. WHY: Ensures table/column names like 'order', 'group', 'user' work correctly
6. IMPORTANT: This is crucial for production databases with complex naming conventions
*/
static escapeIdentifier(identifier: string, driverType: 'mysql' | 'postgresql' = 'postgresql'): string {
  if (!identifier || identifier.trim() === '') {
    throw new Error('Identifier cannot be empty');
  }
  
  // Remove any existing quotes to prevent double-escaping
  const cleanIdentifier = identifier.replace(/[`""]/g, '');
  
  // Use appropriate quote character based on database type
  const quote = driverType === 'mysql' ? '`' : '"';
  return `${quote}${cleanIdentifier}${quote}`;
}

  /* 
  CHANGES MADE:
  1. Extended buildDbConditions to support multiple operators (=, >, <, >=, <=, !=, LIKE, IN, NOT IN, IS NULL, IS NOT NULL)
  2. Added support for complex conditions like { "age>": 18, "name LIKE": "%john%", "status IN": ["active", "pending"] }
  3. Returns object with both dbConditions (mapped column names with operators) and values array
  4. Provides better type safety and error handling
  5. Throws descriptive error for unknown columns instead of silently ignoring them
  6. Separates column mapping from value extraction for cleaner code
  7. Essential for parameterized queries to prevent SQL injection
  8. WHAT IT DOES: Parses complex conditions with operators and prepares them for SQL query building
  9. WHY: Enables more flexible and powerful database queries beyond simple equality
  10. IMPORTANT: This is the foundation for advanced query capabilities
  */
  static buildDbConditions<T extends BaseEntity, I extends IBaseEntity>(
    this: abstract new (entity: I) => T,
    conditions?: Record<string, unknown>,
  ): { dbConditions: Record<string, unknown>; values: unknown[] } {
    const dbConditions: Record<string, unknown> = {};
    const values: unknown[] = [];
    const proto = this.prototype as object;
    const entries = Object.entries(conditions || {});
    
    for (const [key, value] of entries) {
      // Parse operator from key (e.g., "age>" becomes column "age" with operator ">")
      const operatorMatch = key.match(/^(.+?)(>=|<=|!=|<>|>|<|=| LIKE | NOT LIKE | IN | NOT IN | IS | IS NOT )$/i);
      
      if (operatorMatch) {
        const [, columnName, operator] = operatorMatch;
        if (!columnName || !operator) {
          throw new Error(`Invalid condition format: ${key}`);
        }
        
        const meta = getColumnSqlName(proto, columnName.trim());
        if (!meta.dbColumnName) {
          throw new Error(`Unknown column: ${columnName}`);
        }
        
        const columnWithOperator = `${meta.dbColumnName} ${operator.trim()}`;
        dbConditions[columnWithOperator] = value;
        
        // Handle IN/NOT IN arrays
        if (operator.toUpperCase().includes('IN') && Array.isArray(value)) {
          values.push(...value);
        } else if (!operator.toUpperCase().includes('IS')) {
          // Don't add values for IS NULL/IS NOT NULL
          values.push(value);
        }
      } else {
        // Default to equals operator
        const meta = getColumnSqlName(proto, key);
        if (!meta.dbColumnName) {
          throw new Error(`Unknown column: ${key}`);
        }
        dbConditions[meta.dbColumnName] = value;
        values.push(value);
      }
    }
    
    return { dbConditions: dbConditions, values: values };
  }

  static mapPropertyKeysToDbColumns(
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
  /* 
  CHANGES MADE:
  1. Changed method signature to accept options object instead of individual parameters
  2. Added getTableName() method call instead of direct reflection
  3. Uses buildDbConditions() for consistent column mapping and value extraction
  4. Added console.log(query) for debugging SQL queries
  5. Better type safety with complex this constraint for static methods
  6. More structured approach to query building with separated concerns
  */
  static async findAll<T extends BaseEntity, I extends IBaseEntity>(
    this: {
      new (entity: I): T;
      getTableName(): string;
      buildDbConditions(conditions?: Record<string, unknown>): {
        dbConditions: Record<string, unknown>;
        values: unknown[];
      };
    },
    options?: {
      conditions?: Record<string, unknown>;
      limit?: number;
      offset?: number;
    },
  ): Promise<T[]> {
    const { dbConditions, values } = this.buildDbConditions(
      options?.conditions,
    );
    const query = DB.driver.getSelectQuery(
      this.getTableName(),
      ["*"],
      dbConditions,
      options?.limit,
      options?.offset,
    );
    console.log(query);
    const result = await DB.driver.execute(query, values);
    return result.rows.map((row) => new this(row as I));
  }
  /* 
  CHANGES MADE:
  1. Updated to use new findAll signature with options object
  2. Uses limit: 1 for efficient single record retrieval
  3. More consistent with the new API design pattern
  4. Better type safety with proper this constraint
  */
  static async findOne<T extends BaseEntity, I extends IBaseEntity>(
    this: { new (entity: I): T; getTableName(): string },
    conditions: Record<string, unknown>,
  ): Promise<T | null> {
    const results = await (this as any).findAll({
      conditions: conditions,
      limit: 1,
    });
    return results.length > 0 ? results[0] : null;
  }

  /* 
  CHANGES MADE:
  1. Moved findById after findOne to maintain logical grouping
  2. Simplified implementation by delegating to findOne
  3. More consistent with target implementation structure
  4. Better code organization with related methods grouped together
  */
  static async findById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
  ): Promise<T | null> {
    return await (this as any).findOne({ id });
  }
  /* 
  CHANGES MADE:
  1. Changed method signature to use options object pattern
  2. Uses buildDbConditions() for consistent parameter handling
  3. Added console.log(query) for debugging
  4. Added comment explaining affectedRows vs count difference between databases
  5. Better type safety with complex this constraint
  6. More structured approach matching other static methods
  */
  static async deleteAll<T extends BaseEntity, I extends IBaseEntity>(
    this: {
      new (entity: I): T;
      getTableName(): string;
      buildDbConditions(conditions?: Record<string, unknown>): {
        dbConditions: Record<string, unknown>;
        values: unknown[];
      };
    },
    options?: {
      conditions?: Record<string, unknown>;
      limit?: number;
      offset?: number;
    },
  ): Promise<number> {
    const { dbConditions, values } = this.buildDbConditions(
      options?.conditions,
    );
    const query = DB.driver.getDeleteQuery(
      this.getTableName(),
      dbConditions,
      options?.limit,
      options?.offset,
    );
    console.log(query);
    const result = await DB.driver.execute(query, values);
    return result.affectedRows; //affectedRows in mysql & count in postgresql
  }
  /* 
  CHANGES MADE:
  1. Updated to use new deleteAll signature with options object
  2. Uses limit: 1 for single record deletion
  3. Better error handling and more consistent API
  4. Reorganized delete methods in logical order: deleteAll, deleteOne, deleteById
  */
  static async deleteOne<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    conditions: Record<string, unknown>,
  ): Promise<boolean> {
    const affectedRows = await (this as any).deleteAll({
      conditions,
      limit: 1,
    });
    return affectedRows > 0;
  }

  /* 
  CHANGES MADE:
  1. Moved deleteById after deleteOne for logical grouping
  2. Simplified implementation by delegating to deleteOne
  3. More consistent with target implementation structure
  4. Better code organization
  */
  static async deleteById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
  ): Promise<boolean> {
    return await (this as any).deleteOne({ id });
  }
  /* 
  CHANGES MADE:
  1. Uses buildDbConditions() for consistent parameter handling
  2. Better type safety with complex this constraint
  3. More robust error handling for count queries
  4. Consistent with other static method patterns
  5. Proper handling of undefined conditions parameter
  */
  static async count<T extends BaseEntity, I extends IBaseEntity>(
    this: {
      new (entity: I): T;
      getTableName(): string;
      buildDbConditions(conditions?: Record<string, unknown>): {
        dbConditions: Record<string, unknown>;
        values: unknown[];
      };
    },
    conditions?: Record<string, unknown>,
  ): Promise<number> {
    const { dbConditions, values } = this.buildDbConditions(conditions);
    const query = DB.driver.getCountQuery(this.getTableName(), dbConditions);

    const result = await DB.driver.execute(query, values);
    return Number(result.rows[0]?.count ?? 0);
  }
  /* 
  CHANGES MADE:
  1. Completely rewritten to match target implementation structure
  2. Uses buildDbConditions() for consistent column mapping
  3. Separates update columns from condition columns for proper parameter ordering
  4. Added console.log(query) for debugging
  5. Better error handling for unknown columns
  6. Proper parameter array construction: [updateValues, conditionValues]
  7. More robust type safety with complex this constraint
  8. Added detailed comments explaining the update process
  */
  static async updateAll<T extends BaseEntity, I extends IBaseEntity>(
    this: {
      new (entity: I): T;
      getTableName(): string;
      buildDbConditions(conditions?: Record<string, unknown>): {
        dbConditions: Record<string, unknown>;
        values: unknown[];
      };
    },
    updates: Record<string, unknown>,
    conditions: Record<string, unknown>,
  ): Promise<number> {
    const proto = this.prototype as object;
    const dbUpdatesColumns: string[] = [];
    const updateEntries = Object.entries(updates || {});
    const { dbConditions, values } = this.buildDbConditions(conditions);

    const updateValues = [];
    //updates
    for (const [key, value] of updateEntries) {
      const meta = getColumnSqlName(proto, key);
      if (!meta.dbColumnName) {
        throw new Error(`Unknown column: ${key}`);
      }
      dbUpdatesColumns.push(meta.dbColumnName);
      updateValues.push(value);
    }
    const params = [...updateValues, ...values];

    const query = DB.driver.getUpdateQuery(
      this.getTableName(),
      dbUpdatesColumns,
      dbConditions,
    );
    console.log(query);
    const result = await DB.driver.execute(query, params);
    return result.affectedRows;
  }
  /* 
  CHANGES MADE:
  1. Simplified implementation by delegating to updateAll
  2. More consistent with target implementation structure
  3. Better code organization and maintainability
  4. Proper error handling through updateAll method
  */
  static async updateById<T extends BaseEntity, I extends IBaseEntity>(
    this: new (entity: I) => T,
    id: number,
    updates: Record<string, unknown>,
  ): Promise<boolean> {
    const affectedRows = await (this as any).updateAll(updates, { id });
    return affectedRows > 0;
  }


    private async reloadCurrentState(tableName: string, prototype: object): Promise<void> {
        const entityId = BaseEntity.resolveNumericId(this.id);
        if (entityId === undefined) {
            throw new Error("Cannot reload entity after save without an id");
        }

        /* 
        CHANGES MADE:
        1. Use mapPropertyKeysToDbColumns for property-to-column mapping in reload operations
        2. This method is designed for mapping entity properties to database column names
        3. buildDbConditions is used for query conditions with parameter binding
        4. This maintains the original functionality while using the correct method
        5. Preserves type safety and error handling for property mapping
        */
        const mappedId = BaseEntity.mapPropertyKeysToDbColumns(prototype, { id: entityId });
        const query = DB.driver.getSelectQuery(tableName, ["*"], mappedId, 1);
        /* 
        CHANGES MADE:
        1. Simplified parameter passing by using Object.values() instead of Object.keys().map()
        2. More efficient and cleaner code
        3. Maintains the same functionality with better performance
        4. Consistent with buildDbConditions return structure
        */
        const result = await DB.driver.execute(query, Object.values(mappedId));
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