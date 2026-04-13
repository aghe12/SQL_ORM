import "reflect-metadata";

export const COLUMN_METADATA_KEY = Symbol("columns");

export interface ColumnOptions {
  name?: string;
  nullable?: boolean;
}

export function Column(options: ColumnOptions = {}) {
  return function (target: object, propertyKey: string) {
    const existingColumns: Record<
      string,
      ColumnOptions & { propertyKey: string }
    > = Reflect.getMetadata(COLUMN_METADATA_KEY, target.constructor) || {};

    existingColumns[propertyKey] = {
      ...options,
      propertyKey,
      name: options.name ?? propertyKey,
    };

    Reflect.defineMetadata(
      COLUMN_METADATA_KEY,
      existingColumns,
      target.constructor,
    );
  };
}

export function getColumns(
  target: Function,
): Record<string, ColumnOptions & { propertyKey: string }> {
  return Reflect.getMetadata(COLUMN_METADATA_KEY, target) || {};
}
