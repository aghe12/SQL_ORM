import "reflect-metadata";

export const COLUMN_METADATA_KEY = Symbol("column");

export interface ColumnOptions {
    name?: string;
}

function normalizeOptions(options?: string | ColumnOptions): ColumnOptions {
    if (options === undefined) {
        return {};
    }
    if (typeof options === "string") {
        return { name: options };
    }
    return options;
}

export function Column(options?: string | ColumnOptions) {
    const resolved = normalizeOptions(options);
    return function (target: object, propertyKey: string | symbol): void {
        Reflect.defineMetadata(COLUMN_METADATA_KEY, resolved, target, propertyKey);
    };
}

export function getColumnSqlName(prototype: object, propertyKey: string): { dbColumnName: string, propertyName: string } {
    const meta = Reflect.getMetadata(COLUMN_METADATA_KEY, prototype, propertyKey) as
        | ColumnOptions
        | undefined;

    return { dbColumnName: meta ? meta.name ?? propertyKey : '', propertyName: propertyKey };
}