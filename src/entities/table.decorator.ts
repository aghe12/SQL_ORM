import 'reflect-metadata';
import type { BaseEntity } from './base.entity.js';

export const TABLE_METADATA_KEY = Symbol('table');
    
export function Table<T extends typeof BaseEntity>(tableName: string) {
    return function(target: T) {
        Reflect.defineMetadata(TABLE_METADATA_KEY, tableName, target);
    };
}