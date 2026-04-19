
import "reflect-metadata";
import type { BaseEntity } from './base.entity.js';

export const TABLE_METADATA_KEY = Symbol('table');

function isConsonant(c: string): boolean {
    return c !== "" && !/[aeiou]/i.test(c);
}

function pluralize(word: string): string {
    const w = word.toLowerCase();
    if (w.length === 0) {
        return w;
    }
    const last = w[w.length - 1]!;
    const secondLast = w.length >= 2 ? w[w.length - 2]! : "";

    if (last === "y" && isConsonant(secondLast)) {
        return `${w.slice(0, -1)}ies`;
    }
    if (last === "s" || last === "x" || last === "z" || w.endsWith("ch") || w.endsWith("sh")) {
        return `${w}es`;
    }
    return `${w}s`;
}

function defaultTableNameFromClass(ctor: Function): string {
    const raw = ctor.name;
    if (!raw) {
        throw new Error('Unable to determine table name from class name');
    }
    const stem = raw.charAt(0).toLowerCase() + raw.slice(1);
    return pluralize(stem);
}

export function Table< TARGET extends typeof BaseEntity>(tableName?: string) {
    return function(target: TARGET) {
        const name = tableName ?? defaultTableNameFromClass(target);
        Reflect.defineMetadata(TABLE_METADATA_KEY, name, target);
    };
}