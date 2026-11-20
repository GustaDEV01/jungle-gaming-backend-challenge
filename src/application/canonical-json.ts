import { createHash } from 'node:crypto';

type Json = null | boolean | string | Json[] | { [key: string]: Json };

/**
 * Canonical JSON: object keys sorted by UTF-16 code unit order, recursively;
 * `undefined` members omitted; no whitespace. Numbers are refused on purpose:
 * every business field is a string (money included), so the canonical form
 * never depends on float formatting.
 */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(normalise(value));
}

function normalise(value: unknown): Json {
  if (value === null) return null;
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'object': {
      if (Array.isArray(value)) return value.map(normalise);
      const out: { [key: string]: Json } = {};
      for (const key of Object.keys(value as object).sort()) {
        const member = (value as Record<string, unknown>)[key];
        if (member !== undefined) out[key] = normalise(member);
      }
      return out;
    }
    default:
      throw new TypeError(`canonicalJson: unsupported value type ${typeof value}`);
  }
}

/** SHA-256 (hex) of the canonical JSON representation. */
export function canonicalHash(value: unknown): string {
  return createHash('sha256').update(canonicalJson(value), 'utf8').digest('hex');
}
