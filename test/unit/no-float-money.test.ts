import { describe, expect, test } from 'bun:test';
import { Glob } from 'bun';
import { resolve } from 'node:path';

/**
 * Static guard for the "no number for money" rule: no money-like field in the
 * source is typed `number`, and no float parsing is applied to money values.
 */
const ROOT = resolve(import.meta.dir, '..', '..', 'src');
const MONEY_FIELD = /\b(amount|balance\w*|money|\w*Amount|\w*Balance)\s*[!?]?\s*:\s*number\b/;
const FLOAT_ON_MONEY = /(parseFloat|Number|\+)\s*\(\s*[\w.?]*(amount|balance|Amount|Balance)/;

describe('E1 — money is never a JavaScript number', () => {
  test('no money-like field typed as number and no float parsing of money', async () => {
    const offenders: string[] = [];
    for await (const file of new Glob('**/*.ts').scan(ROOT)) {
      const lines = (await Bun.file(resolve(ROOT, file)).text()).split('\n');
      lines.forEach((line, i) => {
        if (MONEY_FIELD.test(line) || FLOAT_ON_MONEY.test(line)) offenders.push(`${file}:${i + 1}: ${line.trim()}`);
      });
    }
    expect(offenders).toEqual([]);
  });
});
