import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const packageDir = fileURLToPath(new URL('../../', import.meta.url));
const read = (relative: string) =>
  readFileSync(new URL(relative, new URL('../../', import.meta.url)), 'utf8').replace(/\r\n/g, '\n');

describe('README sample service', () => {
  it('is reproduced verbatim in the README', () => {
    const sample = read('examples/sample-service.ts').trimEnd();
    const readme = read('README.md');
    expect(readme).toContain(`\`\`\`ts\n${sample}\n\`\`\``);
  });

  it('compiles against the published types of the package (dist/*.d.ts)', () => {
    expect(
      existsSync(new URL('../../dist/index.d.ts', import.meta.url)),
      'dist/ is missing: run `npm run build` first (the package test script does it)',
    ).toBe(true);
    const tsc = createRequire(import.meta.url).resolve('typescript/bin/tsc');
    const result = spawnSync(process.execPath, [tsc, '-p', 'tsconfig.examples.json'], {
      cwd: packageDir,
      encoding: 'utf8',
    });
    expect(result.stdout + result.stderr).toBe('');
    expect(result.status).toBe(0);
  }, 60_000);
});
