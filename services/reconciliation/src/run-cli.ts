/**
 * The CLI logic, separate from the entry point (cli.ts) so tests can run it in
 * process. Every command first applies pending migrations, so it also works
 * against an empty database.
 *
 * Exit codes:
 *   0  success (`check`: no mismatch)
 *   1  `check` found at least one mismatch
 *   2  usage error (unknown command, `reset-state` without `--yes`); nothing changed
 *   3  operational error (database unreachable, query failed...)
 */
import pg from 'pg';
import { checkConsistency, formatBalances, listBalances, resetState } from './commands.js';
import { databaseUrl } from './db.js';
import { migrate } from './migrate.js';

export const EXIT = { ok: 0, mismatch: 1, usage: 2, error: 3 } as const;

export interface CliIo {
  stdout: (text: string) => void;
  stderr: (text: string) => void;
  env?: NodeJS.ProcessEnv;
}

export const USAGE = `Usage: reconciliation-cli <command>

Commands:
  check              Compare merchant balances and counters with the ledger.
                     Prints a JSON report; exits 1 if any mismatch is found.
  balances           Print every merchant balance as JSON sorted by merchant_id.
  reset-state --yes  Delete all reconciliation state (stop the consumers first).

Environment: DATABASE_URL (default postgres://pix:pix@localhost:5432/reconciliation)
`;

export async function runCli(argv: readonly string[], io: CliIo): Promise<number> {
  const [command, ...rest] = argv;
  if (command === undefined || command === 'help' || command === '--help' || command === '-h') {
    (command === undefined ? io.stderr : io.stdout)(USAGE);
    return command === undefined ? EXIT.usage : EXIT.ok;
  }
  if (!['check', 'balances', 'reset-state'].includes(command)) {
    io.stderr(`Unknown command: ${command}\n\n${USAGE}`);
    return EXIT.usage;
  }
  const unknownFlags = rest.filter((arg) => !(command === 'reset-state' && arg === '--yes'));
  if (unknownFlags.length > 0) {
    io.stderr(`Unexpected arguments for ${command}: ${unknownFlags.join(' ')}\n\n${USAGE}`);
    return EXIT.usage;
  }
  if (command === 'reset-state' && !rest.includes('--yes')) {
    io.stderr(
      'reset-state deletes all reconciliation state (processed events, payments, ledger, balances).\n' +
        'Stop the reconciliation consumers first, then run again with --yes to confirm. Nothing was deleted.\n',
    );
    return EXIT.usage;
  }

  let url: string;
  try {
    url = databaseUrl(io.env ?? process.env);
  } catch (error) {
    io.stderr(`${error instanceof Error ? error.message : String(error)}\n`);
    return EXIT.usage;
  }

  const client = new pg.Client({ connectionString: url });
  try {
    await client.connect();
    await migrate(client);
    switch (command) {
      case 'balances': {
        io.stdout(formatBalances(await listBalances(client)));
        return EXIT.ok;
      }
      case 'check': {
        const report = await checkConsistency(client);
        io.stdout(`${JSON.stringify(report, null, 2)}\n`);
        for (const m of report.mismatches) {
          io.stderr(`MISMATCH merchant=${m.merchant_id} field=${m.field} stored=${m.stored} ledger=${m.ledger}\n`);
        }
        return report.ok ? EXIT.ok : EXIT.mismatch;
      }
      default: {
        await resetState(client);
        io.stderr('reconciliation state reset: processed_events, payments, ledger_entries, merchant_balances truncated\n');
        return EXIT.ok;
      }
    }
  } catch (error) {
    io.stderr(`${command} failed: ${error instanceof Error ? error.message : String(error)}\n`);
    return EXIT.error;
  } finally {
    await client.end().catch(() => undefined);
  }
}
