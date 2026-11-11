/**
 * Entry point of the reconciliation CLI:
 *   node services/reconciliation/dist/cli.js <check | balances | reset-state --yes>
 * Only the command's output goes to stdout; diagnostics go to stderr.
 */
import { runCli } from './run-cli.js';

const code = await runCli(process.argv.slice(2), {
  stdout: (text) => process.stdout.write(text),
  stderr: (text) => process.stderr.write(text),
});
process.exitCode = code;
