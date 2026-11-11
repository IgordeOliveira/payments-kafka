import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { waitFor, type RuntimeTopics } from '@payments-kafka/consumer-runtime/testing';
import { afterEach } from 'vitest';

const fixture = fileURLToPath(new URL('../fixtures/child-service.ts', import.meta.url));

export interface ChildService {
  process: ChildProcess;
  /** Parsed stdout lines whose `type` is `notification`: what an experiment would count. */
  notifications(): Array<Record<string, unknown>>;
  exitCode: Promise<number | null>;
  next(type: string): Promise<Record<string, unknown>>;
}

const children: ChildProcess[] = [];

afterEach(() => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  }
});

/** Spawns test/fixtures/child-service.ts in its own Node.js process (via tsx). */
export function spawnChildService(args: {
  bootstrapServers: string;
  groupId: string;
  topics: RuntimeTopics;
  databaseUrl: string;
  mode: 'hang-after-send' | 'normal';
}): ChildService {
  const child = spawn(process.execPath, ['--import', 'tsx', fixture], {
    env: {
      ...process.env,
      BOOTSTRAP: args.bootstrapServers,
      GROUP: args.groupId,
      PAYMENTS_TOPIC: args.topics.payments,
      ALERTS_TOPIC: args.topics.fraudAlerts,
      RETRY_TOPIC: args.topics.retry,
      DLQ_TOPIC: args.topics.dlq,
      DATABASE_URL: args.databaseUrl,
      MODE: args.mode,
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  children.push(child);
  const messages: Array<Record<string, unknown>> = [];
  let stdout = '';
  let stderr = '';
  child.on('message', (message) => messages.push(message as Record<string, unknown>));
  child.stdout?.setEncoding('utf8').on('data', (chunk: string) => {
    stdout += chunk;
  });
  child.stderr?.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  const exitCode = new Promise<number | null>((resolve) => child.on('exit', (code) => resolve(code)));

  return {
    process: child,
    exitCode,
    notifications: () =>
      stdout
        .split('\n')
        .filter((line) => line.trim().startsWith('{'))
        .map((line) => JSON.parse(line) as Record<string, unknown>)
        .filter((line) => line['type'] === 'notification'),
    next: (type) =>
      waitFor(() => messages.find((m) => m['type'] === type), {
        timeoutMs: 45_000,
        message: `child message "${type}"${stderr ? `; stderr: ${stderr.slice(-2000)}` : ''}`,
      }),
  };
}
