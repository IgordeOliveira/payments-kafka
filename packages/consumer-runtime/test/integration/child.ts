import { spawn, type ChildProcess } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterEach } from 'vitest';
import { waitFor } from '../../src/testing/index.js';
import type { RuntimeTopics } from '../../src/testing/index.js';

const fixture = fileURLToPath(new URL('../fixtures/child-consumer.ts', import.meta.url));

export interface ChildConsumer {
  process: ChildProcess;
  messages: Array<Record<string, unknown> & { type: string }>;
  stdout: string[];
  exitCode: Promise<number | null>;
  next(type: string, predicate?: (m: Record<string, unknown>) => boolean): Promise<Record<string, unknown>>;
}

const children: ChildProcess[] = [];

afterEach(() => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  }
});

/** Spawns test/fixtures/child-consumer.ts in its own Node.js process (via tsx). */
export function spawnChildConsumer(args: {
  bootstrapServers: string;
  groupId: string;
  topics: RuntimeTopics;
  mode: 'hang' | 'release';
}): ChildConsumer {
  const child = spawn(process.execPath, ['--import', 'tsx', fixture], {
    env: {
      ...process.env,
      BOOTSTRAP: args.bootstrapServers,
      GROUP: args.groupId,
      TOPIC: args.topics.payments,
      RETRY_TOPIC: args.topics.retry,
      DLQ_TOPIC: args.topics.dlq,
      MODE: args.mode,
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  children.push(child);
  const messages: ChildConsumer['messages'] = [];
  const stdout: string[] = [];
  let stderr = '';
  child.on('message', (message) => messages.push(message as ChildConsumer['messages'][number]));
  child.stdout?.setEncoding('utf8').on('data', (chunk: string) => {
    stdout.push(...chunk.split('\n').filter((line) => line.trim()));
  });
  child.stderr?.setEncoding('utf8').on('data', (chunk: string) => {
    stderr += chunk;
  });
  const exitCode = new Promise<number | null>((resolve) => child.on('exit', (code) => resolve(code)));

  return {
    process: child,
    messages,
    stdout,
    exitCode,
    next: (type, predicate = () => true) =>
      waitFor(() => messages.find((m) => m.type === type && predicate(m)), {
        timeoutMs: 45_000,
        message: `child message "${type}"${stderr ? `; stderr: ${stderr.slice(-2000)}` : ''}`,
      }),
  };
}
