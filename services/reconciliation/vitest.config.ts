import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'unit',
          include: ['test/unit/**/*.test.ts'],
        },
      },
      {
        test: {
          name: 'integration',
          include: ['test/integration/**/*.test.ts'],
          // One Kafka and one Postgres container for the whole run; tests
          // isolate themselves with unique topics, group ids and databases.
          globalSetup: ['test/integration/global-setup.ts'],
          testTimeout: 120_000,
          hookTimeout: 180_000,
        },
      },
    ],
  },
});
