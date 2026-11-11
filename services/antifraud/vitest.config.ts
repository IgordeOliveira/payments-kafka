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
          // One Kafka container for the whole run; tests isolate themselves
          // with unique topic names and consumer group ids.
          globalSetup: ['test/integration/global-setup.ts'],
          testTimeout: 90_000,
          hookTimeout: 180_000,
        },
      },
    ],
  },
});
