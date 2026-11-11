import { startKafka } from '@payments-kafka/consumer-runtime/testing';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import type { TestProject } from 'vitest/node';

declare module 'vitest' {
  export interface ProvidedContext {
    kafkaBootstrapServers: string;
    /** Connection URL of the test Postgres server's admin database; tests create their own databases. */
    postgresAdminUrl: string;
  }
}

/** Same image as docker-compose.yml. Override with POSTGRES_TEST_IMAGE. */
const POSTGRES_IMAGE = process.env.POSTGRES_TEST_IMAGE ?? 'postgres:18.6';

export default async function setup(project: TestProject) {
  const [kafka, postgres] = await Promise.all([
    startKafka(),
    new PostgreSqlContainer(POSTGRES_IMAGE).withUsername('pix').withPassword('pix').withDatabase('postgres')
      // Test files run in parallel, each with its own pools; fsync off for speed.
      .withCommand(['postgres', '-c', 'max_connections=500', '-c', 'fsync=off'])
      .start(),
  ]);
  project.provide('kafkaBootstrapServers', kafka.bootstrapServers);
  project.provide('postgresAdminUrl', postgres.getConnectionUri());
  return async () => {
    await Promise.all([kafka.stop(), postgres.stop()]);
  };
}
