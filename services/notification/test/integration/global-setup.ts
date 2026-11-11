import { startKafka } from '@payments-kafka/consumer-runtime/testing';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import type { TestProject } from 'vitest/node';
import { POSTGRES_IMAGE } from './postgres.js';

declare module 'vitest' {
  export interface ProvidedContext {
    kafkaBootstrapServers: string;
    /** Superuser URL of the shared Postgres container; tests create their own databases. */
    postgresAdminUrl: string;
  }
}

export default async function setup(project: TestProject) {
  const [kafka, postgres] = await Promise.all([startKafka(), new PostgreSqlContainer(POSTGRES_IMAGE).start()]);
  project.provide('kafkaBootstrapServers', kafka.bootstrapServers);
  project.provide('postgresAdminUrl', postgres.getConnectionUri());
  return async () => {
    await Promise.all([kafka.stop(), postgres.stop()]);
  };
}
