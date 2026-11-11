import type { TestProject } from 'vitest/node';
import { startKafka } from '@payments-kafka/consumer-runtime/testing';

declare module 'vitest' {
  export interface ProvidedContext {
    kafkaBootstrapServers: string;
  }
}

export default async function setup(project: TestProject) {
  const kafka = await startKafka();
  project.provide('kafkaBootstrapServers', kafka.bootstrapServers);
  return async () => {
    await kafka.stop();
  };
}
