import { Kafka, logLevel, type Consumer, type Producer } from 'kafkajs';
import type { Config } from './config';
import { TOPICS } from './events';
import type { HandlerDeps } from './handlers';
import { log } from './lib/logger';
import { ALL_TOPICS, processMessage, type DlqSender } from './processor';

export interface Worker {
  consumer: Consumer;
  producer: Producer;
  isConnected: () => boolean;
  stop: () => Promise<void>;
}

export async function ensureTopics(kafka: Kafka, partitions = 3): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = ALL_TOPICS.filter((t) => !existing.has(t));
    if (missing.length) {
      await admin.createTopics({ waitForLeaders: true, topics: missing.map((topic) => ({ topic, numPartitions: partitions })) });
      log.info('created topics', { topics: missing });
    }
  } finally {
    await admin.disconnect();
  }
}

export function createKafka(config: Pick<Config, 'kafkaBrokers' | 'kafkaClientId'>): Kafka {
  return new Kafka({ clientId: config.kafkaClientId, brokers: config.kafkaBrokers, logLevel: logLevel.WARN, retry: { retries: 8 } });
}

export async function startWorker(
  kafka: Kafka,
  config: Pick<Config, 'kafkaGroupId' | 'maxAttempts' | 'retryBackoffMs'>,
  deps: HandlerDeps,
): Promise<Worker> {
  await ensureTopics(kafka);
  const producer = kafka.producer();
  await producer.connect();
  const dlq: DlqSender = {
    send: async (topic, m) => {
      await producer.send({ topic, messages: [m] });
    },
  };

  const consumer = kafka.consumer({ groupId: config.kafkaGroupId });
  let connected = false;
  consumer.on(consumer.events.GROUP_JOIN, () => (connected = true));
  consumer.on(consumer.events.CRASH, (e) => {
    connected = false;
    log.error('consumer crashed', { err: String(e.payload.error) });
  });
  consumer.on(consumer.events.DISCONNECT, () => (connected = false));
  await consumer.connect();
  await consumer.subscribe({ topics: [TOPICS.orderCreated, TOPICS.orderCancelled], fromBeginning: true });
  await consumer.run({
    // Orders are keyed by id, so each partition keeps per-order ordering; partitions run in parallel.
    partitionsConsumedConcurrently: 3,
    eachMessage: async ({ topic, partition, message }) => {
      await processMessage({ topic, partition, ...message }, deps, dlq, config);
    },
  });
  log.info('consumer running', { groupId: config.kafkaGroupId });

  return {
    consumer,
    producer,
    isConnected: () => connected,
    stop: async () => {
      await consumer.disconnect();
      await producer.disconnect();
    },
  };
}
