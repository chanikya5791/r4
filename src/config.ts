export interface Config {
  kafkaBrokers: string[];
  kafkaClientId: string;
  kafkaGroupId: string;
  databaseUrl: string;
  mongoUrl: string;
  mongoDb: string;
  healthPort: number;
  maxAttempts: number;
  retryBackoffMs: number;
  processingDelayMs: number;
  summaryIntervalMs: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9094').split(',').map((b) => b.trim()).filter(Boolean),
    kafkaClientId: env.KAFKA_CLIENT_ID ?? 'shoplite-jobs',
    kafkaGroupId: env.KAFKA_GROUP_ID ?? 'shoplite-jobs',
    databaseUrl: env.DATABASE_URL ?? 'postgres://shoplite:shoplite@localhost:5432/orders',
    mongoUrl: env.MONGO_URL ?? 'mongodb://localhost:27017',
    mongoDb: env.MONGO_DB ?? 'catalog',
    healthPort: Number(env.HEALTH_PORT ?? 3004),
    maxAttempts: Math.max(1, Number(env.MAX_ATTEMPTS ?? 3)),
    retryBackoffMs: Number(env.RETRY_BACKOFF_MS ?? 500),
    processingDelayMs: Number(env.PROCESSING_DELAY_MS ?? 0),
    summaryIntervalMs: Number(env.SUMMARY_INTERVAL_MS ?? 60_000),
  };
}
