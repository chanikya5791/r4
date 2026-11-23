import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { loadConfig } from './config';
import { startHealthServer } from './health';
import { createKafka, startWorker } from './kafka';
import { log } from './lib/logger';
import { ensureMongoIndexes, MongoActivityStore, MongoProcessedEventStore, MongoStockStore, PgOrderStatusStore } from './stores';
import { buildDailySummary, todayUtc } from './summary';

async function main() {
  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl, max: 5 });
  const mongo = await new MongoClient(config.mongoUrl).connect();
  const db = mongo.db(config.mongoDb);
  await ensureMongoIndexes(db);

  const worker = await startWorker(createKafka(config), config, {
    stock: new MongoStockStore(db),
    activity: new MongoActivityStore(db),
    orders: new PgOrderStatusStore(pool),
    processed: new MongoProcessedEventStore(db),
    processingDelayMs: config.processingDelayMs,
  });

  let summaryTimer: NodeJS.Timeout | undefined;
  if (config.summaryIntervalMs > 0) {
    const run = () => buildDailySummary(pool, db, todayUtc()).catch((err) => log.error('summary failed', { err: String(err) }));
    void run();
    summaryTimer = setInterval(run, config.summaryIntervalMs);
  }

  const health = startHealthServer(config.healthPort, {
    kafka: worker.isConnected,
    postgres: () => pool.query('SELECT 1').then(() => true),
    mongodb: () => db.command({ ping: 1 }).then(() => true),
  });
  log.info('jobs worker started', { healthPort: config.healthPort });

  const shutdown = async () => {
    log.info('shutting down');
    clearInterval(summaryTimer);
    health.close();
    await Promise.allSettled([worker.stop(), pool.end(), mongo.close()]);
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

main().catch((err) => {
  log.error('failed to start', { err: String(err?.stack ?? err) });
  process.exit(1);
});
