/** One-off run of the daily summary job: npm run job:summary -- [YYYY-MM-DD] */
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { loadConfig } from '../config';
import { buildDailySummary, todayUtc } from '../summary';

async function run() {
  const date = process.argv[2] ?? todayUtc();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('Usage: job:summary [YYYY-MM-DD]');
  const config = loadConfig();
  const pool = new Pool({ connectionString: config.databaseUrl });
  const mongo = await new MongoClient(config.mongoUrl).connect();
  try {
    const summary = await buildDailySummary(pool, mongo.db(config.mongoDb), date);
    console.log(JSON.stringify(summary, null, 2));
  } finally {
    await Promise.allSettled([pool.end(), mongo.close()]);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
