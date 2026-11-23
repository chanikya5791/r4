import type { Db } from 'mongodb';
import type { Pool } from 'pg';
import { log } from './lib/logger';

export interface DailySummary {
  date: string;
  ordersByStatus: Record<string, number>;
  orderCount: number;
  revenueCents: number;
  generatedAt: Date;
}

/**
 * Builds the summary for one UTC day from Postgres and upserts it into MongoDB
 * (daily_summary, keyed by date), where the Catalog API serves it at /reports/daily.
 * Revenue counts PROCESSED orders only.
 */
export async function buildDailySummary(pool: Pool, db: Db, date: string): Promise<DailySummary> {
  const { rows } = await pool.query<{ status: string; count: number; revenue: string | null }>(
    `SELECT status, count(*)::int AS count, sum(total_cents) AS revenue
       FROM orders
      WHERE created_at >= $1::date AND created_at < ($1::date + interval '1 day')
      GROUP BY status`,
    [date],
  );
  const ordersByStatus = Object.fromEntries(rows.map((r) => [r.status, r.count]));
  const summary: DailySummary = {
    date,
    ordersByStatus,
    orderCount: rows.reduce((n, r) => n + r.count, 0),
    revenueCents: Number(rows.find((r) => r.status === 'PROCESSED')?.revenue ?? 0),
    generatedAt: new Date(),
  };
  await db.collection<DailySummary & { _id: string }>('daily_summary').replaceOne({ _id: date }, summary, { upsert: true });
  log.info('daily summary built', { date, orderCount: summary.orderCount });
  return summary;
}

export const todayUtc = () => new Date().toISOString().slice(0, 10);
