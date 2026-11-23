import { ObjectId, type Db } from 'mongodb';
import type { Pool } from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { ensureMongoIndexes, MongoActivityStore, MongoProcessedEventStore, MongoStockStore, PgOrderStatusStore } from '../../src/stores';
import { buildDailySummary } from '../../src/summary';
import { startMongo, startPostgres } from '../helpers/containers';

let db: Db;
let pool: Pool;
const stops: (() => Promise<void>)[] = [];

beforeAll(async () => {
  const [m, p] = await Promise.all([startMongo(), startPostgres()]);
  db = m.db;
  pool = p.pool;
  stops.push(m.stop, p.stop);
  await ensureMongoIndexes(db);
});
afterAll(async () => {
  await Promise.all(stops.map((s) => s()));
});

async function product(stock: number) {
  const _id = new ObjectId();
  await db.collection('products').insertOne({ _id, sku: `S-${_id}`, stock });
  return _id.toHexString();
}
const stockOf = async (id: string) => (await db.collection('products').findOne({ _id: new ObjectId(id) }))!.stock;

describe('MongoStockStore', () => {
  it('reserves every line', async () => {
    const [a, b] = [await product(5), await product(2)];
    const res = await new MongoStockStore(db).reserve([{ productId: a, quantity: 3 }, { productId: b, quantity: 2 }]);
    expect(res).toEqual({ ok: true });
    expect([await stockOf(a), await stockOf(b)]).toEqual([2, 0]);
  });

  it('is all-or-nothing and reports each short line', async () => {
    const [a, b, c] = [await product(5), await product(1), await product(0)];
    const res = await new MongoStockStore(db).reserve([
      { productId: a, quantity: 2 },
      { productId: b, quantity: 2 },
      { productId: c, quantity: 1 },
    ]);
    expect(res).toEqual({
      ok: false,
      shortfall: [
        { productId: b, requested: 2, available: 1 },
        { productId: c, requested: 1, available: 0 },
      ],
    });
    expect([await stockOf(a), await stockOf(b), await stockOf(c)]).toEqual([5, 1, 0]);
  });

  it('treats deleted or malformed product ids as zero stock', async () => {
    const res = await new MongoStockStore(db).reserve([
      { productId: new ObjectId().toHexString(), quantity: 1 },
      { productId: 'bad-id', quantity: 1 },
    ]);
    expect(res.ok).toBe(false);
  });

  it('never oversells under concurrent reservations', async () => {
    const a = await product(10);
    const store = new MongoStockStore(db);
    const results = await Promise.all(Array.from({ length: 25 }, () => store.reserve([{ productId: a, quantity: 1 }])));
    expect(results.filter((r) => r.ok)).toHaveLength(10);
    expect(await stockOf(a)).toBe(0);
  });

  it('release adds stock back', async () => {
    const a = await product(1);
    await new MongoStockStore(db).release([{ productId: a, quantity: 4 }]);
    expect(await stockOf(a)).toBe(5);
  });
});

describe('MongoActivityStore and MongoProcessedEventStore', () => {
  it('writes activity in the shape the Catalog API reads', async () => {
    await new MongoActivityStore(db).add({ orderId: 'o-1', userId: 'u-1', type: 'ORDER_PROCESSED', message: 'done' });
    const doc = await db.collection('activity').findOne({ orderId: 'o-1' });
    expect(doc).toMatchObject({ orderId: 'o-1', userId: 'u-1', type: 'ORDER_PROCESSED', message: 'done', data: {} });
    expect(doc!.createdAt).toBeInstanceOf(Date);
  });

  it('remembers processed event ids and tolerates marking twice', async () => {
    const store = new MongoProcessedEventStore(db);
    expect(await store.has('e-1')).toBe(false);
    await store.mark('e-1', 'order.created');
    await store.mark('e-1', 'order.created');
    expect(await store.has('e-1')).toBe(true);
  });
});

describe('PgOrderStatusStore', () => {
  const insert = async (status: string) =>
    (await pool.query(`INSERT INTO orders (user_id, total_cents, status) VALUES (gen_random_uuid(), 100, $1) RETURNING id`, [status])).rows[0].id as string;
  const statusOf = async (id: string) => (await pool.query('SELECT status, failure_reason FROM orders WHERE id = $1', [id])).rows[0];

  it('moves PENDING to PROCESSED only once', async () => {
    const id = await insert('PENDING');
    const store = new PgOrderStatusStore(pool);
    expect(await store.markProcessed(id)).toBe(true);
    expect(await store.markProcessed(id)).toBe(false);
    expect((await statusOf(id)).status).toBe('PROCESSED');
  });

  it('does not overwrite a cancelled order', async () => {
    const id = await insert('CANCELLED');
    const store = new PgOrderStatusStore(pool);
    expect(await store.markProcessed(id)).toBe(false);
    expect(await store.markFailed(id, 'X')).toBe(false);
    expect((await statusOf(id)).status).toBe('CANCELLED');
  });

  it('records the failure reason', async () => {
    const id = await insert('PENDING');
    await new PgOrderStatusStore(pool).markFailed(id, 'INSUFFICIENT_STOCK');
    expect(await statusOf(id)).toEqual({ status: 'FAILED', failure_reason: 'INSUFFICIENT_STOCK' });
  });
});

describe('buildDailySummary', () => {
  beforeEach(async () => {
    await pool.query('TRUNCATE orders');
  });

  it('counts orders by status for one UTC day and sums PROCESSED revenue', async () => {
    const rows: [string, number, string][] = [
      ['PROCESSED', 1000, '2026-03-10T00:00:00Z'],
      ['PROCESSED', 2500, '2026-03-10T23:59:59Z'],
      ['FAILED', 700, '2026-03-10T12:00:00Z'],
      ['CANCELLED', 300, '2026-03-10T12:00:00Z'],
      ['PROCESSED', 9999, '2026-03-11T00:00:00Z'],
    ];
    for (const [status, total, at] of rows) {
      await pool.query(`INSERT INTO orders (user_id, total_cents, status, created_at) VALUES (gen_random_uuid(), $1, $2, $3)`, [total, status, at]);
    }
    await pool.query(`SET TIME ZONE 'UTC'`);
    const summary = await buildDailySummary(pool, db, '2026-03-10');
    expect(summary).toMatchObject({
      date: '2026-03-10',
      ordersByStatus: { PROCESSED: 2, FAILED: 1, CANCELLED: 1 },
      orderCount: 4,
      revenueCents: 3500,
    });
    const doc = await db.collection('daily_summary').findOne({ _id: '2026-03-10' as never });
    expect(doc).toMatchObject({ orderCount: 4, revenueCents: 3500 });
  });

  it('writes an empty summary for a day with no orders and overwrites on re-run', async () => {
    await buildDailySummary(pool, db, '2026-04-01');
    await buildDailySummary(pool, db, '2026-04-01');
    const docs = await db.collection('daily_summary').find({ _id: '2026-04-01' as never }).toArray();
    expect(docs).toHaveLength(1);
    expect(docs[0]).toMatchObject({ orderCount: 0, revenueCents: 0, ordersByStatus: {} });
  });
});
