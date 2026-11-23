import type { Kafka, Producer } from 'kafkajs';
import { ObjectId, type Db } from 'mongodb';
import type { Pool } from 'pg';
import type { StartedTestContainer } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createKafka, startWorker, type Worker } from '../../src/kafka';
import { MongoActivityStore, MongoProcessedEventStore, MongoStockStore, PgOrderStatusStore } from '../../src/stores';
import { startKafka, startMongo, startPostgres, waitFor } from '../helpers/containers';

let db: Db;
let pool: Pool;
let kafka: Kafka;
let kafkaContainer: StartedTestContainer;
let worker: Worker;
let producer: Producer;
const stops: (() => Promise<void>)[] = [];

beforeAll(async () => {
  const [m, p, k] = await Promise.all([startMongo(), startPostgres(), startKafka()]);
  db = m.db;
  pool = p.pool;
  kafkaContainer = k.container;
  stops.push(m.stop, p.stop);
  kafka = createKafka({ kafkaBrokers: k.brokers, kafkaClientId: 'jobs-test' });
  worker = await startWorker(kafka, { kafkaGroupId: `jobs-test-${Date.now()}`, maxAttempts: 2, retryBackoffMs: 50 }, {
    stock: new MongoStockStore(db),
    activity: new MongoActivityStore(db),
    orders: new PgOrderStatusStore(pool),
    processed: new MongoProcessedEventStore(db),
  });
  producer = kafka.producer();
  await producer.connect();
  await waitFor(async () => worker.isConnected(), 60_000);
}, 240_000);

afterAll(async () => {
  await producer?.disconnect();
  await worker?.stop();
  await kafkaContainer?.stop();
  await Promise.all(stops.map((s) => s()));
});

async function setup(stock: number, quantity: number) {
  const productId = new ObjectId();
  await db.collection('products').insertOne({ _id: productId, sku: `S-${productId}`, name: 'Thing', stock });
  const userId = '22222222-2222-4222-8222-222222222222';
  const { rows } = await pool.query(`INSERT INTO orders (user_id, total_cents) VALUES ($1, 500) RETURNING id`, [userId]);
  const order = {
    id: rows[0].id as string,
    userId,
    status: 'PENDING',
    totalCents: 500,
    items: [{ productId: productId.toHexString(), sku: 'S', name: 'Thing', unitPriceCents: 500, quantity }],
  };
  return { order, productId };
}

const send = (topic: string, key: string, value: unknown) =>
  producer.send({ topic, messages: [{ key, value: typeof value === 'string' ? value : JSON.stringify(value) }] });

const orderStatus = async (id: string) => (await pool.query('SELECT status FROM orders WHERE id = $1', [id])).rows[0].status as string;
const stockOf = async (id: ObjectId) => (await db.collection('products').findOne({ _id: id }))!.stock as number;

describe('jobs worker against real Kafka, Postgres and MongoDB', () => {
  it('processes order.created: PENDING -> PROCESSED, stock reserved, activity written', async () => {
    const { order, productId } = await setup(5, 2);
    await send('order.created', order.id, { eventId: `e-${order.id}`, type: 'order.created', occurredAt: new Date().toISOString(), order });
    await waitFor(async () => (await orderStatus(order.id)) === 'PROCESSED');
    expect(await stockOf(productId)).toBe(3);
    const types = (await db.collection('activity').find({ orderId: order.id }).sort({ createdAt: 1 }).toArray()).map((a) => a.type);
    expect(types).toEqual(['ORDER_RECEIVED', 'STOCK_RESERVED', 'ORDER_PROCESSED']);
  });

  it('fails the order when stock is short', async () => {
    const { order, productId } = await setup(1, 2);
    await send('order.created', order.id, { eventId: `e-${order.id}`, type: 'order.created', occurredAt: new Date().toISOString(), order });
    await waitFor(async () => (await orderStatus(order.id)) === 'FAILED');
    expect(await stockOf(productId)).toBe(1);
  });

  it('releases stock for a cancelled PROCESSED order', async () => {
    const { order, productId } = await setup(5, 2);
    await send('order.created', order.id, { eventId: `c-${order.id}`, type: 'order.created', occurredAt: new Date().toISOString(), order });
    await waitFor(async () => (await orderStatus(order.id)) === 'PROCESSED');
    await pool.query(`UPDATE orders SET status = 'CANCELLED' WHERE id = $1`, [order.id]);
    await send('order.cancelled', order.id, {
      eventId: `x-${order.id}`,
      type: 'order.cancelled',
      occurredAt: new Date().toISOString(),
      previousStatus: 'PROCESSED',
      order: { ...order, status: 'CANCELLED' },
    });
    await waitFor(async () => (await stockOf(productId)) === 5);
    await waitFor(() => db.collection('activity').findOne({ orderId: order.id, type: 'ORDER_CANCELLED' }));
  });

  it('ignores a redelivered event', async () => {
    const { order, productId } = await setup(5, 1);
    const event = { eventId: `d-${order.id}`, type: 'order.created', occurredAt: new Date().toISOString(), order };
    await send('order.created', order.id, event);
    await waitFor(async () => (await orderStatus(order.id)) === 'PROCESSED');
    await send('order.created', order.id, event);
    // Give the duplicate time to be consumed, then check nothing changed.
    await new Promise((r) => setTimeout(r, 1500));
    expect(await stockOf(productId)).toBe(4);
    expect(await db.collection('activity').countDocuments({ orderId: order.id, type: 'ORDER_RECEIVED' })).toBe(1);
  });

  it('sends a malformed message to order.created.dlq with error headers', async () => {
    const consumer = kafka.consumer({ groupId: `dlq-reader-${Date.now()}` });
    const received: { value: string; headers: Record<string, string> }[] = [];
    await consumer.connect();
    await consumer.subscribe({ topic: 'order.created.dlq', fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        received.push({
          value: message.value!.toString(),
          headers: Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, String(v)])),
        });
      },
    });
    await send('order.created', 'bad', '{"this is": "not an order"}');
    const dead = await waitFor(async () => received.find((r) => r.value.includes('not an order')), 30_000);
    expect(dead.headers['x-original-topic']).toBe('order.created');
    expect(dead.headers['x-error']).toMatch(/Invalid order.created payload/);
    await consumer.disconnect();
  });
});
