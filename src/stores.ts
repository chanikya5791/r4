import { MongoServerError, ObjectId, type Collection, type Db } from 'mongodb';
import type { Pool } from 'pg';
import type { OrderItem } from './events';

export interface Shortfall {
  productId: string;
  requested: number;
  available: number;
}

export interface StockStore {
  /** Reserves all items or none. Returns the items that could not be reserved. */
  reserve(items: Pick<OrderItem, 'productId' | 'quantity'>[]): Promise<{ ok: true } | { ok: false; shortfall: Shortfall[] }>;
  release(items: Pick<OrderItem, 'productId' | 'quantity'>[]): Promise<void>;
}

export interface ActivityInput {
  orderId: string;
  userId: string;
  type: string;
  message: string;
  data?: Record<string, unknown>;
}

export interface ActivityStore {
  add(a: ActivityInput): Promise<void>;
}

export interface OrderStatusStore {
  /** PENDING -> PROCESSED. Returns false if the order was not PENDING (e.g. cancelled meanwhile). */
  markProcessed(orderId: string): Promise<boolean>;
  /** PENDING -> FAILED. Returns false if the order was not PENDING. */
  markFailed(orderId: string, reason: string): Promise<boolean>;
}

export interface ProcessedEventStore {
  has(eventId: string): Promise<boolean>;
  mark(eventId: string, topic: string): Promise<void>;
}

// ---------- MongoDB (catalog database) ----------

interface ProductStockDoc {
  _id: ObjectId;
  stock: number;
}

const toObjectId = (id: string) => (/^[0-9a-f]{24}$/i.test(id) ? new ObjectId(id) : null);

export class MongoStockStore implements StockStore {
  private readonly products: Collection<ProductStockDoc>;

  constructor(db: Db) {
    this.products = db.collection<ProductStockDoc>('products');
  }

  async reserve(items: Pick<OrderItem, 'productId' | 'quantity'>[]) {
    const reserved: typeof items = [];
    for (const item of items) {
      const _id = toObjectId(item.productId);
      const res = _id
        ? await this.products.updateOne(
            { _id, stock: { $gte: item.quantity } },
            { $inc: { stock: -item.quantity }, $set: { updatedAt: new Date() } },
          )
        : { modifiedCount: 0 };
      if (res.modifiedCount === 1) {
        reserved.push(item);
        continue;
      }
      // Roll back what we already took, then report every line that cannot be met.
      await this.release(reserved);
      const shortfall: Shortfall[] = [];
      for (const i of items) {
        const oid = toObjectId(i.productId);
        const doc = oid ? await this.products.findOne({ _id: oid }) : null;
        const available = doc?.stock ?? 0;
        if (available < i.quantity) shortfall.push({ productId: i.productId, requested: i.quantity, available });
      }
      if (shortfall.length === 0) shortfall.push({ productId: item.productId, requested: item.quantity, available: 0 });
      return { ok: false as const, shortfall };
    }
    return { ok: true as const };
  }

  async release(items: Pick<OrderItem, 'productId' | 'quantity'>[]) {
    for (const item of items) {
      const _id = toObjectId(item.productId);
      if (_id) await this.products.updateOne({ _id }, { $inc: { stock: item.quantity }, $set: { updatedAt: new Date() } });
    }
  }
}

export class MongoActivityStore implements ActivityStore {
  constructor(private readonly db: Db) {}

  async add(a: ActivityInput) {
    await this.db.collection('activity').insertOne({ _id: new ObjectId(), ...a, data: a.data ?? {}, createdAt: new Date() });
  }
}

export class MongoProcessedEventStore implements ProcessedEventStore {
  private readonly col: Collection<{ _id: string; topic: string; processedAt: Date }>;

  constructor(db: Db) {
    this.col = db.collection('processed_events');
  }

  async has(eventId: string) {
    return (await this.col.countDocuments({ _id: eventId }, { limit: 1 })) > 0;
  }

  async mark(eventId: string, topic: string) {
    try {
      await this.col.insertOne({ _id: eventId, topic, processedAt: new Date() });
    } catch (err) {
      if (!(err instanceof MongoServerError && err.code === 11000)) throw err;
    }
  }
}

export async function ensureMongoIndexes(db: Db) {
  await db.collection('activity').createIndex({ orderId: 1, createdAt: 1 });
  await db.collection('processed_events').createIndex({ processedAt: 1 }, { expireAfterSeconds: 7 * 24 * 3600 });
}

// ---------- Postgres (orders database) ----------

export class PgOrderStatusStore implements OrderStatusStore {
  constructor(private readonly pool: Pool) {}

  async markProcessed(orderId: string) {
    const res = await this.pool.query(
      `UPDATE orders SET status = 'PROCESSED', updated_at = now() WHERE id = $1 AND status = 'PENDING'`,
      [orderId],
    );
    return res.rowCount === 1;
  }

  async markFailed(orderId: string, reason: string) {
    const res = await this.pool.query(
      `UPDATE orders SET status = 'FAILED', failure_reason = $2, updated_at = now() WHERE id = $1 AND status = 'PENDING'`,
      [orderId, reason],
    );
    return res.rowCount === 1;
  }
}
