import type { ActivityInput, ActivityStore, OrderStatusStore, ProcessedEventStore, Shortfall, StockStore } from '../../src/stores';
import type { HandlerDeps } from '../../src/handlers';

export class FakeStock implements StockStore {
  constructor(public levels: Record<string, number> = {}) {}
  failNext = 0;

  async reserve(items: { productId: string; quantity: number }[]) {
    if (this.failNext > 0) {
      this.failNext--;
      throw new Error('mongo timeout');
    }
    const shortfall: Shortfall[] = items
      .filter((i) => (this.levels[i.productId] ?? 0) < i.quantity)
      .map((i) => ({ productId: i.productId, requested: i.quantity, available: this.levels[i.productId] ?? 0 }));
    if (shortfall.length) return { ok: false as const, shortfall };
    for (const i of items) this.levels[i.productId] -= i.quantity;
    return { ok: true as const };
  }

  async release(items: { productId: string; quantity: number }[]) {
    for (const i of items) this.levels[i.productId] = (this.levels[i.productId] ?? 0) + i.quantity;
  }
}

export class FakeActivity implements ActivityStore {
  items: ActivityInput[] = [];
  async add(a: ActivityInput) {
    this.items.push(a);
  }
  types(orderId?: string) {
    return this.items.filter((a) => !orderId || a.orderId === orderId).map((a) => a.type);
  }
}

export class FakeOrders implements OrderStatusStore {
  constructor(public statuses: Record<string, string> = {}) {}
  reasons: Record<string, string> = {};
  async markProcessed(id: string) {
    if (this.statuses[id] !== 'PENDING') return false;
    this.statuses[id] = 'PROCESSED';
    return true;
  }
  async markFailed(id: string, reason: string) {
    if (this.statuses[id] !== 'PENDING') return false;
    this.statuses[id] = 'FAILED';
    this.reasons[id] = reason;
    return true;
  }
}

export class FakeProcessed implements ProcessedEventStore {
  ids = new Set<string>();
  async has(id: string) {
    return this.ids.has(id);
  }
  async mark(id: string) {
    this.ids.add(id);
  }
}

export function fakeDeps(stock: Record<string, number>, statuses: Record<string, string>) {
  const deps = {
    stock: new FakeStock(stock),
    activity: new FakeActivity(),
    orders: new FakeOrders(statuses),
    processed: new FakeProcessed(),
  };
  return deps satisfies HandlerDeps;
}

let n = 0;
export const ORDER_ID = '11111111-1111-4111-8111-111111111111';
export const USER_ID = '22222222-2222-4222-8222-222222222222';

export function createdEvent(items: { productId: string; quantity: number }[], orderId = ORDER_ID) {
  return {
    eventId: `evt-${++n}`,
    type: 'order.created' as const,
    occurredAt: new Date().toISOString(),
    order: {
      id: orderId,
      userId: USER_ID,
      status: 'PENDING',
      totalCents: 100,
      items: items.map((i) => ({ ...i, sku: `SKU-${i.productId}`, name: i.productId, unitPriceCents: 100 })),
    },
  };
}

export function cancelledEvent(items: { productId: string; quantity: number }[], previousStatus: 'PENDING' | 'PROCESSED', orderId = ORDER_ID) {
  const created = createdEvent(items, orderId);
  return { ...created, type: 'order.cancelled' as const, previousStatus, order: { ...created.order, status: 'CANCELLED' } };
}
