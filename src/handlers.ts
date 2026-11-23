import type { OrderCancelledEvent, OrderCreatedEvent } from './events';
import { log } from './lib/logger';
import type { ActivityStore, OrderStatusStore, ProcessedEventStore, StockStore } from './stores';

export interface HandlerDeps {
  stock: StockStore;
  activity: ActivityStore;
  orders: OrderStatusStore;
  processed: ProcessedEventStore;
  processingDelayMs?: number;
  sleep?: (ms: number) => Promise<void>;
}

export type Outcome = 'processed' | 'failed' | 'released' | 'cancelled' | 'duplicate';

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * order.created: reserve stock for every line, then PENDING -> PROCESSED.
 * Not enough stock -> FAILED (INSUFFICIENT_STOCK). If the order was cancelled while we
 * were reserving, give the stock back.
 */
export async function handleOrderCreated(event: OrderCreatedEvent, deps: HandlerDeps): Promise<Outcome> {
  const { order } = event;
  const base = { orderId: order.id, userId: order.userId };
  if (await deps.processed.has(event.eventId)) {
    log.info('duplicate event skipped', { eventId: event.eventId, orderId: order.id });
    return 'duplicate';
  }

  await deps.activity.add({ ...base, type: 'ORDER_RECEIVED', message: `Order received with ${order.items.length} line(s)`, data: { totalCents: order.totalCents } });
  if (deps.processingDelayMs) await (deps.sleep ?? defaultSleep)(deps.processingDelayMs);

  const result = await deps.stock.reserve(order.items);
  if (!result.ok) {
    await deps.orders.markFailed(order.id, 'INSUFFICIENT_STOCK');
    await deps.activity.add({ ...base, type: 'ORDER_FAILED', message: 'Not enough stock to fulfil the order', data: { shortfall: result.shortfall } });
    await deps.processed.mark(event.eventId, 'order.created');
    log.info('order failed', { orderId: order.id, reason: 'INSUFFICIENT_STOCK' });
    return 'failed';
  }
  const reserved = order.items.map((i) => ({ productId: i.productId, sku: i.sku, quantity: i.quantity }));
  await deps.activity.add({ ...base, type: 'STOCK_RESERVED', message: 'Stock reserved', data: { items: reserved } });

  if (!(await deps.orders.markProcessed(order.id))) {
    await deps.stock.release(order.items);
    await deps.activity.add({ ...base, type: 'STOCK_RELEASED', message: 'Order is no longer pending; stock returned', data: { items: reserved } });
    await deps.processed.mark(event.eventId, 'order.created');
    log.info('order no longer pending, stock released', { orderId: order.id });
    return 'released';
  }

  await deps.activity.add({ ...base, type: 'ORDER_PROCESSED', message: 'Order processed' });
  await deps.processed.mark(event.eventId, 'order.created');
  log.info('order processed', { orderId: order.id });
  return 'processed';
}

/** order.cancelled: return stock if it had been reserved (the order was PROCESSED). */
export async function handleOrderCancelled(event: OrderCancelledEvent, deps: HandlerDeps): Promise<Outcome> {
  const { order } = event;
  const base = { orderId: order.id, userId: order.userId };
  if (await deps.processed.has(event.eventId)) return 'duplicate';

  if (event.previousStatus === 'PROCESSED') {
    await deps.stock.release(order.items);
    await deps.activity.add({
      ...base,
      type: 'STOCK_RELEASED',
      message: 'Stock returned after cancellation',
      data: { items: order.items.map((i) => ({ productId: i.productId, sku: i.sku, quantity: i.quantity })) },
    });
  }
  await deps.activity.add({ ...base, type: 'ORDER_CANCELLED', message: 'Order cancelled', data: { previousStatus: event.previousStatus } });
  await deps.processed.mark(event.eventId, 'order.cancelled');
  log.info('order cancelled', { orderId: order.id, previousStatus: event.previousStatus });
  return 'cancelled';
}
