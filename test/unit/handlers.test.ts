import { beforeAll, describe, expect, it, vi } from 'vitest';
import { handleOrderCancelled, handleOrderCreated } from '../../src/handlers';
import { cancelledEvent, createdEvent, fakeDeps, ORDER_ID } from '../helpers/fakes';

beforeAll(() => {
  process.env.LOG_LEVEL = 'silent';
});

describe('handleOrderCreated', () => {
  it('reserves stock and marks the order PROCESSED', async () => {
    const deps = fakeDeps({ a: 5, b: 1 }, { [ORDER_ID]: 'PENDING' });
    const outcome = await handleOrderCreated(createdEvent([{ productId: 'a', quantity: 2 }, { productId: 'b', quantity: 1 }]), deps);
    expect(outcome).toBe('processed');
    expect(deps.stock.levels).toEqual({ a: 3, b: 0 });
    expect(deps.orders.statuses[ORDER_ID]).toBe('PROCESSED');
    expect(deps.activity.types()).toEqual(['ORDER_RECEIVED', 'STOCK_RESERVED', 'ORDER_PROCESSED']);
  });

  it('fails the order without touching stock when any line is short', async () => {
    const deps = fakeDeps({ a: 5, b: 0 }, { [ORDER_ID]: 'PENDING' });
    const outcome = await handleOrderCreated(createdEvent([{ productId: 'a', quantity: 2 }, { productId: 'b', quantity: 1 }]), deps);
    expect(outcome).toBe('failed');
    expect(deps.stock.levels).toEqual({ a: 5, b: 0 });
    expect(deps.orders.statuses[ORDER_ID]).toBe('FAILED');
    expect(deps.orders.reasons[ORDER_ID]).toBe('INSUFFICIENT_STOCK');
    expect(deps.activity.types()).toEqual(['ORDER_RECEIVED', 'ORDER_FAILED']);
    expect(deps.activity.items[1].data).toEqual({ shortfall: [{ productId: 'b', requested: 1, available: 0 }] });
  });

  it('gives stock back if the order was cancelled before it was processed', async () => {
    const deps = fakeDeps({ a: 5 }, { [ORDER_ID]: 'CANCELLED' });
    const outcome = await handleOrderCreated(createdEvent([{ productId: 'a', quantity: 2 }]), deps);
    expect(outcome).toBe('released');
    expect(deps.stock.levels).toEqual({ a: 5 });
    expect(deps.activity.types()).toEqual(['ORDER_RECEIVED', 'STOCK_RESERVED', 'STOCK_RELEASED']);
  });

  it('skips an event it has already handled', async () => {
    const deps = fakeDeps({ a: 5 }, { [ORDER_ID]: 'PENDING' });
    const event = createdEvent([{ productId: 'a', quantity: 2 }]);
    await handleOrderCreated(event, deps);
    deps.orders.statuses[ORDER_ID] = 'PENDING';
    expect(await handleOrderCreated(event, deps)).toBe('duplicate');
    expect(deps.stock.levels).toEqual({ a: 3 });
  });

  it('waits PROCESSING_DELAY_MS before reserving', async () => {
    const deps = fakeDeps({ a: 5 }, { [ORDER_ID]: 'PENDING' });
    const sleep = vi.fn(async () => undefined);
    await handleOrderCreated(createdEvent([{ productId: 'a', quantity: 1 }]), { ...deps, processingDelayMs: 250, sleep });
    expect(sleep).toHaveBeenCalledWith(250);
  });
});

describe('handleOrderCancelled', () => {
  it('returns stock when the order had been PROCESSED', async () => {
    const deps = fakeDeps({ a: 3 }, {});
    const outcome = await handleOrderCancelled(cancelledEvent([{ productId: 'a', quantity: 2 }], 'PROCESSED'), deps);
    expect(outcome).toBe('cancelled');
    expect(deps.stock.levels).toEqual({ a: 5 });
    expect(deps.activity.types()).toEqual(['STOCK_RELEASED', 'ORDER_CANCELLED']);
  });

  it('does not touch stock when the order was still PENDING', async () => {
    const deps = fakeDeps({ a: 3 }, {});
    await handleOrderCancelled(cancelledEvent([{ productId: 'a', quantity: 2 }], 'PENDING'), deps);
    expect(deps.stock.levels).toEqual({ a: 3 });
    expect(deps.activity.types()).toEqual(['ORDER_CANCELLED']);
  });

  it('does not release twice for a redelivered event', async () => {
    const deps = fakeDeps({ a: 3 }, {});
    const event = cancelledEvent([{ productId: 'a', quantity: 2 }], 'PROCESSED');
    await handleOrderCancelled(event, deps);
    expect(await handleOrderCancelled(event, deps)).toBe('duplicate');
    expect(deps.stock.levels).toEqual({ a: 5 });
  });
});
