import { beforeAll, describe, expect, it } from 'vitest';
import { parseEvent, PoisonMessageError } from '../../src/events';
import { processMessage, type DlqSender } from '../../src/processor';
import { cancelledEvent, createdEvent, fakeDeps, ORDER_ID } from '../helpers/fakes';

beforeAll(() => {
  process.env.LOG_LEVEL = 'silent';
});

class FakeDlq implements DlqSender {
  sent: { topic: string; headers: Record<string, string>; value: string | undefined }[] = [];
  async send(topic: string, m: { value: Buffer | null; headers: Record<string, string> }) {
    this.sent.push({ topic, headers: m.headers, value: m.value?.toString() });
  }
}

const msg = (topic: string, value: unknown) => ({
  topic,
  partition: 0,
  offset: '42',
  key: Buffer.from(ORDER_ID),
  value: Buffer.from(typeof value === 'string' ? value : JSON.stringify(value)),
  headers: { 'x-request-id': Buffer.from('trace-7') },
});

const opts = { maxAttempts: 3, retryBackoffMs: 10, sleep: async () => undefined };

describe('parseEvent', () => {
  it('parses valid events', () => {
    expect(parseEvent('order.created', Buffer.from(JSON.stringify(createdEvent([{ productId: 'a', quantity: 1 }]))))).toMatchObject({ type: 'order.created' });
    expect(parseEvent('order.cancelled', Buffer.from(JSON.stringify(cancelledEvent([{ productId: 'a', quantity: 1 }], 'PENDING'))))).toMatchObject({ type: 'order.cancelled' });
  });

  it.each([
    ['order.created', 'not json'],
    ['order.created', JSON.stringify({ type: 'order.created' })],
    ['order.created', JSON.stringify({ ...createdEvent([]), eventId: 'x' })],
    ['order.cancelled', JSON.stringify(createdEvent([{ productId: 'a', quantity: 1 }]))],
    ['some.topic', '{}'],
  ])('treats a bad %s payload as poison', (topic, raw) => {
    expect(() => parseEvent(topic, Buffer.from(raw))).toThrow(PoisonMessageError);
  });
});

describe('processMessage', () => {
  it('dispatches order.created', async () => {
    const deps = fakeDeps({ a: 1 }, { [ORDER_ID]: 'PENDING' });
    const dlq = new FakeDlq();
    expect(await processMessage(msg('order.created', createdEvent([{ productId: 'a', quantity: 1 }])), deps, dlq, opts)).toBe('processed');
    expect(dlq.sent).toEqual([]);
  });

  it('retries transient errors and then succeeds', async () => {
    const deps = fakeDeps({ a: 1 }, { [ORDER_ID]: 'PENDING' });
    deps.stock.failNext = 2;
    const sleeps: number[] = [];
    const outcome = await processMessage(msg('order.created', createdEvent([{ productId: 'a', quantity: 1 }])), deps, new FakeDlq(), {
      ...opts,
      sleep: async (ms) => void sleeps.push(ms),
    });
    expect(outcome).toBe('processed');
    expect(sleeps).toEqual([10, 20]);
  });

  it('dead-letters after the last attempt, keeping the payload and headers', async () => {
    const deps = fakeDeps({ a: 1 }, { [ORDER_ID]: 'PENDING' });
    deps.stock.failNext = 3;
    const dlq = new FakeDlq();
    const event = createdEvent([{ productId: 'a', quantity: 1 }]);
    expect(await processMessage(msg('order.created', event), deps, dlq, opts)).toBe('dead-lettered');
    expect(dlq.sent).toHaveLength(1);
    expect(dlq.sent[0].topic).toBe('order.created.dlq');
    expect(JSON.parse(dlq.sent[0].value!)).toEqual(event);
    expect(dlq.sent[0].headers).toMatchObject({
      'x-original-topic': 'order.created',
      'x-original-offset': '42',
      'x-error': 'mongo timeout',
      'x-request-id': 'trace-7',
    });
  });

  it('dead-letters poison messages without retrying', async () => {
    const deps = fakeDeps({}, {});
    const dlq = new FakeDlq();
    let sleeps = 0;
    const outcome = await processMessage(msg('order.created', '{oops'), deps, dlq, { ...opts, sleep: async () => void sleeps++ });
    expect(outcome).toBe('dead-lettered');
    expect(sleeps).toBe(0);
    expect(dlq.sent[0].headers['x-error']).toBe('Message is not valid JSON');
  });
});
