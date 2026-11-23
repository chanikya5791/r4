import { dlqTopic, parseEvent, PoisonMessageError, TOPICS } from './events';
import { handleOrderCancelled, handleOrderCreated, type HandlerDeps, type Outcome } from './handlers';
import { log } from './lib/logger';

export interface RawMessage {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>;
}

export interface DlqSender {
  send(topic: string, message: { key: Buffer | null; value: Buffer | null; headers: Record<string, string> }): Promise<void>;
}

export interface ProcessorOptions {
  maxAttempts: number;
  retryBackoffMs: number;
  sleep?: (ms: number) => Promise<void>;
}

export const stats = { processed: 0, failed: 0, cancelled: 0, released: 0, duplicate: 0, retried: 0, deadLettered: 0 };

const sleepDefault = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/**
 * Handles one Kafka message: parse, dispatch, retry transient errors with linear backoff, and
 * send to <topic>.dlq when the payload is bad or the attempts run out. Never throws, so one bad
 * message cannot block the partition.
 */
export async function processMessage(
  msg: RawMessage,
  deps: HandlerDeps,
  dlq: DlqSender,
  opts: ProcessorOptions,
): Promise<Outcome | 'dead-lettered'> {
  const sleep = opts.sleep ?? sleepDefault;
  let lastError: unknown;
  for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
    try {
      const event = parseEvent(msg.topic, msg.value);
      const outcome =
        event.type === 'order.created' ? await handleOrderCreated(event, deps) : await handleOrderCancelled(event, deps);
      stats[outcome]++;
      return outcome;
    } catch (err) {
      lastError = err;
      if (err instanceof PoisonMessageError) break;
      log.warn('handler failed', { topic: msg.topic, offset: msg.offset, attempt, err: String(err) });
      if (attempt < opts.maxAttempts) {
        stats.retried++;
        await sleep(opts.retryBackoffMs * attempt);
      }
    }
  }

  const headers: Record<string, string> = {
    'x-original-topic': msg.topic,
    'x-original-partition': String(msg.partition),
    'x-original-offset': msg.offset,
    'x-error': String(lastError instanceof Error ? lastError.message : lastError).slice(0, 1000),
    'x-failed-at': new Date().toISOString(),
  };
  for (const [k, v] of Object.entries(msg.headers ?? {})) {
    if (v !== undefined && !Array.isArray(v)) headers[k] = v.toString();
  }
  await dlq.send(dlqTopic(msg.topic), { key: msg.key, value: msg.value, headers });
  stats.deadLettered++;
  log.error('message sent to DLQ', { topic: msg.topic, offset: msg.offset, err: headers['x-error'] });
  return 'dead-lettered';
}

export const ALL_TOPICS = [TOPICS.orderCreated, TOPICS.orderCancelled, dlqTopic(TOPICS.orderCreated), dlqTopic(TOPICS.orderCancelled)];
