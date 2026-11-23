import { z } from 'zod';

/** Topics and payloads published by the Orders API (shoplite-orders-api). */
export const TOPICS = {
  orderCreated: 'order.created',
  orderCancelled: 'order.cancelled',
} as const;

export const dlqTopic = (topic: string) => `${topic}.dlq`;

const orderSchema = z.object({
  id: z.string().uuid(),
  userId: z.string().min(1),
  status: z.string(),
  totalCents: z.number().int(),
  items: z
    .array(
      z.object({
        productId: z.string().min(1),
        sku: z.string(),
        name: z.string(),
        unitPriceCents: z.number().int(),
        quantity: z.number().int().positive(),
      }),
    )
    .min(1),
});

export const orderCreatedSchema = z.object({
  eventId: z.string().min(1),
  type: z.literal('order.created'),
  occurredAt: z.string(),
  order: orderSchema,
});

export const orderCancelledSchema = z.object({
  eventId: z.string().min(1),
  type: z.literal('order.cancelled'),
  occurredAt: z.string(),
  previousStatus: z.enum(['PENDING', 'PROCESSED']),
  order: orderSchema,
});

export type OrderCreatedEvent = z.infer<typeof orderCreatedSchema>;
export type OrderCancelledEvent = z.infer<typeof orderCancelledSchema>;
export type OrderItem = OrderCreatedEvent['order']['items'][number];

/** An error that retrying will not fix (bad payload). The message goes straight to the DLQ. */
export class PoisonMessageError extends Error {}

export function parseEvent(topic: string, raw: Buffer | null): OrderCreatedEvent | OrderCancelledEvent {
  let json: unknown;
  try {
    json = JSON.parse(raw?.toString('utf8') ?? '');
  } catch {
    throw new PoisonMessageError('Message is not valid JSON');
  }
  const schema = topic === TOPICS.orderCreated ? orderCreatedSchema : topic === TOPICS.orderCancelled ? orderCancelledSchema : null;
  if (!schema) throw new PoisonMessageError(`Unexpected topic ${topic}`);
  const parsed = schema.safeParse(json);
  if (!parsed.success) {
    throw new PoisonMessageError(`Invalid ${topic} payload: ${parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ')}`);
  }
  return parsed.data;
}
