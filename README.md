# ShopLite jobs worker (r4)

The asynchronous half of ShopLite. It consumes order events from **Kafka**, reserves or releases stock in
**MongoDB**, moves orders along in **PostgreSQL**, writes the activity timeline, and builds a daily summary.
Part of the ShopLite test app; see the [infra repo (r5)](../r5) for the whole system.

## What it does

**`order.created`**

1. Skips the event if its `eventId` was already handled (`processed_events` collection), so redelivery is safe.
2. Writes `ORDER_RECEIVED`, then waits `PROCESSING_DELAY_MS`.
3. Reserves stock for every line with conditional `$inc` updates, all or nothing. It never oversells under
   concurrency.
4. If not enough stock: sets the order to `FAILED` / `INSUFFICIENT_STOCK` and writes `ORDER_FAILED` with the
   shortfall.
5. Otherwise writes `STOCK_RESERVED`, then `PENDING → PROCESSED` and `ORDER_PROCESSED`. If the order was cancelled
   in the meantime, it returns the stock and writes `STOCK_RELEASED`.

**`order.cancelled`**: if the order had been `PROCESSED`, returns the stock (`STOCK_RELEASED`). Always writes
`ORDER_CANCELLED`.

**Retries and dead letters**: a failing handler is retried up to `MAX_ATTEMPTS` times with linear backoff. After
that, or right away for malformed messages, the message goes to `<topic>.dlq` with `x-original-topic`,
`x-original-partition`, `x-original-offset`, `x-error` and `x-failed-at` headers.

**Daily summary**: every `SUMMARY_INTERVAL_MS`, counts today's (UTC) orders by status and the revenue of
`PROCESSED` orders, and upserts it into `daily_summary` (served by the Catalog API at `/reports/daily`). Run it
once for any date with `npm run job:summary -- 2026-11-23`.

**Health**: `GET :3004/health` reports Kafka, Postgres and MongoDB, plus counters (`processed`, `failed`,
`cancelled`, `released`, `duplicate`, `retried`, `deadLettered`).

On start-up it creates any missing topics (`order.created`, `order.cancelled` and their `.dlq` topics, 3
partitions each). Partitions are consumed in parallel; messages are keyed by order id, so events for one order
stay in order.

## Run

```bash
cp .env.example .env
npm ci
npm run dev
```

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `localhost:9094` |
| `KAFKA_GROUP_ID` | `shoplite-jobs` |
| `DATABASE_URL` | `postgres://shoplite:shoplite@localhost:5432/orders` (the Orders API database) |
| `MONGO_URL` / `MONGO_DB` | `mongodb://localhost:27017` / `catalog` (the Catalog API database) |
| `HEALTH_PORT` | `3004` |
| `MAX_ATTEMPTS` / `RETRY_BACKOFF_MS` | `3` / `500` |
| `PROCESSING_DELAY_MS` | `0` (docker-compose uses 1500 so `PENDING` is visible) |
| `SUMMARY_INTERVAL_MS` | `60000` (`0` turns it off) |

## Tests

```bash
npm run test:unit          # handlers and retry/DLQ logic with in-memory fakes
npm run test:integration   # real Kafka, Postgres and MongoDB via Testcontainers (needs Docker)
```

The integration tests run the real worker against a Kafka broker. They cover processing, failure, cancellation
with stock release, duplicate delivery, and dead-lettering, plus the Mongo and Postgres stores (all-or-nothing
reservation, no overselling under 25 concurrent reservations) and the summary.
