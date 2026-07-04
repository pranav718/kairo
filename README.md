# kairo

architectural design for an ai-native creator marketing platform.

this repository documents the systems design decisions, database schemas, and background worker concurrency models for scaling creator matching and automated outreach campaigns.

---

## assumptions

these assumptions define the scope, data scale, and workload constraints of the platform:

### platform scale and workload
- brand users create campaigns specifying target audience, budgets, and criteria.
- creator profiles are ingested from social platform APIs.
- automated outreach templates are generated using LLM providers.
- background jobs handle profile enrichment, analytical calculations, and campaign matching.
- database storage must manage transactional metadata and timeseries engagement metrics.

### tech stack constraints
- services are built using go.
- postgresql handles relational transactional metadata.
- redis manages ephemeral data, rate limits, and job state caches.
- nats jetstream acts as the durable event bus.
- object storage stores uploaded assets and exported reports.
- clients receive updates via websocket endpoints.

---

## system architecture

### high level system overview

```
                     +-------------------+
                     |   user requests   |
                     +---------+---------+
                               |
                               v
                     +-------------------+
                     |    api gateway    |
                     +---------+---------+
                               |
                               v
                     +-------------------+
                     |  core go services |
                     +----+----+----+----+
                          |    |    |
       +------------------+    |    +------------------+
       |                       |                       |
       v                       v                       v
+------+------+         +------+------+         +------+------+
| postgresql  |         |    redis    |         |    nats     |
| (metadata)  |         |   (cache)   |         |  jetstream  |
+-------------+         +-------------+         +------+------+
                                                       |
                                                       v
                                                +------+------+
                                                | background  |
                                                |   workers   |
                                                +------+------+
                                                       |
                                                       v
                                                +------+------+
                                                |    llms     |
                                                +-------------+
```

the system is split into three main layers:
1. edge gateway: handles routing, auth verification, and websocket persistent connections.
2. core services: contains the restful api server, campaign managers, and the matching engine.
3. async execution layer: powered by nats queues and concurrent go worker pools.

### ingestion and processing data flow

```
+------------+
| webhooks   | ────+
+------------+     │
                   ├───► +------------+       +------------+
+------------+     │     | raw data   | ────► | enrichment | ──► [redis cache]
| scrapers   | ────+     | normalizer |       | workers    | ──► [postgresql]
+------------+           +-----+------+       +------------+
                               │
                               ▼
                        [object storage]
                               │
                               ▼
                        [scoring engine] ──► [postgresql]
```

incoming data flows through normalized stages:
- webhook payloads and social scraper workers normalize raw json payloads.
- enrichment workers call external APIs to update follower counts and engagement data.
- scoring logic writes updated records to postgresql and triggers caching updates in redis.

### service interaction map

```
+------------+       create_campaign       +------------+       write       +------------+
| api server | ──────────────────────────► | campaign   | ────────────────► | postgresql |
+------------+                             | service    |                   +------------+
                                           +-----+------+
                                                 │
                                                 ▼ publish / subscribe
                                           +-----+------+
                                           |    nats    |
                                           | jetstream  |
                                           +-----+------+
                                                 │
      +------------------------------------------+-------------------------------------+
      │                                          │                                     │
      ▼                                          ▼                                     ▼
+-----+------+                             +-----+------+                        +-----+------+
| outreach   |                             | analytics  |                        | notify     |
| worker     |                             | worker     |                        | worker     |
+-----+------+                             +-----+------+                        +-----+------+
      │                                          │                                     │
      ▼ call                                     ▼ write                               ▼ send
  [llm router]                               [analytics db]                        [websockets]
```

synchronous REST APIs process administrative and dashboard requests. heavy tasks publish events to nats jetstream, where dedicated consumer worker groups process them out of band.

---

## request lifecycle

### campaign creation path

```
[brand dashboard] ──► [api gateway] ──► [api server] ──► [postgresql] (status: draft)
                                             │
                                             ▼ publish campaign.created
                                      [nats jetstream]
                                             │
                                             ▼ consume
                                      [outreach worker] ──► [llm router] (generate)
                                             │
                                             ▼ update status to active
                                       [postgresql]
                                             │
                                             ▼ publish campaign.ready
                                      [nats jetstream] ──► [websocket srv] ──► [brand]
```

1. the brand submits campaign details to the API gateway.
2. the api server validates parameters and writes the campaign record with status `draft`.
3. the server publishes a `campaign.created` event to the stream and returns a 201 status to the brand immediately.
4. the outreach worker consumes the event, coordinates with the LLM router to generate draft templates, and updates the database.
5. a notification event is published, causing the websocket server to push an update to the brand dashboard.

### creator onboarding path

```
[creator] ──► [api server] ──► [postgresql] (status: pending_enrichment)
                                    │
                                    ▼ publish creator.registered
                             [nats jetstream]
                                    │
                                    ▼ consume
                             [enrich worker] ──► [social apis] (fetch stats)
                                    │
                                    ▼ update status to active
                              [postgresql]
```

1. a creator logs in and links their social profiles.
2. the API server inserts a record with status `pending_enrichment`.
3. the api server publishes `creator.registered` to nats.
4. enrichment workers fetch external statistics, update profile database fields, calculate internal tiers, and publish `creator.enriched`.

---

## event driven architecture

### event streaming model
events represent status changes and immutable updates. using an event driven architecture solves key operational challenges:
- decoupling: the core API server does not block on long-running worker tasks.
- failure isolation: transient worker issues do not affect user API uptime.
- independent scaling: workers processing outreach generation can scale up without modifying the API layer.

### queue selection
we select nats jetstream over apache kafka. nats provides:
- low operational complexity: runs as a single lightweight binary.
- built-in jetstream engine: supports durable streams, ack tracking, and consumer groups.
- pub/sub engine: handles transient websocket message routing natively.

---

## database design

### relational schema

```
+------------+       1:1       +------------+
|   users    | ◄------------►  |   brands   |
+-----+------+                 +-----+------+
      │                              │
      │ 1:1                          │ 1:many
      ▼                              ▼
+-----+------+                 +-----+------+
|  creators  | ◄─────────────► | campaigns  |
+-----+------+      many:many  +-----+------+
      │                              │
      │ 1:many                       │ 1:many
      ▼                              ▼
+-----+------+                 +-----+------+
|   metrics  |                 |   tasks    |
+------------+                 +-----+------+
                                     │
                                     │ 1:1
                                     ▼
                               +-----+------+
                               |task_results|
                               +------------+
```

metadata tables (`users`, `brands`, `creators`, `campaigns`) use uuid primary keys to ensure safe ID generation without centralized write locks. semi-structured options use `jsonb` fields to enable quick updates without database migration overhead.

### indexes
```sql
-- campaign lookups
create index idx_campaigns_brand_status on campaigns(brand_id, status);

-- creator queries
create index idx_creators_niche_tier on creators using gin(niches) where status = 'active';

-- job picking
create index idx_tasks_pending on tasks(status, priority, scheduled_at) where status = 'pending';
```

---

## worker architecture

### task execution model

```
[nats jetstream] ──► [dispatcher] ──► [worker pool] ──► [job handlers]
                                                               │
                                         +────────────────-----+─────────────────────+
                                         ▼                                           ▼
                                 [success: ack]                              [transient err: nack]
                                                                                     │
                                                                                     ▼
                                                                             [exponential backoff]
                                                                                     │
                                                                                     ▼
                                                                             [max tries: dlq]
```

workers fetch jobs from nats jetstream. each worker is controlled by a concurrency controller using channels to manage backpressure.

### fault tolerance and retries
- transient errors: retry with exponential backoff and random jitter.
- permanent failures: skip retries and write payloads directly to the dead letter queue stream.
- duplicate delivery: workers query idempotency tables before starting any execution.

---

## scaling strategy

1. modular monolith: build code inside a single go binary with strict domain division.
2. runtime separation: run separate container pools for HTTP APIs and background workers using the same binary image.
3. service extraction: isolate modules (like analytics) into separate services only when storage engine or CPU needs diverge.

---

## go specific ideas

go is the perfect language for this infrastructure:
- concurrency is native: goroutines and channels allow us to implement robust worker pools without heavy resource footprints.
- context propagation: standard `context.Context` cancellation allows us to abort slow database queries or llm requests if a client cancels or a timeout is reached.
- connection pools: built-in connection pooling for databases ensures we do not saturate database resources.
- compile to single binary: simple containerization and fast deployment.

---

## observability

- logs: json format logs using `log/slog` to easily parse logs in remote aggregators.
- metrics: prometheus metrics tracking queue duration, token usage, and database connection counts.
- tracing: opentelemetry tracing with trace context headers passed through nats payloads.

---

## failure scenarios

### llm timeouts
- mitigation: set timeout limits on outgoing HTTP client contexts and fallback to alternative models if the primary provider fails.

### queue saturation
- mitigation: apply nats jetstream consumer limits to prevent memory overflow and scale container instances dynamically based on queue depth metrics.

### database outages
- mitigation: keep events in nats queues. if database writes fail, workers nack the messages so they are re-queued until database connectivity is restored.

---

## tradeoffs

### api protocols
- tradeoff: rest is chosen over grpc for the API layer due to tooling familiarity and easy of debugging. hotspots can be converted to grpc later.

### storage engines
- tradeoff: postgresql is used for all records including timeseries metrics to avoid managing multiple database technologies during the early stages of development.

---

## future ideas

- agent workflow engine: coordinate multi-step agent actions with human approval stages.
- vector search: store profile embeddings using pgvector for semantic creator recommendations.
- streaming aggregations: calculate campaign metrics continuously using real-time event consumer streams.

---

## go implementation files

the codebase contains working implementations of the core architectural patterns:
- [worker.go](worker.go): concurrent worker pool pattern.
- [limiter.go](limiter.go): rate limiting wrapper for API providers.
- [retry.go](retry.go): retry implementation using backoff and jitter.
- [event.go](event.go): structured event publishing wrapper for nats.
- [main.go](main.go): server initialization and clean system shutdown.
