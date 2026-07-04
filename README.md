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

## goals

these are the engineering goals for this project:
- scalability: design for growth without unnecessary complexity.
- observability: ensure visibility into async flows and llm calls.
- fault tolerance: handle transient failures and rate limits gracefully.
- fast iteration: keep the codebase simple so a small team can move fast.
- asynchronous workflows: offload heavy computing and api calls from the request path.
- low operational complexity: choose reliable, well-understood infrastructure.
