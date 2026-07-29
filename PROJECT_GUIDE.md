# Kafka, Messaging & Microservices — Project Learning Guide

> **Purpose of this file:** a single walkthrough of what this repository teaches, how the pieces fit together, and **where to open the code** for each concept. Treat it as a map, not a replacement for reading the source.

---

## 1. What this project is

This lab simulates a **distributed order-processing system**:

1. **Phase 1 — Kafka fundamentals:** multi-broker cluster, producers/consumers, partitions, consumer groups, lag, transactions, compaction.
2. **Phase 2 / 3 — Event-driven architecture:** stop dual-writing to DB + Kafka directly; use **Transactional Outbox + Debezium CDC**, then build a **CQRS-style read model**.

High-level flow today:

```text
Client / TrafficGenerator
        │
        ▼
   Order.Api  ──(same DB transaction)──►  Postgres (orders + outbox_messages)
                                                    │
                                              Debezium CDC
                                                    │
                                                    ▼
                                            Kafka topic: orders
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                            PaymentService  Notification   InventoryService
                                                    │
                                                    ▼
                                          ReadModel.Processor
                                                    │
                                                    ▼
                                      compacted topic: order-read-model
                                      + in-memory query API :5181
```

---

## 2. Code entry points (start here)

| Goal                                | Open this first                                                                                                                                                                            | What you will see                                                               |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| HTTP write API (place order)        | [`Order.Api/Program.cs`](Order.Api/Program.cs)                                                                                                                                             | Minimal API `POST /orders`, wires Application + Infrastructure                  |
| Domain behavior + domain events     | [`src/Order.Domain/Orders/Order.cs`](src/Order.Domain/Orders/Order.cs)                                                                                                                     | `Order.Place()` raises `OrderPlacedDomainEvent`                                 |
| Use-case / command handler          | [`src/Order.Application/Orders/Commands/PlaceOrder/PlaceOrderCommandHandler.cs`](src/Order.Application/Orders/Commands/PlaceOrder/PlaceOrderCommandHandler.cs)                             | Persist order + `SaveChanges` (outbox filled by interceptor)                    |
| **Outbox pattern (critical)**       | [`src/Order.Infrastructure/Persistence/Interceptors/OutboxSaveChangesInterceptor.cs`](src/Order.Infrastructure/Persistence/Interceptors/OutboxSaveChangesInterceptor.cs)                   | Domain events → `outbox_messages` in the **same** EF transaction                |
| Outbox entity shape for Debezium    | [`src/Order.Domain/Outbox/OutboxMessage.cs`](src/Order.Domain/Outbox/OutboxMessage.cs)                                                                                                     | `aggregatetype`, `aggregateid`, `type`, `payload`                               |
| Manual CQRS dispatcher (no MediatR) | [`src/Order.Application/Dispatcher.cs`](src/Order.Application/Dispatcher.cs)                                                                                                               | Resolves handlers from DI                                                       |
| Debezium connector config           | [`docker/debezium/outbox-connector.json`](docker/debezium/outbox-connector.json)                                                                                                           | Postgres connector + Outbox Event Router SMT → topic `orders`                   |
| Cluster + Postgres + Connect        | [`docker-compose.yml`](docker-compose.yml)                                                                                                                                                 | 3× Kafka (KRaft), Schema Registry, UI, Postgres (`wal_level=logical`), Debezium |
| Topic bootstrap                     | [`scripts/create-topics.sh`](scripts/create-topics.sh)                                                                                                                                     | `orders`, `payment-results`, compact topics, etc.                               |
| Shared topic / group names          | [`Shared/Kafka/KafkaConstants.cs`](Shared/Kafka/KafkaConstants.cs)                                                                                                                         | Single source of truth for topic & group IDs                                    |
| Consumer group choreography         | [`PaymentService/Program.cs`](PaymentService/Program.cs), [`NotificationService/Program.cs`](NotificationService/Program.cs), [`InventoryService/Program.cs`](InventoryService/Program.cs) | Three independent groups on `orders`                                            |
| Shared consumer loop helpers        | [`Shared/Kafka/OrderConsumerHost.cs`](Shared/Kafka/OrderConsumerHost.cs)                                                                                                                   | Manual commit, assignor, artificial lag                                         |
| Exactly-once Kafka→Kafka            | [`PaymentService/Program.cs`](PaymentService/Program.cs)                                                                                                                                   | `BeginTransaction` / `SendOffsetsToTransaction` / `CommitTransaction`           |
| Read model (KTable-like)            | [`src/ReadModel.Processor/Program.cs`](src/ReadModel.Processor/Program.cs)                                                                                                                 | Consume `orders` → memory store → produce `order-read-model`                    |
| Load / chaos notes                  | [`src/TrafficGenerator.Worker/Program.cs`](src/TrafficGenerator.Worker/Program.cs)                                                                                                         | Bogus traffic + chaos guidance                                                  |
| Event DTO contract                  | [`Shared/Models/OrderEvents.cs`](Shared/Models/OrderEvents.cs)                                                                                                                             | JSON shape consumers deserialize                                                |

**Suggested reading order for a newcomer**

1. `Order.Api/Program.cs` → `PlaceOrderCommandHandler` → `Order.Place` → `OutboxSaveChangesInterceptor`
2. `docker/debezium/outbox-connector.json` + `docker-compose.yml`
3. One consumer (`NotificationService`) + `OrderConsumerHost`
4. `PaymentService` (transactions)
5. `ReadModel.Processor`

---

## 3. Microservices concepts in this repo

### 3.1 Service boundaries

We intentionally split **write**, **integration**, and **read** concerns:

| Service                                                       | Role                                           | Independence                         |
| ------------------------------------------------------------- | ---------------------------------------------- | ------------------------------------ |
| `Order.Api`                                                   | Write model / command side                     | Owns Postgres schema                 |
| `PaymentService` / `NotificationService` / `InventoryService` | Downstream reactions (choreography saga style) | Separate processes & consumer groups |
| `ReadModel.Processor`                                         | Query-side projection                          | Can lag; eventual consistency        |
| `TrafficGenerator.Worker`                                     | Fake clients                                   | Not part of domain                   |

This mirrors real microservice messaging: **services do not share databases for integration**; they share **events** (here via Kafka after CDC).

### 3.2 Choreography vs orchestration

Three consumer groups listen to the same `orders` topic. No central orchestrator tells Payment then Inventory then Notification what to do — each reacts independently. That is **choreography**.

- Topic & group constants: [`Shared/Kafka/KafkaConstants.cs`](Shared/Kafka/KafkaConstants.cs)
- Example independent consumer: [`NotificationService/Program.cs`](NotificationService/Program.cs)

### 3.3 CQRS (Command Query Responsibility Segregation)

- **Commands / writes:** `POST /orders` → Application handlers → Postgres write model.  
  Entry: [`Order.Api/Program.cs`](Order.Api/Program.cs), [`PlaceOrderCommandHandler.cs`](src/Order.Application/Orders/Commands/PlaceOrder/PlaceOrderCommandHandler.cs)
- **Queries on write DB:** `GET /orders/{id}` still hits Postgres (simple).  
  Entry: [`GetOrderByIdQueryHandler.cs`](src/Order.Application/Orders/Queries/GetOrderById/GetOrderByIdQueryHandler.cs)
- **Queries on read model:** `http://localhost:5181/orders` — projected, eventually consistent.  
  Entry: [`ReadModel.Processor/Program.cs`](src/ReadModel.Processor/Program.cs) (`ReadModelHttpHost`)

Interview takeaway: if the read model lags, you have **eventual consistency** — CQRS systems usually accept that for scalability.

### 3.4 Clean / onion-style layering

Dependency rule used here:

```text
Order.Domain  ←  Order.Application  ←  Order.Infrastructure
                         ↑
                   Order.Api (composition root)
```

- Domain has **no** EF/Kafka packages.
- Outbox message type lives in Domain; persistence mapping in Infrastructure.  
  See [`OutboxMessage.cs`](src/Order.Domain/Outbox/OutboxMessage.cs) and [`OutboxMessageConfiguration.cs`](src/Order.Infrastructure/Persistence/Configurations/OutboxMessageConfiguration.cs).

### 3.5 Domain events vs integration events

- **Domain events** (`OrderPlacedDomainEvent`) are raised inside the aggregate.  
  Entry: [`Order.cs`](src/Order.Domain/Orders/Order.cs), [`OrderDomainEvents.cs`](src/Order.Domain/Orders/Events/OrderDomainEvents.cs)
- They are serialized into **outbox rows**, then become **integration events** on Kafka after Debezium.  
  Payload is shaped like [`OrderCreatedEvent`](Shared/Models/OrderEvents.cs) so Phase-1 consumers keep working.

---

## 4. Messaging fundamentals (and where we use them)

### 4.1 Why messaging?

Synchronous HTTP between every service creates **temporal coupling** (everyone must be up) and **scalability bottlenecks**. Async messaging lets producers publish and consumers process at their own pace, with buffering in the broker log.

In this lab, Order.Api no longer publishes to Kafka in-process. Integration is **async via the outbox + CDC**.

### 4.2 Dual-write problem

**Bad pattern:**

```text
Save order to DB;
Publish event to Kafka;   // if this fails after DB commit → inconsistent world
```

Or the reverse: Kafka succeeds, DB fails → phantom events.

**Fix used here:** Transactional Outbox — only write the DB (entity + outbox row) atomically; a reliable relay (Debezium) publishes later.

- Interceptor: [`OutboxSaveChangesInterceptor.cs`](src/Order.Infrastructure/Persistence/Interceptors/OutboxSaveChangesInterceptor.cs)
- Same `DbContext`: [`AppDbContext.cs`](src/Order.Infrastructure/Persistence/AppDbContext.cs)

### 4.3 At-most-once / at-least-once / exactly-once

| Semantics                  | Idea                                      | In this code                                                                                 |
| -------------------------- | ----------------------------------------- | -------------------------------------------------------------------------------------------- |
| At-most-once               | Commit offset before processing           | Env `COMMIT_BEFORE_PROCESS=true` on [`OrderConsumerHost`](Shared/Kafka/OrderConsumerHost.cs) |
| At-least-once              | Process then commit (default)             | Default path in `OrderConsumerHost`                                                          |
| Exactly-once (Kafka→Kafka) | Transaction spans produce + offset commit | [`PaymentService/Program.cs`](PaymentService/Program.cs)                                     |

**Important:** idempotent producer (`enable.idempotence`) alone does **not** make the whole pipeline exactly-once for consumers. Consumers may still see duplicates under at-least-once; handle idempotency in business logic or use transactional APIs end-to-end where applicable.

### 4.4 Consumer groups & rebalancing

- Same `group.id` → partitions of a topic are **shared** among instances (competing consumers).
- Different `group.id` → each group gets a **full copy** of the stream (fan-out). That is why Payment, Notification, and Inventory all see every order.

Env vars on consumers:

- `PARTITION_ASSIGNMENT=Range|CooperativeSticky` — compare stop-the-world vs incremental rebalance ([`OrderConsumerHost`](Shared/Kafka/OrderConsumerHost.cs), PaymentService).
- `PROCESS_DELAY_MS` — induce **consumer lag** for monitoring exercises.

### 4.5 Partition keys & ordering

Kafka guarantees order **per partition**, not globally.

We use **CustomerId as the message key** (via outbox `aggregateid`) so events for one customer land on one partition and stay ordered for that customer.

- Outbox key choice: interceptor prefers `order.CustomerId` as `aggregateId`.  
  See [`OutboxSaveChangesInterceptor.cs`](src/Order.Infrastructure/Persistence/Interceptors/OutboxSaveChangesInterceptor.cs).
- Debezium Event Router uses that field as Kafka key: [`outbox-connector.json`](docker/debezium/outbox-connector.json).

### 4.6 Topics used in the lab

Defined in [`KafkaConstants.cs`](Shared/Kafka/KafkaConstants.cs) and created in [`create-topics.sh`](scripts/create-topics.sh):

| Topic                    | Role                                             |
| ------------------------ | ------------------------------------------------ |
| `orders`                 | Integration stream of placed orders              |
| `payment-results`        | Output of transactional Payment processing       |
| `customer-latest-status` | Log compaction demo (latest per customer key)    |
| `order-read-model`       | Compacted changelog of the read-model projection |

### 4.7 Log compaction

Compaction keeps the **latest value per key**, useful for “current state” topics (KTable changelogs). Configured for `customer-latest-status` and `order-read-model` in [`create-topics.sh`](scripts/create-topics.sh).

### 4.8 Schema / serialization

Phase-1/2 consumers use **JSON** ([`JsonSerdes.cs`](Shared/Kafka/JsonSerdes.cs)). An Avro schema file exists for evolution exercises: [`Shared/Schemas/OrderCreated.avsc`](Shared/Schemas/OrderCreated.avsc). Schema Registry runs in Compose for that learning path.

---

## 5. Kafka cluster concepts (Phase 1 infrastructure)

### 5.1 Brokers, leaders, replicas, ISR

Compose runs **three brokers** in KRaft mode (no ZooKeeper): [`docker-compose.yml`](docker-compose.yml).

- Topic `orders` is created with **6 partitions** and **replication factor 3** so you can inspect Leader / Replicas / ISR in Kafka UI (`http://localhost:8080`) or via `kafka-topics.sh --describe`.
- Host bootstrap: `localhost:29092,39092,49092` ([`KafkaDefaults`](Shared/Kafka/KafkaConstants.cs)).

Learning experiments (from the original lab HTML docs):

- Stop one broker while producing → watch ISR shrink and leader election.
- Compare producer `acks=all` vs weaker acks (historically in the old direct producer; today reliability sits on DB commit + Debezium).

### 5.2 KRaft

Controllers and brokers are combined in each node (`KAFKA_PROCESS_ROLES: broker,controller`). Quorum voters list all three nodes. This is modern Kafka ops — Zookeeper is gone.

### 5.3 Consumer lag

Lag ≈ how far committed offsets trail the log high watermark. Slow down a consumer with `PROCESS_DELAY_MS` and describe the group (see [`scripts/describe-lab.ps1`](scripts/describe-lab.ps1)).

---

## 6. Outbox + CDC (the Phase 2/3 heart)

### 6.1 Transactional Outbox

**In code:**

1. Aggregate raises domain events (`Order.Place`).
2. Handler saves the aggregate (`PlaceOrderCommandHandler`).
3. Before `SaveChanges` completes, `OutboxSaveChangesInterceptor` appends `OutboxMessage` rows.
4. One commit → either both order + outbox exist, or neither.

That removes dual-write between application and Kafka.

### 6.2 Why Debezium instead of a polling publisher?

| Approach                        | Pros                                                                     | Cons                                               |
| ------------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------- |
| Poll `outbox` table and produce | Simple to understand                                                     | Extra worker, polling delay, you own failure/retry |
| **CDC (Debezium)**              | Reads WAL; low lag; no app-side publisher; crash-safe with offsets/slots | Ops complexity; duplicates possible on reconnect   |

Debezium on crash mid-WAL generally yields **at-least-once** delivery to Kafka: **duplicates possible, lost committed DB events unlikely**. Downstream consumers should be idempotent.

Connector entry point: [`docker/debezium/outbox-connector.json`](docker/debezium/outbox-connector.json)  
Registration script: [`scripts/register-debezium.sh`](scripts/register-debezium.sh)

**Outbox Event Router SMT** unwraps rows into clean events on topic `orders` (`route.topic.replacement`), using `aggregateid` as key and expanded JSON `payload` as value.

### 6.3 Postgres requirement

Logical decoding needs `wal_level=logical` — set on the `postgres` service command in [`docker-compose.yml`](docker-compose.yml). Lab DB listens on host port **5433** to avoid clashing with a local Postgres on 5432.

Connection string: [`Order.Api/appsettings.json`](Order.Api/appsettings.json).

---

## 7. Read models & “Kafka Streams without JVM”

Kafka Streams is **JVM-only**. This repo follows the lab’s recommendation: simulate the idea with `Confluent.Kafka` in .NET.

In [`ReadModel.Processor/Program.cs`](src/ReadModel.Processor/Program.cs):

1. **Consume** `orders` (stream of facts).
2. **Upsert** into an in-memory store ≈ local KTable state.
3. **Produce** to compacted `order-read-model` (changelog / queryable current state by `OrderId`).

Concepts to verbalize in interviews:

- **KStream** — unbounded sequence of events.
- **KTable** — changelog-derived latest state per key.
- Read-model lag ⇒ **eventual consistency**.

---

## 8. Payment transactional pipeline (advanced Kafka API)

[`PaymentService/Program.cs`](PaymentService/Program.cs) demonstrates **read–process–write** atomicity inside Kafka:

- `InitTransactions` / `BeginTransaction`
- Produce to `payment-results`
- `SendOffsetsToTransaction` with `consumer.ConsumerGroupMetadata`
- `CommitTransaction` or `AbortTransaction` (`SIMULATE_FAILURE=true`)

This is exactly-once **between Kafka topics**, not automatically exactly-once into an external database (that usually needs Outbox / 2PC-style patterns again).

---

## 9. How to run (quick)

```powershell
cd D:\Projects\Testbench\Arch.Kafka
docker compose up -d

dotnet run --project Order.Api
dotnet run --project src/ReadModel.Processor
# optional:
dotnet run --project src/TrafficGenerator.Worker
dotnet run --project PaymentService
dotnet run --project NotificationService
dotnet run --project InventoryService
```

Smoke test:

```powershell
curl -X POST http://localhost:5180/orders -H "Content-Type: application/json" -d "{\"customerId\":\"cust-42\",\"productId\":\"sku-1\",\"quantity\":1,\"amount\":99}"
curl http://localhost:5181/orders
```

UIs / ports: Kafka UI `:8080`, Schema Registry `:8081`, Connect `:8083`, Order API `:5180`, Read model `:5181`, Postgres `:5433`.

---

## 10. Mental model cheat sheet

| Question                               | Short answer                   | Code anchor                           |
| -------------------------------------- | ------------------------------ | ------------------------------------- |
| Where does an order “start”?           | `POST /orders`                 | `Order.Api/Program.cs`                |
| Where is business rule + event raised? | Aggregate `Place`              | `Order.Domain/.../Order.cs`           |
| How do we avoid dual-write?            | Outbox in same EF transaction  | `OutboxSaveChangesInterceptor.cs`     |
| Who publishes to Kafka?                | Debezium, not Order.Api        | `outbox-connector.json`               |
| Why three consumers all see the event? | Different consumer groups      | `ConsumerGroups` + three `Program.cs` |
| How is per-customer order preserved?   | Key = CustomerId               | Interceptor + Event Router            |
| Where is CQRS read side?               | ReadModel processor            | `ReadModel.Processor/Program.cs`      |
| Where is EOS Kafka→Kafka?              | Payment transactional producer | `PaymentService/Program.cs`           |

---

## 11. Related HTML lab notes in the repo

| File                                                             | Focus                                                              |
| ---------------------------------------------------------------- | ------------------------------------------------------------------ |
| [`1- Kafka Project.html`](1-%20Kafka%20Project.html)             | Hands-on Kafka cluster, producer/consumer drills                   |
| [`2- Project Struct.html`](2-%20Project%20Struct.html)           | C# solution structure, Outbox interceptor sketch, TrafficGenerator |
| [`3- Kafka Event Driven.html`](3-%20Kafka%20Event%20Driven.html) | Outbox, Debezium, read model, failure questions                    |

This markdown file ties those narratives to **the actual code paths** that were implemented.

---

## 12. What is still incomplete (honest backlog)

Useful so you do not assume production completeness:

- Automated chaos (kill API mid-batch) is guided, not fully scripted.
- Read-model state is **in-memory** (not RocksDB/SQLite).
- No unit/integration test projects yet.
- No FluentValidation; cancel-order HTTP API not exposed.
- Connector registration timing vs `EnsureCreated` can race on first boot — check Connect status if `orders` stays empty.

---

_Last aligned with the Event-Driven / Outbox phase of `kafka-hands-on`. When you add features, extend the entry-point table in §2 so this file stays the map of the system._
