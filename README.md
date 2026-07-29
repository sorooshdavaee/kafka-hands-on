# Kafka Hands-on → Event-Driven Lab

Phase 1 (raw Kafka) + Phase 2 (Outbox / Debezium CDC / CQRS Read Model).

```
Order.Api  →  Postgres (orders + outbox_messages)  →  Debezium  →  topic: orders
                                                                      ├─ Payment / Notification / Inventory
                                                                      └─ ReadModel.Processor → order-read-model (compacted)
TrafficGenerator.Worker  →  POST /orders
```

## Solution layout

```
Arch.Kafka/
├── docker-compose.yml
├── docker/debezium/outbox-connector.json
├── docker/init-scripts/
├── Order.Api/                      # Composition root (Minimal API)
├── src/
│   ├── Order.Domain/               # Aggregates, DomainEvents, OutboxMessage
│   ├── Order.Application/          # Manual CQRS dispatcher (no MediatR)
│   ├── Order.Infrastructure/       # EF Core + OutboxSaveChangesInterceptor
│   ├── ReadModel.Processor/        # Consume orders → in-memory KTable + compacted topic
│   └── TrafficGenerator.Worker/    # Bogus traffic + chaos notes
├── PaymentService / NotificationService / InventoryService   # Phase-1 consumers
└── Shared/
```

## Prerequisites

- Docker Desktop
- .NET 10 SDK

## 1) Infrastructure

```powershell
cd D:\Projects\Testbench\Arch.Kafka
docker compose up -d
```

| Service | URL / Port |
|---------|------------|
| Kafka UI | http://localhost:8080 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect (Debezium) | http://localhost:8083 |
| Postgres (lab) | `localhost:5433` user/pass/db = `orders` |
| Brokers | `localhost:29092,39092,49092` |

> Lab Postgres uses **5433** so it won't clash with an existing local Postgres on 5432.

Register connector status:

```powershell
curl http://localhost:8083/connectors/orders-outbox-connector/status
```

## 2) Run apps

```powershell
dotnet run --project Order.Api
dotnet run --project src/ReadModel.Processor
dotnet run --project src/TrafficGenerator.Worker   # optional load
dotnet run --project PaymentService                # optional Phase-1 consumer
```

Place an order (writes DB + outbox in **one transaction** — no direct Kafka produce):

```powershell
curl -X POST http://localhost:5180/orders -H "Content-Type: application/json" -d "{\"customerId\":\"cust-42\",\"customerName\":\"Ada\",\"productId\":\"sku-100\",\"quantity\":2,\"amount\":150,\"discount\":10}"
```

Query write model:

```powershell
curl http://localhost:5180/orders/{orderId}
```

Query read model (eventually consistent):

```powershell
curl http://localhost:5181/orders
```

## Phase 2 flow

1. `POST /orders` → `Order.Place()` raises `OrderPlacedDomainEvent`
2. `OutboxSaveChangesInterceptor` inserts `outbox_messages` in the **same** `SaveChanges`
3. Debezium reads Postgres WAL → Outbox Event Router SMT → topic `orders` (key = `CustomerId`)
4. Consumers / ReadModel process `orders`
5. ReadModel upserts in-memory store and produces compacted `order-read-model`

## Chaos / dual-write demo

1. Start TrafficGenerator
2. Kill `Order.Api` mid-flight
3. Check `outbox_messages` in Postgres — committed rows remain; uncommitted vanish
4. Restart API + ensure Debezium is up — CDC continues from WAL (duplicates possible, **no lost committed events**)

```sql
SELECT id, aggregatetype, aggregateid, type, payload FROM outbox_messages ORDER BY occurred_on DESC LIMIT 20;
```

## Docs in repo

- `1- Kafka Project.html` — Phase 1 Kafka lab
- `2- Project Struct.html` — stack / solution structure (this phase)
- `3- Kafka Event Driven.html` — Outbox / CDC / Read Model goals
