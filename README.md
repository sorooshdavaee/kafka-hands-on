# Kafka Hands-on — Distributed Order Processing

Lab from `1- Kafka Project.html`: 3-broker KRaft cluster, Order API producer, three independent consumer groups (Choreography Saga).

```
Order.Api  -->  orders (6p / RF=3)  -->  payment-group
                                     -->  notification-group
                                     -->  inventory-group
```

## Structure

```
Arch.Kafka/
├── docker-compose.yml          # 3 brokers + Schema Registry + Kafka UI + topic init
├── scripts/
│   ├── create-topics.sh
│   └── describe-lab.ps1
├── Shared/                     # models, JSON serdes, consumer host helper
├── Order.Api/                  # ASP.NET Core — POST /orders (acks=all, idempotent)
├── PaymentService/             # Console — transactional EOS → payment-results
├── NotificationService/        # Console — notification-group
└── InventoryService/           # Console — inventory-group
```

## Prerequisites

- Docker Desktop
- .NET 10 SDK

## 1) Start Kafka cluster

```powershell
cd D:\Projects\Testbench\Arch.Kafka
docker compose up -d
```

- Kafka UI: http://localhost:8080  
- Schema Registry: http://localhost:8081  
- Brokers (host): `localhost:29092,39092,49092`

Topics created by `kafka-init`:

| Topic | Purpose |
|-------|---------|
| `orders` | 6 partitions, RF=3 |
| `payment-results` | Step 5 transactional target |
| `customer-latest-status` | Step 7 compacted |

Describe Leader/Replicas/ISR:

```powershell
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 --describe --topic orders
```

## 2) Run apps

```powershell
dotnet run --project Order.Api
dotnet run --project PaymentService
dotnet run --project NotificationService
dotnet run --project InventoryService
```

Create an order:

```powershell
curl -X POST http://localhost:5180/orders -H "Content-Type: application/json" -d "{\"customerId\":\"cust-42\",\"productId\":\"sku-100\",\"quantity\":2,\"amount\":150,\"discount\":10}"
```

Partition key = `customerId` (ordering per customer).

## Lab experiments

| Step | What to try |
|------|-------------|
| 2 Producer / ISR | While producing: `docker stop kafka-2` — watch Leader election & ISR in Kafka UI |
| 2 acks | Set `Kafka:Acks` to `1` in `appsettings.json` and repeat broker stop |
| 3 Rebalance | Run a second `PaymentService` instance — partitions split; try `PARTITION_ASSIGNMENT=Range` vs `CooperativeSticky` |
| 3 Commit mode | `COMMIT_BEFORE_PROCESS=true` on Notification/Inventory → at-most-once |
| 4 Lag | `PROCESS_DELAY_MS=500` on PaymentService, then `.\scripts\describe-lab.ps1` |
| 5 Exactly-once | `SIMULATE_FAILURE=true` on PaymentService — AbortTransaction, no `payment-results` + offset not committed |
| 6 Avro | Schema file: `Shared/Schemas/OrderCreated.avsc` — register via Schema Registry UI / API |
| 7 Compaction | Produce several statuses for same `customerId`, wait, consume `customer-latest-status` |

## Env vars (consumers)

| Variable | Effect |
|----------|--------|
| `KAFKA_BOOTSTRAP` | Override bootstrap (default localhost:29092,39092,49092) |
| `PARTITION_ASSIGNMENT` | `Range` or `CooperativeSticky` |
| `PROCESS_DELAY_MS` | Artificial processing delay (lag) |
| `COMMIT_BEFORE_PROCESS` | `true` = at-most-once (Notification/Inventory) |
| `SIMULATE_FAILURE` | PaymentService transactional abort demo |

## Notes

- Files `2- Project Struct.html` and `3- Kafka Event Driven.html` are the next phases (Outbox / Debezium / CQRS) — not part of this scaffold.
- Messages use JSON for a fast working lab; Avro schema is included for Step 6 exercises.
