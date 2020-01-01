# Technical Design Document
## Real-Time Analytics Platform — Enterprise Order Lifecycle Visibility

**Version:** 1.0  
**Stack:** PostgreSQL/Oracle → Debezium/MSK Connect → Amazon MSK → Apache Flink (Amazon Managed) → ScyllaDB + OpenSearch → FastAPI → React

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: SOURCE SYSTEMS                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │ Oracle EBS   │  │ WMS          │  │ TMS          │  │ MES          │    │
│  │ (Orders/Inv) │  │ (Warehouse)  │  │ (Shipments)  │  │ (Production) │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
└─────────┼──────────────────┼──────────────────┼──────────────────┼───────────┘
          │ WAL/Redo Log     │ WAL              │ WAL              │ WAL
┌─────────▼──────────────────▼──────────────────▼──────────────────▼───────────┐
│  LAYER 2: CDC (Debezium via MSK Connect)                                       │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │  Debezium Connectors (Oracle LogMiner / PostgreSQL pgoutput)              │ │
│  │  Schema Registry (AWS Glue Schema Registry — Avro/JSON Schema)           │ │
│  └────────────────────────────────────┬─────────────────────────────────────┘ │
└───────────────────────────────────────┼───────────────────────────────────────┘
                                        │ Structured CDC Events
┌───────────────────────────────────────▼───────────────────────────────────────┐
│  LAYER 3: STREAMING INGESTION (Amazon MSK — Apache Kafka)                      │
│                                                                                 │
│  Topic: orders.cdc          partitions=64,  key=order_id,     RF=3            │
│  Topic: inventory.cdc       partitions=32,  key=product_id,   RF=3            │
│  Topic: shipments.cdc       partitions=32,  key=order_id,     RF=3            │
│  Topic: production.cdc      partitions=16,  key=order_id,     RF=3            │
│  Topic: orders.enriched     partitions=64,  key=order_id,     RF=3            │
│  Topic: backlog.metrics     partitions=16,  key=bu_product,   RF=3            │
│  Topic: dlq.failed_events   partitions=8,   key=source_topic, RF=3            │
└───────────────────────────────────────────────────────────────────────────────┘
          │                         │                       │
          ▼                         ▼                       ▼
┌─────────────────┐       ┌─────────────────┐    ┌─────────────────────────────┐
│  LAYER 4: STREAM PROCESSING (Amazon Managed Service for Apache Flink)         │
│                                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ Job 1: Order Lifecycle Aggregation                                       │  │
│  │   • Keyed by order_id                                                   │  │
│  │   • Stateful fold: CDC events → current order state                     │  │
│  │   • Enrichment: join with inventory and production state                │  │
│  │   • Output: orders.enriched topic + ScyllaDB upsert                    │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ Job 2: Backlog Computation                                               │  │
│  │   • Keyed by (business_unit, product_family, horizon_bucket)            │  │
│  │   • Sliding window (1h, 24h, 7d) aggregation of unfulfilled demand      │  │
│  │   • Supply-demand correlation with inventory CDC stream                 │  │
│  │   • Output: backlog.metrics topic + ScyllaDB upsert                    │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ Job 3: Fulfillment Metrics                                               │  │
│  │   • Windowed: on-time rate, cycle time distribution, exception counts   │  │
│  │   • Output: ScyllaDB metrics table                                      │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  Checkpoint: S3 every 30 seconds  |  Restart: from last checkpoint           │
└───────────────────────┬────────────────────────────────────────────────────────┘
                        │
          ┌─────────────┴──────────────┐
          ▼                            ▼
┌──────────────────┐         ┌──────────────────────────────────────┐
│  LAYER 5: STORAGE│         │  LAYER 6: INDEXING                    │
│  ScyllaDB        │         │  Amazon OpenSearch Service            │
│  (3-node RF=3)   │         │  (3-node cluster, 6 shards/index)    │
│                  │─sync──▶ │                                       │
│  Keyspace:       │  < 3s   │  Index: orders                       │
│  orders          │         │  Index: backlog_metrics               │
│  backlog         │         │  Index: fulfillment_metrics           │
│  fulfillment     │         │                                       │
│  metrics         │         │  Index Sync Consumer (Python)         │
└────────┬─────────┘         └────────────┬─────────────────────────┘
         │                                │
         └─────────────┬──────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 7: API (FastAPI — ECS Fargate, 4+ replicas)               │
│                                                                    │
│  GET  /v1/orders/{order_id}              → ScyllaDB direct read   │
│  GET  /v1/orders/search                  → OpenSearch query        │
│  GET  /v1/backlog/summary                → OpenSearch aggregation  │
│  GET  /v1/backlog/{bu}/{product_family}  → OpenSearch             │
│  GET  /v1/fulfillment/metrics            → ScyllaDB metrics table  │
│  GET  /v1/supply/availability/{sku}      → ScyllaDB direct read   │
│  GET  /v1/stream/dashboard               → SSE endpoint           │
│                                                                    │
│  Auth: JWT (RS256) + RBAC per business unit                       │
│  Rate limit: 1,000 req/min (user), 10,000 req/min (system)       │
│  Cache: Redis (ElastiCache) — 15–30s TTL on aggregation results  │
└──────────────────────────────────────────────────────────────────┘
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│  LAYER 8: DASHBOARDS (React — CloudFront + S3)                   │
│                                                                    │
│  • Supply Chain Planner view  (backlog depth, supply heatmap)    │
│  • Fulfillment Ops view       (exception queue, order drill-down) │
│  • Finance view               (cycle time, on-time rate, BU KPIs)│
│  • Customer Service view      (order search, status, ETA)        │
│                                                                    │
│  Real-time updates: SSE (sub-minute refresh)                      │
│  Alerts: configurable thresholds → browser + email/SNS           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Models

### 2.1 Kafka Topic Schema — CDC Event (Avro)

```json
{
  "type": "record",
  "name": "CdcEvent",
  "namespace": "com.enterprise.orders.cdc",
  "fields": [
    {"name": "source_table",    "type": "string"},
    {"name": "operation",       "type": {"type": "enum", "name": "Op", "symbols": ["INSERT","UPDATE","DELETE","SNAPSHOT"]}},
    {"name": "lsn",             "type": "long"},
    {"name": "timestamp_ms",    "type": "long"},
    {"name": "schema_version",  "type": "int"},
    {"name": "before",          "type": ["null", {"type": "map", "values": ["null","string","int","long","double","boolean"]}]},
    {"name": "after",           "type": ["null", {"type": "map", "values": ["null","string","int","long","double","boolean"]}]}
  ]
}
```

### 2.2 Enriched Order Event — Kafka (Avro)

```json
{
  "type": "record",
  "name": "EnrichedOrderEvent",
  "namespace": "com.enterprise.orders.enriched",
  "fields": [
    {"name": "order_id",           "type": "string"},
    {"name": "customer_id",        "type": "string"},
    {"name": "customer_name",      "type": "string"},
    {"name": "channel",            "type": "string"},
    {"name": "business_unit",      "type": "string"},
    {"name": "product_family",     "type": "string"},
    {"name": "order_status",       "type": "string"},
    {"name": "priority_tier",      "type": "int"},
    {"name": "created_at",         "type": "long"},
    {"name": "updated_at",         "type": "long"},
    {"name": "line_items",         "type": {"type": "array", "items": "LineItem"}},
    {"name": "allocation_records", "type": {"type": "array", "items": "AllocationRecord"}},
    {"name": "shipment_data",      "type": ["null", "ShipmentData"]},
    {"name": "atp_feasible",       "type": "boolean"},
    {"name": "expected_ship_date", "type": ["null", "long"]},
    {"name": "pipeline_timestamp", "type": "long"}
  ]
}
```

### 2.3 ScyllaDB Schema

```cql
CREATE KEYSPACE orders WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'us-east-1': 3,
    'us-west-2': 3
} AND durable_writes = true;

-- Primary order state table
CREATE TABLE orders.order_state (
    order_id          TEXT,
    business_unit     TEXT,
    customer_id       TEXT,
    customer_name     TEXT,
    channel           TEXT,
    product_family    TEXT,
    order_status      TEXT,
    priority_tier     INT,
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP,
    total_value       DECIMAL,
    currency          TEXT,
    line_items        TEXT,       -- JSON blob
    allocation_records TEXT,      -- JSON blob
    shipment_data     TEXT,       -- JSON blob
    status_history    LIST<TEXT>, -- JSON entries
    atp_feasible      BOOLEAN,
    expected_ship_date TIMESTAMP,
    pipeline_ts       TIMESTAMP,
    PRIMARY KEY (order_id)
) WITH default_time_to_live = 7776000  -- 90 days
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 86400;

-- Lookup by business unit + status (for dashboard queries)
CREATE TABLE orders.orders_by_bu_status (
    business_unit TEXT,
    order_status  TEXT,
    updated_at    TIMESTAMP,
    order_id      TEXT,
    customer_name TEXT,
    product_family TEXT,
    priority_tier  INT,
    PRIMARY KEY ((business_unit, order_status), updated_at, order_id)
) WITH CLUSTERING ORDER BY (updated_at DESC)
  AND default_time_to_live = 7776000;

-- Backlog metrics
CREATE TABLE orders.backlog_metrics (
    business_unit    TEXT,
    product_family   TEXT,
    horizon_bucket   TEXT,    -- 'D1','D7','D30','D90'
    window_type      TEXT,    -- 'sliding_1h','sliding_24h','tumbling_daily'
    window_start     TIMESTAMP,
    unfulfilled_demand DECIMAL,
    available_supply   DECIMAL,
    demand_gap         DECIMAL,
    order_count        INT,
    avg_priority       DOUBLE,
    computed_at        TIMESTAMP,
    PRIMARY KEY ((business_unit, product_family, horizon_bucket), window_type, window_start)
) WITH CLUSTERING ORDER BY (window_type ASC, window_start DESC)
  AND default_time_to_live = 2592000;  -- 30 days

-- Fulfillment metrics
CREATE TABLE orders.fulfillment_metrics (
    business_unit  TEXT,
    metric_date    DATE,
    on_time_rate   DOUBLE,
    cycle_time_p50 DOUBLE,
    cycle_time_p95 DOUBLE,
    exception_count INT,
    total_orders   INT,
    shipped_orders INT,
    computed_at    TIMESTAMP,
    PRIMARY KEY (business_unit, metric_date)
) WITH CLUSTERING ORDER BY (metric_date DESC)
  AND default_time_to_live = 31536000;  -- 1 year

-- Supply availability
CREATE TABLE orders.supply_availability (
    sku           TEXT,
    warehouse_id  TEXT,
    available_qty DECIMAL,
    allocated_qty DECIMAL,
    on_order_qty  DECIMAL,
    last_receipt  TIMESTAMP,
    next_receipt  TIMESTAMP,
    updated_at    TIMESTAMP,
    PRIMARY KEY (sku, warehouse_id)
);
```

### 2.4 OpenSearch Index Mappings

**Index: `orders`**
```json
{
  "mappings": {
    "properties": {
      "order_id":        {"type": "keyword"},
      "customer_id":     {"type": "keyword"},
      "customer_name":   {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
      "channel":         {"type": "keyword"},
      "business_unit":   {"type": "keyword"},
      "product_family":  {"type": "keyword"},
      "order_status":    {"type": "keyword"},
      "priority_tier":   {"type": "integer"},
      "created_at":      {"type": "date"},
      "updated_at":      {"type": "date"},
      "total_value":     {"type": "double"},
      "atp_feasible":    {"type": "boolean"},
      "expected_ship_date": {"type": "date"},
      "line_items": {
        "type": "nested",
        "properties": {
          "sku":         {"type": "keyword"},
          "description": {"type": "text"},
          "quantity":    {"type": "double"},
          "unit_price":  {"type": "double"}
        }
      },
      "pipeline_ts": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards":   6,
    "number_of_replicas": 1,
    "refresh_interval":   "1s"
  }
}
```

---

## 3. Flink Job Design

### 3.1 Order Lifecycle Aggregation Job

```
Input:  orders.cdc, inventory.cdc, production.cdc
Output: orders.enriched (Kafka), orders.order_state (ScyllaDB)

Topology:
  KafkaSource(orders.cdc)
    → DeserializeDebeziumEvent
    → keyBy(order_id)
    → OrderStateAggregator(ValueState<OrderState>)
        - fold CDC event into current state
        - apply inventory enrichment from broadcast state
        - compute ATP feasibility
    → EnrichedOrderSink
        ├── KafkaSink(orders.enriched)
        └── ScyllaDBSink(orders.order_state, idempotent upsert)

Side input:
  KafkaSource(inventory.cdc) → BroadcastState<sku → InventoryState>
  KafkaSource(production.cdc) → BroadcastState<order_id → ProductionState>

Checkpointing:
  Mode: EXACTLY_ONCE (Flink + Kafka 2PC)
  Interval: 30 seconds
  Timeout: 60 seconds
  Storage: S3://bucket/flink-checkpoints/order-lifecycle/
```

### 3.2 Backlog Computation Job

```
Input:  orders.enriched (Kafka)
Output: backlog.metrics (Kafka), backlog.backlog_metrics (ScyllaDB)

Topology:
  KafkaSource(orders.enriched)
    → keyBy(business_unit, product_family, horizon_bucket)
    → SlidingEventTimeWindows(size=1h, slide=5min)
    │   → BacklogAggregateFunction
    │       - sum unfulfilled demand
    │       - join with supply position from broadcast
    │       - compute demand_gap = unfulfilled_demand - available_supply
    └── TumblingEventTimeWindows(size=1day)
        → DailyBacklogAggregateFunction

Watermark strategy:
  BoundedOutOfOrdernessWatermarks(maxOutOfOrderness=30s)
  Late data: side output → late_data topic for reconciliation
```

### 3.3 State Management

- **ValueState<OrderState>**: Per-order current state (keyed by order_id)
- **BroadcastState<String, InventoryState>**: Global inventory snapshot (SKU → qty)
- **MapState<String, Long>**: Per-order event sequence numbers (deduplication)
- **ListState<StatusEntry>**: Order status history

---

## 4. API Design

### 4.1 Endpoint Contract

```
GET /v1/orders/{order_id}
  Response: {
    order_id, customer, channel, business_unit, status,
    line_items[], allocation[], shipment, status_history[],
    atp_feasible, expected_ship_date, pipeline_ts
  }
  Source: ScyllaDB (consistency=LOCAL_QUORUM)

GET /v1/orders/search?q=&status=&bu=&product_family=&date_from=&date_to=&page=&size=
  Response: { total, orders[], facets{} }
  Source: OpenSearch

GET /v1/backlog/summary?bu=&horizon=
  Response: {
    business_unit, total_unfulfilled, supply_gap,
    by_product_family[{ family, demand, supply, gap, order_count }],
    trends[{ window, demand, supply }]
  }
  Source: OpenSearch aggregation (cached 30s)

GET /v1/backlog/{bu}/{product_family}
  Response: { metrics_by_horizon[], recent_orders[], supply_schedule[] }
  Source: ScyllaDB + OpenSearch

GET /v1/fulfillment/metrics?bu=&date_from=&date_to=
  Response: {
    on_time_rate, cycle_time_p50, cycle_time_p95,
    exception_count, by_date[]
  }
  Source: ScyllaDB

GET /v1/supply/availability/{sku}
  Response: { sku, warehouses[{ id, available, allocated, on_order, next_receipt }] }
  Source: ScyllaDB (consistency=LOCAL_ONE for low latency)

GET /v1/stream/dashboard?bu=&role=
  Response: text/event-stream (SSE)
  Events: { type: "backlog_update"|"order_update"|"alert", data: {} }
  Push interval: every 30s or on threshold breach
```

### 4.2 Authentication and Authorization

```
Auth: Bearer JWT (RS256)
Claims: { sub, role, business_units[], exp }

RBAC Matrix:
  supply_chain_planner  → backlog/*, supply/*, orders/search
  fulfillment_ops       → orders/*, fulfillment/*
  finance_controller    → fulfillment/metrics, orders/search
  customer_service      → orders/search, orders/{id}
  platform_engineer     → all endpoints + /v1/health/*, /v1/metrics/*

Data scoping: queries automatically filtered to JWT business_units[]
```

---

## 5. Kafka Topic Design

| Topic | Partitions | Key | Retention | Compaction |
|-------|-----------|-----|-----------|------------|
| `orders.cdc` | 64 | `order_id` | 7 days | None |
| `inventory.cdc` | 32 | `product_id` | 7 days | None |
| `shipments.cdc` | 32 | `order_id` | 7 days | None |
| `production.cdc` | 16 | `order_id` | 7 days | None |
| `orders.enriched` | 64 | `order_id` | 3 days | None |
| `backlog.metrics` | 16 | `bu+product_family` | 30 days | Log-compacted |
| `dlq.failed_events` | 8 | `source_topic` | 30 days | None |

**Hotspot mitigation**: Customers generating >5% of partition traffic receive a secondary hash sub-partition suffix applied to their order_id, distributing load across multiple partitions while preserving per-order ordering within each sub-partition.

---

## 6. Deployment Topology (AWS)

```
Region: us-east-1 (primary)    Region: us-west-2 (replica)

VPC (10.0.0.0/16)
  Private Subnets (3 AZs):
    - Amazon MSK (3 brokers, one per AZ)
    - ScyllaDB EKS cluster (3 nodes, one per AZ)
    - Amazon OpenSearch (3 nodes, one per AZ)
    - Amazon ElastiCache Redis (2 nodes, Multi-AZ)

  Application Subnets (3 AZs):
    - Amazon Managed Flink (auto-scaling task managers)
    - ECS Fargate (FastAPI, 4–16 replicas, auto-scaling)
    - MSK Connect workers (Debezium)

Public Subnets:
    - Application Load Balancer (API + Dashboard)

  Storage:
    - S3: Flink checkpoints, archived events, audit logs
    - AWS Glue Schema Registry: Avro schemas

  Monitoring:
    - Amazon CloudWatch: MSK, Flink, OpenSearch metrics
    - AWS X-Ray: distributed tracing
    - Grafana (ECS): custom dashboards over CloudWatch

  Security:
    - AWS KMS: encryption keys for S3, MSK, OpenSearch, ScyllaDB
    - AWS Secrets Manager: ScyllaDB credentials, API signing keys
    - VPC Security Groups: component-level network isolation
    - AWS WAF: API Gateway protection
```

---

## 7. Observability and Alerting

### 7.1 Key Metrics

| Component | Metric | Alert Threshold |
|-----------|--------|----------------|
| MSK | Consumer lag per partition | > 10,000 messages |
| MSK | Under-replicated partitions | > 0 |
| Flink | Job restarts | > 2 in 10 min |
| Flink | Checkpoint duration | > 45 seconds |
| Flink | Backpressure ratio | > 0.8 |
| ScyllaDB | Write latency P99 | > 5ms |
| ScyllaDB | Dropped messages | > 0 |
| OpenSearch | Index latency | > 5 seconds |
| OpenSearch | JVM heap | > 80% |
| API | Response time P95 | > 3 seconds |
| API | Error rate | > 1% |
| Pipeline | E2E latency P95 | > 5 seconds |

### 7.2 Distributed Tracing

Each event carries a `trace_id` (UUID) from CDC injection through the entire pipeline. Spans are emitted at:
- CDC event publication
- Kafka consumer poll
- Flink operator execution
- ScyllaDB write
- OpenSearch index sync
- API response

Traces are collected via OpenTelemetry → AWS X-Ray.

---

## 8. Schema Evolution Strategy

1. **Debezium**: Publishes schema changes to AWS Glue Schema Registry before publishing events with new schema.
2. **Glue Schema Registry**: Enforces backward/forward compatibility rules (BACKWARD_TRANSITIVE mode).
3. **Flink consumers**: Use schema registry client; deserialize with latest compatible schema version.
4. **Forward-compatible fields**: New nullable fields in CDC events do not break existing Flink operators (schema evolution handled by Avro GenericRecord deserialization).
5. **Breaking changes**: Require coordinated deployment — schema version bump + consumer upgrade before source publishes new schema version.

---

## 9. Fault Tolerance and Recovery

| Failure Scenario | Detection | Recovery |
|-----------------|-----------|----------|
| MSK broker failure | CloudWatch, topic under-replication | MSK auto-replaces broker; consumers rebalance; no data loss (RF=3) |
| Flink job failure | Job manager, CloudWatch restart metric | Restart from last checkpoint (max 30s data re-processed, exactly-once prevents double-counting) |
| ScyllaDB node failure | ScyllaDB metrics, nodetool status | RF=3 absorbs single-node loss; reads/writes continue; replacement node streams data automatically |
| OpenSearch node failure | OpenSearch metrics | Replica shards promote; index sync consumer reconnects; max index lag = sync consumer reconnect time (~5s) |
| Index sync consumer failure | Consumer lag alarm | ECS task replacement (< 30s); consumer resumes from last committed Kafka offset |
| API pod failure | ECS health check | ELB routes around unhealthy task; ECS replaces task (< 30s) |
| Full AZ failure | CloudWatch AZ metrics | Multi-AZ deployment absorbs single-AZ loss; MSK, ScyllaDB, OpenSearch all span 3 AZs |

---

## 10. Capacity Planning

At 50,000 events/second sustained:

| Component | Sizing |
|-----------|--------|
| MSK | 3 brokers × m5.4xlarge (16 vCPU, 64GB RAM), 10TB EBS each |
| Flink | 4–16 task managers × 4 vCPU / 16GB (auto-scale on consumer lag) |
| ScyllaDB | 3 nodes × i4i.4xlarge (16 vCPU, 128GB RAM, 3.75TB NVMe) |
| OpenSearch | 3 data nodes × r6g.2xlarge (8 vCPU, 64GB RAM, 1TB EBS) |
| ElastiCache | 2 nodes × r6g.large (Multi-AZ) |
| FastAPI | 4–16 ECS Fargate tasks × 2 vCPU / 4GB (auto-scale on CPU/request count) |

Write throughput budget:
- ScyllaDB: 50K writes/sec → well within i4i.4xlarge 3-node capacity (~500K writes/sec)
- OpenSearch: ~50K docs/sec → within r6g.2xlarge 3-node capacity with refresh_interval=1s

---

## 11. Implementation Phases

| Phase | Scope | Target Duration |
|-------|-------|----------------|
| P1 | Infrastructure (MSK, ScyllaDB, OpenSearch, Flink) + CDC pipeline | 4 weeks |
| P2 | Flink Order Lifecycle Job + ScyllaDB + basic API | 4 weeks |
| P3 | Backlog Computation Job + OpenSearch index + full API | 3 weeks |
| P4 | React dashboard + SSE + RBAC + alerting | 3 weeks |
| P5 | Load testing, performance tuning, multi-region replication | 2 weeks |
| P6 | Production deployment + parallel run vs batch baseline | 2 weeks |
