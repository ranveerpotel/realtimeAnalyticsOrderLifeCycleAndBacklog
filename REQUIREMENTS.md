# Requirements Document
## Real-Time Analytics Platform for Enterprise Order Lifecycle Visibility and Backlog Management

**Version:** 1.0  
**Based on:** IJRAI 2019 — Ranveer Potel  
**Target Scale:** Enterprise (50,000+ events/sec, 500+ concurrent users, multi-region)  
**Target Infra:** AWS (MSK + Managed Flink + ScyllaDB + OpenSearch)

---

## 1. Problem Statement

Enterprise order management systems rely on overnight batch ETL pipelines, introducing up to 24-hour latency between transactional events and analytical insight. This prevents operational teams from responding to demand fluctuations, supply disruptions, and fulfillment anomalies in time to prevent revenue leakage and customer dissatisfaction.

This platform replaces batch pipelines with a streaming analytics architecture delivering sub-second order visibility across distributed enterprise systems.

---

## 2. Stakeholders and User Roles

| Role | Responsibility | Primary Use Case |
|------|---------------|-----------------|
| Supply Chain Planner | Inventory allocation, demand balancing | Backlog depth by product family and horizon |
| Fulfillment Operations Manager | Order execution, exception handling | Real-time order status, exception queues |
| Finance Controller | Revenue recognition, commit accuracy | Cycle time distribution, fulfillment rate |
| Customer Service Representative | Order status queries, delivery ETAs | Per-order lifecycle timeline |
| Platform Engineer | Operational health, SLA monitoring | Pipeline lag, throughput, error rates |

---

## 3. Functional Requirements

### 3.1 Change Data Capture (CDC) Layer

| ID | Requirement |
|----|-------------|
| FR-CDC-01 | The system MUST capture row-level inserts, updates, and deletes from Oracle EBS / PostgreSQL source systems via log-based CDC without polling. |
| FR-CDC-02 | CDC events MUST include: operation type (INSERT/UPDATE/DELETE), before/after row payload, source table name, schema name, log sequence number (LSN), and high-resolution timestamp. |
| FR-CDC-03 | The CDC connector MUST support schema evolution — addition of new columns in source tables MUST NOT crash downstream consumers. |
| FR-CDC-04 | CDC-to-broker end-to-end propagation latency MUST be under 500ms at the 99th percentile under peak load. |
| FR-CDC-05 | Source systems MUST experience no measurable query performance degradation from CDC activity (log-read only, no query-based extraction). |

### 3.2 Streaming Ingestion Layer (Apache Kafka / Amazon MSK)

| ID | Requirement |
|----|-------------|
| FR-INS-01 | The broker MUST sustain 50,000 events per second ingest throughput at a 3-broker minimum configuration without message loss. |
| FR-INS-02 | Topics MUST be partitioned by order_id to preserve per-order event ordering across all consumers. |
| FR-INS-03 | A secondary hash-based sub-partitioning scheme MUST be applied to high-cardinality customer keys to prevent partition hotspots. |
| FR-INS-04 | Consumer group isolation MUST allow independent consumption by: Flink processing jobs, audit log consumers, and replay consumers. |
| FR-INS-05 | Topic retention MUST be configurable per topic (default: 7 days) to support event replay and recovery. |
| FR-INS-06 | The platform MUST support at-least-once delivery with idempotent consumer semantics enforced at the processing layer. |

### 3.3 Stream Processing Layer (Apache Flink / Amazon Managed Flink)

| ID | Requirement |
|----|-------------|
| FR-PROC-01 | The processing engine MUST maintain per-order keyed state aggregating all CDC events into a current-state order record. |
| FR-PROC-02 | The system MUST compute the following derived metrics continuously: order intake rate, fulfillment cycle time, backlog depth, supply-demand gap, and on-time shipment rate. |
| FR-PROC-03 | Windowed aggregations MUST support tumbling windows (hourly, daily) and sliding windows (configurable 1–72 hours). |
| FR-PROC-04 | The processing engine MUST perform supply-demand correlation: joining order demand records with available inventory and scheduled supply receipts to compute real-time ATP (Available-to-Promise) feasibility. |
| FR-PROC-05 | State MUST be checkpointed to durable storage (S3) at 30-second intervals. Recovery from processor failure MUST complete within 60 seconds with zero data loss. |
| FR-PROC-06 | The system MUST detect and handle schema version mismatches gracefully, using a schema registry for forward-compatibility enforcement. |
| FR-PROC-07 | Processing latency (event ingestion to ScyllaDB commit) MUST be under 500ms at the 95th percentile, under 2 seconds at the 99th percentile, under peak load. |
| FR-PROC-08 | Exactly-once semantics MUST be enforced for backlog metric updates to prevent double-counting under failure and recovery. |

### 3.4 Storage Layer (ScyllaDB)

| ID | Requirement |
|----|-------------|
| FR-STOR-01 | The storage layer MUST support sustained write throughput matching the processing engine output without write-path bottlenecks. |
| FR-STOR-02 | Order entity records MUST be keyed by order_id and stored as denormalized documents aggregating line items, status history, allocation records, and shipment data. |
| FR-STOR-03 | Partition-aware data distribution MUST be aligned with Kafka partitioning scheme to minimize cross-partition operations. |
| FR-STOR-04 | The system MUST support tunable consistency: eventual consistency for real-time dashboard reads, strong consistency for financial audit reads. |
| FR-STOR-05 | Multi-region replication MUST be configured to support globally distributed operational teams with sub-100ms read-path latency per region. |
| FR-STOR-06 | TTL-based data expiration MUST manage storage costs for historical records beyond a configurable active analytics window (default: 90 days hot, 1 year warm). |

### 3.5 Indexing and Search Layer (Amazon OpenSearch / Elasticsearch)

| ID | Requirement |
|----|-------------|
| FR-SRCH-01 | The index MUST be synchronized from ScyllaDB within 3 seconds of storage commit at the 95th percentile under normal load. |
| FR-SRCH-02 | The index MUST support full-text and structured search across: customer name, product description, order reference, geographic region, status, business unit. |
| FR-SRCH-03 | Pre-computed aggregations for backlog summaries, fulfillment performance, and demand trend analysis MUST be returned in under 1 second at the 95th percentile. |
| FR-SRCH-04 | Faceted filtering MUST be supported across: order status, business unit, product family, date range, priority tier, and channel. |
| FR-SRCH-05 | Time-series histogram analysis for order intake, fulfillment, and cancellation trends MUST be supported over configurable windows. |
| FR-SRCH-06 | Index sharding MUST be aligned to business unit boundaries so regional queries are served by dedicated shard resources. |

### 3.6 API Layer (FastAPI)

| ID | Requirement |
|----|-------------|
| FR-API-01 | All API endpoints MUST be versioned (v1, v2) and documented via OpenAPI 3.0 specification. |
| FR-API-02 | The API MUST expose distinct endpoint families for: order entity queries, backlog summary metrics, fulfillment performance reporting, and supply availability lookups. |
| FR-API-03 | Response payloads MUST include pre-computed aggregations and inline enrichment data returned directly from the index — no client-side computation required. |
| FR-API-04 | The API MUST support Server-Sent Events (SSE) for dashboard real-time push — sub-minute refresh cadence. |
| FR-API-05 | Rate limiting MUST be enforced per API key: 1,000 req/min for operational users, 10,000 req/min for system integrations. |
| FR-API-06 | Frequently requested aggregation results MUST be cached with a TTL of 15–30 seconds to protect the indexing layer under high-concurrency dashboard refreshes. |
| FR-API-07 | The API MUST support role-based access control (RBAC) with JWT authentication, scoping data access by business unit and operational role. |

### 3.7 Analytics Delivery and Dashboards

| ID | Requirement |
|----|-------------|
| FR-DASH-01 | Operational dashboards MUST provide role-tailored views for: supply chain planners, fulfillment ops, finance controllers, and customer service reps. |
| FR-DASH-02 | The following metrics MUST be surfaced in the dashboard: global order volume and intake rate, current backlog by product family / BU / horizon, fulfillment performance (on-time rate, cycle time, exception count), per-order status drill-down with lifecycle timeline, supply availability heatmap with fulfillment date confidence intervals. |
| FR-DASH-03 | Dashboard data MUST refresh on a sub-minute cadence using SSE. |
| FR-DASH-04 | Alert thresholds MUST be configurable per dashboard: backlog depth, cycle time, and fulfillment exception rate. Alerts MUST trigger notifications when thresholds are breached. |
| FR-DASH-05 | The platform MUST support embedding analytics within ERP and customer service platforms via iframe and widget integration patterns. |

---

## 4. Non-Functional Requirements

### 4.1 Performance

| Metric | Target |
|--------|--------|
| End-to-end data latency (source commit → dashboard-queryable) | Median < 1 second; P99 < 8 seconds under peak load |
| Dashboard query response time | Median < 500ms; P95 < 2–3 seconds for complex multi-dimensional queries |
| Streaming ingest throughput | 50,000 events/sec sustained at 3-broker configuration |
| Processing throughput | 30,000 events/sec per Flink worker; horizontal scaling via worker addition |
| Index refresh latency | P95 < 3 seconds; P99 < 8 seconds |
| Concurrent dashboard users | 500+ simultaneous active sessions without query SLA degradation |

### 4.2 Availability and Reliability

| Metric | Target |
|--------|--------|
| Platform availability (SLA) | 99.9% (excluding planned maintenance) |
| Recovery Time Objective (RTO) | < 60 seconds for any single-component failure |
| Recovery Point Objective (RPO) | Zero — CDC buffers events during recovery periods |
| Broker node failure recovery | Automatic, < 60 seconds |
| Processing worker failure recovery | Automatic from last checkpoint (30-second interval), < 60 seconds |
| Storage node failure recovery | Automatic via replication (RF=3) |

### 4.3 Scalability

- The streaming ingestion layer MUST demonstrate linear throughput scaling by adding brokers/partitions.
- The processing layer MUST scale horizontally by adding Flink workers without architectural changes.
- The storage and indexing layers MUST scale by adding nodes without service interruption.
- All components MUST support auto-scaling in response to consumer lag metrics.

### 4.4 Security

- All data in transit MUST be encrypted using TLS 1.2+.
- All data at rest MUST be encrypted using AES-256 (AWS KMS).
- API authentication MUST use JWT with RS256 signing.
- Access control MUST be enforced at the business unit level — users MUST NOT access data outside their assigned units.
- VPC-isolated deployment with private subnets for all data-tier components.
- Audit logging of all API access MUST be retained for 1 year.

### 4.5 Observability

- Consumer lag per Kafka topic/partition MUST be tracked and alerted (threshold: > 10,000 messages).
- Processing throughput, latency percentiles, error rates, and state store utilization MUST be exposed as CloudWatch metrics.
- Distributed tracing (AWS X-Ray or OpenTelemetry) MUST be enabled across CDC → Kafka → Flink → ScyllaDB → API.
- A dedicated operational runbook MUST exist for each failure mode.

---

## 5. User Stories

### Supply Chain Planner
- As a supply chain planner, I want to see the current backlog depth by product family and fulfillment horizon so I can prioritize allocation decisions.
- As a supply chain planner, I want to be alerted when backlog depth for a product family exceeds a threshold so I can take proactive action within the same operational shift.

### Fulfillment Operations Manager
- As a fulfillment operations manager, I want to see all orders currently in exception status (stuck, delayed, allocation failure) so I can assign resolution resources.
- As a fulfillment operations manager, I want to drill into a single order and see its full lifecycle timeline (creation → allocation → production → shipment) so I can identify which step caused the delay.

### Finance Controller
- As a finance controller, I want to see the fulfillment cycle time distribution and on-time shipment rate by business unit so I can report operational performance.
- As a finance controller, I want to export a point-in-time snapshot of backlog and open order position for financial close reconciliation.

### Customer Service Rep
- As a customer service representative, I want to search an order by customer name, order reference, or product description and immediately see its current status and estimated delivery date so I can answer customer inquiries accurately.

---

## 6. Acceptance Criteria

| ID | Criterion | Pass Condition |
|----|-----------|----------------|
| AC-01 | End-to-end latency under normal load | P50 < 1s, P99 < 8s measured in load test at 50K events/sec |
| AC-02 | Dashboard query response | P95 < 3s for complex aggregation queries with 500 concurrent users |
| AC-03 | Fault tolerance | System recovers within 60 seconds from single broker/worker/storage node failure with zero data loss |
| AC-04 | Throughput under load | 50,000 events/sec sustained for 30 minutes without message loss or consumer lag growth |
| AC-05 | Schema evolution | Adding a nullable column to source table does not break any downstream consumer |
| AC-06 | Security | All API endpoints reject unauthenticated requests; RBAC prevents cross-business-unit data access |
| AC-07 | Backlog computation accuracy | Backlog metrics match independently computed batch values within 0.1% after settling period |
| AC-08 | Index refresh | P95 index-to-queryable latency < 3 seconds measured over 1-hour window |

---

## 7. Out of Scope (v1.0)

- Machine learning demand forecasting within the streaming pipeline (future roadmap)
- Real-time ATP order promising logic replacing batch CTP computation (future roadmap)
- Financial-grade strict consistency audit reporting (supplementary batch process, not this platform)
- Mobile native applications (web-responsive dashboard covers this initially)
