# Stack Decision: Paper Stack vs Modern Cloud-Native

## Context

The paper (IJRAI 2019) describes an implementation using Oracle EBS → Debezium → Apache Kafka → Apache Spark Structured Streaming → Apache Cassandra → Apache Solr. This document evaluates that stack against a modern cloud-native alternative and records the decision rationale.

---

## Stack Comparison

### Layer 1 — Source System
| | Paper Stack | Modern Stack (Selected) |
|--|------------|------------------------|
| System | Oracle EBS | PostgreSQL (Aurora) or Oracle EBS |
| Notes | Enterprise standard | Aurora adds CDC-friendly WAL; Oracle supported via LogMiner |

### Layer 2 — Change Data Capture
| | Paper Stack | Modern Stack (Selected) |
|--|------------|------------------------|
| Tool | Debezium | Debezium (same) |
| Transport | Direct to Kafka | MSK Connect (managed Debezium) |
| Notes | Debezium is the de facto standard — no reason to change | MSK Connect eliminates connector VM management |

### Layer 3 — Message Broker
| | Paper Stack | Modern Stack (Selected) |
|--|------------|------------------------|
| Technology | Apache Kafka (self-managed) | Amazon MSK (managed Kafka) |
| Throughput | 50K+ events/sec ✓ | 50K+ events/sec ✓ |
| Ops burden | High — ZooKeeper/KRaft, broker upgrades, rebalancing | Low — AWS manages brokers, patches, storage |
| Cost model | EC2 + EBS (fixed) | MSK pricing by broker-hour (predictable) |
| **Verdict** | | **MSK wins** — same API, zero ops, multi-AZ by default |

### Layer 4 — Stream Processing
| Criterion | Paper: Apache Spark Structured Streaming | **Modern: Apache Flink (Amazon Managed)** |
|-----------|------------------------------------------|------------------------------------------|
| Latency | 100ms–1s (micro-batch) | < 10ms (true streaming) |
| State management | External (Redis/RocksDB sidecar) | Native RocksDB embedded per operator |
| Windowing | Limited — batch-oriented | Rich — event-time, watermarks, late data handling |
| Exactly-once | Limited support | Native exactly-once via two-phase commit |
| Checkpoint/recovery | Slower (Spark checkpoint to HDFS) | Fast (incremental checkpoints to S3, 30s interval) |
| Per-event processing | Requires DStream or micro-batch tricks | First-class citizen |
| Python support | PySpark (mature) | PyFlink (mature since Flink 1.16) |
| AWS managed service | EMR (heavy, cluster-oriented) | Amazon Managed Service for Apache Flink (lightweight) |
| **Verdict** | | **Flink wins decisively** — lower latency, better state, true streaming semantics |

### Layer 5 — Storage
| Criterion | Paper: Apache Cassandra | **Modern: ScyllaDB** |
|-----------|------------------------|----------------------|
| API compatibility | CQL | CQL (drop-in replacement) |
| Throughput | ~50K writes/sec (JVM GC pauses) | ~1M writes/sec (C++, no GC) |
| P99 write latency | 10–50ms (GC spikes) | < 1ms consistently |
| Memory efficiency | Lower (JVM overhead) | Higher (C++ native) |
| AWS deployment | EC2 + manual ops OR Amazon Keyspaces (limited) | EC2 / EKS — same ops as Cassandra but better perf |
| Cassandra driver compatibility | Full | Full — same driver, same CQL |
| Shard-aware driver | No | Yes (ScyllaDB shard-aware driver for 3–5x throughput) |
| **Verdict** | | **ScyllaDB wins** — same CQL, dramatically lower latency, no GC pauses at 50K+ events/sec |

### Layer 6 — Indexing and Search
| Criterion | Paper: Apache Solr | **Modern: Amazon OpenSearch (Elasticsearch)** |
|-----------|-------------------|----------------------------------------------|
| Query latency | Comparable | Comparable |
| Aggregation support | Good | Excellent (rich Agg API, scripted metrics) |
| AWS managed service | No | Yes (Amazon OpenSearch Service) |
| Operational complexity | High (SolrCloud + ZooKeeper) | Low (fully managed, auto-scaling) |
| Kibana/Dashboards | No native equivalent | OpenSearch Dashboards built-in |
| Schema management | Solr schema.xml (rigid) | Dynamic mappings + explicit templates |
| Vector search (future ML) | Limited | Native (k-NN plugin) |
| **Verdict** | | **OpenSearch wins** — managed, richer aggregation, future ML-ready |

### Layer 7 — API
| | Paper Stack | Modern Stack (Selected) |
|--|------------|------------------------|
| Technology | REST (unspecified) | FastAPI (Python, async) |
| Notes | Paper specifies REST + OpenAPI | FastAPI generates OpenAPI 3.0 natively, async I/O matches streaming workloads |

### Layer 8 — Dashboard
| | Paper Stack | Modern Stack (Selected) |
|--|------------|------------------------|
| Technology | Web dashboards (unspecified) | React + TypeScript + Recharts |
| Real-time updates | Server push | Server-Sent Events (SSE) via FastAPI |

---

## Final Stack Decision

```
Source DB (PostgreSQL/Oracle EBS)
    │
    ├── Debezium CDC (via MSK Connect)
    │
    ▼
Amazon MSK (Apache Kafka)
    │
    ├── Topic: orders.cdc          (partitioned by order_id)
    ├── Topic: inventory.cdc       (partitioned by product_id)
    ├── Topic: shipments.cdc       (partitioned by order_id)
    └── Topic: orders.enriched     (output from Flink)
    │
    ▼
Amazon Managed Service for Apache Flink (PyFlink)
    │
    ├── Order Lifecycle Job       — keyed state per order_id
    ├── Backlog Computation Job   — windowed aggregation
    └── Fulfillment Metrics Job   — supply-demand correlation
    │
    ├──────────────────┬──────────────────────┐
    ▼                  ▼                      ▼
ScyllaDB           OpenSearch              Amazon S3
(order records)    (search index)         (checkpoint + archive)
    │                  │
    └──────────────────┘
           │
           ▼
      FastAPI (REST + SSE)
           │
           ▼
    React Dashboard
```

---

## Key Trade-offs Accepted

| Trade-off | Impact | Mitigation |
|-----------|--------|-----------|
| ScyllaDB requires EC2/EKS (no fully managed AWS service) | Ops burden for ScyllaDB cluster vs Amazon Keyspaces | Use Terraform + ScyllaDB Helm chart for automated provisioning; Amazon Keyspaces as fallback if ops burden unacceptable |
| PyFlink is younger than PySpark | Smaller community, some rough edges in Table API | Pin to Flink 1.19+; use DataStream API for complex ops where Table API is immature |
| Flink exactly-once requires 2PC sinks | Higher write latency for exactly-once ScyllaDB writes | Use at-least-once + idempotent writes via upserts on order_id primary key |
| OpenSearch managed service costs | Higher than self-managed Solr at extreme scale | Reserved instance pricing; shard lifecycle policies to reduce hot index size |

---

## Technologies Not Chosen and Why

| Technology | Why Excluded |
|-----------|--------------|
| Amazon Kinesis Data Streams | Kafka is better for replay, consumer groups, and schema registry integration; MSK gives Kafka without ops cost |
| Apache Pulsar | Operationally complex; smaller ecosystem than Kafka for CDC integration |
| Apache Cassandra (self-managed) | ScyllaDB is a strict superset — no reason to use Cassandra at this scale |
| Amazon DynamoDB | No CQL API, limited aggregation, expensive at high write throughput with complex access patterns |
| Apache Spark Structured Streaming | Micro-batch latency (100ms+) does not meet < 1 second e2e target; Flink's native streaming is better |
| ClickHouse | Excellent for analytics but optimized for batch reads, not high-velocity individual event writes |
