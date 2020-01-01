# Real-Time Analytics Architecture for Enterprise Order Lifecycle Visibility and Backlog Management

> Implementation of the architecture described in the peer-reviewed paper published in the **International Journal of Research and Applied Innovations (IJRAI)**, Volume 2, Issue 6, November–December 2019.

**Paper:** [A Real-Time Analytics Architecture for Enterprise Order Lifecycle Visibility and Backlog Management](https://doi.org/10.15662/IJRAI.2019.0206004)  
**DOI:** `10.15662/IJRAI.2019.0206004`  
**Author:** Ranveer Potel — Independent Researcher, USA  
**Published:** IJRAI Vol. 2, Issue 6, Nov–Dec 2019 | ISSN: 2455-1864

---

## Overview

Enterprise order management systems traditionally rely on overnight batch ETL pipelines, introducing up to **24-hour latency** between transactional events and analytical insight. This platform replaces batch pipelines with a streaming analytics architecture that delivers **sub-second order visibility** across distributed enterprise systems.

Key results demonstrated in the paper:

| Metric | Legacy Batch System | This Platform |
|--------|---------------------|---------------|
| Data Latency | ~24 hours | < 1 second |
| Report Load Time | Several minutes | Seconds |
| Order Visibility | End-of-day | Immediate |
| Operational Response | Reactive | Proactive |
| Concurrent Users | ~50 | 500+ |
| Pipeline Failure Recovery | Next batch cycle | Automatic, milliseconds |

---

## Architecture

The system is built as six decoupled layers, each communicating through standardized interfaces:

```
Source DB (PostgreSQL / Oracle EBS)
    │
    ├── Debezium CDC  (log-based, zero query load on source)
    │
    ▼
Amazon MSK  (Apache Kafka — 64 partitions, keyed by order_id)
    │
    ▼
Amazon Managed Service for Apache Flink  (PyFlink)
    ├── Job 1: Order Lifecycle Aggregation   (keyed state per order)
    ├── Job 2: Backlog Computation           (sliding + tumbling windows)
    └── Job 3: Fulfillment Metrics           (cycle time, on-time rate)
    │
    ├─────────────────────┬──────────────────────┐
    ▼                     ▼                      ▼
ScyllaDB              OpenSearch              Amazon S3
(order records)       (search + agg)          (checkpoints)
    │                     │
    └──────────┬───────────┘
               ▼
         FastAPI  (REST + Server-Sent Events)
               ▼
         React Dashboard  (role-based, real-time push)
```

---

## Tech Stack

| Layer | Technology | Reason over Paper's Original Stack |
|-------|------------|-------------------------------------|
| CDC | Debezium via MSK Connect | Same; MSK Connect removes connector VM ops |
| Message Broker | Amazon MSK (Apache Kafka) | Managed Kafka — same API, zero ops |
| Stream Processing | Apache Flink (PyFlink) | True streaming vs Spark micro-batch; <10ms latency |
| Storage | ScyllaDB | C++ engine, no JVM GC; 10× throughput vs Cassandra |
| Search / Index | Amazon OpenSearch | Managed Solr alternative; richer aggregation API |
| API | FastAPI | Async Python; native OpenAPI 3.0 generation |
| Dashboard | React + TypeScript + Recharts | SSE real-time push; role-based views |
| Infrastructure | Terraform + AWS | Reproducible infrastructure-as-code |

The paper's original stack (Spark + Cassandra + Solr) is evaluated against this modern stack in [`STACK_DECISION.md`](STACK_DECISION.md).

---

## Repository Structure

```
├── REQUIREMENTS.md          # Functional + non-functional requirements
├── STACK_DECISION.md        # Paper stack vs modern stack comparison
├── TECHNICAL_DESIGN.md      # Architecture, data models, API contracts, deployment
├── docker-compose.yml       # Full local dev environment
│
├── cdc/                     # Debezium connector config + registration script
├── producer/                # Order lifecycle event simulator (asyncpg)
├── flink_jobs/              # PyFlink streaming jobs
│   ├── order_lifecycle_job.py   # Keyed-state order aggregation
│   ├── backlog_job.py           # Windowed backlog computation
│   └── utils/schemas.py         # Pydantic data models
│
├── storage/
│   ├── init_postgres.sql    # Source schema with logical replication
│   └── schema.cql           # ScyllaDB keyspace + tables
│
├── search/
│   ├── mappings.json        # OpenSearch index mappings
│   └── sync_consumer.py     # Kafka → OpenSearch sync service
│
├── api/                     # FastAPI application
│   ├── routers/             # orders, backlog, fulfillment, SSE stream
│   └── clients/             # ScyllaDB + Elasticsearch async clients
│
├── dashboard/               # React + TypeScript frontend
│   └── src/
│       ├── components/      # BacklogPanel, OrderTable
│       └── hooks/useSSE.ts  # Server-Sent Events hook
│
└── infra/terraform/         # AWS infrastructure (MSK, Flink, OpenSearch, ScyllaDB)
```

---

## Running Locally

**Prerequisites:** Docker, Docker Compose, Python 3.12+

```bash
# 1. Start all infrastructure services
docker-compose up -d

# 2. Register the Debezium CDC connector
bash cdc/register_connector.sh

# 3. Apply ScyllaDB schema
docker exec -i $(docker ps -qf name=scylladb) cqlsh < storage/schema.cql

# 4. Create OpenSearch indices
curl -X PUT http://localhost:9200/orders     -H 'Content-Type: application/json' -d @search/mappings.json
curl -X PUT http://localhost:9200/backlog_metrics -H 'Content-Type: application/json' -d @search/mappings.json

# 5. Start the order event simulator
pip install -r producer/requirements.txt
python producer/order_simulator.py --rate 500

# 6. Start the index sync consumer
pip install -r search/requirements.txt
python search/sync_consumer.py

# 7. Start the API
pip install -r api/requirements.txt
uvicorn api.main:app --reload --port 8000

# 8. Start the dashboard
cd dashboard && npm install && npm start
```

**API docs:** http://localhost:8000/docs  
**Kafka UI:** http://localhost:8080  
**Dashboard:** http://localhost:3000

---

## Performance Targets

- End-to-end latency (source commit → dashboard): **P50 < 1s, P99 < 8s**
- Streaming ingest throughput: **50,000 events/sec** (3-broker MSK)
- Dashboard query response: **P95 < 3s** with 500 concurrent users
- Fault recovery: **< 60 seconds** from any single-component failure, zero data loss

---

## Citation

If you use this work, please cite the original paper:

```
Ranveer Potel, "A Real-Time Analytics Architecture for Enterprise Order Lifecycle
Visibility and Backlog Management," International Journal of Research and Applied
Innovations (IJRAI), vol. 2, no. 6, pp. 2460–2469, Nov.–Dec. 2019.
DOI: 10.15662/IJRAI.2019.0206004
```

---

## License

This implementation is released for research and educational use. See the paper at https://doi.org/10.15662/IJRAI.2019.0206004 for the full architectural specification.
