-- ScyllaDB keyspace and table definitions
-- Run with: cqlsh -f schema.cql

-- ─── Keyspace ────────────────────────────────────────────────────────────────
-- For local dev use SimpleStrategy; in AWS use NetworkTopologyStrategy
CREATE KEYSPACE IF NOT EXISTS orders
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    AND durable_writes = true;

USE orders;

-- ─── Primary Order State ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_state (
    order_id           TEXT,
    business_unit      TEXT,
    customer_id        TEXT,
    customer_name      TEXT,
    channel            TEXT,
    product_family     TEXT,
    order_status       TEXT,
    priority_tier      INT,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    total_value        DECIMAL,
    currency           TEXT,
    line_items         TEXT,
    allocation_records TEXT,
    shipment_data      TEXT,
    status_history     LIST<TEXT>,
    atp_feasible       BOOLEAN,
    expected_ship_date TIMESTAMP,
    pipeline_ts        TIMESTAMP,
    PRIMARY KEY (order_id)
) WITH default_time_to_live = 7776000
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 86400
  AND comment = 'Current denormalized state of each order';

-- ─── Orders by BU + Status (dashboard listing view) ─────────────────────────
CREATE TABLE IF NOT EXISTS orders_by_bu_status (
    business_unit  TEXT,
    order_status   TEXT,
    updated_at     TIMESTAMP,
    order_id       TEXT,
    customer_name  TEXT,
    product_family TEXT,
    priority_tier  INT,
    total_value    DECIMAL,
    PRIMARY KEY ((business_unit, order_status), updated_at, order_id)
) WITH CLUSTERING ORDER BY (updated_at DESC)
  AND default_time_to_live = 7776000
  AND comment = 'Efficient lookup of orders by BU+status, sorted by recency';

-- ─── Backlog Metrics ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS backlog_metrics (
    business_unit      TEXT,
    product_family     TEXT,
    horizon_bucket     TEXT,
    window_type        TEXT,
    window_start       TIMESTAMP,
    unfulfilled_demand DECIMAL,
    available_supply   DECIMAL,
    demand_gap         DECIMAL,
    order_count        INT,
    avg_priority       DOUBLE,
    computed_at        TIMESTAMP,
    PRIMARY KEY ((business_unit, product_family, horizon_bucket), window_type, window_start)
) WITH CLUSTERING ORDER BY (window_type ASC, window_start DESC)
  AND default_time_to_live = 2592000
  AND comment = 'Windowed backlog aggregations per BU/product/horizon';

-- ─── Fulfillment Metrics ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fulfillment_metrics (
    business_unit   TEXT,
    metric_date     DATE,
    on_time_rate    DOUBLE,
    cycle_time_p50  DOUBLE,
    cycle_time_p95  DOUBLE,
    exception_count INT,
    total_orders    INT,
    shipped_orders  INT,
    computed_at     TIMESTAMP,
    PRIMARY KEY (business_unit, metric_date)
) WITH CLUSTERING ORDER BY (metric_date DESC)
  AND default_time_to_live = 31536000
  AND comment = 'Daily fulfillment KPIs per business unit';

-- ─── Supply Availability ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS supply_availability (
    sku           TEXT,
    warehouse_id  TEXT,
    available_qty DECIMAL,
    allocated_qty DECIMAL,
    on_order_qty  DECIMAL,
    last_receipt  TIMESTAMP,
    next_receipt  TIMESTAMP,
    updated_at    TIMESTAMP,
    PRIMARY KEY (sku, warehouse_id)
) WITH comment = 'Current inventory position per SKU and warehouse';

-- ─── Pipeline Event Log (for audit/replay) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_event_log (
    order_id      TEXT,
    event_ts      TIMESTAMP,
    event_type    TEXT,
    source_lsn    BIGINT,
    payload       TEXT,
    PRIMARY KEY (order_id, event_ts)
) WITH CLUSTERING ORDER BY (event_ts DESC)
  AND default_time_to_live = 604800
  AND comment = 'Last 7 days of raw pipeline events per order for debugging';
