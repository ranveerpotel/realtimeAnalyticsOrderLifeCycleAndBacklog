-- Source schema simulating Oracle EBS order management tables

CREATE TABLE orders (
    order_id          VARCHAR(50)    PRIMARY KEY,
    customer_id       VARCHAR(50)    NOT NULL,
    customer_name     VARCHAR(200)   NOT NULL,
    channel           VARCHAR(50)    NOT NULL,
    business_unit     VARCHAR(100)   NOT NULL,
    order_status      VARCHAR(50)    NOT NULL DEFAULT 'CREATED',
    priority_tier     INTEGER        NOT NULL DEFAULT 3,
    total_value       NUMERIC(15,2)  NOT NULL DEFAULT 0,
    currency          VARCHAR(3)     NOT NULL DEFAULT 'USD',
    created_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    expected_ship_date DATE,
    notes             TEXT
);

CREATE TABLE order_line_items (
    line_id       VARCHAR(50)   PRIMARY KEY,
    order_id      VARCHAR(50)   NOT NULL REFERENCES orders(order_id),
    sku           VARCHAR(100)  NOT NULL,
    product_family VARCHAR(100) NOT NULL,
    description   VARCHAR(500),
    quantity      NUMERIC(15,3) NOT NULL,
    unit_price    NUMERIC(15,2) NOT NULL,
    line_status   VARCHAR(50)   NOT NULL DEFAULT 'OPEN',
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory (
    sku           VARCHAR(100)  NOT NULL,
    warehouse_id  VARCHAR(50)   NOT NULL,
    available_qty NUMERIC(15,3) NOT NULL DEFAULT 0,
    allocated_qty NUMERIC(15,3) NOT NULL DEFAULT 0,
    on_order_qty  NUMERIC(15,3) NOT NULL DEFAULT 0,
    last_receipt  TIMESTAMPTZ,
    next_receipt  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (sku, warehouse_id)
);

CREATE TABLE shipments (
    shipment_id   VARCHAR(50)  PRIMARY KEY,
    order_id      VARCHAR(50)  NOT NULL REFERENCES orders(order_id),
    carrier       VARCHAR(100),
    tracking_number VARCHAR(200),
    status        VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    ship_date     TIMESTAMPTZ,
    delivery_date TIMESTAMPTZ,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE allocation_records (
    allocation_id VARCHAR(50)   PRIMARY KEY,
    order_id      VARCHAR(50)   NOT NULL REFERENCES orders(order_id),
    sku           VARCHAR(100)  NOT NULL,
    warehouse_id  VARCHAR(50)   NOT NULL,
    allocated_qty NUMERIC(15,3) NOT NULL,
    status        VARCHAR(50)   NOT NULL DEFAULT 'ALLOCATED',
    allocated_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Enable logical replication for all tables
ALTER TABLE orders           REPLICA IDENTITY FULL;
ALTER TABLE order_line_items REPLICA IDENTITY FULL;
ALTER TABLE inventory        REPLICA IDENTITY FULL;
ALTER TABLE shipments        REPLICA IDENTITY FULL;
ALTER TABLE allocation_records REPLICA IDENTITY FULL;

-- Indexes for common query patterns
CREATE INDEX idx_orders_bu_status      ON orders(business_unit, order_status);
CREATE INDEX idx_orders_customer       ON orders(customer_id);
CREATE INDEX idx_orders_updated_at     ON orders(updated_at DESC);
CREATE INDEX idx_line_items_order_id   ON order_line_items(order_id);
CREATE INDEX idx_shipments_order_id    ON shipments(order_id);
CREATE INDEX idx_allocations_order_id  ON allocation_records(order_id);

-- Publication for CDC (Debezium will use this)
CREATE PUBLICATION orders_pub FOR TABLE
    orders,
    order_line_items,
    inventory,
    shipments,
    allocation_records;
