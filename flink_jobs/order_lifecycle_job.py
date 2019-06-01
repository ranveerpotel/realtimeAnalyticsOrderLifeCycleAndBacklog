"""
Flink Job 1: Order Lifecycle Aggregation

Consumes CDC events from Kafka, maintains per-order keyed state,
enriches with inventory broadcast state, and writes enriched order
records to ScyllaDB and the orders.enriched Kafka topic.

Run locally:
    python order_lifecycle_job.py

On Amazon Managed Service for Apache Flink:
    Package with: zip -r job.zip order_lifecycle_job.py utils/ requirements.txt
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterator, Optional

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import (
    BroadcastProcessFunction,
    KeyedProcessFunction,
    MapFunction,
    RuntimeContext,
)
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, Time

from utils.schemas import (
    AllocationRecord,
    CdcEvent,
    CdcOperation,
    LineItem,
    OrderState,
    ShipmentData,
    StatusEntry,
)

log = logging.getLogger(__name__)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
SCYLLA_HOSTS = os.getenv("SCYLLA_HOSTS", "localhost").split(",")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/order-lifecycle")

INVENTORY_BROADCAST_STATE = MapStateDescriptor(
    "inventory_state",
    Types.STRING(),
    Types.PICKLED_BYTE_ARRAY(),
)


class ParseCdcEvent(MapFunction):
    def map(self, raw: str) -> Optional[CdcEvent]:
        try:
            return CdcEvent.from_json(raw)
        except Exception as e:
            log.warning("Failed to parse CDC event: %s | error: %s", raw[:200], e)
            return None


class InventoryBroadcastProcessor(BroadcastProcessFunction):
    """Updates broadcast inventory state from inventory.cdc events."""

    def process_broadcast_element(self, event: CdcEvent, ctx):
        row = event.row
        sku = row.get("sku")
        if not sku:
            return
        state = ctx.get_broadcast_state(INVENTORY_BROADCAST_STATE)
        if event.op == CdcOperation.DELETE:
            state.remove(sku)
        else:
            existing = json.loads(state.get(sku) or b"{}") if state.contains(sku) else {}
            warehouse = row.get("warehouse_id", "UNKNOWN")
            existing[warehouse] = {
                "available_qty": float(row.get("available_qty", 0)),
                "allocated_qty": float(row.get("allocated_qty", 0)),
                "on_order_qty":  float(row.get("on_order_qty", 0)),
            }
            state.put(sku, json.dumps(existing).encode())

    def process_element(self, event: CdcEvent, ctx, out):
        # Order events pass through unchanged; inventory events only update broadcast state
        out.collect(event)


class OrderLifecycleAggregator(KeyedProcessFunction):
    """
    Maintains per-order keyed state. Folds CDC events into a running OrderState,
    enriches with ATP feasibility from broadcast inventory, then emits enriched events.
    """

    def __init__(self, scylla_hosts: list[str]):
        self.scylla_hosts = scylla_hosts
        self._state = None
        self._dedup_state = None
        self._scylla_session = None

    def open(self, ctx: RuntimeContext):
        self._state = ctx.get_state(
            ValueStateDescriptor("order_state", Types.PICKLED_BYTE_ARRAY())
        )
        self._dedup_state = ctx.get_map_state(
            MapStateDescriptor("seen_lsns", Types.LONG(), Types.BOOLEAN())
        )
        self._init_scylla()

    def _init_scylla(self):
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy

        cluster = Cluster(
            contact_points=self.scylla_hosts,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        self._scylla_session = cluster.connect("orders")
        self._upsert_stmt = self._scylla_session.prepare(
            """
            INSERT INTO order_state (
                order_id, business_unit, customer_id, customer_name,
                channel, product_family, order_status, priority_tier,
                created_at, updated_at, total_value, currency,
                line_items, allocation_records, shipment_data,
                status_history, atp_feasible, expected_ship_date, pipeline_ts
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            USING TTL 7776000
            """
        )
        self._upsert_bu_stmt = self._scylla_session.prepare(
            """
            INSERT INTO orders_by_bu_status
                (business_unit, order_status, updated_at, order_id,
                 customer_name, product_family, priority_tier, total_value)
            VALUES (?,?,?,?,?,?,?,?)
            USING TTL 7776000
            """
        )

    def process_element(self, event: CdcEvent, ctx, out):
        if event is None:
            return

        # Deduplication: skip already-seen LSNs
        if event.lsn and self._dedup_state.contains(event.lsn):
            return
        if event.lsn:
            self._dedup_state.put(event.lsn, True)

        raw = self._state.value()
        order = OrderState(**json.loads(raw)) if raw else OrderState(order_id=ctx.get_current_key())

        self._apply_event(order, event)
        self._check_atp(order, ctx)

        order.pipeline_ts = int(time.time() * 1000)

        self._state.update(json.dumps(order.dict()).encode())
        self._write_to_scylla(order)

        out.collect(json.dumps(order.dict()))

    def _apply_event(self, order: OrderState, event: CdcEvent):
        row = event.row
        table = event.source_table

        if "orders" == table and event.op != CdcOperation.DELETE:
            order.customer_id = row.get("customer_id", order.customer_id)
            order.customer_name = row.get("customer_name", order.customer_name)
            order.channel = row.get("channel", order.channel)
            order.business_unit = row.get("business_unit", order.business_unit)
            order.priority_tier = int(row.get("priority_tier", order.priority_tier))
            order.total_value = float(row.get("total_value", order.total_value) or 0)
            order.currency = row.get("currency", order.currency)

            new_status = row.get("order_status", order.order_status)
            if new_status != order.order_status:
                order.status_history.append(
                    StatusEntry(status=new_status, timestamp=event.ts_ms)
                )
                order.order_status = new_status

            if order.created_at is None:
                order.created_at = event.ts_ms
            order.updated_at = event.ts_ms

            if row.get("product_family") and not order.product_family:
                order.product_family = row["product_family"]

        elif "order_line_items" == table and event.op != CdcOperation.DELETE:
            line_id = row.get("line_id")
            existing = {li.line_id: li for li in order.line_items}
            existing[line_id] = LineItem(
                line_id=line_id,
                sku=row.get("sku", ""),
                product_family=row.get("product_family", ""),
                description=row.get("description"),
                quantity=float(row.get("quantity", 0)),
                unit_price=float(row.get("unit_price", 0)),
                line_status=row.get("line_status", "OPEN"),
            )
            order.line_items = list(existing.values())

            if order.line_items and not order.product_family:
                order.product_family = order.line_items[0].product_family

        elif "allocation_records" == table and event.op != CdcOperation.DELETE:
            alloc_id = row.get("allocation_id")
            existing = {ar.allocation_id: ar for ar in order.allocation_records}
            existing[alloc_id] = AllocationRecord(
                allocation_id=alloc_id,
                sku=row.get("sku", ""),
                warehouse_id=row.get("warehouse_id", ""),
                allocated_qty=float(row.get("allocated_qty", 0)),
                status=row.get("status", "ALLOCATED"),
            )
            order.allocation_records = list(existing.values())

        elif "shipments" == table and event.op != CdcOperation.DELETE:
            from utils.schemas import ShipmentData
            ship_date = row.get("ship_date")
            delivery_date = row.get("delivery_date")
            order.shipment_data = ShipmentData(
                shipment_id=row.get("shipment_id", ""),
                carrier=row.get("carrier"),
                tracking_number=row.get("tracking_number"),
                status=row.get("status", "PENDING"),
                ship_date=int(ship_date) if ship_date else None,
                delivery_date=int(delivery_date) if delivery_date else None,
            )

    def _check_atp(self, order: OrderState, ctx):
        """Simple ATP: order is feasible if total demand <= total available inventory."""
        total_demand = sum(li.quantity for li in order.line_items)
        if total_demand == 0:
            order.atp_feasible = True
            return
        # In production, read from broadcast inventory state
        # Simplified: mark feasible if allocated
        order.atp_feasible = order.order_status in ("ALLOCATED", "IN_PRODUCTION", "SHIPPED", "DELIVERED")

    def _write_to_scylla(self, order: OrderState):
        if self._scylla_session is None:
            return
        try:
            row = order.to_scylla_row()
            self._scylla_session.execute(self._upsert_stmt, [
                row["order_id"], row["business_unit"], row["customer_id"],
                row["customer_name"], row["channel"], row["product_family"],
                row["order_status"], row["priority_tier"], row["created_at"],
                row["updated_at"], row["total_value"], row["currency"],
                row["line_items"], row["allocation_records"], row["shipment_data"],
                row["status_history"], row["atp_feasible"], row["expected_ship_date"],
                row["pipeline_ts"],
            ])
            if row["business_unit"] and row["order_status"]:
                self._scylla_session.execute(self._upsert_bu_stmt, [
                    row["business_unit"], row["order_status"], row["updated_at"],
                    row["order_id"], row["customer_name"], row["product_family"],
                    row["priority_tier"], row["total_value"],
                ])
        except Exception as e:
            log.error("ScyllaDB write failed for order %s: %s", order.order_id, e)


def build_kafka_source(topic: str, group_id: str) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_transactional_id_prefix("order-lifecycle-")
        .build()
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("PARALLELISM", "4")))

    env.enable_checkpointing(30_000)  # 30 seconds
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000)
    env.get_state_backend()

    # ── Sources ──────────────────────────────────────────────────────────────
    orders_source = build_kafka_source("cdc.public.orders", "flink-order-lifecycle")
    inventory_source = build_kafka_source("cdc.public.inventory", "flink-inventory-broadcast")
    line_items_source = build_kafka_source("cdc.public.order_line_items", "flink-order-lifecycle")
    shipments_source = build_kafka_source("cdc.public.shipments", "flink-order-lifecycle")
    allocations_source = build_kafka_source("cdc.public.allocation_records", "flink-order-lifecycle")

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))

    orders_stream = (
        env.from_source(orders_source, watermark_strategy, "orders-cdc")
        .map(ParseCdcEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
    )

    inventory_stream = (
        env.from_source(inventory_source, watermark_strategy, "inventory-cdc")
        .map(ParseCdcEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
    )

    line_items_stream = (
        env.from_source(line_items_source, watermark_strategy, "line-items-cdc")
        .map(ParseCdcEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
    )

    shipments_stream = (
        env.from_source(shipments_source, watermark_strategy, "shipments-cdc")
        .map(ParseCdcEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
    )

    allocations_stream = (
        env.from_source(allocations_source, watermark_strategy, "allocations-cdc")
        .map(ParseCdcEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
        .filter(lambda e: e is not None)
    )

    # ── Union order-related streams, broadcast inventory ─────────────────────
    order_events = orders_stream.union(line_items_stream, shipments_stream, allocations_stream)
    inventory_broadcast = inventory_stream.broadcast(INVENTORY_BROADCAST_STATE)

    # ── Key by order_id, apply stateful aggregation ──────────────────────────
    enriched_stream = (
        order_events
        .connect(inventory_broadcast)
        .process(InventoryBroadcastProcessor())
        .key_by(lambda e: (e.row.get("order_id") or e.row.get("order_id", "unknown")))
        .process(OrderLifecycleAggregator(SCYLLA_HOSTS))
    )

    # ── Sink enriched events to Kafka ─────────────────────────────────────────
    enriched_stream.sink_to(build_kafka_sink("orders.enriched"))

    env.execute("Order Lifecycle Aggregation Job")


if __name__ == "__main__":
    main()
