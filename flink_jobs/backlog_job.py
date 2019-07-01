"""
Flink Job 2: Backlog Computation

Consumes enriched order events from orders.enriched, computes windowed
backlog metrics (unfulfilled demand, supply gap) by (business_unit,
product_family, horizon_bucket), and writes results to ScyllaDB.

Sliding window: 1 hour, slide every 5 minutes
Tumbling daily window: for persistent metrics
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Iterator

from pyflink.common import Row, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingEventTimeWindows, TumblingEventTimeWindows

log = logging.getLogger(__name__)

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
SCYLLA_HOSTS = os.getenv("SCYLLA_HOSTS", "localhost").split(",")

HORIZON_BUCKETS = {
    "D1":  1,
    "D7":  7,
    "D30": 30,
    "D90": 90,
}


def classify_horizon(expected_ship_date_ms: int | None) -> str:
    if expected_ship_date_ms is None:
        return "D90"
    days_out = (expected_ship_date_ms - int(time.time() * 1000)) / (1000 * 86400)
    if days_out <= 1:
        return "D1"
    if days_out <= 7:
        return "D7"
    if days_out <= 30:
        return "D30"
    return "D90"


class ExtractBacklogKey(MapFunction):
    """Extracts (bu, product_family, horizon) composite key from enriched order."""

    def map(self, raw: str):
        try:
            order = json.loads(raw)
            bu = order.get("business_unit", "UNKNOWN")
            pf = order.get("product_family", "UNKNOWN")
            horizon = classify_horizon(order.get("expected_ship_date"))
            status = order.get("order_status", "")
            total_value = float(order.get("total_value", 0))
            priority = int(order.get("priority_tier", 3))
            atp = bool(order.get("atp_feasible", False))
            ts = int(order.get("updated_at") or time.time() * 1000)
            return Row(
                bu=bu, product_family=pf, horizon=horizon,
                status=status, total_value=total_value,
                priority=priority, atp_feasible=atp, ts=ts,
            )
        except Exception as e:
            log.warning("Failed to parse enriched order: %s", e)
            return None


class BacklogAccumulator:
    def __init__(self):
        self.unfulfilled_demand: float = 0.0
        self.available_supply: float = 0.0
        self.order_count: int = 0
        self.priority_sum: float = 0.0


class BacklogAggregateFunction(AggregateFunction):
    UNFULFILLED_STATUSES = {"CREATED", "ALLOCATED", "IN_PRODUCTION"}

    def create_accumulator(self) -> BacklogAccumulator:
        return BacklogAccumulator()

    def add(self, row: Row, acc: BacklogAccumulator) -> BacklogAccumulator:
        if row.status in self.UNFULFILLED_STATUSES:
            acc.unfulfilled_demand += row.total_value
            acc.order_count += 1
            acc.priority_sum += row.priority
        if row.atp_feasible:
            acc.available_supply += row.total_value
        return acc

    def get_result(self, acc: BacklogAccumulator) -> dict:
        return {
            "unfulfilled_demand": acc.unfulfilled_demand,
            "available_supply":   acc.available_supply,
            "demand_gap":         acc.unfulfilled_demand - acc.available_supply,
            "order_count":        acc.order_count,
            "avg_priority":       acc.priority_sum / acc.order_count if acc.order_count > 0 else 0,
        }

    def merge(self, acc1: BacklogAccumulator, acc2: BacklogAccumulator) -> BacklogAccumulator:
        acc1.unfulfilled_demand += acc2.unfulfilled_demand
        acc1.available_supply   += acc2.available_supply
        acc1.order_count        += acc2.order_count
        acc1.priority_sum       += acc2.priority_sum
        return acc1


class BacklogWindowSink(ProcessWindowFunction):
    """Writes windowed backlog results to ScyllaDB."""

    def __init__(self, scylla_hosts: list[str], window_type: str):
        self.scylla_hosts = scylla_hosts
        self.window_type = window_type
        self._session = None
        self._stmt = None

    def open(self, ctx: RuntimeContext):
        from cassandra.cluster import Cluster
        from cassandra.policies import DCAwareRoundRobinPolicy

        cluster = Cluster(
            contact_points=self.scylla_hosts,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        self._session = cluster.connect("orders")
        self._stmt = self._session.prepare(
            """
            INSERT INTO backlog_metrics (
                business_unit, product_family, horizon_bucket,
                window_type, window_start,
                unfulfilled_demand, available_supply, demand_gap,
                order_count, avg_priority, computed_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            USING TTL 2592000
            """
        )

    def process(self, key: tuple, ctx, aggregated_results: Iterator[dict], out):
        bu, product_family, horizon = key
        for result in aggregated_results:
            window_start = datetime.fromtimestamp(
                ctx.window().start / 1000, tz=timezone.utc
            )
            computed_at = datetime.now(tz=timezone.utc)
            try:
                self._session.execute(self._stmt, [
                    bu, product_family, horizon,
                    self.window_type, window_start,
                    result["unfulfilled_demand"],
                    result["available_supply"],
                    result["demand_gap"],
                    result["order_count"],
                    result["avg_priority"],
                    computed_at,
                ])
                out.collect(json.dumps({
                    "bu": bu, "product_family": product_family, "horizon": horizon,
                    "window_type": self.window_type,
                    "window_start": window_start.isoformat(),
                    **result,
                }))
            except Exception as e:
                log.error("Failed to write backlog metric: %s", e)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("PARALLELISM", "4")))

    env.enable_checkpointing(30_000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKERS)
        .set_topics("orders.enriched")
        .set_group_id("flink-backlog-computation")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    base_stream = (
        env.from_source(source, watermark_strategy, "orders-enriched")
        .map(ExtractBacklogKey())
        .filter(lambda r: r is not None)
        .key_by(lambda r: (r.bu, r.product_family, r.horizon))
    )

    # ── Sliding window: 1h size, 5min slide ──────────────────────────────────
    base_stream.window(
        SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))
    ).aggregate(
        BacklogAggregateFunction(),
        BacklogWindowSink(SCYLLA_HOSTS, "sliding_1h"),
    )

    # ── Tumbling daily window ─────────────────────────────────────────────────
    base_stream.window(
        TumblingEventTimeWindows.of(Time.hours(24))
    ).aggregate(
        BacklogAggregateFunction(),
        BacklogWindowSink(SCYLLA_HOSTS, "tumbling_daily"),
    )

    env.execute("Backlog Computation Job")


if __name__ == "__main__":
    main()
