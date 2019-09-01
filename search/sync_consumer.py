"""
Index Sync Consumer

Consumes enriched order events from Kafka and upserts them into
Elasticsearch/OpenSearch, providing the search and aggregation layer.
Runs as a standalone service (ECS task).
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from elasticsearch import Elasticsearch, helpers

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
ES_URL        = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
BATCH_SIZE    = int(os.getenv("BATCH_SIZE", "200"))
FLUSH_INTERVAL = float(os.getenv("FLUSH_INTERVAL_SEC", "2.0"))
TOPICS = [
    "orders.enriched",
    "backlog.metrics",
]


def ms_to_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def transform_order(raw: str) -> dict[str, Any] | None:
    try:
        order = json.loads(raw)
        doc = {
            "order_id":           order.get("order_id"),
            "customer_id":        order.get("customer_id"),
            "customer_name":      order.get("customer_name"),
            "channel":            order.get("channel"),
            "business_unit":      order.get("business_unit"),
            "product_family":     order.get("product_family"),
            "order_status":       order.get("order_status"),
            "priority_tier":      order.get("priority_tier"),
            "total_value":        order.get("total_value"),
            "currency":           order.get("currency"),
            "atp_feasible":       order.get("atp_feasible"),
            "created_at":         ms_to_iso(order.get("created_at")),
            "updated_at":         ms_to_iso(order.get("updated_at")),
            "expected_ship_date": ms_to_iso(order.get("expected_ship_date")),
            "pipeline_ts":        ms_to_iso(order.get("pipeline_ts")),
        }

        # Flatten line_items (nested)
        raw_items = order.get("line_items", [])
        if isinstance(raw_items, str):
            raw_items = json.loads(raw_items)
        doc["line_items"] = raw_items

        # Flatten shipment
        ship = order.get("shipment_data")
        if isinstance(ship, str):
            ship = json.loads(ship) if ship else None
        if ship:
            doc["shipment"] = {
                "carrier":       ship.get("carrier"),
                "status":        ship.get("status"),
                "ship_date":     ms_to_iso(ship.get("ship_date")),
                "delivery_date": ms_to_iso(ship.get("delivery_date")),
            }

        return doc
    except Exception as e:
        log.warning("Failed to transform order event: %s", e)
        return None


def transform_backlog_metric(raw: str) -> dict[str, Any] | None:
    try:
        metric = json.loads(raw)
        return {
            "business_unit":      metric.get("bu"),
            "product_family":     metric.get("product_family"),
            "horizon_bucket":     metric.get("horizon"),
            "window_type":        metric.get("window_type"),
            "window_start":       metric.get("window_start"),
            "unfulfilled_demand": metric.get("unfulfilled_demand"),
            "available_supply":   metric.get("available_supply"),
            "demand_gap":         metric.get("demand_gap"),
            "order_count":        metric.get("order_count"),
            "avg_priority":       metric.get("avg_priority"),
            "computed_at":        datetime.now(tz=timezone.utc).isoformat(),
        }
    except Exception as e:
        log.warning("Failed to transform backlog metric: %s", e)
        return None


class IndexSyncConsumer:
    def __init__(self):
        self.running = True
        self.es = Elasticsearch(ES_URL)
        self._ensure_indices()

        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id":          "index-sync-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe(TOPICS)

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _ensure_indices(self):
        with open(os.path.join(os.path.dirname(__file__), "mappings.json")) as f:
            mappings = json.load(f)

        for index_name, config in mappings.items():
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name, body=config)
                log.info("Created index: %s", index_name)

    def _shutdown(self, *_):
        log.info("Shutting down sync consumer...")
        self.running = False

    def _build_action(self, topic: str, value: str) -> dict | None:
        if topic == "orders.enriched":
            doc = transform_order(value)
            if doc and doc.get("order_id"):
                return {
                    "_index": "orders",
                    "_id":    doc["order_id"],
                    "_source": doc,
                }
        elif topic == "backlog.metrics":
            doc = transform_backlog_metric(value)
            if doc:
                doc_id = f"{doc['business_unit']}|{doc['product_family']}|{doc['horizon_bucket']}|{doc['window_type']}|{doc['window_start']}"
                return {
                    "_index": "backlog_metrics",
                    "_id":    doc_id,
                    "_source": doc,
                }
        return None

    def run(self):
        buffer: list[dict] = []
        last_flush = time.monotonic()

        while self.running:
            msg = self.consumer.poll(timeout=0.1)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
            else:
                action = self._build_action(msg.topic(), msg.value().decode("utf-8"))
                if action:
                    buffer.append(action)

            should_flush = (
                len(buffer) >= BATCH_SIZE
                or (buffer and time.monotonic() - last_flush >= FLUSH_INTERVAL)
            )
            if should_flush:
                self._flush(buffer)
                self.consumer.commit(asynchronous=False)
                buffer.clear()
                last_flush = time.monotonic()

        if buffer:
            self._flush(buffer)
            self.consumer.commit(asynchronous=False)

        self.consumer.close()

    def _flush(self, actions: list[dict]):
        if not actions:
            return
        try:
            success, errors = helpers.bulk(self.es, actions, raise_on_error=False)
            if errors:
                log.error("Bulk index errors: %s", errors[:5])
            else:
                log.info("Indexed %d documents", success)
        except Exception as e:
            log.error("Bulk index failed: %s", e)


if __name__ == "__main__":
    IndexSyncConsumer().run()
