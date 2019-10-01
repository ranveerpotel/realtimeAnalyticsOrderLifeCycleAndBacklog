"""ScyllaDB async client wrapper."""

import json
import logging
from typing import Any, Optional

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import ConsistencyLevel, SimpleStatement

log = logging.getLogger(__name__)


class ScyllaClient:
    def __init__(self, hosts: list[str], keyspace: str):
        cluster = Cluster(
            contact_points=hosts,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        self._session = cluster.connect(keyspace)
        self._prepare_statements()

    def _prepare_statements(self):
        self._get_order = self._session.prepare(
            "SELECT * FROM order_state WHERE order_id=?"
        )
        self._get_orders_by_bu = self._session.prepare(
            """
            SELECT order_id, order_status, updated_at, customer_name,
                   product_family, priority_tier, total_value
              FROM orders_by_bu_status
             WHERE business_unit=? AND order_status=?
             LIMIT ?
            """
        )
        self._get_backlog = self._session.prepare(
            """
            SELECT * FROM backlog_metrics
             WHERE business_unit=? AND product_family=? AND horizon_bucket=?
               AND window_type=?
             LIMIT 100
            """
        )
        self._get_fulfillment = self._session.prepare(
            """
            SELECT * FROM fulfillment_metrics
             WHERE business_unit=?
             LIMIT 90
            """
        )
        self._get_supply = self._session.prepare(
            "SELECT * FROM supply_availability WHERE sku=?"
        )

    def get_order(self, order_id: str) -> Optional[dict[str, Any]]:
        stmt = SimpleStatement(
            self._get_order.query_string,
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        )
        rows = self._session.execute(self._get_order, [order_id])
        row = rows.one()
        if not row:
            return None
        return self._row_to_dict(row)

    def get_orders_by_bu_status(
        self, business_unit: str, status: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        rows = self._session.execute(self._get_orders_by_bu, [business_unit, status, limit])
        return [self._row_to_dict(r) for r in rows]

    def get_backlog_metrics(
        self, business_unit: str, product_family: str, horizon: str, window_type: str
    ) -> list[dict[str, Any]]:
        rows = self._session.execute(
            self._get_backlog, [business_unit, product_family, horizon, window_type]
        )
        return [self._row_to_dict(r) for r in rows]

    def get_fulfillment_metrics(self, business_unit: str) -> list[dict[str, Any]]:
        rows = self._session.execute(self._get_fulfillment, [business_unit])
        return [self._row_to_dict(r) for r in rows]

    def get_supply_availability(self, sku: str) -> list[dict[str, Any]]:
        rows = self._session.execute(self._get_supply, [sku])
        return [self._row_to_dict(r) for r in rows]

    def shutdown(self):
        self._session.cluster.shutdown()

    @staticmethod
    def _row_to_dict(row) -> dict[str, Any]:
        d = dict(row._asdict())
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        for field in ("line_items", "allocation_records", "shipment_data"):
            if field in d and isinstance(d[field], str):
                try:
                    d[field] = json.loads(d[field])
                except (json.JSONDecodeError, TypeError):
                    pass
        if "status_history" in d and isinstance(d["status_history"], list):
            d["status_history"] = [
                json.loads(e) if isinstance(e, str) else e
                for e in d["status_history"]
            ]
        return d
