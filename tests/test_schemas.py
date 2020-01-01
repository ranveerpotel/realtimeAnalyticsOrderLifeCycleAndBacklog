"""
Unit tests for Flink job Pydantic schemas.
No Kafka, Flink, or database connections required.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from flink_jobs.utils.schemas import (
    CdcEvent, CdcOperation, LineItem, AllocationRecord,
    ShipmentData, StatusEntry, OrderState,
)


# ─── CdcOperation enum ────────────────────────────────────────────────────

class TestCdcOperation:
    def test_values(self):
        assert CdcOperation.INSERT == "c"
        assert CdcOperation.UPDATE == "u"
        assert CdcOperation.DELETE == "d"
        assert CdcOperation.SNAPSHOT == "r"


# ─── CdcEvent ─────────────────────────────────────────────────────────────

class TestCdcEvent:
    def _make(self, op="c", before=None, after=None):
        return CdcEvent(
            op=op,
            **{"__table": "orders"},
            ts_ms=1_700_000_000_000,
            before=before,
            after=after,
        )

    def test_insert_row_returns_after(self):
        ev = self._make(op="c", after={"order_id": "ORD-001", "status": "CREATED"})
        assert ev.row == {"order_id": "ORD-001", "status": "CREATED"}

    def test_update_row_returns_after(self):
        ev = self._make(
            op="u",
            before={"status": "CREATED"},
            after={"status": "SHIPPED"},
        )
        assert ev.row["status"] == "SHIPPED"

    def test_delete_row_returns_before(self):
        ev = self._make(op="d", before={"order_id": "ORD-999"})
        assert ev.row == {"order_id": "ORD-999"}

    def test_snapshot_row_returns_after(self):
        ev = self._make(op="r", after={"order_id": "ORD-002"})
        assert ev.row["order_id"] == "ORD-002"

    def test_from_json_roundtrip(self):
        payload = json.dumps({
            "op": "c",
            "__table": "line_items",
            "ts_ms": 1_700_000_000_000,
            "after": {"sku": "NET-001", "qty": 5},
        })
        ev = CdcEvent.from_json(payload)
        assert ev.op == CdcOperation.INSERT
        assert ev.source_table == "line_items"
        assert ev.row["sku"] == "NET-001"

    def test_from_json_bytes(self):
        payload = json.dumps({
            "op": "u",
            "__table": "orders",
            "ts_ms": 1_700_000_001_000,
            "after": {"status": "SHIPPED"},
        }).encode()
        ev = CdcEvent.from_json(payload)
        assert ev.op == CdcOperation.UPDATE

    def test_optional_lsn_defaults_none(self):
        ev = self._make()
        assert ev.lsn is None


# ─── LineItem ──────────────────────────────────────────────────────────────

class TestLineItem:
    def test_defaults(self):
        li = LineItem(
            line_id="LI-001", sku="NET-500",
            product_family="Networking",
            quantity=10, unit_price=499.99,
        )
        assert li.line_status == "OPEN"
        assert li.description is None

    def test_custom_status(self):
        li = LineItem(
            line_id="LI-002", sku="SEC-200",
            product_family="Security",
            quantity=5, unit_price=1200.0,
            line_status="ALLOCATED",
        )
        assert li.line_status == "ALLOCATED"


# ─── AllocationRecord ─────────────────────────────────────────────────────

class TestAllocationRecord:
    def test_defaults(self):
        ar = AllocationRecord(
            allocation_id="ALLOC-001", sku="NET-500",
            warehouse_id="WH-SJC", allocated_qty=10,
        )
        assert ar.status == "ALLOCATED"

    def test_partial_allocation(self):
        ar = AllocationRecord(
            allocation_id="ALLOC-002", sku="NET-500",
            warehouse_id="WH-AMS", allocated_qty=4.5,
        )
        assert ar.allocated_qty == pytest.approx(4.5)


# ─── OrderState ───────────────────────────────────────────────────────────

class TestOrderState:
    @pytest.fixture
    def sample_order(self):
        return OrderState(
            order_id="ORD-2019-001",
            customer_id="CUST-ABC",
            customer_name="Acme Corp",
            channel="Direct",
            business_unit="North America",
            product_family="Networking",
            order_status="CREATED",
            priority_tier=1,
            created_at=1_700_000_000_000,
            updated_at=1_700_000_000_000,
            total_value=24_999.0,
            currency="USD",
        )

    def test_defaults(self, sample_order):
        assert sample_order.order_status == "CREATED"
        assert sample_order.line_items == []
        assert sample_order.allocation_records == []
        assert sample_order.atp_feasible is False

    def test_add_line_item(self, sample_order):
        sample_order.line_items.append(
            LineItem(
                line_id="LI-001", sku="NET-500",
                product_family="Networking",
                quantity=10, unit_price=499.99,
            )
        )
        assert len(sample_order.line_items) == 1
        assert sample_order.line_items[0].sku == "NET-500"

    def test_to_scylla_row_keys(self, sample_order):
        row = sample_order.to_scylla_row()
        for key in ("order_id", "customer_id", "order_status", "total_value",
                    "line_items", "allocation_records", "status_history"):
            assert key in row, f"Missing key: {key}"

    def test_to_scylla_row_serializes_line_items_as_json(self, sample_order):
        sample_order.line_items.append(
            LineItem(
                line_id="LI-001", sku="NET-500",
                product_family="Networking",
                quantity=2, unit_price=999.0,
            )
        )
        row = sample_order.to_scylla_row()
        parsed = json.loads(row["line_items"])
        assert isinstance(parsed, list)
        assert parsed[0]["sku"] == "NET-500"

    def test_to_scylla_row_timestamps_converted(self, sample_order):
        import datetime
        row = sample_order.to_scylla_row()
        assert isinstance(row["created_at"], datetime.datetime)
        assert row["created_at"].tzinfo is not None

    def test_status_history_append(self, sample_order):
        sample_order.status_history.append(
            StatusEntry(status="ALLOCATED", timestamp=1_700_000_001_000)
        )
        assert len(sample_order.status_history) == 1
        assert sample_order.status_history[0].status == "ALLOCATED"
