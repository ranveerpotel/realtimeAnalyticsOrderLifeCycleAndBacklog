"""Pydantic models for CDC events and enriched order state."""

from __future__ import annotations

import json
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class CdcOperation(str, Enum):
    INSERT = "c"    # Debezium uses 'c' for create/insert
    UPDATE = "u"
    DELETE = "d"
    SNAPSHOT = "r"  # initial snapshot read


class CdcEvent(BaseModel):
    op: CdcOperation
    source_table: str = Field(alias="__table")
    ts_ms: int
    lsn: Optional[int] = None
    before: Optional[dict[str, Any]] = None
    after: Optional[dict[str, Any]] = None

    class Config:
        populate_by_name = True

    @classmethod
    def from_json(cls, raw: str | bytes) -> "CdcEvent":
        data = json.loads(raw)
        return cls(**data)

    @property
    def row(self) -> dict[str, Any]:
        """Returns the current row state (after for INSERT/UPDATE, before for DELETE)."""
        if self.op == CdcOperation.DELETE:
            return self.before or {}
        return self.after or {}


class LineItem(BaseModel):
    line_id: str
    sku: str
    product_family: str
    description: Optional[str] = None
    quantity: float
    unit_price: float
    line_status: str = "OPEN"


class AllocationRecord(BaseModel):
    allocation_id: str
    sku: str
    warehouse_id: str
    allocated_qty: float
    status: str = "ALLOCATED"


class ShipmentData(BaseModel):
    shipment_id: str
    carrier: Optional[str] = None
    tracking_number: Optional[str] = None
    status: str = "PENDING"
    ship_date: Optional[int] = None        # epoch ms
    delivery_date: Optional[int] = None    # epoch ms


class StatusEntry(BaseModel):
    status: str
    timestamp: int                          # epoch ms
    source: str = "pipeline"


class OrderState(BaseModel):
    order_id: str
    customer_id: str = ""
    customer_name: str = ""
    channel: str = ""
    business_unit: str = ""
    product_family: str = ""
    order_status: str = "CREATED"
    priority_tier: int = 3
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    total_value: float = 0.0
    currency: str = "USD"
    line_items: list[LineItem] = Field(default_factory=list)
    allocation_records: list[AllocationRecord] = Field(default_factory=list)
    shipment_data: Optional[ShipmentData] = None
    status_history: list[StatusEntry] = Field(default_factory=list)
    atp_feasible: bool = False
    expected_ship_date: Optional[int] = None
    pipeline_ts: int = 0

    def to_scylla_row(self) -> dict[str, Any]:
        import datetime
        def ms_to_dt(ms: Optional[int]):
            if ms is None:
                return None
            return datetime.datetime.fromtimestamp(ms / 1000, tz=datetime.timezone.utc)

        return {
            "order_id":           self.order_id,
            "business_unit":      self.business_unit,
            "customer_id":        self.customer_id,
            "customer_name":      self.customer_name,
            "channel":            self.channel,
            "product_family":     self.product_family,
            "order_status":       self.order_status,
            "priority_tier":      self.priority_tier,
            "created_at":         ms_to_dt(self.created_at),
            "updated_at":         ms_to_dt(self.updated_at),
            "total_value":        self.total_value,
            "currency":           self.currency,
            "line_items":         json.dumps([li.dict() for li in self.line_items]),
            "allocation_records": json.dumps([ar.dict() for ar in self.allocation_records]),
            "shipment_data":      json.dumps(self.shipment_data.dict()) if self.shipment_data else None,
            "status_history":     [json.dumps(se.dict()) for se in self.status_history],
            "atp_feasible":       self.atp_feasible,
            "expected_ship_date": ms_to_dt(self.expected_ship_date),
            "pipeline_ts":        ms_to_dt(self.pipeline_ts),
        }
