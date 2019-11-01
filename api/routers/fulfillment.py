"""Fulfillment metrics and supply availability endpoints."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Query, Request

log = logging.getLogger(__name__)
router = APIRouter()


@router.get("/metrics")
async def get_fulfillment_metrics(
    request: Request,
    business_unit: str = Query(..., description="Business unit name"),
):
    cache_key = f"fulfillment:metrics:{business_unit}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    rows = request.app.state.scylla.get_fulfillment_metrics(business_unit)
    result = {"business_unit": business_unit, "metrics": rows}
    await request.app.state.redis.setex(cache_key, 60, json.dumps(result, default=str))
    return result


@router.get("/supply/{sku}")
async def get_supply_availability(sku: str, request: Request):
    cache_key = f"supply:{sku}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    warehouses = request.app.state.scylla.get_supply_availability(sku)
    result = {"sku": sku, "warehouses": warehouses}
    await request.app.state.redis.setex(cache_key, 10, json.dumps(result, default=str))
    return result
