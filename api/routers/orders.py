"""Order entity endpoints."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

log = logging.getLogger(__name__)
router = APIRouter()

CACHE_TTL = 5  # seconds — short TTL for individual orders (near-real-time)


@router.get("/search")
async def search_orders(
    request: Request,
    q:              Optional[str] = Query(None, description="Full-text search"),
    status:         Optional[str] = Query(None),
    business_unit:  Optional[str] = Query(None),
    product_family: Optional[str] = Query(None),
    date_from:      Optional[str] = Query(None, description="ISO8601 date"),
    date_to:        Optional[str] = Query(None, description="ISO8601 date"),
    page:           int           = Query(1, ge=1),
    size:           int           = Query(20, ge=1, le=200),
):
    cache_key = f"search:{q}:{status}:{business_unit}:{product_family}:{date_from}:{date_to}:{page}:{size}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    result = await request.app.state.es.search_orders(
        q=q,
        status=status,
        business_unit=business_unit,
        product_family=product_family,
        date_from=date_from,
        date_to=date_to,
        page=page,
        size=size,
    )
    await request.app.state.redis.setex(cache_key, 15, json.dumps(result, default=str))
    return result


@router.get("/by-status/{business_unit}/{status}")
# NOTE: this route must stay before /{order_id} — FastAPI matches in definition order
async def list_orders_by_status(
    business_unit: str,
    status: str,
    request: Request,
    limit: int = Query(100, ge=1, le=500),
):
    return request.app.state.scylla.get_orders_by_bu_status(business_unit, status, limit)


@router.get("/{order_id}")
async def get_order(order_id: str, request: Request):
    cache_key = f"order:{order_id}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    order = request.app.state.scylla.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    await request.app.state.redis.setex(cache_key, CACHE_TTL, json.dumps(order, default=str))
    return order
