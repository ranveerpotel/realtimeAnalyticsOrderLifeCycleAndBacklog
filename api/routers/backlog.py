"""Backlog analytics endpoints."""

import json
import logging
from typing import Optional

from fastapi import APIRouter, Query, Request

log = logging.getLogger(__name__)
router = APIRouter()

CACHE_TTL = 30  # seconds — backlog summaries tolerate 30s staleness


@router.get("/summary")
async def get_backlog_summary(
    request: Request,
    business_unit: Optional[str] = Query(None),
    horizon:       Optional[str] = Query(None, description="D1 | D7 | D30 | D90"),
):
    cache_key = f"backlog:summary:{business_unit}:{horizon}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    result = await request.app.state.es.get_backlog_summary(
        business_unit=business_unit, horizon=horizon
    )
    await request.app.state.redis.setex(cache_key, CACHE_TTL, json.dumps(result, default=str))
    return result


@router.get("/{business_unit}/{product_family}")
async def get_backlog_detail(
    business_unit:  str,
    product_family: str,
    request: Request,
    horizon:      Optional[str] = Query("D30"),
    window_type:  Optional[str] = Query("sliding_1h"),
):
    cache_key = f"backlog:{business_unit}:{product_family}:{horizon}:{window_type}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    metrics = request.app.state.scylla.get_backlog_metrics(
        business_unit, product_family, horizon, window_type
    )
    result = {
        "business_unit":  business_unit,
        "product_family": product_family,
        "horizon":        horizon,
        "window_type":    window_type,
        "metrics":        metrics,
    }
    await request.app.state.redis.setex(cache_key, CACHE_TTL, json.dumps(result, default=str))
    return result


@router.get("/trends/{business_unit}")
async def get_order_trends(
    business_unit: str,
    request: Request,
    interval: str = Query("1h", description="Calendar interval: 1h | 1d | 1w"),
):
    cache_key = f"backlog:trends:{business_unit}:{interval}"
    cached = await request.app.state.redis.get(cache_key)
    if cached:
        return json.loads(cached)

    trends = await request.app.state.es.get_order_trends(
        business_unit=business_unit, interval=interval
    )
    await request.app.state.redis.setex(cache_key, 60, json.dumps(trends, default=str))
    return {"business_unit": business_unit, "interval": interval, "data": trends}
