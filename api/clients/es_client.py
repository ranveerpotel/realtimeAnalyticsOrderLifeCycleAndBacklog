"""Elasticsearch/OpenSearch async client wrapper."""

import logging
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch

log = logging.getLogger(__name__)


class ESClient:
    def __init__(self, url: str):
        self._es = AsyncElasticsearch(url)

    async def search_orders(
        self,
        q: Optional[str] = None,
        status: Optional[str] = None,
        business_unit: Optional[str] = None,
        product_family: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        must = []
        filters = []

        if q:
            must.append({"multi_match": {
                "query": q,
                "fields": ["customer_name^2", "order_id", "line_items.description", "line_items.sku"],
            }})
        if status:
            filters.append({"term": {"order_status": status}})
        if business_unit:
            filters.append({"term": {"business_unit": business_unit}})
        if product_family:
            filters.append({"term": {"product_family": product_family}})
        if date_from or date_to:
            date_range: dict[str, str] = {}
            if date_from:
                date_range["gte"] = date_from
            if date_to:
                date_range["lte"] = date_to
            filters.append({"range": {"updated_at": date_range}})

        query = {"bool": {"must": must or [{"match_all": {}}], "filter": filters}}

        resp = await self._es.search(
            index="orders",
            body={
                "query": query,
                "from": (page - 1) * size,
                "size": size,
                "sort": [{"updated_at": {"order": "desc"}}],
                "aggs": {
                    "by_status":         {"terms": {"field": "order_status", "size": 10}},
                    "by_business_unit":  {"terms": {"field": "business_unit", "size": 20}},
                    "by_product_family": {"terms": {"field": "product_family", "size": 20}},
                },
            },
        )
        return {
            "total":  resp["hits"]["total"]["value"],
            "orders": [h["_source"] for h in resp["hits"]["hits"]],
            "facets": {
                "status":         {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_status"]["buckets"]},
                "business_unit":  {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_business_unit"]["buckets"]},
                "product_family": {b["key"]: b["doc_count"] for b in resp["aggregations"]["by_product_family"]["buckets"]},
            },
        }

    async def get_backlog_summary(
        self, business_unit: Optional[str] = None, horizon: Optional[str] = None
    ) -> dict[str, Any]:
        filters = []
        if business_unit:
            filters.append({"term": {"business_unit": business_unit}})
        if horizon:
            filters.append({"term": {"horizon_bucket": horizon}})

        resp = await self._es.search(
            index="backlog_metrics",
            body={
                "query":   {"bool": {"filter": filters}} if filters else {"match_all": {}},
                "size":    0,
                "aggs": {
                    "total_unfulfilled": {"sum": {"field": "unfulfilled_demand"}},
                    "total_supply":      {"sum": {"field": "available_supply"}},
                    "total_gap":         {"sum": {"field": "demand_gap"}},
                    "by_product_family": {
                        "terms": {"field": "product_family", "size": 50},
                        "aggs": {
                            "demand":  {"sum": {"field": "unfulfilled_demand"}},
                            "supply":  {"sum": {"field": "available_supply"}},
                            "gap":     {"sum": {"field": "demand_gap"}},
                            "orders":  {"sum": {"field": "order_count"}},
                        },
                    },
                },
            },
        )
        aggs = resp["aggregations"]
        by_pf = [
            {
                "product_family": b["key"],
                "demand":  b["demand"]["value"],
                "supply":  b["supply"]["value"],
                "gap":     b["gap"]["value"],
                "order_count": int(b["orders"]["value"]),
            }
            for b in aggs["by_product_family"]["buckets"]
        ]
        return {
            "business_unit":     business_unit or "ALL",
            "total_unfulfilled": aggs["total_unfulfilled"]["value"],
            "total_supply":      aggs["total_supply"]["value"],
            "total_gap":         aggs["total_gap"]["value"],
            "by_product_family": by_pf,
        }

    async def get_order_trends(
        self, business_unit: Optional[str] = None, interval: str = "1h"
    ) -> list[dict[str, Any]]:
        filters = []
        if business_unit:
            filters.append({"term": {"business_unit": business_unit}})

        resp = await self._es.search(
            index="orders",
            body={
                "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
                "size":  0,
                "aggs": {
                    "intake_over_time": {
                        "date_histogram": {
                            "field":             "created_at",
                            "calendar_interval": interval,
                        },
                        "aggs": {
                            "total_value": {"sum": {"field": "total_value"}},
                        },
                    },
                },
            },
        )
        return [
            {
                "timestamp":   b["key_as_string"],
                "order_count": b["doc_count"],
                "total_value": b["total_value"]["value"],
            }
            for b in resp["aggregations"]["intake_over_time"]["buckets"]
        ]
