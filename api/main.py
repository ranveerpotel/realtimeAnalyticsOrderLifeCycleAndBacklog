"""FastAPI application entry point."""

import logging
import os
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from clients.es_client import ESClient
from clients.scylla_client import ScyllaClient
from routers import backlog, fulfillment, orders, stream

log = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.scylla = ScyllaClient(
        hosts=os.getenv("SCYLLA_HOSTS", "localhost").split(","),
        keyspace="orders",
    )
    app.state.es = ESClient(url=os.getenv("ELASTICSEARCH_URL", "http://localhost:9200"))
    app.state.redis = aioredis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379"),
        encoding="utf-8",
        decode_responses=True,
    )
    log.info("All clients initialized.")
    yield
    await app.state.redis.aclose()
    app.state.scylla.shutdown()


app = FastAPI(
    title="Real-Time Order Analytics API",
    version="1.0.0",
    description="Sub-second order lifecycle visibility and backlog management",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.include_router(orders.router,      prefix="/v1/orders",      tags=["Orders"])
app.include_router(backlog.router,     prefix="/v1/backlog",     tags=["Backlog"])
app.include_router(fulfillment.router, prefix="/v1/fulfillment", tags=["Fulfillment"])
app.include_router(stream.router,      prefix="/v1/stream",      tags=["Streaming"])


@app.get("/health")
async def health():
    return {"status": "ok"}
