"""Server-Sent Events endpoint for real-time dashboard push."""

import asyncio
import json
import logging
import time
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

log = logging.getLogger(__name__)
router = APIRouter()

PUSH_INTERVAL_SEC = 30


async def generate_dashboard_events(
    request: Request,
    business_unit: Optional[str],
) -> AsyncGenerator[str, None]:
    """
    Pushes dashboard updates every PUSH_INTERVAL_SEC seconds.
    Fetches backlog summary and recent order counts from cache/ES.
    Disconnects cleanly when client closes connection.
    """
    client_id = id(request)
    log.info("SSE client connected: %s (bu=%s)", client_id, business_unit)

    try:
        while True:
            if await request.is_disconnected():
                break

            try:
                backlog = await request.app.state.es.get_backlog_summary(
                    business_unit=business_unit
                )
                payload = json.dumps({
                    "type":      "backlog_update",
                    "timestamp": int(time.time() * 1000),
                    "data":      backlog,
                }, default=str)
                yield f"data: {payload}\n\n"
            except Exception as e:
                log.warning("SSE event generation error: %s", e)
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

            await asyncio.sleep(PUSH_INTERVAL_SEC)

    except asyncio.CancelledError:
        pass
    finally:
        log.info("SSE client disconnected: %s", client_id)


@router.get("/dashboard")
async def dashboard_stream(
    request: Request,
    business_unit: Optional[str] = Query(None),
):
    return StreamingResponse(
        generate_dashboard_events(request, business_unit),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
