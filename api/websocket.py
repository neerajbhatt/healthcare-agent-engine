"""WebSocket endpoint for real-time investigation progress streaming."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from orchestrator.engine import engine
from utils.logging import get_logger

logger = get_logger(__name__)

ws_router = APIRouter()


@ws_router.websocket("/ws/investigate")
async def websocket_investigate(websocket: WebSocket):
    """Stream investigation progress over WebSocket.

    Client sends: {"query": "your investigation query"}
    Server streams: {"event": "...", "data": {...}} for each progress step
    """
    await websocket.accept()

    try:
        # Receive the query
        data = await websocket.receive_json()
        query = data.get("query", "")

        if not query:
            await websocket.send_json({"event": "error", "data": {"message": "No query provided"}})
            await websocket.close()
            return

        await websocket.send_json({
            "event": "investigation_started",
            "data": {"query": query},
        })

        # Progress callback streams events to the WebSocket
        async def on_progress(inv_id: str, event: str, data: dict[str, Any]):
            try:
                await websocket.send_json({
                    "event": event,
                    "data": {"investigation_id": inv_id, **data},
                })
            except Exception:
                pass  # Client may have disconnected

        # Run the investigation with streaming
        result = await engine.investigate(query, on_progress=on_progress)

        # Send final result
        await websocket.send_json({
            "event": "investigation_result",
            "data": result.to_dict(),
        })

    except WebSocketDisconnect:
        logger.info("websocket_disconnected")
    except Exception as e:
        logger.error("websocket_error", error=str(e))
        try:
            await websocket.send_json({"event": "error", "data": {"message": str(e)}})
        except Exception:
            pass
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
