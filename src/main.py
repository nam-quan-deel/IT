"""FastAPI entrypoint for Google Calendar webhook notifications."""

from __future__ import annotations

import logging
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

from .calendar_client import fetch_recent_events
from .config import get_settings
from .event_processor import push_ooo_events
from .state_store import StateStore

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

app = FastAPI(title="Calendar to Excel automation")
settings = get_settings()
state_store = StateStore(settings.state_dir)


@app.get("/healthz")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/notifications/google-calendar")
async def handle_notification(request: Request, background_tasks: BackgroundTasks) -> dict[str, str]:
    channel_token = request.headers.get("X-Goog-Channel-Token")
    if not channel_token:
        raise HTTPException(status_code=400, detail="Missing channel token")

    background_tasks.add_task(process_calendar_update, channel_token)
    return {"status": "accepted"}


def process_calendar_update(calendar_id: str) -> None:
    LOGGER.info("Processing notification for %s", calendar_id)
    sync_token = state_store.get_sync_token(calendar_id)
    events, next_sync_token = fetch_recent_events(calendar_id, sync_token)
    if not events:
        LOGGER.info("No new events for %s", calendar_id)
    else:
        pushed = push_ooo_events(calendar_id, events)
        LOGGER.info("Pushed %s OOO events for %s", pushed, calendar_id)
    if next_sync_token:
        state_store.set_sync_token(calendar_id, next_sync_token)

