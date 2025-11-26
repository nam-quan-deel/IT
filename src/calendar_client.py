"""Google Calendar helpers for watcher registration and delta sync."""

from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from .config import get_settings

SCOPES = ["https://www.googleapis.com/auth/calendar.events.readonly"]
LOGGER = logging.getLogger(__name__)


def build_calendar_service() -> Resource:
    """Create an authenticated Calendar API service."""

    settings = get_settings()
    credentials = service_account.Credentials.from_service_account_file(
        str(settings.google_service_account_file),
        scopes=SCOPES,
    )
    if settings.google_impersonation_subject:
        credentials = credentials.with_subject(settings.google_impersonation_subject)

    return build("calendar", "v3", credentials=credentials, cache_discovery=False)


def watch_calendar(calendar_id: str) -> Dict[str, str]:
    """Register a webhook channel for a calendar."""

    settings = get_settings()
    service = build_calendar_service()
    channel_id = str(uuid.uuid4())
    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": settings.watch_callback_url,
        "token": calendar_id,
    }

    LOGGER.info("Registering watch for %s", calendar_id)
    response = service.events().watch(calendarId=calendar_id, body=body).execute()
    return {
        "calendarId": calendar_id,
        "channelId": response["id"],
        "resourceId": response["resourceId"],
        "expiration": response.get("expiration"),
        "token": calendar_id,
    }


def fetch_recent_events(
    calendar_id: str, sync_token: Optional[str] = None
) -> Tuple[List[Dict], Optional[str]]:
    """Fetch changes since the previous sync token."""

    service = build_calendar_service()
    events: List[Dict] = []
    request_kwargs: Dict[str, str] = {
        "calendarId": calendar_id,
        "singleEvents": True,
        "showDeleted": False,
        "maxResults": 250,
        "orderBy": "startTime",
    }

    if sync_token:
        request_kwargs["syncToken"] = sync_token
    else:
        now = dt.datetime.utcnow().isoformat() + "Z"
        request_kwargs["timeMin"] = now

    try:
        while True:
            response = service.events().list(**request_kwargs).execute()
            events.extend(response.get("items", []))

            page_token = response.get("nextPageToken")
            if page_token:
                request_kwargs["pageToken"] = page_token
            else:
                next_sync = response.get("nextSyncToken")
                return events, next_sync
    except HttpError as exc:  # pragma: no cover - network branch
        if exc.resp.status == 410:
            LOGGER.warning("Sync token expired for %s; refetching from scratch", calendar_id)
            return fetch_recent_events(calendar_id)
        raise

