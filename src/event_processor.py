"""Filter Google Calendar events and push OOO rows into Google Sheets."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

from .sheets_client import SheetsClient

LOGGER = logging.getLogger(__name__)


def _extract_datetime(event: Dict, key: str) -> str:
    value = event.get(key, {})
    return value.get("dateTime") or value.get("date") or ""


def _format_attendees(attendees: Optional[Iterable[Dict]]) -> str:
    if not attendees:
        return ""
    emails = [person.get("email") for person in attendees if person.get("email")]
    return ", ".join(sorted(set(emails)))


def event_to_row(calendar_id: str, event: Dict) -> List[str]:
    """Map a Calendar event into a sheet row."""

    start = _extract_datetime(event, "start")
    end = _extract_datetime(event, "end")
    organizer = (event.get("organizer") or {}).get("email", "")
    attendees = _format_attendees(event.get("attendees"))
    description = (event.get("description") or "")[:5000]

    return [
        calendar_id,
        event.get("id", ""),
        event.get("summary", ""),
        start,
        end,
        organizer,
        attendees,
        description,
        event.get("htmlLink", ""),
    ]


def is_ooo_event(event: Dict) -> bool:
    summary = (event.get("summary") or "").strip().lower()
    return summary.startswith("ooo")


def push_ooo_events(calendar_id: str, events: List[Dict]) -> int:
    """Send qualifying events to Google Sheets. Returns count."""

    sheets_client = SheetsClient()
    pushed = 0
    for event in events:
        if not is_ooo_event(event):
            continue
        row = event_to_row(calendar_id, event)
        LOGGER.info("Appending OOO event %s from %s", event.get("id"), calendar_id)
        sheets_client.append_row(row)
        pushed += 1
    return pushed

