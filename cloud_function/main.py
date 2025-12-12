import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import requests

from flask import jsonify, make_response
from google.cloud import firestore
from google.cloud import secretmanager
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
LOGGER = logging.getLogger(__name__)

CALENDAR_SCOPES = [
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/calendar.events.readonly",
]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@dataclass(frozen=True)
class Config:
    target_users: List[str]
    sheet_id: str
    sheet_range: str
    sheet_name: str
    webhook_url: str
    project_id: str
    sa_secret_name: str
    watch_collection: str
    processed_collection: str
    alert_collection: str
    min_lease_seconds: int
    watch_ttl_seconds: int
    pto_threshold: int
    slack_webhook_url: str
    slack_mentions: str
    slack_cc_mentions: str
    timezone: str
    user_labels: Dict[str, str]


CONFIG: Optional[Config] = None
FIRESTORE_CLIENT: Optional[firestore.Client] = None
SECRET_PAYLOAD: Optional[dict] = None
SHEETS_SERVICE = None


def _load_config() -> Config:
    global CONFIG
    if CONFIG:
        return CONFIG

    users = [
        email.strip()
        for email in os.environ.get("TARGET_USERS", "").split(",")
        if email.strip()
    ]
    if not users:
        raise RuntimeError(
            "TARGET_USERS env variable must include at least one user email"
        )

    sheet_id = os.environ.get("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("Missing required env variable SHEET_ID")

    webhook = os.environ.get("CALENDAR_WEBHOOK_URL")
    if not webhook:
        raise RuntimeError("Missing CALENDAR_WEBHOOK_URL env variable")

    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
    if not project_id:
        raise RuntimeError("Missing GCP_PROJECT env variable")

    secret_name = os.environ.get("SA_SECRET_NAME")
    if not secret_name:
        raise RuntimeError("Missing SA_SECRET_NAME env variable")

    user_labels_raw = os.environ.get("USER_LABELS_JSON", "").strip()
    user_labels: Dict[str, str] = {}
    if user_labels_raw:
        try:
            user_labels = json.loads(user_labels_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("USER_LABELS_JSON must be valid JSON") from exc

    CONFIG = Config(
        target_users=users,
        sheet_id=sheet_id,
        sheet_range=os.environ.get("SHEET_RANGE", "OOO_Events!A:E"),
        sheet_name=os.environ.get("SHEET_NAME", "OOO"),
        webhook_url=webhook,
        project_id=project_id,
        sa_secret_name=secret_name,
        watch_collection=os.environ.get("WATCH_COLLECTION", "calendar_watches"),
        processed_collection=os.environ.get("PROCESSED_COLLECTION", "ooo_events"),
        alert_collection=os.environ.get("ALERT_COLLECTION", "ooo_conflict_alerts"),
        min_lease_seconds=int(os.environ.get("MIN_LEASE_SECONDS", "3600")),
        watch_ttl_seconds=int(os.environ.get("WATCH_TTL_SECONDS", "604800")),
        pto_threshold=int(os.environ.get("PTO_THRESHOLD", "3")),
        slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL", "").strip(),
        slack_mentions=os.environ.get("SLACK_MENTIONS", "").strip(),
        slack_cc_mentions=os.environ.get("SLACK_CC_MENTIONS", "").strip(),
        timezone=os.environ.get("TIMEZONE", "UTC"),
        user_labels=user_labels,
    )
    return CONFIG


def _get_secret_payload() -> dict:
    global SECRET_PAYLOAD
    if SECRET_PAYLOAD:
        return SECRET_PAYLOAD

    config = _load_config()
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{config.project_id}/secrets/{config.sa_secret_name}/versions/latest"
    response = client.access_secret_version(name=name)
    SECRET_PAYLOAD = json.loads(response.payload.data.decode("utf-8"))
    return SECRET_PAYLOAD


def _base_credentials(scopes: List[str]) -> service_account.Credentials:
    payload = _get_secret_payload()
    return service_account.Credentials.from_service_account_info(payload, scopes=scopes)


def _firestore() -> firestore.Client:
    global FIRESTORE_CLIENT
    if FIRESTORE_CLIENT:
        return FIRESTORE_CLIENT

    config = _load_config()
    FIRESTORE_CLIENT = firestore.Client(project=config.project_id)
    return FIRESTORE_CLIENT


def _build_calendar_service(user_email: str):
    creds = _base_credentials(CALENDAR_SCOPES).with_subject(user_email)
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _build_sheets_service():
    global SHEETS_SERVICE
    if SHEETS_SERVICE:
        return SHEETS_SERVICE

    SHEETS_SERVICE = build(
        "sheets", "v4", credentials=_base_credentials(SHEETS_SCOPES), cache_discovery=False
    )
    return SHEETS_SERVICE


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_rfc3339(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _tz() -> ZoneInfo:
    config = _load_config()
    try:
        return ZoneInfo(config.timezone)
    except Exception as exc:
        raise RuntimeError(f"Invalid TIMEZONE value: {config.timezone}") from exc


def _parse_rfc3339(value: str) -> datetime:
    # Handles "...Z" and offsets. Calendar API returns RFC3339.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _event_active_dates(event: Dict) -> List[date]:
    """
    Returns the list of local dates the event covers (end is treated as exclusive).
    - All-day events use {date, date} and the end date is exclusive.
    - Timed events use {dateTime, dateTime} and the end instant is exclusive.
    """
    tz = _tz()
    start = event.get("start", {}) or {}
    end = event.get("end", {}) or {}

    if "date" in start and start.get("date") and end.get("date"):
        start_date = date.fromisoformat(start["date"])
        end_date_exclusive = date.fromisoformat(end["date"])
        days: List[date] = []
        cur = start_date
        while cur < end_date_exclusive:
            days.append(cur)
            cur = cur + timedelta(days=1)
        return days

    if start.get("dateTime") and end.get("dateTime"):
        start_dt = _parse_rfc3339(start["dateTime"]).astimezone(tz)
        end_dt = _parse_rfc3339(end["dateTime"]).astimezone(tz)
        if end_dt <= start_dt:
            return [start_dt.date()]
        # End is exclusive; subtract a tiny delta to get the last included local date.
        end_inclusive_date = (end_dt - timedelta(microseconds=1)).date()
        days = []
        cur = start_dt.date()
        while cur <= end_inclusive_date:
            days.append(cur)
            cur = cur + timedelta(days=1)
        return days

    # Fallback: treat as "today" in UTC if payload is malformed.
    return [datetime.utcnow().date()]


def _is_ooo_event(event: Dict) -> bool:
    if event.get("status") == "cancelled":
        return False
    summary = (event.get("summary") or "").strip()
    return summary.upper().startswith("OOO")


def _derive_label_from_email(email: str) -> str:
    local = (email or "").split("@", 1)[0].strip()
    return local.replace(".", " ").replace("_", " ").upper() if local else "UNKNOWN"


def _extract_person_label(user_email: str, summary: str) -> str:
    """
    Prefer USER_LABELS_JSON[email]; else attempt to parse label from the event summary:
      "OOO - ROHAN (APAC)" -> "ROHAN (APAC)"
    """
    config = _load_config()
    if user_email in (config.user_labels or {}):
        return str(config.user_labels[user_email]).strip()

    s = (summary or "").strip()
    if not s:
        return _derive_label_from_email(user_email)

    # Remove leading "OOO" (any case) and common separators.
    upper = s.upper()
    if upper.startswith("OOO"):
        s = s[3:].strip()
        for sep in [":", "-", "–", "—", "|"]:
            if s.startswith(sep):
                s = s[len(sep) :].strip()
                break
    return s or _derive_label_from_email(user_email)


class AlertStore:
    def __init__(self):
        self._collection = _firestore().collection(_load_config().alert_collection)

    def get_for_day(self, day_iso: str) -> Optional[Dict]:
        doc = self._collection.document(day_iso).get()
        if not doc.exists:
            return None
        return doc.to_dict()

    def save_for_day(self, day_iso: str, payload: Dict) -> None:
        payload["updated_at"] = firestore.SERVER_TIMESTAMP
        self._collection.document(day_iso).set(payload, merge=True)


def _slack_post(text: str) -> None:
    config = _load_config()
    if not config.slack_webhook_url:
        LOGGER.info("SLACK_WEBHOOK_URL not set; skipping Slack notification")
        return
    resp = requests.post(
        config.slack_webhook_url, json={"text": text}, timeout=10
    )
    if resp.status_code < 200 or resp.status_code >= 300:
        raise RuntimeError(
            f"Slack webhook returned {resp.status_code}: {resp.text[:500]}"
        )


def _format_conflict_message(
    *,
    threshold: int,
    last_member: str,
    sheet_name: str,
    day: date,
    people_off_labels: List[str],
) -> str:
    config = _load_config()
    weekday = day.strftime("%A")
    day_iso = day.isoformat()
    mentions = config.slack_mentions
    cc = config.slack_cc_mentions
    conflict_count = len(people_off_labels)
    people_off = ", ".join(people_off_labels)

    lines = [
        f":rotating_light: {mentions} TIME-OFF CONFLICT ALERT:".rstrip(),
        f"Threshold: {threshold} maximum off",
        f"Last Member Edited: {last_member}",
        f"Sheet: {sheet_name}",
        f"Date: {day_iso} ({weekday})",
        f"Conflict Count: {conflict_count} people off (Limit: {threshold})",
        f"Day/Event: {weekday}",
        f"People Off: {people_off}",
    ]
    if cc:
        lines.append(f"CC: {cc}")
    return "\n".join(lines)


def _list_ooo_people_for_day(day: date) -> List[str]:
    """
    Returns unique person labels for calendars that have an OOO event overlapping 'day'.
    """
    config = _load_config()
    tz = _tz()
    day_start = datetime(day.year, day.month, day.day, tzinfo=tz)
    day_end = day_start + timedelta(days=1)

    people: Set[str] = set()
    for user in config.target_users:
        service = _build_calendar_service(user)
        try:
            resp = (
                service.events()
                .list(
                    calendarId="primary",
                    timeMin=day_start.isoformat(),
                    timeMax=day_end.isoformat(),
                    singleEvents=True,
                    showDeleted=False,
                    maxResults=250,
                    orderBy="startTime",
                )
                .execute()
            )
        except HttpError:
            LOGGER.exception("Failed listing events for %s on %s", user, day.isoformat())
            continue

        for ev in resp.get("items", []) or []:
            if not _is_ooo_event(ev):
                continue
            label = _extract_person_label(user, (ev.get("summary") or "").strip())
            people.add(label)
            break  # one person counts once per day

    return sorted(people)


def _maybe_send_conflict_alert_for_day(day: date, last_member: str) -> Optional[Dict]:
    config = _load_config()
    threshold = config.pto_threshold

    people_off = _list_ooo_people_for_day(day)
    count = len(people_off)
    if count <= threshold:
        return {"day": day.isoformat(), "sent": False, "count": count}

    day_iso = day.isoformat()
    store = AlertStore()
    existing = store.get_for_day(day_iso) or {}
    last_alert_count = int(existing.get("last_alert_count") or 0)

    # Avoid spamming: only alert if this is the first alert for the day or the count increased.
    if last_alert_count >= count:
        return {"day": day_iso, "sent": False, "count": count, "deduped": True}

    message = _format_conflict_message(
        threshold=threshold,
        last_member=last_member,
        sheet_name=config.sheet_name,
        day=day,
        people_off_labels=people_off,
    )
    _slack_post(message)
    store.save_for_day(
        day_iso,
        {
            "day": day_iso,
            "last_alert_count": count,
            "threshold": threshold,
            "last_member": last_member,
            "people_off": people_off,
        },
    )
    return {"day": day_iso, "sent": True, "count": count}


class WatchStore:
    def __init__(self):
        self._collection = _firestore().collection(_load_config().watch_collection)

    def get_for_user(self, user_email: str) -> Optional[Dict]:
        doc = self._collection.document(user_email).get()
        if not doc.exists:
            return None
        data = doc.to_dict()
        data["user_email"] = user_email
        return data

    def save(self, user_email: str, payload: Dict) -> None:
        payload["user_email"] = user_email
        self._collection.document(user_email).set(payload, merge=True)

    def get_by_channel(self, channel_id: str) -> Optional[Dict]:
        stream = (
            self._collection.where("channel_id", "==", channel_id).limit(1).stream()
        )
        docs = list(stream)
        if not docs:
            return None
        data = docs[0].to_dict()
        if "user_email" not in data:
            data["user_email"] = docs[0].id
        return data

    def update_sync_token(self, user_email: str, sync_token: str) -> None:
        self._collection.document(user_email).set({"sync_token": sync_token}, merge=True)


class ProcessedEventStore:
    def __init__(self):
        self._collection = _firestore().collection(_load_config().processed_collection)

    def already_processed(self, event_id: str) -> bool:
        doc = self._collection.document(event_id).get()
        return doc.exists

    def mark_processed(self, event_id: str, payload: Dict) -> None:
        payload["processed_at"] = firestore.SERVER_TIMESTAMP
        self._collection.document(event_id).set(payload, merge=True)


def register_channels(request):
    """HTTP function invoked via Cloud Scheduler to (re)register calendar watches."""
    config = _load_config()
    watch_store = WatchStore()
    results = []

    for user in config.target_users:
        result = _ensure_watch_for_user(user, watch_store)
        results.append(result)

    return jsonify({"results": results})


def calendar_webhook(request):
    """HTTP endpoint that receives Google Calendar push notifications."""
    channel_id = request.headers.get("X-Goog-Channel-Id")
    resource_state = request.headers.get("X-Goog-Resource-State")

    if not channel_id:
        return make_response("Missing X-Goog-Channel-Id header", 400)

    watch_store = WatchStore()
    watch = watch_store.get_by_channel(channel_id)
    if not watch:
        LOGGER.warning("Received notification for unknown channel %s", channel_id)
        return ("Ignored unknown channel", 200)

    if resource_state == "sync":
        LOGGER.info("Sync handshake for %s acknowledged", channel_id)
        return ("SYNC", 200)

    sync_token = watch.get("sync_token")
    if not sync_token:
        sync_token = _bootstrap_sync_token(watch["user_email"])

    try:
        events, next_token = _fetch_incremental_events(
            watch["user_email"], sync_token, watch.get("calendar_id", "primary")
        )
    except HttpError as exc:
        if exc.resp.status == 410:
            LOGGER.warning(
                "Sync token expired for %s; bootstrapping a new token", watch["user_email"]
            )
            sync_token = _bootstrap_sync_token(watch["user_email"])
            events, next_token = _fetch_incremental_events(
                watch["user_email"], sync_token, watch.get("calendar_id", "primary")
            )
        else:
            LOGGER.exception("Calendar API error while fetching events")
            return make_response("Calendar API error", 500)

    processed_store = ProcessedEventStore()
    appended = _process_events(
        watch["user_email"], events, processed_store, watch.get("calendar_id", "primary")
    )

    if next_token:
        watch_store.update_sync_token(watch["user_email"], next_token)

    return jsonify({"processed": appended})


def _ensure_watch_for_user(user_email: str, watch_store: WatchStore) -> Dict:
    config = _load_config()
    existing = watch_store.get_for_user(user_email)
    now_ms = _now_ms()

    if existing:
        expires = int(existing.get("expiration", 0))
        if expires - now_ms > config.min_lease_seconds * 1000:
            LOGGER.debug(
                "Watch for %s still healthy (expires %s)", user_email, existing["expiration"]
            )
            return {"user": user_email, "status": "healthy", "expiration": expires}

    service = _build_calendar_service(user_email)
    channel_id = str(uuid.uuid4())
    body = {
        "id": channel_id,
        "type": "web_hook",
        "address": config.webhook_url,
        "token": user_email,
        "params": {"ttl": str(config.watch_ttl_seconds)},
    }

    LOGGER.info("Registering watch for %s", user_email)
    response = service.events().watch(calendarId="primary", body=body).execute()
    sync_token = _bootstrap_sync_token(user_email)

    watch_store.save(
        user_email,
        {
            "channel_id": response["id"],
            "resource_id": response["resourceId"],
            "calendar_id": "primary",
            "expiration": int(response["expiration"]),
            "sync_token": sync_token,
        },
    )

    return {"user": user_email, "status": "refreshed", "expiration": response["expiration"]}


def _bootstrap_sync_token(user_email: str) -> str:
    service = _build_calendar_service(user_email)
    page_token = None
    sync_token = None
    while True:
        request = service.events().list(
            calendarId="primary",
            singleEvents=True,
            showDeleted=False,
            timeMin=_utc_rfc3339(datetime.utcnow() - timedelta(days=30)),
            maxResults=250,
            pageToken=page_token,
        )
        response = request.execute()
        page_token = response.get("nextPageToken")
        if not page_token:
            sync_token = response.get("nextSyncToken")
            break

    if not sync_token:
        raise RuntimeError(f"Unable to bootstrap sync token for {user_email}")

    LOGGER.info("Bootstrap sync token generated for %s", user_email)
    return sync_token


def _fetch_incremental_events(
    user_email: str, sync_token: str, calendar_id: str
) -> Tuple[List[Dict], Optional[str]]:
    service = _build_calendar_service(user_email)
    page_token = None
    events: List[Dict] = []

    while True:
        request = service.events().list(
            calendarId=calendar_id,
            syncToken=sync_token,
            pageToken=page_token,
            showDeleted=True,
            singleEvents=True,
            maxResults=250,
        )
        response = request.execute()
        events.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            return events, response.get("nextSyncToken")


def _process_events(
    user_email: str,
    events: List[Dict],
    processed_store: ProcessedEventStore,
    calendar_id: str,
) -> List[Dict]:
    appended: List[Dict] = []
    dates_to_check: Dict[date, str] = {}
    for event in events:
        summary = (event.get("summary") or "").strip()
        if event.get("status") == "cancelled":
            LOGGER.debug("Skipping cancelled event %s", event.get("id"))
            continue
        if not summary.upper().startswith("OOO"):
            continue

        # Always consider conflict checks on OOO changes (even if already processed for Sheets).
        last_member = _extract_person_label(user_email, summary)
        for d in _event_active_dates(event):
            dates_to_check[d] = last_member

        event_id = event.get("id")
        if not event_id or processed_store.already_processed(event_id):
            continue
        _append_event_to_sheet(user_email, event)
        processed_store.mark_processed(
            event_id,
            {
                "user_email": user_email,
                "calendar_id": calendar_id,
                "summary": summary,
                "start": _pick_time(event.get("start", {})),
                "end": _pick_time(event.get("end", {})),
            },
        )
        appended.append({"event_id": event_id, "summary": summary})

    # Run conflict checks once per unique day touched by OOO changes in this webhook batch.
    for d in sorted(dates_to_check.keys()):
        try:
            _maybe_send_conflict_alert_for_day(d, dates_to_check[d])
        except Exception:
            LOGGER.exception("Failed sending conflict alert for %s", d.isoformat())
    return appended


def _append_event_to_sheet(user_email: str, event: Dict) -> None:
    config = _load_config()
    sheets = _build_sheets_service()
    start = _pick_time(event.get("start", {}))
    end = _pick_time(event.get("end", {}))
    payload = [
        [
            user_email,
            event.get("summary", ""),
            start,
            end,
            event.get("htmlLink", ""),
        ]
    ]
    sheets.spreadsheets().values().append(
        spreadsheetId=config.sheet_id,
        range=config.sheet_range,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": payload},
    ).execute()


def _pick_time(time_payload: Dict) -> str:
    return (
        time_payload.get("dateTime")
        or time_payload.get("date")
        or datetime.utcnow().isoformat()
    )


def healthcheck(request):
    """Simple function to keep the service warm / health monitored."""
    _load_config()
    return ("ok", 200)
