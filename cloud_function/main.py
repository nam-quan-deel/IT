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


# ==========================
# Jira user update summarizer
# ==========================


@dataclass(frozen=True)
class JiraSummaryConfig:
    # Prefer MCP; fallback to REST if configured.
    atlassian_mcp_url: str
    atlassian_mcp_bearer_token: str
    atlassian_mcp_headers_json: str

    jira_base_url: str
    jira_email: str
    jira_api_token: str

    default_users: List[str]
    default_since_days: int
    max_issues_per_user: int
    timezone: str


JIRA_SUMMARY_CONFIG: Optional[JiraSummaryConfig] = None


def _load_jira_summary_config() -> JiraSummaryConfig:
    global JIRA_SUMMARY_CONFIG
    if JIRA_SUMMARY_CONFIG:
        return JIRA_SUMMARY_CONFIG

    default_users = [
        u.strip()
        for u in os.environ.get("JIRA_USERS", "").split(",")
        if u.strip()
    ]
    JIRA_SUMMARY_CONFIG = JiraSummaryConfig(
        atlassian_mcp_url=os.environ.get("ATLASSIAN_MCP_URL", "").strip(),
        atlassian_mcp_bearer_token=os.environ.get("ATLASSIAN_MCP_BEARER_TOKEN", "").strip(),
        atlassian_mcp_headers_json=os.environ.get("ATLASSIAN_MCP_HEADERS_JSON", "").strip(),
        jira_base_url=os.environ.get("JIRA_BASE_URL", "").strip().rstrip("/"),
        jira_email=os.environ.get("JIRA_EMAIL", "").strip(),
        jira_api_token=os.environ.get("JIRA_API_TOKEN", "").strip(),
        default_users=default_users,
        default_since_days=int(os.environ.get("JIRA_SINCE_DAYS", "7")),
        max_issues_per_user=int(os.environ.get("JIRA_MAX_ISSUES_PER_USER", "50")),
        timezone=os.environ.get("JIRA_TIMEZONE", os.environ.get("TIMEZONE", "UTC")),
    )
    return JIRA_SUMMARY_CONFIG


def _jira_tzinfo() -> ZoneInfo:
    cfg = _load_jira_summary_config()
    try:
        return ZoneInfo(cfg.timezone)
    except Exception as exc:
        raise RuntimeError(f"Invalid JIRA_TIMEZONE/TIMEZONE value: {cfg.timezone}") from exc


def _as_dt(value: str) -> datetime:
    # RFC3339-ish; Jira often returns "2023-01-01T12:34:56.789+0000"
    # or "2023-01-01T12:34:56.789+00:00"
    if not value:
        return datetime.now(timezone.utc)
    v = value.strip()
    try:
        if v.endswith("Z"):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return datetime.fromisoformat(v)
    except ValueError:
        # Handle Jira "+0000" offsets (no colon).
        if len(v) >= 5:
            tail = v[-5:]
            # Example: 2025-01-01T00:00:00.000+0000 -> 2025-01-01T00:00:00.000+00:00
            if tail[0] in {"+", "-"} and tail[1:].isdigit():
                v2 = v[:-5] + tail[:3] + ":" + tail[3:]
                return datetime.fromisoformat(v2)
        raise


def _dt_floor_now_utc() -> datetime:
    return datetime.now(timezone.utc)


class _McpError(RuntimeError):
    pass


class _AtlassianMcpClient:
    """
    Minimal MCP (Model Context Protocol) client over HTTP for Cloud Functions.

    Expects an MCP server that supports JSON-RPC 2.0 methods:
      - tools/list
      - tools/call
    """

    def __init__(self, base_url: str, headers: Dict[str, str]):
        self._base_url = base_url.rstrip("/")
        self._headers = dict(headers or {})
        self._tools_cache: Optional[List[Dict]] = None

    def _post(self, payload: Dict) -> Dict:
        resp = requests.post(
            self._base_url,
            json=payload,
            headers={**self._headers, "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise _McpError(f"MCP HTTP {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        if "error" in data and data["error"]:
            raise _McpError(f"MCP error: {data['error']}")
        return data

    def list_tools(self) -> List[Dict]:
        if self._tools_cache is not None:
            return self._tools_cache
        data = self._post({"jsonrpc": "2.0", "id": "tools_list", "method": "tools/list", "params": {}})
        tools = (data.get("result") or {}).get("tools") or []
        self._tools_cache = tools
        return tools

    def call_tool(self, name: str, arguments: Dict) -> Dict:
        payload = {
            "jsonrpc": "2.0",
            "id": f"tools_call_{name}_{uuid.uuid4()}",
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments or {}},
        }
        data = self._post(payload)
        return data.get("result") or {}


def _mcp_headers_from_env(cfg: JiraSummaryConfig) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if cfg.atlassian_mcp_headers_json:
        try:
            parsed = json.loads(cfg.atlassian_mcp_headers_json)
            if isinstance(parsed, dict):
                headers.update({str(k): str(v) for k, v in parsed.items()})
        except json.JSONDecodeError as exc:
            raise RuntimeError("ATLASSIAN_MCP_HEADERS_JSON must be valid JSON object") from exc
    if cfg.atlassian_mcp_bearer_token and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {cfg.atlassian_mcp_bearer_token}"
    return headers


class _JiraRestClient:
    def __init__(self, base_url: str, email: str, api_token: str):
        self._base_url = base_url.rstrip("/")
        self._auth = (email, api_token)

    def _get(self, path: str, params: Optional[Dict] = None) -> Dict:
        url = f"{self._base_url}{path}"
        resp = requests.get(
            url,
            params=params or {},
            auth=self._auth,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"Jira HTTP {resp.status_code} for {path}: {resp.text[:500]}")
        return resp.json()

    def search(self, jql: str, fields: List[str], max_results: int) -> Dict:
        return self._get(
            "/rest/api/3/search",
            params={
                "jql": jql,
                "fields": ",".join(fields),
                "maxResults": str(max_results),
            },
        )

    def user_search(self, query: str) -> List[Dict]:
        data = self._get("/rest/api/3/user/search", params={"query": query, "maxResults": "10"})
        return data if isinstance(data, list) else []

    def issue_changelog(self, issue_key: str, max_results: int = 100) -> Dict:
        return self._get(
            f"/rest/api/3/issue/{issue_key}/changelog",
            params={"maxResults": str(max_results)},
        )

    def issue_comments(self, issue_key: str, max_results: int = 100) -> Dict:
        return self._get(
            f"/rest/api/3/issue/{issue_key}/comment",
            params={"maxResults": str(max_results), "orderBy": "-created"},
        )


class _JiraClient:
    """
    Unified Jira client. Prefers Atlassian MCP when configured; can fall back to Jira REST.
    """

    def __init__(self, cfg: JiraSummaryConfig):
        self._cfg = cfg
        self._mcp: Optional[_AtlassianMcpClient] = None
        self._rest: Optional[_JiraRestClient] = None

        if cfg.atlassian_mcp_url:
            self._mcp = _AtlassianMcpClient(cfg.atlassian_mcp_url, _mcp_headers_from_env(cfg))
        if cfg.jira_base_url and cfg.jira_email and cfg.jira_api_token:
            self._rest = _JiraRestClient(cfg.jira_base_url, cfg.jira_email, cfg.jira_api_token)

    def _available_tools(self) -> List[str]:
        if not self._mcp:
            return []
        return [t.get("name") for t in self._mcp.list_tools() if t.get("name")]

    def _pick_tool(self, candidates: List[str]) -> Optional[str]:
        tools = set(self._available_tools())
        for name in candidates:
            if name in tools:
                return name
        # fuzzy: case-insensitive exact match
        lower_map = {str(t).lower(): t for t in tools}
        for name in candidates:
            if name.lower() in lower_map:
                return lower_map[name.lower()]
        return None

    def _require_backend(self) -> None:
        if not self._mcp and not self._rest:
            raise RuntimeError(
                "No Jira backend configured. Set ATLASSIAN_MCP_URL (recommended) "
                "or set JIRA_BASE_URL + JIRA_EMAIL + JIRA_API_TOKEN."
            )

    def user_lookup(self, user_value: str) -> Optional[Dict]:
        """
        Returns a normalized dict with:
          - accountId
          - displayName
          - emailAddress (if available)
        """
        self._require_backend()
        q = (user_value or "").strip()
        if not q:
            return None

        # If it's already an accountId-like value, keep it.
        if "@" not in q and len(q) >= 8:
            return {"accountId": q, "displayName": q}

        # Try MCP first
        if self._mcp:
            tool = self._pick_tool(["jira_user_search", "jira_search_users", "jira_get_user"])
            if tool:
                for args in [{"query": q}, {"q": q}, {"accountId": q}, {"email": q}]:
                    try:
                        res = self._mcp.call_tool(tool, args)
                        payload = res.get("content") or res.get("data") or res
                        # Some MCP servers return [{"type":"json","json":...}]
                        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                            if "json" in payload[0]:
                                payload = payload[0]["json"]
                        if isinstance(payload, list) and payload:
                            u = payload[0]
                        elif isinstance(payload, dict):
                            u = payload
                        else:
                            continue
                        account_id = u.get("accountId") or u.get("account_id")
                        if account_id:
                            return {
                                "accountId": account_id,
                                "displayName": u.get("displayName") or u.get("display_name") or q,
                                "emailAddress": u.get("emailAddress") or u.get("email") or None,
                            }
                    except Exception:
                        continue

        # REST fallback
        if self._rest:
            matches = self._rest.user_search(q)
            if matches:
                u = matches[0]
                return {
                    "accountId": u.get("accountId"),
                    "displayName": u.get("displayName") or q,
                    "emailAddress": u.get("emailAddress"),
                }
        return None

    def search_issues(self, jql: str, fields: List[str], max_results: int) -> List[Dict]:
        self._require_backend()
        if self._mcp:
            tool = self._pick_tool(["jira_search", "jira_search_issues", "jira_jql_search", "jira_searchIssues"])
            if tool:
                arg_variants = [
                    {"jql": jql, "fields": fields, "max_results": max_results},
                    {"jql": jql, "fields": fields, "maxResults": max_results},
                    {"query": jql, "fields": fields, "limit": max_results},
                ]
                last_exc = None
                for args in arg_variants:
                    try:
                        res = self._mcp.call_tool(tool, args)
                        payload = res.get("content") or res.get("data") or res
                        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                            if "json" in payload[0]:
                                payload = payload[0]["json"]
                        if isinstance(payload, dict) and "issues" in payload:
                            return payload.get("issues") or []
                        if isinstance(payload, list):
                            return payload
                    except Exception as exc:
                        last_exc = exc
                if last_exc:
                    raise RuntimeError(f"MCP jira search failed: {last_exc}") from last_exc
        if self._rest:
            data = self._rest.search(jql, fields=fields, max_results=max_results)
            return data.get("issues") or []

        raise RuntimeError(
            "Jira search unavailable: MCP tools not found and REST not configured. "
            f"Available MCP tools: {self._available_tools()}"
        )

    def issue_changelog(self, issue_key: str) -> List[Dict]:
        self._require_backend()
        if self._mcp:
            tool = self._pick_tool(["jira_issue_changelog", "jira_get_issue_changelog", "jira_get_changelog"])
            if tool:
                for args in [{"issue_key": issue_key}, {"issueKey": issue_key}, {"key": issue_key}]:
                    try:
                        res = self._mcp.call_tool(tool, args)
                        payload = res.get("content") or res.get("data") or res
                        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                            if "json" in payload[0]:
                                payload = payload[0]["json"]
                        if isinstance(payload, dict) and "values" in payload:
                            return payload.get("values") or []
                        if isinstance(payload, dict) and "histories" in payload:
                            return payload.get("histories") or []
                        if isinstance(payload, list):
                            return payload
                    except Exception:
                        continue
        if self._rest:
            data = self._rest.issue_changelog(issue_key)
            return data.get("values") or []
        return []

    def issue_comments(self, issue_key: str) -> List[Dict]:
        self._require_backend()
        if self._mcp:
            tool = self._pick_tool(["jira_issue_comments", "jira_get_issue_comments", "jira_get_comments"])
            if tool:
                for args in [{"issue_key": issue_key}, {"issueKey": issue_key}, {"key": issue_key}]:
                    try:
                        res = self._mcp.call_tool(tool, args)
                        payload = res.get("content") or res.get("data") or res
                        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                            if "json" in payload[0]:
                                payload = payload[0]["json"]
                        if isinstance(payload, dict) and "comments" in payload:
                            return payload.get("comments") or []
                        if isinstance(payload, dict) and "values" in payload:
                            return payload.get("values") or []
                        if isinstance(payload, list):
                            return payload
                    except Exception:
                        continue
        if self._rest:
            data = self._rest.issue_comments(issue_key)
            return data.get("comments") or []
        return []


def _jql_quote(value: str) -> str:
    v = (value or "").replace('"', '\\"')
    return f'"{v}"'


def _normalize_issue(issue: Dict) -> Dict:
    fields = issue.get("fields") or {}
    project = (fields.get("project") or {}).get("key") or (fields.get("project") or {}).get("name")
    issuetype = (fields.get("issuetype") or {}).get("name")
    status = (fields.get("status") or {}).get("name")
    assignee = (fields.get("assignee") or {}) or {}
    reporter = (fields.get("reporter") or {}) or {}
    return {
        "key": issue.get("key"),
        "id": issue.get("id"),
        "summary": fields.get("summary"),
        "project": project,
        "issuetype": issuetype,
        "status": status,
        "created": fields.get("created"),
        "updated": fields.get("updated"),
        "assignee": {"accountId": assignee.get("accountId"), "displayName": assignee.get("displayName")},
        "reporter": {"accountId": reporter.get("accountId"), "displayName": reporter.get("displayName")},
    }


def _extract_text_from_adf(adf: Dict, limit: int = 240) -> str:
    """
    Jira Cloud comments are often ADF (Atlassian Document Format).
    Extract plain-ish text with a conservative cap.
    """
    if not isinstance(adf, dict):
        return ""
    out: List[str] = []

    def walk(node):
        if not node or len(" ".join(out)) >= limit:
            return
        if isinstance(node, dict):
            if node.get("type") == "text" and isinstance(node.get("text"), str):
                out.append(node["text"])
            for c in node.get("content") or []:
                walk(c)
        elif isinstance(node, list):
            for c in node:
                walk(c)

    walk(adf)
    text = " ".join(" ".join(out).split())
    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def _summarize_user_activity(
    jira: _JiraClient,
    *,
    user: Dict,
    since: datetime,
    max_issues: int,
) -> Dict:
    account_id = user["accountId"]
    fields = [
        "summary",
        "status",
        "issuetype",
        "project",
        "updated",
        "created",
        "assignee",
        "reporter",
    ]

    # Pull a manageable set of candidate issues around the user and time window.
    # Note: JQL can't reliably express "updated by user" on Jira Cloud, so we:
    # - fetch issues created/reported/assigned in window
    # - then inspect changelog + comments to find actual user-authored updates
    jqls = [
        f'created >= -{max(1, int(((_dt_floor_now_utc() - since).total_seconds() // 86400) or 1))}d AND reporter = {_jql_quote(account_id)} ORDER BY created DESC',
        f'updated >= -{max(1, int(((_dt_floor_now_utc() - since).total_seconds() // 86400) or 1))}d AND assignee = {_jql_quote(account_id)} ORDER BY updated DESC',
        f'assignee = {_jql_quote(account_id)} AND statusCategory != Done ORDER BY updated DESC',
    ]

    issues_map: Dict[str, Dict] = {}
    for jql in jqls:
        for issue in jira.search_issues(jql, fields=fields, max_results=max_issues):
            norm = _normalize_issue(issue)
            if norm.get("key"):
                issues_map[norm["key"]] = norm

    issues = list(issues_map.values())

    # Inspect detailed activity (comments/changelog) for these candidate issues.
    touched_projects: Dict[str, int] = {}
    created_keys: List[str] = []
    commented: List[Dict] = []
    transitions: List[Dict] = []
    field_changes: List[Dict] = []

    for it in issues:
        key = it["key"]
        proj = it.get("project") or "UNKNOWN"
        touched_projects[proj] = touched_projects.get(proj, 0) + 1

        created_dt = _as_dt(it.get("created") or "").astimezone(timezone.utc)
        if created_dt >= since and (it.get("reporter") or {}).get("accountId") == account_id:
            created_keys.append(key)

        # Comments
        for c in jira.issue_comments(key)[:200]:
            author = (c.get("author") or {}).get("accountId")
            if author != account_id:
                continue
            c_created = _as_dt(c.get("created") or "").astimezone(timezone.utc)
            if c_created < since:
                continue
            body = c.get("body")
            excerpt = ""
            if isinstance(body, dict):
                excerpt = _extract_text_from_adf(body)
            elif isinstance(body, str):
                excerpt = (body or "").strip().replace("\n", " ")
                excerpt = excerpt[:240] + ("…" if len(excerpt) > 240 else "")
            commented.append(
                {
                    "issue": key,
                    "created": c.get("created"),
                    "excerpt": excerpt,
                }
            )

        # Changelog (status transitions + notable field changes)
        for h in jira.issue_changelog(key)[:200]:
            h_author = (h.get("author") or {}).get("accountId")
            if h_author != account_id:
                continue
            h_created = _as_dt(h.get("created") or "").astimezone(timezone.utc)
            if h_created < since:
                continue
            items = h.get("items") or []
            for item in items:
                field = item.get("field")
                if field == "status":
                    transitions.append(
                        {
                            "issue": key,
                            "at": h.get("created"),
                            "from": item.get("fromString"),
                            "to": item.get("toString"),
                        }
                    )
                else:
                    # Track a few common fields; otherwise keep minimal.
                    if field in {"assignee", "summary", "priority", "Fix Version", "labels", "sprint"}:
                        field_changes.append(
                            {
                                "issue": key,
                                "at": h.get("created"),
                                "field": field,
                                "from": item.get("fromString"),
                                "to": item.get("toString"),
                            }
                        )

    # Sort lists by time descending where possible.
    def _sort_key_created(x):
        try:
            return _as_dt(x.get("created") or x.get("at") or "").timestamp()
        except Exception:
            return 0

    commented = sorted(commented, key=_sort_key_created, reverse=True)[:50]
    transitions = sorted(transitions, key=_sort_key_created, reverse=True)[:50]
    field_changes = sorted(field_changes, key=_sort_key_created, reverse=True)[:50]

    return {
        "user": user,
        "since": since.isoformat(),
        "touched_projects": dict(sorted(touched_projects.items(), key=lambda kv: (-kv[1], kv[0]))),
        "issues_considered": issues,
        "created_issues": created_keys,
        "comments": commented,
        "transitions": transitions,
        "field_changes": field_changes,
    }


def _format_markdown_summary(payload: Dict) -> str:
    since = payload.get("since")
    users = payload.get("users") or []
    lines = [f"Jira user update summary (since {since})", ""]
    for u in users:
        user = (u.get("user") or {})
        name = user.get("displayName") or user.get("accountId") or "Unknown user"
        lines.append(f"## {name}")

        tp = u.get("touched_projects") or {}
        if tp:
            parts = [f"{k}: {v}" for k, v in list(tp.items())[:10]]
            lines.append(f"- Projects touched: {', '.join(parts)}")
        created = u.get("created_issues") or []
        if created:
            lines.append(f"- Created issues: {', '.join(created[:15])}" + ("…" if len(created) > 15 else ""))

        transitions = u.get("transitions") or []
        if transitions:
            lines.append("- Status changes:")
            for t in transitions[:10]:
                lines.append(f"  - {t.get('issue')}: {t.get('from')} → {t.get('to')} ({t.get('at')})")

        comments = u.get("comments") or []
        if comments:
            lines.append("- Recent comments:")
            for c in comments[:10]:
                excerpt = (c.get("excerpt") or "").strip()
                if excerpt:
                    lines.append(f"  - {c.get('issue')}: {excerpt}")
                else:
                    lines.append(f"  - {c.get('issue')}: (comment)")

        if not (tp or created or transitions or comments):
            lines.append("- No activity found in the sampled issue set.")

        lines.append("")
    return "\n".join(lines).strip() + "\n"


def jira_user_update_summary(request):
    """
    HTTP Cloud Function: Summarize Jira activity for specific users over a time window.

    Intended to use an Atlassian MCP server (preferred) or Jira REST (fallback).

    Request JSON (optional):
      - users: ["user@company.com", "<accountId>", ...]
      - since_days: 7
      - max_issues_per_user: 50

    Env vars:
      - ATLASSIAN_MCP_URL (recommended)
      - ATLASSIAN_MCP_BEARER_TOKEN or ATLASSIAN_MCP_HEADERS_JSON
      - JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN (REST fallback)
      - JIRA_USERS (default user list), JIRA_SINCE_DAYS, JIRA_MAX_ISSUES_PER_USER, JIRA_TIMEZONE
    """
    cfg = _load_jira_summary_config()
    jira = _JiraClient(cfg)

    body = request.get_json(silent=True) or {}
    users_in = body.get("users")
    if users_in is None:
        users_in = cfg.default_users
    if not users_in:
        return make_response("Provide users[] in JSON body or set JIRA_USERS env var", 400)

    since_days = int(body.get("since_days") or cfg.default_since_days)
    max_issues = int(body.get("max_issues_per_user") or cfg.max_issues_per_user)
    since_days = max(1, min(since_days, 90))
    max_issues = max(1, min(max_issues, 200))

    since = (_dt_floor_now_utc() - timedelta(days=since_days)).replace(microsecond=0)

    resolved_users: List[Dict] = []
    unresolved: List[str] = []
    for u in users_in:
        try:
            resolved = jira.user_lookup(str(u))
            if resolved and resolved.get("accountId"):
                resolved_users.append(resolved)
            else:
                unresolved.append(str(u))
        except Exception:
            unresolved.append(str(u))

    if not resolved_users:
        return make_response(
            jsonify(
                {
                    "error": "Could not resolve any users to Jira accountId",
                    "unresolved": unresolved,
                }
            ),
            400,
        )

    user_summaries = []
    for u in resolved_users:
        try:
            user_summaries.append(
                _summarize_user_activity(jira, user=u, since=since, max_issues=max_issues)
            )
        except Exception as exc:
            user_summaries.append({"user": u, "error": str(exc), "since": since.isoformat()})

    payload = {
        "since": since.isoformat(),
        "timezone": cfg.timezone,
        "users": user_summaries,
        "unresolved": unresolved,
    }
    payload["summary_markdown"] = _format_markdown_summary(payload)
    return jsonify(payload)
