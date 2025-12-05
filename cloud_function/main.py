import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

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
    webhook_url: str
    project_id: str
    sa_secret_name: str
    watch_collection: str
    processed_collection: str
    min_lease_seconds: int
    watch_ttl_seconds: int


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

    CONFIG = Config(
        target_users=users,
        sheet_id=sheet_id,
        sheet_range=os.environ.get("SHEET_RANGE", "OOO_Events!A:E"),
        webhook_url=webhook,
        project_id=project_id,
        sa_secret_name=secret_name,
        watch_collection=os.environ.get("WATCH_COLLECTION", "calendar_watches"),
        processed_collection=os.environ.get("PROCESSED_COLLECTION", "ooo_events"),
        min_lease_seconds=int(os.environ.get("MIN_LEASE_SECONDS", "3600")),
        watch_ttl_seconds=int(os.environ.get("WATCH_TTL_SECONDS", "604800")),
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
    for event in events:
        summary = (event.get("summary") or "").strip()
        if event.get("status") == "cancelled":
            LOGGER.debug("Skipping cancelled event %s", event.get("id"))
            continue
        if not summary.upper().startswith("OOO"):
            continue
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
