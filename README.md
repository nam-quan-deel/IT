# Google Calendar → Google Sheets OOO Automation

This repo contains a reference implementation for an automation that listens for
new Google Calendar events and mirrors "OOO" (out-of-office) entries into a
central Google Sheet that the whole team can reference.

## High-level flow

1. A Google Workspace service account (with domain-wide delegation) impersonates
   each teammate calendar and registers a **watch channel** that points to this
   app's HTTPS endpoint.
2. Google Calendar sends lightweight webhook notifications whenever a calendar
   changes.
3. The FastAPI app fetches only the delta of events via the Calendar API using
   stored sync tokens.
4. Events whose titles start with `OOO` are transformed into rows and appended
   to a worksheet inside the shared Google Sheet.

```
Google Calendar  ──▶ HTTPS webhook ──▶ FastAPI app ──▶ Google Sheets table
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/main.py` | FastAPI entrypoint for webhook handling |
| `src/calendar_client.py` | Google Calendar helper utilities |
| `src/event_processor.py` | Filters events and builds sheet rows |
| `src/sheets_client.py` | Google Sheets client for appending values |
| `src/state_store.py` | Lightweight JSON store for channels/sync tokens |
| `scripts/register_channels.py` | CLI to register/refresh watch channels |
| `state/` | Persisted channel + sync token data (git-kept as placeholder) |

## Environment variables

| Variable | Description |
| --- | --- |
| `GOOGLE_SERVICE_ACCOUNT_FILE` | Absolute path to the service account JSON file |
| `GOOGLE_IMPERSONATION_SUBJECT` | (Optional) User to impersonate when using domain-wide delegation |
| `WATCH_CALLBACK_URL` | Public HTTPS endpoint that receives Calendar push notifications |
| `TARGET_CALENDARS` | Comma-separated list of calendar IDs (usually emails) to monitor |
| `GOOGLE_SPREADSHEET_ID` | ID of the spreadsheet that aggregates OOO events |
| `GOOGLE_SHEET_NAME` | Worksheet/tab that stores the table (default `OOOEvents`) |
| `STATE_DIR` | Directory to persist sync tokens (default `/workspace/state`) |

Create a `.env` file or export them before running the app; `pydantic-settings`
will read from the environment automatically.

## Google Workspace setup

1. **Service account**  
   Create a service account within your Google Cloud project and download the
   JSON credential. Enable the *Google Calendar API* and *Google Sheets API* in
   the same project.
2. **Domain-wide delegation** (only if you need to access user calendars)  
   - In Google Admin → Security → API Controls → Domain-wide Delegation, add the
     service account client ID with the scopes:  
     `https://www.googleapis.com/auth/calendar.events.readonly`  
     `https://www.googleapis.com/auth/spreadsheets`
3. **Share target calendars**  
   - Ensure each teammate's calendar grants "See all event details" to the
     service account (or to the impersonated subject if using delegation).
4. **Watch endpoint**  
   - Deploy `src/main.py` (Cloud Run, Cloud Functions, Azure Container Apps,
     etc.) and expose `POST /notifications/google-calendar` over HTTPS.  
   - Set `WATCH_CALLBACK_URL` to that endpoint.
5. **Register channels**  
   - After exporting the env vars locally run:  
     ```
     pip install -r requirements.txt
     python scripts/register_channels.py
     ```
   - A JSON file under `state/` tracks channel IDs, resource IDs, and sync
     tokens. Re-run with `--force` when you need to renew channels (Google
     expires them ~every 7 days).

## Google Sheets setup

1. Create a new Google Sheet (e.g., `OOO-tracker`). Copy its Spreadsheet ID from
   the URL (`https://docs.google.com/spreadsheets/d/<spreadsheetId>/...`).
2. Rename or create a worksheet/tab (default `OOOEvents`). In row 1 add headers
   in this order:
   ```
   calendarId | eventId | summary | start | end | organizer | attendees | description | htmlLink
   ```
3. Share the sheet with the service account email so it can edit rows. If you
   use domain-wide delegation, ensure the impersonated subject also has access.
4. Set `GOOGLE_SPREADSHEET_ID` and (optionally) `GOOGLE_SHEET_NAME` to match.

## Running locally

```bash
pip install -r requirements.txt
uvicorn src.main:app --host 0.0.0.0 --port 8080 --reload
```

Expose the local server using `ngrok` or Cloud Run during development so Google
Calendar can reach it. Verify health at `GET /healthz`.

## Handling notifications

Google Calendar push notifications only indicate "something changed". The app:

1. Looks up the calendar ID from `X-Goog-Channel-Token`.
2. Reads the last stored sync token (if any).
3. Calls `events.list` with that token to fetch deltas.
4. Filters events whose summary starts with `OOO` (case-insensitive).
5. Appends each qualifying event as a new row in the configured Google Sheet.

## Testing & next steps

- Use the Google Calendar API's `events.insert` (or the Calendar UI) to create
  dummy `OOO` events and confirm that rows land in the sheet.
- Expand `event_processor.py` if you need richer logic (timezones, custom
  fields, deduplication, etc.).
- Swap the simple JSON `state_store` with Firestore, DynamoDB, or any other
  resilient storage if you deploy this in production.

Happy automating! 🎉
