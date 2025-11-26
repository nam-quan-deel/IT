# Google Calendar → Excel OOO Automation

This repo contains a reference implementation for an automation that listens for
new Google Calendar events and mirrors "OOO" (out-of-office) entries into a
central Excel workbook (for example, in OneDrive/SharePoint).

## High-level flow

1. A Google Workspace service account (with domain-wide delegation) impersonates
   each teammate calendar and registers a **watch channel** that points to this
   app's HTTPS endpoint.
2. Google Calendar sends lightweight webhook notifications whenever a calendar
   changes.
3. The FastAPI app fetches only the delta of events via the Calendar API using
   stored sync tokens.
4. Events whose titles start with `OOO` are transformed into rows and appended
   to an Excel table through the Microsoft Graph API.

```
Google Calendar  ──▶ HTTPS webhook ──▶ FastAPI app ──▶ Microsoft Graph ──▶ Excel table
```

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/main.py` | FastAPI entrypoint for webhook handling |
| `src/calendar_client.py` | Google Calendar helper utilities |
| `src/event_processor.py` | Filters events and builds Excel rows |
| `src/excel_client.py` | Microsoft Graph client for Excel updates |
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
| `MS_CLIENT_ID` / `MS_CLIENT_SECRET` / `MS_TENANT_ID` | Azure AD app registration for Microsoft Graph |
| `EXCEL_DRIVE_ID` / `EXCEL_ITEM_ID` | Drive + item IDs of the workbook (see Graph `driveItem` response) |
| `EXCEL_TABLE_NAME` | Excel table to append rows to (default: `OOOEvents`) |
| `STATE_DIR` | Directory to persist sync tokens (default: `/workspace/state`) |

Create a `.env` file or export them before running the app; `pydantic-settings`
will read from the environment automatically.

## Google Workspace setup

1. **Service account**  
   Create a service account within your Google Cloud project and download the
   JSON credential. Enable the *Google Calendar API* in the same project.
2. **Domain-wide delegation** (if you need to access user calendars)  
   - In Google Admin → Security → API Controls → Domain-wide Delegation, add the
     service account client ID with the scope
     `https://www.googleapis.com/auth/calendar.events.readonly`.
3. **Watch endpoint**  
   - Deploy `src/main.py` (e.g., Cloud Run, Cloud Functions, Azure Container
     Apps, etc.) and expose `POST /notifications/google-calendar` over HTTPS.
   - Set `WATCH_CALLBACK_URL` to that endpoint.
4. **Register channels**  
   - After exporting the env vars locally run:
     ```
     pip install -r requirements.txt
     python scripts/register_channels.py
     ```
   - A JSON file under `state/` tracks channel IDs, resource IDs, and sync
     tokens. Re-run with `--force` when you need to renew channels (Google
     expires them ~every 7 days).

## Microsoft 365 setup

1. Create an Azure AD app registration with **client credentials**.
2. Grant the application the `Files.ReadWrite.All` permission (application)
   and **admin consent**.
3. Store the target workbook (e.g., `OOO-report.xlsx`) in OneDrive or
   SharePoint and create an Excel table (Insert ▶ Table) named `OOOEvents`
   that has the following columns (order matters):
   ```
   calendarId | eventId | summary | start | end | organizer | attendees | description | htmlLink
   ```
4. Use Microsoft Graph Explorer (or `GET /me/drive/root:/path:/`) to capture
   the workbook's `driveId` and `id`, then set `EXCEL_DRIVE_ID` and
   `EXCEL_ITEM_ID` accordingly.

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
5. Appends each qualifying event as a new row in Excel via Microsoft Graph.

## Testing & next steps

- Use the Google Calendar API's `events.insert` to create dummy `OOO` events
  and confirm that rows land in Excel.
- Expand `event_processor.py` if you need richer logic (timezones, custom
  fields, deduplication, etc.).
- Swap the simple JSON `state_store` with Firestore, DynamoDB, or any other
  resilient storage if you deploy this in production.

Happy automating! 🎉
