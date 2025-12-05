# Calendar OOO Automation

Automation blueprint for capturing Out-of-Office (OOO) events from specific Google Calendar users and logging each qualifying event into a central Google Sheet by way of Google Cloud Functions.

## Architecture
- `register_channels`: HTTP Cloud Function (triggered by Cloud Scheduler) that ensures Calendar push channels are alive for each target user via the Calendar `events.watch` API.
- `calendar_webhook`: HTTP Cloud Function that receives push notifications, pulls incremental changes with sync tokens, filters for events whose summaries start with `OOO`, and appends a row to Sheets.
- `healthcheck`: lightweight endpoint you can point uptime checks at or use to keep the container warm.

Supporting services:
- **Secret Manager** stores the JSON key for a service account that has domain-wide delegation.
- **Firestore** keeps channel metadata (`calendar_watches` collection) and deduplicates processed events (`ooo_events` collection).
- **Google Sheets** receives appended rows with `user_email`, `summary`, `start`, `end`, `htmlLink`.

## Prerequisites
1. Enable APIs: Cloud Functions, Cloud Build, Secret Manager, Firestore, Google Calendar, Google Sheets.
2. Create a service account, grant it `roles/secretmanager.secretAccessor`, `roles/datastore.user`, and enable [domain-wide delegation](https://support.google.com/a/answer/162106). Add the Calendar & Sheets scopes to the Admin console.
3. Upload the service-account JSON into Secret Manager (e.g., secret name `calendar-ooo-sa`).
4. Initialize Firestore in native mode within the same project.
5. Create/identify a Google Sheet and note its spreadsheet ID and target range (default `OOO_Events!A:E`).
6. Decide on the list of user emails whose primary calendars should be monitored.

## Deploying the Functions (2nd gen)
```bash
gcloud functions deploy register_channels \
  --gen2 --runtime python311 --region=us-central1 \
  --entry-point register_channels --trigger-http \
  --set-env-vars="SA_SECRET_NAME=calendar-ooo-sa" \
  --set-env-vars="TARGET_USERS=user1@acme.com,user2@acme.com" \
  --set-env-vars="SHEET_ID=1abc...,SHEET_RANGE=OOO_Events!A:E" \
  --set-env-vars="CALENDAR_WEBHOOK_URL=https://REGION-PROJECT.cloudfunctions.net/calendar_webhook" \
  --set-env-vars="WATCH_COLLECTION=calendar_watches,PROCESSED_COLLECTION=ooo_events" \
  --allow-unauthenticated \
  --source=cloud_function

gcloud functions deploy calendar_webhook \
  --gen2 --runtime python311 --region=us-central1 \
  --entry-point calendar_webhook --trigger-http \
  --set-env-vars="SA_SECRET_NAME=calendar-ooo-sa" \
  --set-env-vars="TARGET_USERS=user1@acme.com,user2@acme.com" \
  --set-env-vars="SHEET_ID=1abc...,SHEET_RANGE=OOO_Events!A:E" \
  --set-env-vars="CALENDAR_WEBHOOK_URL=https://REGION-PROJECT.cloudfunctions.net/calendar_webhook" \
  --set-env-vars="WATCH_COLLECTION=calendar_watches,PROCESSED_COLLECTION=ooo_events" \
  --allow-unauthenticated \
  --source=cloud_function
```

Create a Cloud Scheduler job (e.g., every 30 minutes) that hits `register_channels` with an authenticated HTTP request so expiring channels are renewed before Google’s lease lapses. Point the Calendar `events.watch` callback (`CALENDAR_WEBHOOK_URL`) to the deployed `calendar_webhook` URL.

## Runtime Behavior
1. Scheduler call keeps Calendar watch channels alive and persists the channel metadata + sync tokens in Firestore.
2. Calendar pushes a notification whenever one of the watched calendars changes.
3. `calendar_webhook` resolves the user/channel, requests incremental changes with the stored sync token, and filters to events whose summary starts with `OOO`.
4. Deduplicated events are appended to Google Sheets via the Sheets API and recorded in the `ooo_events` collection.

## Local Testing
```bash
pip install -r cloud_function/requirements.txt
functions-framework --target=register_channels --debug
```

Populate the needed environment variables locally (Target users, sheet IDs, webhook URL, etc.) and use `ngrok` to expose the webhook if you want to receive real Calendar notifications while developing.