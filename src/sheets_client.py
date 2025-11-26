"""Google Sheets helper for appending OOO rows."""

from __future__ import annotations

import logging
from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build

from .config import get_settings

SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
LOGGER = logging.getLogger(__name__)


def build_sheets_service() -> Resource:
    """Return a Google Sheets API service bound to the service account."""

    settings = get_settings()
    credentials = service_account.Credentials.from_service_account_file(
        str(settings.google_service_account_file),
        scopes=SHEETS_SCOPE,
    )
    if settings.google_impersonation_subject:
        credentials = credentials.with_subject(settings.google_impersonation_subject)

    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


class SheetsClient:
    """Thin wrapper around the Sheets API for appending values."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._service = build_sheets_service()

    def append_row(self, row_values: List[str]) -> None:
        LOGGER.debug("Appending row to sheet %s", self.settings.google_sheet_name)
        body = {"values": [row_values]}
        self._service.spreadsheets().values().append(
            spreadsheetId=self.settings.google_spreadsheet_id,
            range=f"{self.settings.google_sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

