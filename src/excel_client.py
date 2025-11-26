"""Minimal Microsoft Graph Excel client."""

from __future__ import annotations

import httpx
from msal import ConfidentialClientApplication

from .config import get_settings

GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]


class ExcelClient:
    """Wrapper around Microsoft Graph to append rows to a table."""

    def __init__(self):
        self.settings = get_settings()
        self._app = ConfidentialClientApplication(
            client_id=self.settings.ms_client_id,
            client_credential=self.settings.ms_client_secret,
            authority=f"https://login.microsoftonline.com/{self.settings.ms_tenant_id}",
        )

    def _get_access_token(self) -> str:
        result = self._app.acquire_token_silent(GRAPH_SCOPE, account=None)
        if not result:
            result = self._app.acquire_token_for_client(scopes=GRAPH_SCOPE)
        if "access_token" not in result:
            raise RuntimeError(f"Failed to acquire Microsoft Graph token: {result}")
        return result["access_token"]

    def append_row(self, row_values: list[str]) -> None:
        token = self._get_access_token()
        url = (
            "https://graph.microsoft.com/v1.0/"
            f"drives/{self.settings.excel_drive_id}/"
            f"items/{self.settings.excel_item_id}/"
            f"workbook/tables/{self.settings.excel_table_name}/rows/add"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {"values": [row_values]}
        response = httpx.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()

