"""Centralized configuration loading for the automation service."""

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Environment-driven settings."""

    google_service_account_file: Path = Field(
        ...,
        alias="GOOGLE_SERVICE_ACCOUNT_FILE",
        description="Path to the Google service account JSON credential.",
    )
    google_impersonation_subject: Optional[str] = Field(
        None,
        alias="GOOGLE_IMPERSONATION_SUBJECT",
        description="User to impersonate when using domain-wide delegation.",
    )
    watch_callback_url: str = Field(
        ...,
        alias="WATCH_CALLBACK_URL",
        description="Public HTTPS endpoint that receives Google Calendar push notifications.",
    )
    target_calendars: List[str] = Field(
        ...,
        alias="TARGET_CALENDARS",
        description="Comma-separated list of calendar IDs to watch.",
    )
    excel_drive_id: str = Field(
        ...,
        alias="EXCEL_DRIVE_ID",
        description="Microsoft 365 drive ID where the workbook lives.",
    )
    excel_item_id: str = Field(
        ...,
        alias="EXCEL_ITEM_ID",
        description="Microsoft 365 drive item ID for the workbook.",
    )
    excel_table_name: str = Field(
        "OOOEvents",
        alias="EXCEL_TABLE_NAME",
        description="Name of the Excel table that stores OOO rows.",
    )
    ms_client_id: str = Field(..., alias="MS_CLIENT_ID")
    ms_client_secret: str = Field(..., alias="MS_CLIENT_SECRET")
    ms_tenant_id: str = Field(..., alias="MS_TENANT_ID")
    state_dir: Path = Field(
        Path("/workspace/state"),
        alias="STATE_DIR",
        description="Directory for persisting channel and sync tokens.",
    )

    @validator("target_calendars", pre=True)
    def split_calendars(cls, value: str) -> List[str]:  # type: ignore[override]
        return [item.strip() for item in value.split(",") if item.strip()]

    @validator("google_service_account_file", "state_dir", pre=True)
    def expand_path(cls, value: str) -> Path:  # type: ignore[override]
        return Path(value).expanduser().resolve()


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""

    return Settings()  # type: ignore[call-arg]

