"""Register Google Calendar watch channels for all target calendars."""

from __future__ import annotations

import argparse
import logging

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.calendar_client import watch_calendar
from src.config import get_settings
from src.state_store import StateStore

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register webhook channels for calendars.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Always register a new channel even if one exists in state.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    settings = get_settings()
    store = StateStore(settings.state_dir)
    existing = store.get_channels()

    for calendar_id in settings.target_calendars:
        if existing.get(calendar_id) and not args.force:
            LOGGER.info("Channel already exists for %s; skipping", calendar_id)
            continue
        channel_data = watch_calendar(calendar_id)
        store.save_channel(calendar_id, channel_data)
        LOGGER.info("Registered channel %s for %s", channel_data["channelId"], calendar_id)


if __name__ == "__main__":
    main()

