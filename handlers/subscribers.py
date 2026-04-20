"""
handlers/subscribers.py — Subscriber list management.

Any user who sends a message or command to the bot is added to
subscribers.json so they receive the automated monthly report.
"""

import json
import logging

from config import SUBSCRIBERS_FILE

logger = logging.getLogger(__name__)


def track_subscriber(chat_id: int) -> None:
    """Add chat_id to subscribers.json if not already present."""
    try:
        with open(SUBSCRIBERS_FILE) as f:
            subs: list[int] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        subs = []

    if chat_id not in subs:
        subs.append(chat_id)
        with open(SUBSCRIBERS_FILE, "w") as f:
            json.dump(subs, f, indent=2)
        logger.info(f"New subscriber registered: {chat_id}")
