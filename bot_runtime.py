"""Момент старта polling — отсечение /start из очереди после рестарта."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from aiogram.types import Message

# Когда последний раз подняли long polling (UTC).
_polling_started_at: Optional[datetime] = None

# Сообщения /start старше этого окна до старта polling — backlog, не обрабатываем.
_BACKLOG_GRACE_BEFORE_START_SEC = 15


def mark_polling_started() -> None:
    global _polling_started_at
    _polling_started_at = datetime.now(timezone.utc)


def polling_started_at_utc() -> Optional[datetime]:
    return _polling_started_at


def start_message_is_backlog(message: Message) -> bool:
    """True — апдейт из хвоста очереди (до рестарта), его можно игнорировать."""
    started = _polling_started_at
    if started is None or message.date is None:
        return False
    msg_dt = message.date
    if msg_dt.tzinfo is None:
        msg_dt = msg_dt.replace(tzinfo=timezone.utc)
    else:
        msg_dt = msg_dt.astimezone(timezone.utc)
    cutoff = started - timedelta(seconds=_BACKLOG_GRACE_BEFORE_START_SEC)
    return msg_dt < cutoff
