"""Доплата за увеличение лимита устройств до конца текущей подписки."""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone

from tariff_resolve import SELF_DEVICES_MAX, effective_user_devices

ADD_DEVICES_RUB_PER_DEVICE_MONTH = 50
_ADD_DEVICES_RE = re.compile(r"^add_(\d+)_devices$")


def parse_add_devices_duration(duration: str) -> int | None:
    m = _ADD_DEVICES_RE.fullmatch((duration or "").strip())
    if not m:
        return None
    n = int(m.group(1))
    if n < 1 or n > 5:
        return None
    return n


def billable_months_until_end(end_dt: datetime, *, now: datetime | None = None) -> int:
    """Округление вверх по 30 дней; минимум 1 месяц (15 дн. → 1, 2 мес. 10 дн. → 3)."""
    if now is None:
        now = datetime.now(timezone.utc)
    if end_dt.tzinfo is None:
        end = end_dt.replace(tzinfo=timezone.utc)
    else:
        end = end_dt.astimezone(timezone.utc)
    now_a = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    days = max((end - now_a).total_seconds() / 86400.0, 0.0)
    return max(1, int(math.ceil(days / 30)))


def add_devices_price_rub(add_count: int, end_dt: datetime) -> int:
    months = billable_months_until_end(end_dt)
    return int(add_count) * ADD_DEVICES_RUB_PER_DEVICE_MONTH * months


def max_add_devices_available(current_limit: int) -> int:
    return max(0, min(5, SELF_DEVICES_MAX - int(current_limit)))


def add_devices_duration_payload(add_count: int) -> str:
    return f"add_{int(add_count)}_devices"


async def current_main_device_limit(
    x3,
    uid: int,
    user,
    *,
    panel_username: str | None = None,
) -> int:
    from tariff_resolve import panel_username_for_site_user

    candidates: list[str] = []
    if panel_username and str(panel_username).strip():
        candidates.append(str(panel_username).strip())
    candidates.append(str(uid))
    if int(uid) < 0:
        candidates.append(panel_username_for_site_user(int(uid), white=False, device_slots=5))

    seen: set[str] = set()
    for username in candidates:
        if username in seen:
            continue
        seen.add(username)
        resp = await x3.get_user_by_username(username)
        panel_user = x3._panel_user_from_response(resp)
        if panel_user and x3._panel_user_is_active(panel_user):
            raw = panel_user.get("hwidDeviceLimit")
            if raw is not None:
                try:
                    return int(raw)
                except (TypeError, ValueError):
                    pass
    return effective_user_devices(getattr(user, "devices", None))
