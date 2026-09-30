"""Цена и описание тарифа по ключу callback (в т.ч. new_*), число дней для панели X3."""
from __future__ import annotations

import re
from typing import Tuple

from lexicon import dct_desc, dct_desc_friends, dct_price, dct_price_friends

# Ключи m{месяцы}_d{устройства}: дни для панели (как у старых r_30 / r_90 / …).
_MONTHS_TO_DAYS = {1: 30, 3: 90, 6: 180, 12: 365}

# Лимит устройств по умолчанию для старых тарифов без суффикса _d* в колбеке.
DEFAULT_DEVICE_SLOTS = 5

SELF_SUBSCRIPTION_MONTHS = (1, 3)
SELF_DEVICES_MIN = 5
SELF_DEVICES_MAX = 15
SELF_BASE_PRICE_RUB = {1: 299, 3: 749}
# Доплата за каждое устройство сверх 5: +50 ₽ (1 мес.) или +120 ₽ (3 мес., 40×3).
SELF_EXTRA_DEVICE_RUB = {1: 50, 3: 120}


def subscription_price_rub(months: int, devices: int) -> int:
    """Цена PRO-подписки на основном аккаунте (5–15 устройств, 1 или 3 месяца)."""
    m = int(months)
    d = max(SELF_DEVICES_MIN, min(SELF_DEVICES_MAX, int(devices)))
    base = SELF_BASE_PRICE_RUB.get(m)
    if base is None:
        return 0
    extra = max(0, d - SELF_DEVICES_MIN) * SELF_EXTRA_DEVICE_RUB.get(m, 0)
    return base + extra


def extra_device_step_rub(months: int) -> int:
    return SELF_EXTRA_DEVICE_RUB.get(int(months), 0)


def effective_user_devices(stored: int | None) -> int:
    """Число устройств для продления: из БД или 5 по умолчанию."""
    if stored is None or int(stored) < SELF_DEVICES_MIN:
        return SELF_DEVICES_MIN
    return min(SELF_DEVICES_MAX, int(stored))


def is_main_variable_device_slots(device_slots: int) -> bool:
    return SELF_DEVICES_MIN <= int(device_slots) <= SELF_DEVICES_MAX


def bot_tariff_purchase_blocked_reason(duration_plain: str) -> str | None:
    """None — тариф доступен в боте; иначе текст для show_alert."""
    from lexicon import lexicon

    key = (duration_plain or "").strip()
    if key in ("5000", "5000sale"):
        return lexicon["tariff_discontinued"]
    m120 = re.fullmatch(r"120_d(\d+)", key)
    if m120 and int(m120.group(1)) != 5:
        return lexicon["tariff_legacy_slots"]
    if m120:
        return None
    m = re.fullmatch(r"m(\d+)_d(\d+)", key)
    if not m:
        return None
    months, devices = int(m.group(1)), int(m.group(2))
    if devices in (3, 10):
        return lexicon["tariff_legacy_slots"]
    if months not in SELF_SUBSCRIPTION_MONTHS:
        return lexicon["tariff_discontinued"]
    if not is_main_variable_device_slots(devices):
        return lexicon["tariff_discontinued"]
    return None


def panel_username(user_id: int, *, white: bool, device_slots: int) -> str:
    """Username в панели: white → «id_white», legacy 3/10 → id_3/id_10, иначе основной id."""
    if white:
        return f"{user_id}_white"
    if device_slots == 3:
        return f"{user_id}_3"
    if device_slots == 10:
        return f"{user_id}_10"
    return str(user_id)


def gift_panel_username(gift_num: int, *, white: bool, device_slots: int) -> str:
    """Username в панели для веб-подарка: gift_N, gift_N_3, gift_N_10, gift_N_white."""
    base = f"gift_{gift_num}"
    if white:
        return f"{base}_white"
    if device_slots == 3:
        return f"{base}_3"
    if device_slots == 10:
        return f"{base}_10"
    return base


def panel_username_for_site_user(
    db_user_id: int,
    *,
    white: bool,
    device_slots: int = 5,
) -> str:
    """Username в панели для email-аккаунта (отрицательный user_id в БД)."""
    n = int(db_user_id)
    if n > 0:
        return panel_username(n, white=white, device_slots=device_slots)
    base = str(n)
    if len(base) < 3:
        base = f"n{n}"
    if white:
        return f"{base}_white"
    if device_slots == 3:
        return f"{base}_3"
    if device_slots == 10:
        return f"{base}_10"
    return base


def device_from_tariff_key(duration_key_plain: str) -> int:
    """
    Число устройств из ключа m{N}_d{D} или 120_d{D};
    для legacy-ключей (30, 90, white_30, …) — DEFAULT_DEVICE_SLOTS.
    """
    m = re.fullmatch(r"m\d+_d(\d+)", duration_key_plain)
    if m:
        return int(m.group(1))
    m120 = re.fullmatch(r"120_d(\d+)", duration_key_plain)
    if m120:
        return int(m120.group(1))
    return DEFAULT_DEVICE_SLOTS


def tariff_rub_and_desc(duration_key: str) -> Tuple[int, str]:
    if duration_key in dct_price_friends:
        return dct_price_friends[duration_key], dct_desc_friends[duration_key]
    if duration_key in dct_price:
        return dct_price[duration_key], dct_desc[duration_key]
    m = re.fullmatch(r"m(\d+)_d(\d+)", duration_key)
    if m:
        months, devices = int(m.group(1)), int(m.group(2))
        if months in SELF_BASE_PRICE_RUB and SELF_DEVICES_MIN <= devices <= SELF_DEVICES_MAX:
            price = subscription_price_rub(months, devices)
            label = f"{months} мес., {devices} устр. — {price} ₽"
            return price, label
    raise KeyError(duration_key)


def tariff_days_for_x3(duration_key_plain: str) -> int:
    """
    Ключ без префикса white_ (уже отрезан при необходимости).
    Примеры: '7', '30', 'new_7', 'new_3000', 'm1_d3'.
    """
    if duration_key_plain.startswith("new_"):
        if duration_key_plain == "new_3000":
            return 3000
        return int(duration_key_plain.replace("new_", "", 1))
    if duration_key_plain in ("5000", "5000sale"):
        return 5000
    if re.fullmatch(r"120_d\d+", duration_key_plain):
        return 120
    m_md = re.fullmatch(r"m(\d+)_d(\d+)", duration_key_plain)
    if m_md:
        months = int(m_md.group(1))
        return _MONTHS_TO_DAYS.get(months, 30 * months)
    return int(duration_key_plain)
