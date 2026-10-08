"""Цена с учётом резерва скидки колеса."""
from __future__ import annotations

import re

from bot import sql
from config import ADMIN_IDS
from config_bd.utils import pro_subscription_end_active
from services.wheel_discount import CheckoutQuote, get_checkout_quote, KIND_GIFT, KIND_SUB
from tariff_resolve import DEFAULT_DEVICE_SLOTS, SELF_DEVICES_MAX, effective_user_devices

ADMIN_TEST_RUB = 1
ADMIN_FK_TEST_RUB = 10
FK_PAYLOAD_METHODS = frozenset({"fk_qr_sbp", "fk_qr_card", "fksbp", "fk_sbp", "fk_card", "sbp", "card"})

_WHEEL_CHECKOUT_TEMPLATE_RE = re.compile(r"^m(\d+)_d5$")


async def wheel_checkout_self_devices(user_id: int) -> tuple[int, bool]:
    """
    Число устройств для покупки «себе» в мини-апп колеса:
    активная subscription_end_date → devices из БД, иначе 5.
    """
    user = await sql.get_user_object_by_user_id(user_id)
    active = bool(user and pro_subscription_end_active(user.subscription_end_date))
    if active:
        return effective_user_devices(user.devices), True
    return DEFAULT_DEVICE_SLOTS, False


def wheel_resolve_self_tariff_key(template_key: str, device_slots: int) -> str:
    """m1_d5 / m3_d5 из UI → m{N}_d{устройства}."""
    key = (template_key or "").strip()
    m = _WHEEL_CHECKOUT_TEMPLATE_RE.fullmatch(key)
    if not m:
        return key
    months = int(m.group(1))
    d = max(DEFAULT_DEVICE_SLOTS, min(SELF_DEVICES_MAX, int(device_slots)))
    return f"m{months}_d{d}"


async def wheel_checkout_purchase_menu(user_id: int) -> dict[str, int | bool]:
    devices, active = await wheel_checkout_self_devices(user_id)
    return {
        "main_subscription_active": active,
        "device_slots": devices,
        "device_slots_gift": DEFAULT_DEVICE_SLOTS,
    }


async def quote_subscription(user_id: int, duration_key: str) -> CheckoutQuote:
    devices, _ = await wheel_checkout_self_devices(user_id)
    tariff_key = wheel_resolve_self_tariff_key(duration_key, devices)
    return await get_checkout_quote(user_id, kind=KIND_SUB, product_key=tariff_key)


async def quote_gift(user_id: int, duration_key: str) -> CheckoutQuote:
    return await get_checkout_quote(user_id, kind=KIND_GIFT, product_key=duration_key)


async def bot_tariff_checkout_quote(
    user_id: int,
    *,
    gift: bool,
    desc_key: str,
) -> CheckoutQuote:
    """Цена тарифа в боте с учётом активной скидки колеса."""
    key = (desc_key or "").strip()
    if gift:
        quote = await quote_gift(user_id, key)
    elif _WHEEL_CHECKOUT_TEMPLATE_RE.fullmatch(key):
        quote = await quote_subscription(user_id, key)
    else:
        quote = await get_checkout_quote(user_id, kind=KIND_SUB, product_key=key)
    return apply_admin_platega_test_price(user_id, quote)


def apply_admin_platega_test_price(user_id: int, quote: CheckoutQuote) -> CheckoutQuote:
    """10 ₽ для теста FreeKassa (СБП/карта); wdpct сохраняем."""
    if user_id not in ADMIN_IDS:
        return quote
    return CheckoutQuote(
        base_rub=quote.base_rub,
        final_rub=ADMIN_FK_TEST_RUB,
        base_stars=quote.base_stars,
        final_stars=quote.final_stars,
        percent=quote.percent,
        token=quote.token,
        payload_suffix=quote.payload_suffix,
    )


def admin_test_payment_ok(payer_user_id: int, paid: int, method: str) -> bool:
    if int(payer_user_id) not in ADMIN_IDS:
        return False
    if method in FK_PAYLOAD_METHODS:
        return paid == ADMIN_FK_TEST_RUB
    return paid == ADMIN_TEST_RUB
