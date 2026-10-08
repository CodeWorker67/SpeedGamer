"""Скидки колеса: активация при выборе, оплата по активной сессии (мини-апп)."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from bot import sql
from lexicon import dct_desc, dct_price, dct_price_friends
from logging_config import logger
from tariff_resolve import tariff_days_for_x3, tariff_rub_and_desc
from wl_traffic.service import parse_traffic_duration

CHECKOUT_TTL_MINUTES = 30

KIND_SUB = "sub"
KIND_GIFT = "gift"
KIND_TRAFFIC = "traffic"

_STARS_PRICES = {
    "7": 99,
    "30": 199,
    "90": 369,
    "180": 699,
    "m1_d5": 299,
    "m3_d5": 749,
}


def normalize_tariff_duration_key(duration_key: str) -> str:
    return (duration_key or "").strip()


@dataclass
class WheelDiscountCounts:
    discount_10: int
    discount_30: int
    discount_50: int


@dataclass
class CheckoutQuote:
    base_rub: int
    final_rub: int
    base_stars: int
    final_stars: int
    percent: int
    token: Optional[str]
    payload_suffix: str


async def wheel_discount_counts(user_id: int) -> WheelDiscountCounts:
    row = await sql.get_wheel_fortuna(user_id)
    if not row:
        return WheelDiscountCounts(0, 0, 0)
    return WheelDiscountCounts(
        int(row.discount_10 or 0),
        int(row.discount_30 or 0),
        int(row.discount_50 or 0),
    )


def wheel_discount_has_any(counts: WheelDiscountCounts) -> bool:
    return bool(counts.discount_10 or counts.discount_30 or counts.discount_50)


def _discounted_amount(base: int, percent: int) -> int:
    if percent <= 0:
        return base
    return max(1, int(round(base * (100 - percent) / 100)))


async def base_rub_for_product(user_id: int, kind: str, product_key: str) -> int:
    key = normalize_tariff_duration_key(product_key)
    if kind == KIND_TRAFFIC:
        from wl_traffic.constants import WL_TRAFFIC_TARIFFS

        return int(WL_TRAFFIC_TARIFFS.get(key, 0))
    try:
        rub, _ = tariff_rub_and_desc(key)
        return int(rub)
    except KeyError:
        pass
    if key in dct_price_friends:
        return int(dct_price_friends[key])
    if key in dct_price:
        return int(dct_price[key])
    return 0


def base_stars_for_product(kind: str, product_key: str) -> int:
    key = normalize_tariff_duration_key(product_key)
    if kind == KIND_TRAFFIC:
        from wl_traffic.constants import WL_TRAFFIC_TARIFFS

        return int(WL_TRAFFIC_TARIFFS.get(key, 0))
    try:
        rub, _ = tariff_rub_and_desc(key)
        return int(rub)
    except KeyError:
        pass
    return int(_STARS_PRICES.get(key, 0))


def product_duration_label(kind: str, product_key: str) -> str:
    key = normalize_tariff_duration_key(product_key)
    m = re.fullmatch(r"m(\d+)_d(\d+)", key)
    if m:
        months, devices = int(m.group(1)), int(m.group(2))
        return f"{months} мес., {devices} устр."
    if key.startswith("120_d"):
        return "4 месяца (акция 3+1)"
    try:
        days = int(tariff_days_for_x3(key))
        return f"{days} дней"
    except (TypeError, ValueError, KeyError):
        return key


def duration_days_for_product(product_key: str) -> int:
    key = normalize_tariff_duration_key(product_key)
    if key.startswith("120_d"):
        return 120
    m = re.fullmatch(r"m(\d+)_d(\d+)", key)
    if m:
        return int(tariff_days_for_x3(key))
    try:
        return int(key)
    except ValueError:
        return 0


async def format_wheel_discount_tariff_inline(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    quote = await get_checkout_quote(user_id, kind=kind, product_key=product_key)
    price = int(quote.final_rub)
    if kind == KIND_TRAFFIC:
        return lexicon["wheel_discount_tariff_traffic"].format(
            gb=product_key,
            price=price,
        )
    duration = product_duration_label(kind, product_key)
    if kind == KIND_GIFT:
        return lexicon["wheel_discount_tariff_gift_inline"].format(
            duration=duration,
            price=price,
        )
    return lexicon["wheel_discount_tariff_sub_inline"].format(
        duration=duration,
        price=price,
    )


async def format_wheel_discount_tariff_line(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    return await format_wheel_discount_tariff_inline(user_id, kind, product_key)


async def build_standard_checkout_text(
    user_id: int,
    kind: str,
    product_key: str,
) -> str:
    from lexicon import payment_tariff_summary_pro, self_payment_method_caption

    key = normalize_tariff_duration_key(product_key)
    quote = await get_checkout_quote(user_id, kind=kind, product_key=key)
    m = re.fullmatch(r"m(\d+)_d(\d+)", key)
    if m and kind in (KIND_SUB, KIND_GIFT):
        months, devices = int(m.group(1)), int(m.group(2))
        return self_payment_method_caption(months, devices, int(quote.final_rub))
    summary = payment_tariff_summary_pro(key)
    if kind == KIND_GIFT:
        return summary
    return summary


def _payload_suffix(kind: str, product_key: str, percent: int, base_rub: int) -> str:
    return f",wdpct:{percent},wdkind:{kind},wdprod:{product_key},wdbase:{base_rub}"


async def activate_wheel_discount(
    user_id: int,
    *,
    kind: str,
    product_key: str,
    percent: int,
) -> bool:
    if percent not in (10, 30, 50):
        return False
    await sql.wheel_discount_checkout_expire()
    base_rub = await base_rub_for_product(user_id, kind, product_key)
    base_stars = base_stars_for_product(kind, product_key)
    final_rub = _discounted_amount(base_rub, percent)
    final_stars = _discounted_amount(base_stars, percent)
    expires = datetime.now() + timedelta(minutes=CHECKOUT_TTL_MINUTES)
    return await sql.wheel_discount_checkout_activate(
        user_id,
        kind=kind,
        product_key=product_key,
        percent=percent,
        base_rub=base_rub,
        final_rub=final_rub,
        base_stars=base_stars,
        final_stars=final_stars,
        expires_at=expires,
    )


async def clear_wheel_discount_checkout(user_id: int) -> None:
    await sql.wheel_discount_checkout_expire()
    await sql.wheel_discount_checkout_clear(user_id)


async def get_checkout_quote(
    user_id: int,
    *,
    kind: str,
    product_key: str,
) -> CheckoutQuote:
    await sql.wheel_discount_checkout_expire()
    base_rub = await base_rub_for_product(user_id, kind, product_key)
    base_stars = base_stars_for_product(kind, product_key)
    active = await sql.wheel_discount_checkout_get(user_id)
    if (
        active
        and str(active.kind) == kind
        and str(active.product_key) == product_key
        and int(active.percent) in (10, 30, 50)
    ):
        pct = int(active.percent)
        return CheckoutQuote(
            base_rub=int(active.base_rub),
            final_rub=int(active.final_rub),
            base_stars=int(active.base_stars),
            final_stars=int(active.final_stars),
            percent=pct,
            token=None,
            payload_suffix=_payload_suffix(kind, product_key, pct, int(active.base_rub)),
        )
    return CheckoutQuote(
        base_rub=base_rub,
        final_rub=base_rub,
        base_stars=base_stars,
        final_stars=base_stars,
        percent=0,
        token=None,
        payload_suffix="",
    )


def _payment_ref(payload: str, transaction_id: Optional[str]) -> str:
    if transaction_id:
        return f"tx:{transaction_id}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"pl:{digest}"


def _payload_matches_product(
    payload_parts: dict[str, str],
    *,
    kind: str,
    product_key: str,
) -> bool:
    raw_duration = str(payload_parts.get("duration", "") or "")
    is_gift = payload_parts.get("gift", "False") == "True"
    traffic_gb = parse_traffic_duration(raw_duration)

    key = normalize_tariff_duration_key(product_key)
    if kind == KIND_TRAFFIC:
        return traffic_gb is not None and str(traffic_gb) == str(key) and not is_gift
    if kind == KIND_GIFT:
        if not is_gift:
            return False
        return _duration_matches_product_key(raw_duration, key)
    if kind == KIND_SUB:
        if is_gift or traffic_gb is not None:
            return False
        return _duration_matches_product_key(raw_duration, key)
    return False


def _duration_matches_product_key(raw_duration: str, product_key: str) -> bool:
    prod = normalize_tariff_duration_key(product_key)
    dur = normalize_tariff_duration_key(raw_duration)
    if dur == prod:
        return True
    try:
        return int(raw_duration) == int(tariff_days_for_x3(prod))
    except (TypeError, ValueError, KeyError):
        return False


def _checkout_from_payload(payload_parts: dict[str, str]) -> tuple[str, str, int] | None:
    pct_s = (payload_parts.get("wdpct") or "").strip()
    kind = (payload_parts.get("wdkind") or "").strip()
    prod = (payload_parts.get("wdprod") or "").strip()
    if not pct_s or not kind or not prod:
        return None
    try:
        pct = int(pct_s)
    except ValueError:
        return None
    if pct not in (10, 30, 50) or kind not in (KIND_SUB, KIND_GIFT, KIND_TRAFFIC):
        return None
    return kind, prod, pct


async def validate_payload_discount(
    payer_user_id: int,
    payload_parts: dict[str, str],
    *,
    payload: str = "",
    transaction_id: Optional[str] = None,
) -> bool:
    parsed = _checkout_from_payload(payload_parts)
    if parsed is None:
        return True

    kind, product_key, pct = parsed
    payment_ref = _payment_ref(payload or "", transaction_id)
    if await sql.wheel_discount_redemption_exists(payment_ref):
        return True

    active = await sql.wheel_discount_checkout_get(payer_user_id)
    if active is None:
        logger.error("Wheel discount: нет активной скидки uid={}", payer_user_id)
        return False
    if (
        str(active.kind) != kind
        or str(active.product_key) != product_key
        or int(active.percent) != pct
    ):
        logger.error("Wheel discount: активная скидка не совпадает с payload uid={}", payer_user_id)
        return False

    if not _payload_matches_product(payload_parts, kind=kind, product_key=product_key):
        logger.error("Wheel discount: тариф в payload не совпадает uid={}", payer_user_id)
        return False

    method = payload_parts.get("method", "")
    paid = int(float(payload_parts.get("amount", 0)))
    expected = int(active.final_stars if method == "stars" else active.final_rub)
    from payments.wheel_checkout import admin_test_payment_ok

    admin_test = admin_test_payment_ok(int(payer_user_id), paid, method)
    if paid != expected and not admin_test:
        logger.error(
            "Wheel discount: сумма {} != {} uid={}",
            paid,
            expected,
            payer_user_id,
        )
        return False
    return True


async def commit_payload_discount(
    payer_user_id: int,
    payload: str,
    payload_parts: dict[str, str],
    *,
    transaction_id: Optional[str] = None,
) -> bool:
    parsed = _checkout_from_payload(payload_parts)
    if parsed is None:
        return True
    if not await validate_payload_discount(
        payer_user_id,
        payload_parts,
        payload=payload,
        transaction_id=transaction_id,
    ):
        return False
    kind, product_key, pct = parsed
    ref = _payment_ref(payload, transaction_id)
    ok = await sql.wheel_discount_checkout_complete(
        payer_user_id,
        payment_ref=ref,
        payload=payload,
        kind=kind,
        product_key=product_key,
        percent=pct,
    )
    if not ok:
        logger.error("Wheel discount: commit failed uid={}", payer_user_id)
    return ok
