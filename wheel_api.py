"""HTTP API для Telegram Mini App «Колесо фортуны» (ВПН ДЛЯ СВОИХ)."""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Annotated, Any, Literal
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from bot import sql
from config import PAYMENT_MAX_PENDING_PER_USER, TG_TOKEN
from lexicon import dct_desc, lexicon
from payments.pay_freekassa import pay as fk_pay, pay_for_gift as fk_pay_for_gift
from payments.payload_source import MINIAPP
from payments.wheel_checkout import (
    apply_admin_platega_test_price,
    quote_gift,
    quote_subscription,
    wheel_checkout_purchase_menu,
    wheel_checkout_self_devices,
    wheel_resolve_self_tariff_key,
)
from services.wheel import wheel_begin_spin, wheel_complete_spin, wheel_public_state, wheel_recent_wins
from tariff_resolve import DEFAULT_DEVICE_SLOTS, device_from_tariff_key, tariff_days_for_x3

_WHEEL_CHECKOUT_DURATIONS = frozenset({"m1_d5", "m3_d5"})

router = APIRouter(prefix="/api/wheel", tags=["wheel"])


def _verify_webapp_init_data(init_data: str) -> dict[str, Any]:
    if not TG_TOKEN:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "TG_TOKEN is not configured")
    if not init_data or not init_data.strip():
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing initData")

    pairs = parse_qsl(init_data, keep_blank_values=True)
    data = dict(pairs)
    received_hash = data.pop("hash", None)
    if not received_hash:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing hash")

    auth_date = data.get("auth_date")
    try:
        ts = int(auth_date)
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid auth_date")
    if abs(int(time.time()) - ts) > 86400:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "initData expired")

    check_lines = [f"{k}={v}" for k, v in sorted(data.items())]
    data_check_string = "\n".join(check_lines)
    secret_key = hmac.new(b"WebAppData", TG_TOKEN.encode(), hashlib.sha256).digest()
    calculated = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
    if calculated != received_hash:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid initData hash")

    user_raw = data.get("user")
    if not user_raw:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Missing user")
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid user payload")
    uid = user.get("id")
    if not isinstance(uid, int):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid user id")
    return {"user_id": uid, "user": user}


class InitDataBody(BaseModel):
    init_data: str = Field(..., min_length=10)


async def wheel_tg_user(body: InitDataBody) -> dict[str, Any]:
    return _verify_webapp_init_data(body.init_data)


WheelUser = Annotated[dict[str, Any], Depends(wheel_tg_user)]


@router.get("/recent-wins")
async def wheel_recent_wins_public(limit: int = Query(24, ge=1, le=50)):
    wins = await wheel_recent_wins(limit=limit)
    return {"wins": wins}


@router.post("/state")
async def wheel_state(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    return await wheel_public_state(uid, auth.get("user"))


@router.post("/spin/begin")
async def wheel_spin_begin(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    try:
        return await wheel_begin_spin(uid, auth.get("user"))
    except ValueError as e:
        if str(e) == "no_attempts":
            raise HTTPException(status.HTTP_403_FORBIDDEN, "У вас нет попыток!")
        raise


@router.post("/spin/complete")
async def wheel_spin_complete(_: Request, auth: WheelUser):
    uid = int(auth["user_id"])
    try:
        return await wheel_complete_spin(uid, auth.get("user"))
    except ValueError as e:
        if str(e) == "invalid_pending_prize":
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Некорректный приз")
        raise


class WheelCheckoutQuoteBody(InitDataBody):
    duration_key: str = Field(..., min_length=1, max_length=16)
    target: Literal["self", "gift"]


class WheelCheckoutCreateBody(WheelCheckoutQuoteBody):
    payment: Literal["sbp", "card"]


def _validate_checkout_duration(duration_key: str) -> str:
    key = duration_key.strip()
    if key not in _WHEEL_CHECKOUT_DURATIONS:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Недоступный тариф")
    return key


def _duration_for_payload(duration_key: str) -> str:
    if duration_key.startswith("m"):
        days = tariff_days_for_x3(duration_key)
        return str(days)
    return duration_key


def _desc_for_key(duration_key: str) -> str:
    try:
        from tariff_resolve import tariff_rub_and_desc

        _, desc = tariff_rub_and_desc(duration_key)
        return desc
    except KeyError:
        pass
    if duration_key in dct_desc:
        return dct_desc[duration_key]
    return f"Подписка {duration_key}"


async def _wheel_checkout_quote(uid: int, duration_key: str, target: str):
    template_key = _validate_checkout_duration(duration_key)
    if target == "gift":
        quote = await quote_gift(uid, template_key)
        return template_key, template_key, DEFAULT_DEVICE_SLOTS, apply_admin_platega_test_price(uid, quote)
    devices, _ = await wheel_checkout_self_devices(uid)
    tariff_key = wheel_resolve_self_tariff_key(template_key, devices)
    quote = await quote_subscription(uid, template_key)
    return template_key, tariff_key, devices, apply_admin_platega_test_price(uid, quote)


@router.post("/checkout/purchase-menu")
async def wheel_checkout_purchase_menu_route(_: Request, auth: WheelUser):
    """При входе в меню покупки: активна ли подписка и на сколько устройств оформлять."""
    uid = int(auth["user_id"])
    return await wheel_checkout_purchase_menu(uid)


@router.post("/checkout/quote")
async def wheel_checkout_quote(_: Request, body: WheelCheckoutQuoteBody, auth: WheelUser):
    uid = int(auth["user_id"])
    template_key, tariff_key, device_slots, quote = await _wheel_checkout_quote(
        uid, body.duration_key, body.target
    )
    _, sub_active = await wheel_checkout_self_devices(uid)
    return {
        "duration_key": template_key,
        "tariff_key": tariff_key,
        "target": body.target,
        "device_slots": device_slots,
        "main_subscription_active": sub_active,
        "base_rub": quote.base_rub,
        "final_rub": quote.final_rub,
        "discount_percent": quote.percent,
    }


@router.post("/checkout/create")
async def wheel_checkout_create(_: Request, body: WheelCheckoutCreateBody, auth: WheelUser):
    uid = int(auth["user_id"])
    template_key, tariff_key, device_slots, quote = await _wheel_checkout_quote(
        uid, body.duration_key, body.target
    )
    rub_amount = quote.final_rub
    suffix = quote.payload_suffix
    pay_key = tariff_key if body.target == "self" else template_key
    duration = _duration_for_payload(pay_key)
    user_id = str(uid)
    ui_kind = "sbp" if body.payment == "sbp" else "card"
    device = device_slots if body.target == "self" else device_from_tariff_key(template_key)
    des = _desc_for_key(pay_key)

    if body.target == "gift":
        payment_info = await fk_pay_for_gift(
            val=str(rub_amount),
            des=des,
            user_id=user_id,
            duration=duration,
            white=False,
            device=device,
            ui_kind=ui_kind,
        )
    else:
        payment_info = await fk_pay(
            val=str(rub_amount),
            des=des,
            user_id=user_id,
            duration=duration,
            white=False,
            device=device,
            ui_kind=ui_kind,
            source=MINIAPP,
            payload_suffix=suffix,
        )

    status_val = payment_info.get("status") or "error"
    if status_val == "rate_limited":
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
        )
    if status_val != "pending" or not payment_info.get("url"):
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "Не удалось создать платёж")

    return {
        "status": "pending",
        "amount_rub": rub_amount,
        "payment_url": payment_info["url"],
        "payment_id": payment_info.get("id") or "",
        "target": body.target,
        "duration_key": template_key,
        "tariff_key": pay_key,
        "device_slots": device,
        "payment": body.payment,
    }
