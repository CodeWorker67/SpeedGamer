"""Оплата дополнительных устройств (duration:add_N_devices)."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery, LabeledPrice

from bot import bot, sql, x3
from config import ADMIN_IDS, PAYMENT_MAX_PENDING_PER_USER
from config_bd.utils import pro_subscription_end_active
from device_addons import (
    add_devices_duration_payload,
    add_devices_price_rub,
    current_main_device_limit,
    max_add_devices_available,
)
from keyboard import BTN_BACK, create_kb, emoji_button, keyboard_payment_sbp, keyboard_payment_stars
from lexicon import lexicon
from payments.pay_cryptobot import create_cryptobot_payment
from payments.pay_freekassa import pay
from payments.payment_limits import payment_creation_allowed
from utils.menu_ui import edit_or_send_photo

router = Router()


async def _validate_add_payment(callback: CallbackQuery, add_n: int) -> tuple[int, int] | None:
    user = await sql.get_user_object_by_user_id(callback.from_user.id)
    if user is None or not pro_subscription_end_active(user.subscription_end_date):
        await callback.answer("Подписка не активна.", show_alert=True)
        return None
    uid = callback.from_user.id
    devices = await current_main_device_limit(x3, uid, user)

    if add_n < 1 or add_n > max_add_devices_available(devices):
        await callback.answer("Нельзя добавить столько устройств.", show_alert=True)
        return None
    price = add_devices_price_rub(add_n, user.subscription_end_date)
    if callback.from_user.id in ADMIN_IDS:
        price = 1
    return price, devices


@router.callback_query(F.data.regexp(r"^add_dev_sbp_([1-5])$"))
async def add_devices_pay_sbp(callback: CallbackQuery):
    add_n = int(callback.data.rsplit("_", 1)[-1])
    validated = await _validate_add_payment(callback, add_n)
    if validated is None:
        return
    price, devices = validated
    await callback.answer()

    user_id = str(callback.from_user.id)
    if not await payment_creation_allowed(int(user_id)):
        await callback.message.answer(
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return

    duration = add_devices_duration_payload(add_n)
    payment_info = await pay(
        val=str(price),
        des=f"Доп. устройства +{add_n}",
        user_id=user_id,
        duration=duration,
        white=False,
        device=devices,
        ui_kind="sbp",
    )
    if payment_info["status"] == "pending":
        await edit_or_send_photo(
            callback,
            "subscription_manage",
            lexicon["add_devices_payment_link"],
            keyboard_payment_sbp("⚡ Оплатить СБП", payment_info["url"]),
        )
    else:
        await callback.message.answer(
            lexicon["error_payment"],
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )


@router.callback_query(F.data.regexp(r"^add_dev_card_([1-5])$"))
async def add_devices_pay_card(callback: CallbackQuery):
    add_n = int(callback.data.rsplit("_", 1)[-1])
    validated = await _validate_add_payment(callback, add_n)
    if validated is None:
        return
    price, devices = validated
    await callback.answer()

    user_id = str(callback.from_user.id)
    if not await payment_creation_allowed(int(user_id)):
        await callback.message.answer(
            lexicon["payment_too_many_pending"].format(PAYMENT_MAX_PENDING_PER_USER),
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return

    duration = add_devices_duration_payload(add_n)
    payment_info = await pay(
        val=str(price),
        des=f"Доп. устройства +{add_n}",
        user_id=user_id,
        duration=duration,
        white=False,
        device=devices,
        ui_kind="card",
    )
    if payment_info["status"] == "pending":
        await edit_or_send_photo(
            callback,
            "subscription_manage",
            lexicon["add_devices_payment_link"],
            keyboard_payment_sbp("💳 Оплатить картой РФ", payment_info["url"]),
        )
    else:
        await callback.message.answer(
            lexicon["error_payment"],
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )


@router.callback_query(F.data.regexp(r"^add_dev_stars_([1-5])$"))
async def add_devices_pay_stars(callback: CallbackQuery):
    add_n = int(callback.data.rsplit("_", 1)[-1])
    validated = await _validate_add_payment(callback, add_n)
    if validated is None:
        return
    price, devices = validated
    await callback.answer()

    user_id = str(callback.from_user.id)
    duration = add_devices_duration_payload(add_n)
    payload = (
        f"user_id:{user_id},duration:{duration},white:False,gift:False,"
        f"method:stars,amount:{price},device:{devices}"
    )
    await bot.send_invoice(
        callback.from_user.id,
        title=f"Доп. устройства +{add_n}",
        description=lexicon["add_devices_stars_desc"].format(n=add_n, price=price),
        prices=[LabeledPrice(label="XTR", amount=price)],
        provider_token="",
        payload=payload,
        currency="XTR",
        reply_markup=keyboard_payment_stars(price),
    )


@router.callback_query(F.data.regexp(r"^add_dev_crypto_([1-5])$"))
async def add_devices_pay_crypto(callback: CallbackQuery):
    add_n = int(callback.data.rsplit("_", 1)[-1])
    validated = await _validate_add_payment(callback, add_n)
    if validated is None:
        return
    price, devices = validated
    await callback.answer()

    user_id = callback.from_user.id
    duration = add_devices_duration_payload(add_n)
    result = await create_cryptobot_payment(
        rub_amount=price,
        description=f"Доп. устройства +{add_n}",
        user_id=user_id,
        duration=duration,
        white=False,
        is_gift=False,
        device=devices,
    )
    if result.get("status") == "pending" and result.get("url"):
        from aiogram.types import InlineKeyboardMarkup

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [emoji_button(text=f"💎 Оплатить криптой · {price} ₽", url=result["url"])],
            ]
        )
        await edit_or_send_photo(
            callback,
            "subscription_manage",
            lexicon["add_devices_payment_link"],
            kb,
        )
    else:
        await callback.message.answer(
            lexicon["error_payment"],
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
