"""Добавление устройств к активной основной подписке."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

from bot import sql, x3
from config_bd.utils import pro_subscription_end_active
from device_addons import (
    add_devices_price_rub,
    billable_months_until_end,
    current_main_device_limit,
    max_add_devices_available,
)
from keyboard import keyboard_add_devices_choices, keyboard_add_devices_payment
from tariff_resolve import SELF_DEVICES_MAX
from utils.menu_ui import _format_date_msk, edit_or_send_photo

router = Router()


async def _require_active_main_sub(callback: CallbackQuery):
    user = await sql.get_user_object_by_user_id(callback.from_user.id)
    if user is None or not pro_subscription_end_active(user.subscription_end_date):
        await callback.answer("Нет активной подписки на 5+ устройств.", show_alert=True)
        return None
    return user


@router.callback_query(F.data == "add_devices_start")
async def add_devices_start(callback: CallbackQuery):
    user = await _require_active_main_sub(callback)
    if user is None:
        return
    await callback.answer()

    uid = callback.from_user.id
    devices = await current_main_device_limit(x3, uid, user)
    end_dt = user.subscription_end_date
    sub_url = await x3.sublink(str(uid)) or "—"

    if devices >= SELF_DEVICES_MAX:
        text = (
            f"Ваша подписка:\n<code>{sub_url}</code>\n\n"
            f"Кол-во возможных устройств: {devices}\n"
            f"Дата окончания: {_format_date_msk(end_dt)}\n\n"
            "У вас максимальное кол-во устройств."
        )
        from keyboard import emoji_button, BTN_BACK
        from aiogram.types import InlineKeyboardMarkup

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [emoji_button(text=BTN_BACK, callback_data="connect_vpn")],
            ]
        )
        await edit_or_send_photo(callback, "subscription_manage", text, kb)
        return

    months = billable_months_until_end(end_dt)
    text = (
        f"Ваша подписка:\n<code>{sub_url}</code>\n\n"
        f"Кол-во возможных устройств: {devices}\n"
        f"Дата окончания: {_format_date_msk(end_dt)}\n\n"
        "Выберите, сколько устройств вы хотите добавить до конца срока подписки:"
    )
    max_add = max_add_devices_available(devices)
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        text,
        keyboard_add_devices_choices(max_add, months),
    )


@router.callback_query(F.data.regexp(r"^add_dev_pick_([1-5])$"))
async def add_devices_pick(callback: CallbackQuery):
    user = await _require_active_main_sub(callback)
    if user is None:
        return

    add_n = int(callback.data.rsplit("_", 1)[-1])
    uid = callback.from_user.id
    devices = await current_main_device_limit(x3, uid, user)
    max_add = max_add_devices_available(devices)
    if add_n < 1 or add_n > max_add:
        await callback.answer("Нельзя добавить столько устройств.", show_alert=True)
        return

    await callback.answer()
    end_dt = user.subscription_end_date
    price = add_devices_price_rub(add_n, end_dt)
    end_str = _format_date_msk(end_dt)
    text = (
        f"Вы добавляете <b>{add_n}</b> "
        f"{'устройство' if add_n == 1 else 'устройства' if 2 <= add_n <= 4 else 'устройств'} "
        f"до окончания срока подписки <b>{end_str}</b>\n\n"
        f"Сумма: <b>{price} ₽</b>\n\n"
        "Выберите метод оплаты:"
    )
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        text,
        keyboard_add_devices_payment(add_n, price),
    )
