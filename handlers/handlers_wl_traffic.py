"""Обработчики покупки трафика на белой ноде."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery

from keyboard import keyboard_wl_traffic_payment_method, keyboard_wl_traffic_tariffs
from lexicon import lexicon
from utils.menu_ui import edit_or_send_photo, show_connect_screen
from wl_traffic.constants import (
    PROFILE_CB,
    WL_TRAFFIC_BUY_CB,
    WL_TRAFFIC_BUY_SUB_CB,
    WL_TRAFFIC_TARIFFS,
)

router = Router()


@router.callback_query(F.data == PROFILE_CB)
async def user_profile_legacy(callback: CallbackQuery):
    """Старые кнопки «Профиль» ведут в управление подпиской."""
    await callback.answer()
    await show_connect_screen(callback)


@router.callback_query(F.data.in_({WL_TRAFFIC_BUY_CB, WL_TRAFFIC_BUY_SUB_CB}))
async def wl_traffic_buy_cb(callback: CallbackQuery):
    back_callback = "buy_vpn_self" if callback.data == WL_TRAFFIC_BUY_SUB_CB else "connect_vpn"
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_traffic",
        lexicon["wl_traffic_buy_prompt"],
        keyboard_wl_traffic_tariffs(back_callback=back_callback),
    )


@router.callback_query(F.data.regexp(r"^wl_traffic(_sub)?_\d+$"))
async def wl_traffic_tariff_cb(callback: CallbackQuery):
    await callback.answer()
    data = callback.data or ""
    from_sub = data.startswith("wl_traffic_sub_")
    gb = data.rsplit("_", 1)[-1]
    if gb not in WL_TRAFFIC_TARIFFS:
        return

    price = WL_TRAFFIC_TARIFFS[gb]
    back_cb = WL_TRAFFIC_BUY_SUB_CB if from_sub else WL_TRAFFIC_BUY_CB
    await edit_or_send_photo(
        callback,
        "buy_traffic",
        lexicon["wl_traffic_payment_intro"].format(gb=gb, price=price),
        keyboard_wl_traffic_payment_method(gb, back_callback=back_cb),
    )
