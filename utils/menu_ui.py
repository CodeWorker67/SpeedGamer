"""UI helpers for photo-based menu screens."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape
from typing import Optional, Union

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    CallbackQuery,
    InaccessibleMessage,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from bot import bot, sql, x3
from config_bd.models import Users
from config_bd.utils import pro_subscription_end_active
from logging_config import logger
from utils.custom_emoji import emojify
from utils.menu_photos import menu_photo
from wl_traffic.service import get_wl_used_gb_for_user, is_forever_end_date
from X3 import panel_username_for_telegram_slot

MAIN_MENU_REPLY_TEXT = (
    "Кнопка <b>Главное меню</b> внизу — нажмите её, чтобы в любой момент вернуться в главное меню."
)
MAIN_MENU_BUTTON_TEXT = "Главное меню"

PROFILE_TIERS: tuple[tuple[str, int, str, str], ...] = (
    ("3", 3, "3 устройства", "subscription_3_end_date"),
    ("main", 5, "5 устройств", "subscription_end_date"),
    ("10", 10, "10 устройств", "subscription_10_end_date"),
)

CONNECT_BTN_BY_SLOT = {
    "3": "🔗 Подключить ВПН (3 устройства)",
    "main": "🔗 Подключить ВПН (5 устройств)",
    "10": "🔗 Подключить ВПН (10 устройств)",
}


def reply_keyboard_main_menu() -> ReplyKeyboardMarkup:
    from keyboard import STYLE_PRIMARY

    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MAIN_MENU_BUTTON_TEXT, style=STYLE_PRIMARY)],
        ],
        resize_keyboard=True,
    )


async def send_main_menu_hint(message: Message) -> None:
    await message.answer(
        MAIN_MENU_REPLY_TEXT,
        parse_mode="HTML",
        reply_markup=reply_keyboard_main_menu(),
    )


def _aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_date_msk(dt: datetime) -> str:
    return (_aware_utc(dt) + timedelta(hours=3)).strftime("%d.%m.%Y")


def end_date_status_text(sub_end) -> str:
    if sub_end is None:
        return "Нет подписки"
    if is_forever_end_date(sub_end):
        return "Активна навсегда ♾️"
    if pro_subscription_end_active(sub_end):
        return f"Активна до {_format_date_msk(sub_end)}"
    return f"Истекла {_format_date_msk(sub_end)}"


def _tier_end(user: Optional[Users], attr: str):
    if user is None:
        return None
    return getattr(user, attr, None)


def has_any_device_subscription(user: Optional[Users]) -> bool:
    if user is None:
        return False
    return any(_tier_end(user, attr) is not None for _slot, _n, _label, attr in PROFILE_TIERS)


def has_active_device_subscription(user: Optional[Users]) -> bool:
    if user is None:
        return False
    return any(
        pro_subscription_end_active(_tier_end(user, attr))
        for _slot, _n, _label, attr in PROFILE_TIERS
    )


async def profile_caption(fullname: str, user: Optional[Users], uid: int) -> str:
    lines = [f"👤 {fullname}"]
    for slot, _n, label, attr in PROFILE_TIERS:
        sub_end = _tier_end(user, attr)
        status = end_date_status_text(sub_end)
        lines.append(f"📲 {label}: {status}")
        if pro_subscription_end_active(sub_end):
            username = panel_username_for_telegram_slot(uid, slot)
            sub_url = await x3.sublink(username)
            if sub_url:
                lines.append(f"<code>{escape(str(sub_url))}</code>")
    return "\n".join(lines)


async def _replace_photo_message(
    chat_id: int,
    message_id: int,
    photo_key: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest:
        pass
    await bot.send_photo(
        chat_id,
        photo=menu_photo(photo_key),
        caption=caption,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


async def edit_or_send_photo(
    source: Union[Message, CallbackQuery],
    photo_key: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    caption = emojify(caption)
    if isinstance(source, CallbackQuery):
        message = source.message
        chat_id = message.chat.id
        message_id = message.message_id
    else:
        message = source
        chat_id = message.chat.id
        message_id = message.message_id

    if isinstance(message, InaccessibleMessage):
        await _replace_photo_message(
            chat_id, message_id, photo_key, caption, reply_markup
        )
        return

    if message.photo:
        try:
            await message.edit_media(
                media=InputMediaPhoto(
                    media=menu_photo(photo_key),
                    caption=caption,
                    parse_mode="HTML",
                ),
                reply_markup=reply_markup,
            )
            return
        except TelegramBadRequest as e:
            logger.warning(
                "menu edit_media failed chat_id={} key={}: {}",
                chat_id,
                photo_key,
                e,
            )
            await _replace_photo_message(
                chat_id, message_id, photo_key, caption, reply_markup
            )
            return

    await bot.send_photo(
        chat_id,
        photo=menu_photo(photo_key),
        caption=caption,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


async def _slot_devices_line(username: str) -> str:
    devices_count = 0
    device_limit = 5
    panel_resp = await x3.get_user_by_username(username)
    panel_user = x3._panel_user_from_response(panel_resp)
    if panel_user:
        device_limit = panel_user.get("hwidDeviceLimit") or device_limit
        panel_user_id = x3._panel_user_id(panel_user)
        if panel_user_id is not None:
            _devices, devices_count = await x3.get_user_hwid_devices(str(panel_user_id))
    return f"📱 Устройства: {devices_count} / {device_limit}"


async def connect_screen_data(
    fullname: str, user: Optional[Users], uid: int
) -> str:
    parts = [f"👤 {fullname}", ""]
    for slot, _devices, label, attr in PROFILE_TIERS:
        status = end_date_status_text(_tier_end(user, attr))
        parts.append(f"📲 {label}: {status}")
        username = panel_username_for_telegram_slot(uid, slot)
        panel_resp = await x3.get_user_by_username(username)
        panel_user = x3._panel_user_from_response(panel_resp)
        if panel_user:
            parts.append(await _slot_devices_line(username))
            sub_url = await x3.sublink(username)
            if sub_url:
                parts.append("🔗 Ссылка для импорта:")
                parts.append(str(sub_url))
        parts.append("")

    used_gb, limit_gb = await sql.get_wl_limits(uid)
    used_gb = await get_wl_used_gb_for_user(x3, uid, used_gb)
    parts.append(f"📡 Антиглушилка: {used_gb:.2f} / {limit_gb:.2f} GB")
    parts.append("")
    parts.append(
        "📱 Нажмите «Если страница не загружается», чтобы получить инструкцию по настройке"
    )
    return "\n".join(parts)


async def show_connect_screen(callback: CallbackQuery) -> bool:
    from keyboard import keyboard_subscription_manage

    uid = callback.from_user.id
    user = await sql.get_user_object_by_user_id(uid)
    fullname = callback.from_user.full_name or callback.from_user.first_name or "Пользователь"
    caption = await connect_screen_data(fullname, user, uid)
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        caption,
        keyboard_subscription_manage(),
    )
    return True


async def _active_connect_buttons(uid: int) -> list[tuple[str, str]]:
    buttons: list[tuple[str, str]] = []
    for slot, _label, _uuid, username in await x3.active_subscription_slots(uid):
        if slot not in CONNECT_BTN_BY_SLOT:
            continue
        url = await x3.sublink(username)
        if url:
            buttons.append((CONNECT_BTN_BY_SLOT[slot], url))
    return buttons


async def show_main_menu(
    source: Union[Message, CallbackQuery],
    *,
    send_hint: bool = False,
) -> None:
    from keyboard import keyboard_start

    user = source.from_user
    user_obj = await sql.get_user_object_by_user_id(user.id)
    fullname = user.full_name or user.first_name or "Пользователь"
    caption = await profile_caption(fullname, user_obj, user.id)
    active = has_active_device_subscription(user_obj)

    if send_hint and isinstance(source, Message):
        await send_main_menu_hint(source)

    connect_buttons = await _active_connect_buttons(user.id)
    kb = keyboard_start(
        connect_buttons=connect_buttons,
        show_manage=has_any_device_subscription(user_obj) or bool(connect_buttons),
        buy_primary=not active,
    )

    if isinstance(source, CallbackQuery):
        await edit_or_send_photo(source, "profile", caption, kb)
    else:
        await source.answer_photo(
            photo=menu_photo("profile"),
            caption=emojify(caption),
            parse_mode="HTML",
            reply_markup=kb,
        )
