"""Админ: /find, /find_transaction, /info и кнопки +дни/+ГБ (как partner_service)."""

from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from typing import Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from bot import bot, sql, x3
from config import ADMIN_IDS
from config_bd.models import Users
from handlers.handlers_admin import (
    _panel_usernames_by_device,
    _split_long_text,
    _SUB_TIER_LABELS,
)
from keyboard import emoji_button, keyboard_sub_after_buy
from lexicon import lexicon
from logging_config import logger
from telegram_ids import is_telegram_chat_id
from wl_traffic.service import (
    any_pro_user_on_limited_squad,
    fetch_all_pro_panel_users,
    get_wl_used_gb_for_user,
    reassign_all_pro_to_active,
)

router = Router()

_INFO_TEXT = (
    "📖 <b>Справка по админ-командам</b>\n\n"
    "/pay — подписки (БД + панель), WL-трафик и платежи\n"
    "Пример: <code>/pay 123456789</code>\n\n"
    "/find — карточка пользователя (с кнопками +дни / +ГБ)\n"
    "Пример: <code>/find 123456789</code>\n\n"
    "/sub — дата подписки в панели и БД\n"
    "Пример: <code>/sub 123456789 2026-03-25 12:00:00</code>\n\n"
    "/add_traffic — изменить лимит WL (GB, можно минус)\n"
    "Пример: <code>/add_traffic 123456789 10</code>\n\n"
    "/partner — статистика партнёра и оплаты приведённых\n"
    "Пример: <code>/partner 123456789</code>\n\n"
    "/partner_remove — списание с partner_balance\n"
    "Пример: <code>/partner_remove 123456789 500</code>\n\n"
    "/find_transaction — поиск платежа по ID\n"
    "Пример: <code>/find_transaction abc-123</code>\n"
)

_DEVICE_TIER_TITLES = {
    3: "Подписка 3 уст.",
    5: "Подписка 5 уст.",
    10: "Подписка 10 уст.",
}


def _admin_message(message: Message) -> bool:
    return message.from_user is not None and message.from_user.id in ADMIN_IDS


def _admin_callback(callback: CallbackQuery) -> bool:
    return callback.from_user is not None and callback.from_user.id in ADMIN_IDS


def _normalize_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _dt_str_find(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d.%m.%Y")


def _find_pay_date(dt: Optional[datetime]) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%d.%m.%Y")


def _subscription_active_until(dt: Optional[datetime]) -> tuple[str, Optional[int]]:
    if dt is None:
        return "не активна", None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    if dt <= now:
        return f"истекла {_dt_str_find(dt)}", 0
    days_left = (dt.date() - now.date()).days
    return f"активна до {_dt_str_find(dt)} (осталось {days_left} дн.)", days_left


def _resolve_longest_subscription(user: Users) -> tuple[int, Optional[datetime]]:
    candidates: list[tuple[int, datetime]] = []
    for slots, end_dt in (
        (3, user.subscription_3_end_date),
        (5, user.subscription_end_date),
        (10, user.subscription_10_end_date),
    ):
        normalized = _normalize_dt(end_dt)
        if normalized is not None:
            candidates.append((slots, normalized))
    if not candidates:
        return 5, None
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0]


def _find_keyboard(tg_id: int, device_slots: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(text="+ 7 дней", callback_data=f"find:d7:{tg_id}:{device_slots}"),
                emoji_button(text="+ 30 дней", callback_data=f"find:d30:{tg_id}:{device_slots}"),
                emoji_button(text="+ 90 дней", callback_data=f"find:d90:{tg_id}:{device_slots}"),
            ],
            [
                emoji_button(text="+ 10 ГБ", callback_data=f"find:g10:{tg_id}"),
                emoji_button(text="+ 50 ГБ", callback_data=f"find:g50:{tg_id}"),
            ],
        ]
    )


def _device_display_name(device: dict) -> str:
    model = (device.get("deviceModel") or "").strip()
    platform = (device.get("platform") or "").strip()
    os_version = (device.get("osVersion") or "").strip()
    if model and platform:
        name = f"{model} · {platform}"
    elif model:
        name = model
    elif platform:
        name = platform
    else:
        hwid = (device.get("hwid") or "устройство")[:12]
        name = f"Устройство {hwid}"
    if os_version:
        name = f"{name} {os_version}"
    return name


def _format_user_header(user: Users, tg_id: int) -> str:
    lines = [f"👤 {tg_id}"]
    fullname = (user.fullname or "").strip()
    username = (user.username or "").strip().lstrip("@")
    if fullname and username:
        lines.append(f"{escape(fullname)} (@{escape(username)})")
    elif fullname:
        lines.append(escape(fullname))
    elif username:
        lines.append(f"@{escape(username)}")
    return "\n".join(lines)


async def _panel_end_from_username(username: str) -> Optional[datetime]:
    data = await x3.get_user_by_username(username)
    panel_user = x3._panel_user_from_response(data)
    if not panel_user:
        return None
    expire_at_str = panel_user.get("expireAt")
    if not expire_at_str:
        return None
    try:
        return datetime.fromisoformat(expire_at_str.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _tier_section(
    user: Users,
    device_slots: int,
    db_end: Optional[datetime],
    panel_username: str,
) -> Optional[str]:
    panel_data = await x3.get_user_by_username(panel_username)
    panel_user = x3._panel_user_from_response(panel_data)
    if db_end is None and panel_user is None:
        return None

    effective_end = _normalize_dt(db_end)
    if effective_end is None:
        effective_end = await _panel_end_from_username(panel_username)

    status_line, _ = _subscription_active_until(effective_end)
    title = _DEVICE_TIER_TITLES[device_slots]
    lines = [f"{title}   {status_line}"]

    sub_link = await x3.sublink(panel_username)
    if sub_link:
        lines.append(f"Ссылка {device_slots} уст.: {sub_link}")

    device_dicts: list[dict] = []
    if panel_user:
        panel_user_id = x3._panel_user_id(panel_user)
        if panel_user_id is not None:
            devices, _total = await x3.get_user_hwid_devices(str(panel_user_id))
            device_dicts = [d for d in devices if isinstance(d, dict)]

    lines.append("Устройства:")
    if device_dicts:
        for idx, device in enumerate(device_dicts, start=1):
            lines.append(f"{idx}. {escape(_device_display_name(device))}")
    elif panel_user:
        lines.append("нет")

    return "\n".join(lines)


async def _build_find_message(tg_id: int, *, notice: str = "") -> tuple[str, Optional[InlineKeyboardMarkup]]:
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        return f"❌ Пользователь {tg_id} не найден.", None

    blocks = [_format_user_header(user, tg_id)]

    reg = _dt_str_find(user.create_user)
    blocks.append(f"Регистрация: {reg}")

    ref_total = await sql.select_ref_count(tg_id)
    if ref_total:
        ref_paid = await sql.select_ref_paid_count(tg_id)
        blocks.append(f"Кол-во рефералов: {ref_paid}/{ref_total}")

    partner_total = await sql.select_partner_count(tg_id)
    if partner_total:
        partner_paid = await sql.select_partner_paid_count(tg_id)
        blocks.append(f"Кол-во партнеров: {partner_paid}/{partner_total}")

    if (user.partner_balance or 0) > 0 or user.partner_flag:
        blocks.append(f"Баланс партнера: {user.partner_balance or 0}")

    if user.email:
        blocks.append(f"email: {escape(user.email)}")

    usernames = _panel_usernames_by_device(user)
    tier_parts: list[str] = []
    for slots in (3, 5, 10):
        db_end = {
            3: user.subscription_3_end_date,
            5: user.subscription_end_date,
            10: user.subscription_10_end_date,
        }[slots]
        section = await _tier_section(user, slots, db_end, usernames[slots])
        if section:
            tier_parts.append(section)

    if tier_parts:
        blocks.append("")
        blocks.extend(tier_parts)

    trafic_wl, limit_wl = await sql.get_wl_limits(tg_id)
    used_gb = await get_wl_used_gb_for_user(x3, tg_id, trafic_wl)
    remaining_gb = max(0.0, round(limit_wl - used_gb, 1))
    blocks.append("")
    blocks.append(
        f"Трафик:     {used_gb:.1f} / {limit_wl:.0f} ГБ  (осталось {remaining_gb:.1f} ГБ)"
    )

    pay_rows = await sql.get_user_find_payments(tg_id)
    blocks.append("")
    blocks.append("Оплаты:")
    if pay_rows:
        for tc, dur_part, label, amount in pay_rows:
            blocks.append(f"  {_find_pay_date(tc)}  {dur_part}  {label}  {amount} ₽")
    else:
        blocks.append("  —")

    longest_slots, longest_end = _resolve_longest_subscription(user)
    footer = ""
    keyboard = None
    if longest_end is not None or tier_parts:
        tier_label = _SUB_TIER_LABELS.get(str(longest_slots), f"{longest_slots} устр.")
        if longest_end is not None:
            footer = (
                f"\n\n<i>Кнопки +дни / +ГБ — к самой длинной подписке "
                f"({tier_label} до {_dt_str_find(longest_end)})</i>"
            )
        else:
            footer = (
                f"\n\n<i>Кнопки +дни / +ГБ — тариф {tier_label} "
                f"(дата в БД не задана, ориентир — панель)</i>"
            )
        keyboard = _find_keyboard(tg_id, longest_slots)

    notice_line = f"\n\n✅ {escape(notice)}" if notice else ""
    return "\n".join(blocks) + footer + notice_line, keyboard


async def _send_find_message(
    message: Message,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    chunks = _split_long_text(text, limit=3500)
    for i, chunk in enumerate(chunks):
        await message.answer(
            chunk,
            parse_mode="HTML",
            reply_markup=keyboard if i == len(chunks) - 1 else None,
            disable_web_page_preview=True,
        )


async def _refresh_find_message(
    callback: CallbackQuery,
    text: str,
    keyboard: Optional[InlineKeyboardMarkup] = None,
) -> None:
    chunks = _split_long_text(text, limit=3500)
    msg = callback.message
    if msg is None:
        return
    if len(chunks) == 1:
        try:
            await msg.edit_text(
                chunks[0], parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning("find callback edit_text failed: %s", e)
            await msg.answer(chunks[0], parse_mode="HTML", reply_markup=keyboard)
        return
    try:
        await msg.edit_text(chunks[0], parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.warning("find callback edit_text failed: %s", e)
        await msg.answer(chunks[0], parse_mode="HTML")
    for chunk in chunks[1:-1]:
        await msg.answer(chunk, parse_mode="HTML", disable_web_page_preview=True)
    await msg.answer(chunks[-1], parse_mode="HTML", reply_markup=keyboard, disable_web_page_preview=True)


async def _apply_find_add_days(tg_id: int, device_slots: int, days: int) -> tuple[bool, str]:
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        return False, "Пользователь не найден"

    usernames = _panel_usernames_by_device(user)
    username = usernames[device_slots]
    ok = await x3.updateClient(days, username, tg_id)
    if not ok:
        return False, "Не удалось продлить подписку в панели"

    ar = await x3.activ(username)
    time_str = ar.get("time", "-")
    if time_str in (None, "", "-"):
        return False, "Панель не вернула дату окончания"

    try:
        new_date = datetime.strptime(str(time_str).replace(" МСК", "").strip(), "%d-%m-%Y %H:%M")
        new_date = new_date.replace(tzinfo=timezone.utc)
    except ValueError:
        return False, "Не удалось разобрать дату из панели"

    await x3._persist_subscription_db(sql, tg_id, username, new_date)

    if is_telegram_chat_id(tg_id):
        try:
            sub_link = await x3.sublink(username)
            tier = _SUB_TIER_LABELS.get(str(device_slots), str(device_slots))
            user_text = lexicon["sub_granted_notify"].format(
                tier=tier,
                end_date=time_str,
            )
            await bot.send_message(
                tg_id,
                user_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard_sub_after_buy(sub_link) if sub_link else None,
            )
        except Exception as e:
            logger.warning("find +days notify uid=%s: %s", tg_id, e)

    return True, f"+{days} дн. → {time_str}"


async def _apply_find_add_traffic(tg_id: int, gb: float) -> tuple[bool, str]:
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        return False, "Пользователь не найден"

    trafic_wl, _ = await sql.get_wl_limits(tg_id)
    used_gb = await get_wl_used_gb_for_user(x3, tg_id, trafic_wl)
    await sql.add_wl_limit(tg_id, gb)
    _, limit_wl = await sql.get_wl_limits(tg_id)
    remaining_gb = max(0.0, round(limit_wl - used_gb, 2))

    if limit_wl > used_gb:
        panel_users = await fetch_all_pro_panel_users(x3, tg_id)
        if any_pro_user_on_limited_squad(panel_users):
            await reassign_all_pro_to_active(x3, tg_id)

    if is_telegram_chat_id(tg_id) and gb > 0:
        try:
            await bot.send_message(
                tg_id,
                lexicon["wl_traffic_admin_grant"].format(
                    gb=gb,
                    limit_gb=limit_wl,
                    used_gb=used_gb,
                    remaining_gb=remaining_gb,
                ),
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("find +gb notify uid=%s: %s", tg_id, e)

    sign = "+" if gb >= 0 else ""
    return True, f"{sign}{gb:g} ГБ → лимит {limit_wl:.1f} ГБ"


@router.message(Command(commands=["find"]))
async def cmd_find(message: Message):
    if not _admin_message(message):
        return

    args = (message.text or "").split()
    if len(args) < 2:
        await message.answer("❌ Использование: /find <telegram_id>")
        return

    try:
        tg_id = int(args[1].strip())
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return

    text, keyboard = await _build_find_message(tg_id)
    await _send_find_message(message, text, keyboard)


@router.callback_query(F.data.startswith("find:"))
async def find_action_callback(callback: CallbackQuery):
    if not _admin_callback(callback):
        await callback.answer("❌ Только для админов", show_alert=True)
        return

    parts = (callback.data or "").split(":")
    if len(parts) < 3:
        await callback.answer("❌ Некорректные данные", show_alert=True)
        return

    action = parts[1]
    try:
        tg_id = int(parts[2])
    except ValueError:
        await callback.answer("❌ Некорректный ID", show_alert=True)
        return

    notice = ""
    if action in ("d7", "d30", "d90"):
        if len(parts) < 4:
            await callback.answer("❌ Некорректные данные", show_alert=True)
            return
        days_map = {"d7": 7, "d30": 30, "d90": 90}
        try:
            device_slots = int(parts[3])
        except ValueError:
            await callback.answer("❌ Некорректный слот", show_alert=True)
            return
        ok, msg = await _apply_find_add_days(tg_id, device_slots, days_map[action])
        if not ok:
            await callback.answer(msg, show_alert=True)
            return
        notice = msg
    elif action in ("g10", "g50"):
        gb_map = {"g10": 10.0, "g50": 50.0}
        ok, msg = await _apply_find_add_traffic(tg_id, gb_map[action])
        if not ok:
            await callback.answer(msg, show_alert=True)
            return
        notice = msg
    else:
        await callback.answer("❌ Неизвестное действие", show_alert=True)
        return

    text, keyboard = await _build_find_message(tg_id, notice=notice)
    await _refresh_find_message(callback, text, keyboard)
    await callback.answer(notice)


@router.message(Command(commands=["find_transaction"]))
async def cmd_find_transaction(message: Message):
    if not _admin_message(message):
        return

    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("❌ Использование: /find_transaction <transaction_id>")
        return

    tid = args[1].strip()
    row = await sql.find_payment_by_transaction_id(tid)
    if row is None:
        await message.answer(f"❌ Платёж <code>{escape(tid)}</code> не найден.", parse_mode="HTML")
        return

    tc = row["time_created"]
    if tc and tc.tzinfo is None:
        tc = tc.replace(tzinfo=timezone.utc)
    created = tc.strftime("%Y-%m-%d %H:%M:%S") if tc else "—"
    gift = "Да" if row["is_gift"] else "Нет"

    text = (
        f"<b>Транзакция</b> <code>{escape(tid)}</code>\n"
        f"Таблица: {escape(str(row['table']))}\n"
        f"status: {escape(str(row['status'] or '—'))}\n"
        f"Создана: {created}\n"
        f"user_id: {row['user_id']}\n"
        f"duration: {escape(str(row['duration']))}\n"
        f"Подарок: {gift}\n"
        f"method: {escape(str(row['method']))}\n"
        f"Сумма: {row['amount']}"
    )
    await message.answer(text, parse_mode="HTML")


@router.message(Command(commands=["info"]))
async def cmd_info(message: Message):
    if not _admin_message(message):
        return

    for chunk in _split_long_text(_INFO_TEXT):
        await message.answer(chunk, parse_mode="HTML")
