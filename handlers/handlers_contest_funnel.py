"""Воронка «выигрыш в конкурсе» для новых пользователей после первого /start."""
from __future__ import annotations

import asyncio

from aiogram import Router, F
from aiogram.types import CallbackQuery, InputMediaPhoto

from bot import bot, sql
from config import CHECKER_ID
from keyboard import (
    keyboard_buy_duration,
    keyboard_contest_win_reveal,
    keyboard_contest_win_take,
    keyboard_contest_win_urgency_buy,
)
from lexicon import buy_text_for_pro_hwid, lexicon
from logging_config import logger
from utils.custom_emoji import emojify
from utils.menu_photos import contest_win_photo

router = Router()

_initial_pending: set[int] = set()
_initial_sent: set[int] = set()
_followup_scheduled: set[int] = set()

_INITIAL_DELAY_SEC = 10
_FOLLOWUP_DELAY_SEC = 15 * 60
_CONTEST_DEVICES = 5

_INITIAL_CAPTION = (
    "⭐️ Поздравляем! Вы выиграли в конкурсе.\n\n"
    "В розыгрыше участвовали пользователи, проявившие любую активность в боте. "
    "Чем выше была активность, тем больше становились шансы на победу.\n\n"
    "⬇️ Нажмите кнопку, чтобы узнать, какой приз вы получили."
)

_REVEAL_CAPTION = (
    "⭐️ Ваш приз — <b>скидки до 50%</b> на подписку!\n"
    "❗️ Подходит на все устройства, гарантия работы или возврат!\n\n"
    "Воспользоваться можно в течение 30 минут⚡️"
)

_URGENCY_CAPTION = (
    "❗️ ВАШ ВЫИГРЫШ СГОРИТ ЧЕРЕЗ 15 МИНУТ ❗️\n\n"
    "Мы подвели итоги розыгрыша среди активных пользователей. "
    "Вы оказались среди победителей, и ваш приз всё ещё закреплён за вами.\n\n"
    "⏳ Осталось 15 минут. После этого выигрыш будет аннулирован "
    "без возможности восстановления"
)


def _buy_menu_caption() -> str:
    buy_txt = buy_text_for_pro_hwid(_CONTEST_DEVICES)
    return f"{buy_txt}\n\n{lexicon['choose_duration']}"


async def _edit_contest_photo(
    callback: CallbackQuery,
    caption: str,
    reply_markup,
    *,
    photo_phase: str | None = None,
) -> None:
    if photo_phase:
        photo_id = contest_win_photo(photo_phase)
    elif callback.message and callback.message.photo:
        photo_id = callback.message.photo[-1].file_id
    else:
        photo_id = contest_win_photo("initial")
    await callback.message.edit_media(
        media=InputMediaPhoto(media=photo_id, caption=emojify(caption), parse_mode="HTML"),
        reply_markup=reply_markup,
    )


async def _notify_checker(text: str) -> None:
    if CHECKER_ID is None:
        return
    try:
        await bot.send_message(CHECKER_ID, text, parse_mode="HTML")
    except Exception as e:
        logger.warning("contest_win: CHECKER_ID notify failed: %s", e)


async def _format_confirmed_purchase_line(user_id: int) -> str:
    rows = await sql.get_user_find_payments(user_id)
    if not rows:
        return "платёж не найден в БД"
    time_created, duration_part, label, amount_rub = rows[0]
    when = time_created.strftime("%d.%m.%Y %H:%M") if time_created else "—"
    return f"{label}, {duration_part}, {amount_rub} ₽ ({when})"


async def _contest_followup_after_reveal(user_id: int) -> None:
    await asyncio.sleep(_FOLLOWUP_DELAY_SEC)
    user_row = await sql.get_user(user_id)
    if not user_row:
        return
    reserve_field = bool(user_row[8])
    if reserve_field:
        purchase = await _format_confirmed_purchase_line(user_id)
        await _notify_checker(
            f"Пользователь <code>{user_id}</code> купил подписку со стартового байта "
            f"({purchase})"
        )
        return

    try:
        await bot.send_photo(
            user_id,
            photo=contest_win_photo("urgency"),
            caption=emojify(_URGENCY_CAPTION),
            parse_mode="HTML",
            reply_markup=keyboard_contest_win_urgency_buy(),
        )
    except Exception as e:
        logger.warning("contest_win: urgency send failed user=%s: %s", user_id, e)


async def _send_initial_contest_win(user_id: int) -> None:
    if user_id in _initial_sent:
        return
    _initial_sent.add(user_id)
    try:
        await bot.send_photo(
            user_id,
            photo=contest_win_photo("initial"),
            caption=emojify(_INITIAL_CAPTION),
            parse_mode="HTML",
            reply_markup=keyboard_contest_win_reveal(),
        )
    except Exception as e:
        logger.warning("contest_win: initial send failed user=%s: %s", user_id, e)


def schedule_contest_win_funnel(user_id: int) -> None:
    """10 секунд после первого захода — стартовое сообщение воронки."""
    if user_id in _initial_pending or user_id in _initial_sent:
        return
    _initial_pending.add(user_id)
    asyncio.create_task(_contest_win_delayed_start(user_id))


async def _contest_win_delayed_start(user_id: int) -> None:
    await asyncio.sleep(_INITIAL_DELAY_SEC)
    _initial_pending.discard(user_id)
    await _send_initial_contest_win(user_id)


@router.callback_query(F.data == "cwin_reveal")
async def contest_win_reveal(callback: CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    try:
        await _edit_contest_photo(
            callback,
            _REVEAL_CAPTION,
            keyboard_contest_win_take(),
        )
    except Exception as e:
        logger.warning("cwin_reveal edit failed for %s: %s", uid, e)
        return

    if uid not in _followup_scheduled:
        _followup_scheduled.add(uid)
        asyncio.create_task(_contest_followup_after_reveal(uid))


@router.callback_query(F.data == "cwin_take")
async def contest_win_take(callback: CallbackQuery):
    await callback.answer()
    uid = callback.from_user.id
    try:
        await _edit_contest_photo(
            callback,
            _buy_menu_caption(),
            keyboard_buy_duration(_CONTEST_DEVICES),
        )
    except Exception as e:
        logger.warning("cwin_take edit failed for %s: %s", uid, e)
        return

    await _notify_checker(f"Пользователь <code>{uid}</code> нажал забрать скидку")


@router.callback_query(F.data == "cwin_buy")
async def contest_win_urgency_buy(callback: CallbackQuery):
    await callback.answer()
    try:
        await _edit_contest_photo(
            callback,
            _buy_menu_caption(),
            keyboard_buy_duration(_CONTEST_DEVICES),
        )
    except Exception as e:
        logger.warning("cwin_buy edit failed for %s: %s", callback.from_user.id, e)
