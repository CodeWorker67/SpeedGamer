import asyncio
import json
import random
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Set

TG_TEXT_LIMIT = 4096

from aiogram import Bot

from bot import sql, x3
from config import CHECKER_ID
from keyboard import (
    keyboard_buy_device_tier,
    keyboard_push_buy_reviews,
    create_kb,
    STYLE_PRIMARY,
)
from lexicon import lexicon
from logging_config import logger
from telegram_ids import is_telegram_chat_id

WINDOW = timedelta(minutes=10)
STATE_VERSION = 2


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _fmt_utc0(dt: datetime) -> str:
    """Naive datetime считается уже в UTC (как везде в time_mes)."""
    return dt.strftime('%Y-%m-%d %H:%M:%S') + ' UTC+0'


def _normalize_end_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
    return dt.replace(microsecond=0)


def _end_key(end: datetime, tier: str) -> str:
    return f"{end.isoformat(timespec='seconds')}|{tier}"


def _in_send_window(now: datetime, moment: datetime) -> bool:
    return moment <= now < moment + WINDOW


def _load_state(raw: Optional[str], end_key: str) -> Set[str]:
    if not raw:
        return set()
    try:
        data = json.loads(raw)
        v = data.get('v')
        if v == STATE_VERSION:
            return set(data.get('ends', {}).get(end_key, {}).get('s', []))
        # v == 1: одна подписка; старый ключ e без суффикса |tier — мигрируем на tier main
        if v == 1:
            old_e = data.get('e')
            if old_e == end_key:
                return set(data.get('s', []))
            if end_key.endswith('|main'):
                iso_part = end_key.split('|', 1)[0]
                if old_e == iso_part:
                    return set(data.get('s', []))
        return set()
    except (json.JSONDecodeError, TypeError):
        return set()


def _dump_state(raw: Optional[str], end_key: str, sent: Set[str]) -> str:
    ends: dict = {}
    if raw:
        try:
            prev = json.loads(raw)
            pv = prev.get('v')
            if pv == STATE_VERSION:
                ends = dict(prev.get('ends', {}))
            elif pv == 1 and prev.get('e'):
                ends[prev['e']] = {'s': sorted(prev.get('s', []))}
        except (json.JSONDecodeError, TypeError):
            pass
    ends[end_key] = {'s': sorted(sent)}
    return json.dumps({'v': STATE_VERSION, 'ends': ends}, separators=(',', ':'))


async def _persist_push_state(
    user_id: int, field_str_1_raw: Optional[str], end_key: str, sent: Set[str]
) -> str:
    new_val = _dump_state(field_str_1_raw, end_key, sent)
    await sql.update_field_str_1(user_id, new_val)
    return new_val


async def _update_subscription_end_by_tier(user_id: int, tier: str, end_date: datetime) -> None:
    updaters = {
        'main': sql.update_subscription_end_date,
        '3': sql.update_subscription_3_end_date,
        '10': sql.update_subscription_10_end_date,
        'white': sql.update_white_subscription_end_date,
    }
    fn = updaters.get(tier)
    if fn:
        await fn(user_id, end_date)


def _keyboard_for_user(in_panel: bool):
    if in_panel:
        return keyboard_buy_device_tier()
    return keyboard_push_buy_reviews()


async def _send_admin_text_chunks(bot: Bot, chat_id: int, text: str):
    """Telegram ограничивает длину сообщения; длинные списки id режем на части."""
    text = text.strip()
    if len(text) <= TG_TEXT_LIMIT:
        await bot.send_message(chat_id, text)
        return
    part = 1
    pos = 0
    n = len(text)
    while pos < n:
        take = TG_TEXT_LIMIT - 80
        chunk = text[pos : pos + take]
        if pos + take < n:
            cut = chunk.rfind('\n')
            if cut > take // 2:
                chunk = chunk[:cut]
            elif (cut := chunk.rfind(', ')) > take // 2:
                chunk = chunk[: cut + 1]
        header = f'Часть {part}\n\n' if part > 1 else ''
        await bot.send_message(chat_id, header + chunk)
        pos += len(chunk)
        part += 1


async def send_message_cron(bot: Bot):
    now = _utc_now_naive()
    window_end = now + WINDOW
    candidate_rows = await sql.select_rows_for_subscription_expiry_push(now, WINDOW)
    if CHECKER_ID is not None:
        await bot.send_message(
            CHECKER_ID,
            'Начинаю рассылку. Окно UTC+0: '
            f'{_fmt_utc0(now)} — {_fmt_utc0(window_end)} '
            f'({WINDOW}). Кандидатов: {len(candidate_rows)}.',
        )
    sent_count_7 = 0
    sent_count_3 = 0
    sent_count_1 = 0
    sent_count_0 = 0
    sent_count_week = 0
    sent_count_second_chance = 0
    failed_count = 0
    ids_7: List[int] = []
    ids_3: List[int] = []
    ids_1: List[int] = []
    ids_0: List[int] = []
    ids_week: List[int] = []
    ids_second_chance: List[int] = []

    push_field_cache: dict[int, Optional[str]] = {}

    for user_id, end_raw, in_panel, ttclid, field_str_1_raw, tier in candidate_rows:
        try:
            if not is_telegram_chat_id(user_id):
                continue

            if user_id not in push_field_cache:
                push_field_cache[user_id] = field_str_1_raw

            end = _normalize_end_utc(end_raw)
            if end is None:
                continue

            end_key = _end_key(end, tier)
            sent = _load_state(push_field_cache[user_id], end_key)
            keyboard = _keyboard_for_user(in_panel)

            if now < end:
                t7 = end - timedelta(days=7)
                t3 = end - timedelta(days=3)
                t1 = end - timedelta(days=1)
                t_h = end - timedelta(hours=1)

                if '7' not in sent and _in_send_window(now, t7):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_7'], reply_markup=keyboard)
                    await asyncio.sleep(0.05)
                    sent.add('7')
                    push_field_cache[user_id] = await _persist_push_state(
                        user_id, push_field_cache[user_id], end_key, sent
                    )
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_7 += 1
                    ids_7.append(user_id)
                    logger.info(f"Отправлено push-уведомление пользователю {user_id} за 7 дней")
                elif '3' not in sent and _in_send_window(now, t3):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_3'], reply_markup=keyboard)
                    await asyncio.sleep(0.05)
                    sent.add('3')
                    push_field_cache[user_id] = await _persist_push_state(
                        user_id, push_field_cache[user_id], end_key, sent
                    )
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_3 += 1
                    ids_3.append(user_id)
                    logger.info(f"Отправлено push-уведомление пользователю {user_id} за 3 дня")
                elif '1' not in sent and _in_send_window(now, t1):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_1'], reply_markup=keyboard)
                    await asyncio.sleep(0.05)
                    sent.add('1')
                    push_field_cache[user_id] = await _persist_push_state(
                        user_id, push_field_cache[user_id], end_key, sent
                    )
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_1 += 1
                    ids_1.append(user_id)
                    logger.info(f"Отправлено push-уведомление пользователю {user_id} за 1 день")
                elif 'h' not in sent and _in_send_window(now, t_h):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_0'], reply_markup=keyboard)
                    await asyncio.sleep(0.05)
                    sent.add('h')
                    push_field_cache[user_id] = await _persist_push_state(
                        user_id, push_field_cache[user_id], end_key, sent
                    )
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_0 += 1
                    ids_0.append(user_id)
                    logger.info(f"Отправлено push-уведомление пользователю {user_id} за 1 час")
            else:
                t_second = end + timedelta(days=7)
                if (
                    'sc' not in sent
                    and _in_send_window(now, t_second)
                    and not ttclid
                ):
                    await bot.send_message(
                        chat_id=user_id,
                        text=lexicon['second_chance_message'],
                        reply_markup=create_kb(
                            1,
                            styles={'connect_vpn': STYLE_PRIMARY},
                            connect_vpn='🔗 Подключить ВПН',
                            video_faq='Видеоинструкция',
                        ),
                    )
                    logger.info(f"Отправлено push-уведомление пользователю {user_id} за second_chance")
                    panel_username = x3.username_for_slot(user_id, tier)
                    try:
                        response = await x3.updateClient(4, panel_username, user_id)
                        if response:
                            result_active = await x3.activ(panel_username)
                            subscription_time = result_active.get('time', '-')
                            if subscription_time != '-':
                                try:
                                    new_end_date = datetime.strptime(
                                        subscription_time, '%d-%m-%Y %H:%M МСК'
                                    )
                                    await _update_subscription_end_by_tier(user_id, tier, new_end_date)
                                    logger.info(
                                        f"✅ Дата подписки для {user_id} ({tier}) обновлена после second_chance"
                                    )
                                except ValueError as e:
                                    logger.error(
                                        f"Ошибка парсинга даты second_chance для {user_id}: {e}"
                                    )
                        else:
                            logger.error(
                                f"❌ Не удалось добавить 4 дня пользователю {user_id} (second_chance)"
                            )
                    except Exception as e:
                        logger.error(f"Ошибка при добавлении 4 дней пользователю {user_id}: {e}")

                    utc_today = now.date()
                    ttclid_value = f"second_chance_{utc_today.strftime('%d%m%y')}"
                    try:
                        await sql.update_ttclid(user_id, ttclid_value)
                        logger.info(f"✅ ttclid для {user_id} установлен: {ttclid_value}")
                    except Exception as e:
                        logger.error(f"Ошибка обновления ttclid для {user_id}: {e}")

                    sent.add('sc')
                    push_field_cache[user_id] = await _persist_push_state(
                        user_id, push_field_cache[user_id], end_key, sent
                    )
                    sent_count_second_chance += 1
                    ids_second_chance.append(user_id)
                else:
                    n = 1
                    while n <= 200:
                        moment = end + timedelta(days=3 * n)
                        if moment > now + WINDOW:
                            break
                        key = f'p{n}'
                        if key not in sent and _in_send_window(now, moment):
                            await bot.send_message(
                                chat_id=user_id,
                                text=random.choice(lexicon['push_off']),
                                reply_markup=keyboard,
                            )
                            await asyncio.sleep(0.05)
                            sent.add(key)
                            push_field_cache[user_id] = await _persist_push_state(
                                user_id, push_field_cache[user_id], end_key, sent
                            )
                            await sql.mark_notification_as_sent(user_id)
                            sent_count_week += 1
                            ids_week.append(user_id)
                            logger.info(
                                f"Отправлено push-уведомление пользователю {user_id} "
                                f"после окончания подписки (+{3 * n} дн от даты end)"
                            )
                            break
                        n += 1
        except Exception:
            failed_count += 1

    all_sent_ids: List[int] = (
        ids_7 + ids_3 + ids_1 + ids_0 + ids_week + ids_second_chance
    )
    report_body = f'''Рассылка об окончании подписки (UTC, окна 10 мин):
за 7 дней: {sent_count_7}
за 3 дня: {sent_count_3}
за 1 день: {sent_count_1}
за 1 час: {sent_count_0}
после окончания каждые 3 дня: {sent_count_week}
повторный триал: {sent_count_second_chance}

Не получилось: {failed_count}
'''
    if CHECKER_ID is not None:
        await _send_admin_text_chunks(bot, CHECKER_ID, report_body)
    total_sent = len(all_sent_ids)
    logger.info(
        f"Уведомлений отправлено: {total_sent}"
    )
    logger.info(f"Не удалось отправить уведомления: {failed_count}")
