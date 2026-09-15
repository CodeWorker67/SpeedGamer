import re
import time
import urllib.parse
import requests

from bot import sql, x3, bot
from config import CHANEL_ID, ADMIN_IDS, BOT_URL, CHECKER_ID, PARTNER_PROCENT, PARTNER_MIN, PARTNER_SUPPORT_URL, PUBLIC_SITE_URL, SITE_URL
from lead_tracker import post_user_registered, tracker_source_from_ref_and_stamp
from keyboard import (create_kb, keyboard_start_bonus, ref_keyboard,
                      keyboard_buy_device_tier, keyboard_buy_duration,
                      keyboard_gift_device_tier, keyboard_gift_duration,
                      keyboard_payment_method, keyboard_sub_after_buy,
                      keyboard_inline_ref, STYLE_PRIMARY,
                      keyboard_buy_menu, keyboard_earn_with_us, keyboard_about_service,
                      keyboard_partner_dashboard, keyboard_inline_partner,
                      keyboard_partner_withdraw, OPEN_SITE_CB, ABOUT_SERVICE_CB, BTN_BACK,
                      partner_bot_link, partner_site_link, emoji_button,
                      keyboard_promo_120_device_tier, keyboard_payment_method_promo_120)
from utils.menu_ui import (
    MAIN_MENU_BUTTON_TEXT,
    edit_or_send_photo,
    show_main_menu,
    show_connect_screen,
)
from utils.custom_emoji import emojify
from web_api import create_bot_site_login_token
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, InlineQuery, InlineQueryResultArticle, \
    InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton, InaccessibleMessage
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import (
    buy_text_for_pro_hwid,
    lexicon,
    payment_tariff_summary_pro,
    promo_120_payment_caption,
    tariff_desc_key_from_payment_callback,
)
from datetime import datetime, timezone
from tariff_resolve import panel_username
from config_bd.utils import user_has_active_pro_subscription


router: Router = Router()

PRO_HWID_DEVICE_LIMIT = 5
REFERRER_REF_BONUS_DAYS = 7
_TRIAL_DAYS = 7
_TRIAL_DEVICE_SLOTS = 3

_NEW_DEVICE_TARIFF_RE = re.compile(r'^(?:r_m(1|3|6|12)_d(3|5|10)|r_5000(?:sale)?)$')
_GIFT_DEVICE_TARIFF_RE = re.compile(r'^gift_r_m(1|3|6|12)_d(3|5|10)$')
_PROMO_120_TARIFF_RE = re.compile(r'^r_120_d(3|5|10)$')

_PROMO_120_DEVICES_CAPTION = (
    '🎁 <b>Акция: 3 + 1 месяц в подарок!</b>\n'
    'Оплачиваете 3 месяца — четвёртый в подарок.\n\n'
    '⬇️ Выберите количество устройств ⬇️'
)


# Этот хэндлер срабатывает на команду /start
@router.message(Command(commands="start"))
async def process_start_command(message: Message, command: Command):

    user_data = await sql.get_user(message.from_user.id)
    had_row_before = user_data is not None
    in_panel = False
    ref_login = ''
    partner_login = ''
    existing = False
    stamp = ''
    ttclid = None

    if user_data:
        in_panel = user_data[4]
        existing = True

    if len((message.text or "").strip().split()) == 1:
        if user_data:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно')
        else:
            logger.success(f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз')

    else:
        start_arg = message.text.split(' ', 1)[1]

        if start_arg.startswith('partner_'):
            if user_data:
                logger.info(
                    f'Юзер {message.from_user.id} - {message.from_user.username} '
                    f'нажал старт повторно с партнёрской ссылкой'
                )
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} '
                    f'зашел в бота в первый раз по партнёрской ссылке'
                )
                raw_partner = start_arg.replace('partner_', '', 1)
                if raw_partner.isdigit() and raw_partner != str(message.from_user.id):
                    partner_login = raw_partner

        elif start_arg.startswith('ref') or 'ref' in start_arg:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с реферальной ссылкой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по реферальной ссылкой')
                ref_login = start_arg.replace('ref', '', 1)

        elif start_arg.startswith('auth_'):
            auth_token = start_arg.replace('auth_', '', 1)
            from web_api import confirm_tg_auth_token
            ok = confirm_tg_auth_token(
                auth_token,
                message.from_user.id,
                first_name=message.from_user.first_name or "",
                username=message.from_user.username,
            )
            if ok:
                logger.info(f'Юзер {message.from_user.id} авторизован на сайте через deeplink')
                dashboard_url = f"{PUBLIC_SITE_URL}/dashboard" if PUBLIC_SITE_URL else ""
                if dashboard_url:
                    kb = InlineKeyboardMarkup(
                        inline_keyboard=[
                            [
                                InlineKeyboardButton(
                                    text="🌐 Перейти в личный кабинет",
                                    url=dashboard_url,
                                )
                            ]
                        ]
                    )
                    await message.answer("✅ Вы авторизованы на сайте!", reply_markup=kb)
                else:
                    await message.answer("✅ Вы авторизованы на сайте! Вернитесь во вкладку с сайтом.")
            else:
                await message.answer("❌ Ссылка устарела. Попробуйте ещё раз на сайте.")
            if not user_data:
                await sql.add_user(message.from_user.id, False, False)
            existing = True

        elif start_arg.startswith('gift_') or 'gift_' in start_arg:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} пытается активировать подарочную подписку')
            gift_id = start_arg.replace('gift_', '', 1)
            in_panel = await activate_gift(message, gift_id)
            await asyncio.sleep(2)
            existing = True
        elif start_arg.startswith('ttclid_') or 'ttclid_' in start_arg:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой ttclid')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке ttclid')
                stamp = 'YuraTT'
                ttclid = start_arg.replace('ttclid_', '', 1).replace('_', '.')

                payload = {
                    'event_source': 'web',
                    'event_source_id': 'D5U8OFJC77U9E3ANE170',
                    'data': [
                        {
                            'event': 'Subscribe',
                            'event_time': int(time.time()),
                            'context': {
                                'ad': {
                                    'callback': ttclid
                                }
                            }
                        }
                    ]
                }
                response = requests.post(
                    'https://business-api.tiktok.com/open_api/v1.3/event/track/',
                    json=payload,
                    headers={
                        'Content-Type': 'application/json',
                        'Access-Token': '7a9d82c42eaccd2393b74f31975fb8cc96bbb5d6'
                    },
                    timeout=2
                )

                if response.status_code == 200:
                    logger.success('Пиксель успешно отправлен в TikTok')
                else:
                    logger.error(f'Ошибка TikTok API: статус {response.status_code}, ответ: {response.text}')
        else:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке')
                stamp = start_arg

    if not existing:
        inserted = await sql.add_user(
            message.from_user.id, False, False,
            ref=ref_login, stamp=stamp, partner=partner_login,
        )
        if inserted:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} добавлен в БД')
            src = tracker_source_from_ref_and_stamp(ref_login, stamp, partner_login)
            await post_user_registered(
                message.from_user.id,
                message.from_user.username,
                message.from_user.full_name,
                src,
            )
        if ttclid:
            await sql.update_ttclid(message.from_user.id, ttclid)
            logger.info(f'Юзеру {message.from_user.id} - {message.from_user.username} присвоен ttclid')

    if had_row_before:
        if ref_login:
            await sql.try_set_ref_from_invite(message.from_user.id, ref_login)
        if stamp:
            await sql.try_set_stamp_from_invite(message.from_user.id, stamp)

    user_data = await sql.get_user(message.from_user.id)

    in_panel = user_data[4] if user_data else in_panel

    if not in_panel:
        pass

    await show_main_menu(message, send_hint=True)


@router.message(F.text == MAIN_MENU_BUTTON_TEXT)
async def main_menu_reply_button(message: Message):
    await show_main_menu(message, send_hint=False)


def _site_base_url() -> str:
    return (PUBLIC_SITE_URL or SITE_URL).rstrip("/")


def _site_login_url(telegram_user_id: int, first_name: str, username: str | None) -> str:
    token = create_bot_site_login_token(
        telegram_user_id=telegram_user_id,
        first_name=first_name,
        username=username,
    )
    return f"{_site_base_url()}/auth/bot?token={urllib.parse.quote(token, safe='')}"


@router.callback_query(F.data == OPEN_SITE_CB)
async def open_site_callback(callback: CallbackQuery):
    """Ссылка на сайт с одноразовым токеном для авто-входа."""
    await callback.answer()
    if not _site_base_url():
        await callback.message.answer(
            "Сайт пока не настроен. Укажите PUBLIC_SITE_URL или SITE_URL в .env.",
            reply_markup=create_kb(1, back_to_main=BTN_BACK),
        )
        return
    u = callback.from_user
    login_url = _site_login_url(
        u.id,
        u.first_name or "",
        u.username,
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="🌐 Открыть сайт",
                    url=login_url,
                )
            ],
            [
                emoji_button(
                    text=BTN_BACK,
                    callback_data="back_to_main",
                )
            ],
        ]
    )
    await edit_or_send_photo(
        callback,
        "our_site",
        lexicon["site_login_hint"],
        kb,
    )


@router.callback_query(F.data == 'buy_vpn')
async def buy_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['buy_menu'],
        keyboard_buy_menu(),
    )


@router.callback_query(F.data == 'buy_vpn_self')
async def buy_vpn_self_cb(callback: CallbackQuery):
    await callback.answer()
    buy_txt = buy_text_for_pro_hwid(PRO_HWID_DEVICE_LIMIT)
    text = f'{buy_txt}\n\n{lexicon["choose_tariff"]}'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_buy_device_tier(),
    )


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await show_connect_screen(callback)


@router.callback_query(F.data == 'r_120')
async def promo_120_choose_devices(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        'buy_subscription',
        _PROMO_120_DEVICES_CAPTION,
        keyboard_promo_120_device_tier(),
    )


@router.callback_query(F.data.regexp(_PROMO_120_TARIFF_RE))
async def promo_120_payment_method(callback: CallbackQuery):
    await callback.answer()
    tariff = callback.data
    dk = tariff_desc_key_from_payment_callback(tariff)
    text = promo_120_payment_caption(dk)
    text += '\n\nВыберите способ оплаты:'
    await edit_or_send_photo(
        callback,
        'buy_subscription',
        text,
        keyboard_payment_method_promo_120(tariff),
    )


@router.callback_query(F.data.regexp(_NEW_DEVICE_TARIFF_RE))
async def process_payment_method(callback: CallbackQuery):
    await callback.answer()
    tariff = callback.data
    dk = tariff_desc_key_from_payment_callback(tariff)
    text = payment_tariff_summary_pro(dk)
    text += '\n\nВыберите способ оплаты:'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_payment_method(tariff),
    )


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_legacy_cb(callback: CallbackQuery):
    """Старые кнопки «бесплатно» в рассылках: ведём на экран покупки подписки."""
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['free_vpn_legacy'],
        keyboard_start_bonus(),
    )


@router.callback_query(F.data.regexp(r'^buy_tier_(3|5|10)$'))
async def buy_tier_chosen(callback: CallbackQuery):
    await callback.answer()
    devices = int(callback.data.split('_')[-1])
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['choose_duration'],
        keyboard_buy_duration(devices),
    )


@router.callback_query(F.data == 'back_buy_tier')
async def buy_back_to_tier(callback: CallbackQuery):
    await callback.answer()
    buy_txt = buy_text_for_pro_hwid(PRO_HWID_DEVICE_LIMIT)
    text = f'{buy_txt}\n\n{lexicon["choose_tariff"]}'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_buy_device_tier(),
    )


async def _edit_callback_message_html(
    callback: CallbackQuery,
    text: str,
    reply_markup,
) -> None:
    message = callback.message
    if message is None or isinstance(message, InaccessibleMessage):
        await bot.send_message(
            callback.from_user.id,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )
        return
    try:
        if message.photo or message.video or message.animation or message.document:
            await message.edit_caption(
                caption=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
        else:
            await message.edit_text(
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
    except TelegramBadRequest as e:
        logger.warning(f"edit callback message failed: {e}")
        await bot.send_message(
            callback.from_user.id,
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )


async def _issue_pro_trial(
    callback: CallbackQuery,
    *,
    days: int,
    log_prefix: str,
    notify_checker: bool = False,
) -> bool:
    uid = callback.from_user.id

    user_id_str = panel_username(uid, white=False, device_slots=_TRIAL_DEVICE_SLOTS)
    existing_user = await x3.get_user_by_username(user_id_str)
    panel_exists = bool(existing_user and existing_user.get("response"))

    try:
        if panel_exists:
            ok = await x3.updateClient(days, user_id_str, uid)
        else:
            ok = await x3.addClient(
                days,
                user_id_str,
                uid,
                hwid_device_limit=_TRIAL_DEVICE_SLOTS,
            )
    except Exception as e:
        logger.error(f"{log_prefix}: ошибка панели для {uid}: {e}")
        ok = False

    if not ok:
        await sql.update_field_bool_3(uid, False)
        logger.error(f"{log_prefix}: не удалось выдать триал user={uid}")
        try:
            await callback.message.answer(
                "Не удалось активировать триал. Попробуйте позже или напишите в поддержку."
            )
        except Exception:
            pass
        return False

    if await sql.get_user(uid) is not None:
        await sql.update_in_panel(uid)
    else:
        await sql.add_user(uid, True)

    result_active = await x3.activ(user_id_str)
    subscription_time = result_active.get("time", "-")
    if subscription_time != "-":
        try:
            subscription_end_date = datetime.strptime(subscription_time, "%d-%m-%Y %H:%M МСК")
            await sql.update_subscription_3_end_date(uid, subscription_end_date)
        except ValueError as e:
            logger.error(f"{log_prefix}: ошибка парсинга даты для {uid}: {e}")

    await sql.init_wl_trial_limits(uid)

    sub_link = await x3.sublink(user_id_str)
    text = lexicon["trial_success"].format(subscription_time, days, sub_link)
    await _edit_callback_message_html(callback, text, keyboard_sub_after_buy(sub_link))
    logger.info(
        f"{log_prefix}: триал активирован user={uid} username={user_id_str} days={days}"
    )

    if notify_checker and CHECKER_ID is not None:
        try:
            await bot.send_message(
                chat_id=CHECKER_ID,
                text=f"Пользователь <code>{uid}</code> взял триал {days} дней",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error(f"{log_prefix}: не удалось уведомить CHECKER_ID user={uid}: {e}")

    return True


async def _any_panel_pro_subscription_active(uid: int) -> bool:
    for device_slots in (3, 5, 10):
        username = panel_username(uid, white=False, device_slots=device_slots)
        existing = await x3.get_user_by_username(username)
        if not existing or not existing.get("response"):
            continue
        user = existing["response"]
        if isinstance(user, list):
            user = user[0]
        expire_at_str = user.get("expireAt")
        if not expire_at_str:
            continue
        expire_at = datetime.fromisoformat(expire_at_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if user.get("status") == "ACTIVE" and expire_at > now:
            return True
    return False


@router.callback_query(F.data == "get_trial")
async def get_trial_cb(callback: CallbackQuery):
    await callback.answer("Акция закончилась", show_alert=True)
    return
    # if not await sql.claim_broadcast_trial(callback.from_user.id):
    #     await callback.answer("Вы уже воспользовались триалом", show_alert=True)
    #     return

    # await callback.answer()
    # await _issue_pro_trial(
    #     callback,
    #     days=_TRIAL_DAYS,
    #     log_prefix="get_trial",
    #     notify_checker=True,
    # )


@router.callback_query(F.data.startswith("trial_gift_"))
async def trial_gift_broadcast_callback(callback: CallbackQuery):
    tail = (callback.data or "")[len("trial_gift_") :]
    if not tail.isdigit():
        await callback.answer("Некорректные данные кнопки.", show_alert=True)
        return
    days = int(tail)

    if not await sql.claim_broadcast_trial(callback.from_user.id):
        await callback.answer("Вы уже воспользовались триалом", show_alert=True)
        return

    await callback.answer()
    await _issue_pro_trial(
        callback,
        days=days,
        log_prefix="trial_gift",
        notify_checker=True,
    )


@router.callback_query(F.data == "info")
async def info_legacy(callback: CallbackQuery):
    """Старая кнопка «Информация» — открываем «О сервисе»."""
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "about_service",
        lexicon['about_service'],
        keyboard_about_service(),
    )


@router.callback_query(F.data == ABOUT_SERVICE_CB)
async def about_service_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "about_service",
        lexicon['about_service'],
        keyboard_about_service(),
    )


@router.callback_query(F.data == 'earn_with_us')
async def earn_with_us_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['earn_menu'],
        keyboard_earn_with_us(),
    )


@router.callback_query(F.data == 'back_to_earn')
async def back_to_earn_cb(callback: CallbackQuery):
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['earn_menu'],
        keyboard_earn_with_us(),
    )


@router.callback_query(F.data == 'ref')
async def referral_program(callback: CallbackQuery):
    await callback.answer()
    count = await sql.select_ref_count(int(callback.from_user.id))
    bonus_days = REFERRER_REF_BONUS_DAYS
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['ref_info'].format(count, callback.from_user.id, bonus_days),
        ref_keyboard(callback.from_user.id),
    )


async def _ensure_user_exists(user_id: int) -> None:
    if await sql.get_user(user_id) is None:
        await sql.add_user(user_id, False, False)


async def _send_partner_dashboard(callback: CallbackQuery) -> None:
    tg_id = callback.from_user.id
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        await _ensure_user_exists(tg_id)
        user = await sql.get_user_object_by_user_id(tg_id)

    referrals = await sql.select_partner_count(tg_id)
    payments_sum = await sql.select_partner_referrals_payments_sum(tg_id)
    balance = user.partner_balance or 0
    paid_out = user.partner_pay or 0
    total_earned = balance + paid_out
    bot_link = partner_bot_link(tg_id)
    site_link = partner_site_link(tg_id)
    site_block = (
        f'🌐 <b>Сайт:</b>\n└ <code>{site_link}</code>\n\n'
        if site_link
        else ''
    )

    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['partner_dashboard'].format(
            bot_link=bot_link,
            site_block=site_block,
            procent=PARTNER_PROCENT,
            min_sum=PARTNER_MIN,
            referrals=referrals,
            payments_sum=payments_sum,
            total_earned=total_earned,
            paid_out=paid_out,
            balance=balance,
        ),
        keyboard_partner_dashboard(tg_id),
    )


@router.callback_query(F.data == 'partner_earn')
async def partner_program(callback: CallbackQuery):
    await callback.answer()
    await _ensure_user_exists(callback.from_user.id)
    await _send_partner_dashboard(callback)


@router.callback_query(F.data == 'partner_withdraw')
async def partner_withdraw(callback: CallbackQuery):
    user = await sql.get_user_object_by_user_id(callback.from_user.id)
    if user is None:
        await callback.answer()
        return

    balance = user.partner_balance or 0
    if balance < PARTNER_MIN:
        await callback.answer(
            lexicon['partner_withdraw_alert'].format(min_sum=PARTNER_MIN),
            show_alert=True,
        )
        return

    await callback.answer()
    support_url = PARTNER_SUPPORT_URL or "https://t.me/"
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        lexicon['partner_withdraw_info'].format(
            balance=balance,
            min_sum=PARTNER_MIN,
        ),
        keyboard_partner_withdraw(support_url),
    )


@router.callback_query(F.data.in_(('buy_gift', 'start_gift')))
async def gift_subscription_start(callback: CallbackQuery):
    """Начало процесса подарка подписки."""
    await callback.answer()
    text = f'{lexicon["gift_start"]}\n\n{lexicon["choose_tariff"]}'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_gift_device_tier(),
    )


@router.callback_query(F.data.regexp(r'^gift_tier_(3|5|10)$'))
async def gift_tier_chosen(callback: CallbackQuery):
    await callback.answer()
    devices = int(callback.data.split('_')[-1])
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['choose_duration'],
        keyboard_gift_duration(devices),
    )


@router.callback_query(F.data == 'gift_back_tier')
async def gift_back_to_tier(callback: CallbackQuery):
    await callback.answer()
    text = f'{lexicon["gift_start"]}\n\n{lexicon["choose_tariff"]}'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_gift_device_tier(),
    )


@router.callback_query(F.data.regexp(_GIFT_DEVICE_TARIFF_RE))
async def process_gift_payment_method(callback: CallbackQuery):
    await callback.answer()
    tariff = callback.data
    dk = tariff_desc_key_from_payment_callback(tariff)
    text = payment_tariff_summary_pro(dk)
    text += '\n\nВыберите способ оплаты <b>подарочной подписки</b>:'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_payment_method(tariff),
    )


async def activate_gift(message: Message, gift_id: str):
    """Активация подарка по gift_id"""
    result = await sql.activate_gift(gift_id, message.from_user.id)

    if not result[0]:
        await message.answer(lexicon['gift_no'])
        logger.warning(f'Ссылка на подарок протухла')
        if await sql.get_user(message.from_user.id) is None:
            await sql.add_user(message.from_user.id, False)
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по подарочной ссылке')
        return False

    duration = result[1]
    white_flag = result[2]
    gift_giver_id = result[3]
    device_slots = result[4] if result[4] is not None else 5
    if not white_flag and device_slots not in (3, 5, 10):
        device_slots = 5

    user_id = message.from_user.id
    user_id_str = panel_username(user_id, white=white_flag, device_slots=device_slots)
    hw_lim = None if white_flag else device_slots

    was_in_db = await sql.get_user(message.from_user.id) is not None
    if not was_in_db:
        ref_as_gift = ''
        if gift_giver_id and int(gift_giver_id) != int(user_id):
            ref_as_gift = str(int(gift_giver_id))
        await sql.add_user(message.from_user.id, False, False, ref=ref_as_gift)

    existing_user = await x3.get_user_by_username(user_id_str)

    if existing_user and 'response' in existing_user and existing_user['response']:
        response = await x3.updateClient(duration, user_id_str, user_id)
    else:
        response = await x3.addClient(
            duration,
            user_id_str,
            user_id,
            hwid_device_limit=hw_lim,
        )

    if response:
        result_active = await x3.activ(user_id_str)
        subscription_time = result_active.get('time', '-')

        await sql.update_in_panel(message.from_user.id)

        if subscription_time != '-':
            try:
                subscription_end_date = datetime.strptime(
                    subscription_time,
                    '%d-%m-%Y %H:%M МСК',
                )
                if white_flag:
                    await sql.update_white_subscription_end_date(user_id, subscription_end_date)
                elif device_slots == 3:
                    await sql.update_subscription_3_end_date(user_id, subscription_end_date)
                elif device_slots == 10:
                    await sql.update_subscription_10_end_date(user_id, subscription_end_date)
                else:
                    await sql.update_subscription_end_date(user_id, subscription_end_date)
            except ValueError as e:
                logger.error(f'Ошибка парсинга даты подарка для {user_id}: {e}')

        if was_in_db:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} получил в подарок подписку, уже был в БД')
        else:
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз и получил подарочную подписку')

        await message.answer(lexicon['gift_yes'].format(duration, subscription_time))

        if not white_flag:
            from wl_traffic.service import apply_wl_subscription_bonus
            await apply_wl_subscription_bonus(sql, x3, user_id, int(duration))
        return True

    else:
        await message.answer("❌ Ошибка при активации подарка. Обратитесь в поддержку.")
        if await sql.get_user(message.from_user.id) is None:
            await sql.add_user(message.from_user.id, False)
        return False


@router.callback_query(F.data == 'video_faq')
async def video_faq(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer_video(
        video='BAACAgQAAxkBAAEruMxqBamHrfafk-HiCQxgz0O7cKwgPQAC_SAAApwDMVCjetgWmRs7KDsE',
        caption=lexicon['push_not_subscribed_3h'],
        reply_markup=create_kb(1, back_to_main=BTN_BACK),
    )


@router.callback_query(F.data == 'back_to_buy_menu')
async def back_to_buy_menu_handler(callback: CallbackQuery):
    """Возврат к выбору тарифа (устаревший callback из оплаты)."""
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        lexicon['buy_menu'],
        keyboard_buy_menu(),
    )


@router.callback_query(F.data == 'back_to_main')
async def back_to_main_handler(callback: CallbackQuery):
    await callback.answer()
    await show_main_menu(callback)


@router.callback_query(F.data == 'back_to_gift_menu')
async def back_to_gift_menu_handler(callback: CallbackQuery):
    await callback.answer()
    text = f'{lexicon["gift_start"]}\n\n{lexicon["choose_tariff"]}'
    await callback.message.edit_text(
        text=text,
        reply_markup=keyboard_gift_device_tier(),
    )


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def user_blocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, True)
    logger.warning(f'Юзер {event.from_user.id} заблокировал бота')


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, False)
    logger.success(f'Юзер {event.from_user.id} разблокировал бота')


@router.chat_member()
async def handle_chat_member_update(update: ChatMemberUpdated):
    if str(update.chat.id) != str(CHANEL_ID):
        return
    user_id = update.new_chat_member.user.id
    user_dct = await sql.get_user(user_id)

    if not user_dct:
        logger.warning(f"User in chanel {user_id} not found in database")
        return

    if update.old_chat_member.status == "left" and update.new_chat_member.status == "member":
        await sql.update_in_chanel(user_id, True)
        logger.success(f"User {user_id} connect to chanel")
    elif update.old_chat_member.status != "left" and update.new_chat_member.status == "left":
        await sql.update_in_chanel(user_id, False)
        logger.warning(f"User {user_id} left chanel")


@router.inline_query(lambda query: query.query == 'partner')
async def inline_partner(inline_query: InlineQuery):
    user_id = inline_query.from_user.id
    bot_link = partner_bot_link(user_id)
    site_link = partner_site_link(user_id)
    site_line = f'\n🌐 Сайт: {site_link}' if site_link else ''

    text = f'''
Привет! Подключись к <b>ВПН ДЛЯ СВОИХ</b> по моей партнёрской ссылке —
быстрый и надёжный ВПН для своих:

🤖 Бот: {bot_link}{site_line}

💥 Стабильный туннель для работы и личных задач
💫 Удобно для видео и сервисов
👌🏻 Делимся доступом только с близким кругом
    '''

    result = InlineQueryResultArticle(
        id="1",
        title='💸 Партнёрское приглашение',
        description="Друг, перешедший по ссылке, станет вашим партнёрским рефералом.",
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode='HTML',
            disable_web_page_preview=False
        ),
        reply_markup=keyboard_inline_partner(user_id),
        thumb_url="https://img.freepik.com/premium-photo/glowing-blue-neon-wifi-signal-icon-dark-background_989822-6238.jpg?semt=ais_hybrid"  # опционально: иконка
    )

    # Отправляем результат обратно в Telegram
    await bot.answer_inline_query(
        inline_query.id,
        results=[result],
        cache_time=0
    )