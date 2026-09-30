import re
import time
import urllib.parse
import requests

from bot import sql, x3, bot
from config import CHANEL_ID, ADMIN_IDS, BOT_URL, CHECKER_ID, PARTNER_PROCENT, PARTNER_MIN, PARTNER_SUPPORT_URL, PUBLIC_SITE_URL, SITE_URL
from lead_tracker import post_user_registered, tracker_source_from_ref_and_stamp
from keyboard import (create_kb, keyboard_start_bonus, ref_keyboard,
                      keyboard_buy_device_tier, keyboard_buy_duration,
                      keyboard_gift_duration_new, keyboard_self_duration_new,
                      keyboard_self_duration_renew, keyboard_self_devices,
                      keyboard_payment_method, keyboard_sub_after_buy,
                      keyboard_inline_ref, STYLE_PRIMARY,
                      keyboard_buy_menu, keyboard_earn_with_us, keyboard_about_service,
                      keyboard_partner_dashboard, keyboard_inline_partner,
                      keyboard_partner_withdraw, OPEN_SITE_CB, ABOUT_SERVICE_CB, BTN_BACK,
                      partner_bot_link, partner_site_link, emoji_button,
                      keyboard_payment_method_promo_120,
                      keyboard_trial_existing_expired,
                      keyboard_subscription_manage,
                      keyboard_sub_after_free)
from utils.menu_ui import (
    MAIN_MENU_BUTTON_TEXT,
    edit_or_send_photo,
    show_main_menu,
    show_connect_screen,
    sync_panel_user_to_db,
    subscription_end_display,
    trial_success_caption,
    trial_existing_active_caption,
    trial_existing_expired_caption,
    end_date_status_text,
)
from wl_traffic.service import get_wl_used_gb_for_user, restore_pro_squads_if_under_limit
from utils.custom_emoji import emojify
from utils.ref_qr import referral_link_qr_png
from web_api import create_bot_site_login_token
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, InlineQuery, InlineQueryResultArticle, \
    InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton, InaccessibleMessage, \
    BufferedInputFile, InputMediaPhoto
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import (
    buy_text_for_pro_hwid,
    lexicon,
    payment_tariff_summary_pro,
    promo_120_payment_caption,
    ru_device_phrase,
    tariff_desc_key_from_payment_callback,
    self_new_duration_caption,
    self_devices_step_caption,
    self_payment_method_caption,
    gift_duration_caption,
)
from datetime import datetime, timezone
from tariff_resolve import (
    panel_username,
    subscription_price_rub,
    SELF_DEVICES_MIN,
    SELF_DEVICES_MAX,
    extra_device_step_rub,
    effective_user_devices,
    bot_tariff_purchase_blocked_reason,
)
from config_bd.utils import pro_subscription_end_active
from handlers.user_profile_sync import (
    SyncTelegramProfileMiddleware,
    tg_profile_fields,
)


router: Router = Router()
_profile_sync_mw = SyncTelegramProfileMiddleware()
router.message.middleware(_profile_sync_mw)
router.callback_query.middleware(_profile_sync_mw)
router.inline_query.middleware(_profile_sync_mw)
router.my_chat_member.middleware(_profile_sync_mw)
router.chat_member.middleware(_profile_sync_mw)

PRO_HWID_DEVICE_LIMIT = 5
REFERRER_REF_BONUS_DAYS = 7
_TRIAL_DAYS = 1
_TRIAL_DEVICE_SLOTS = 5

_NEW_DEVICE_TARIFF_RE = re.compile(r'^r_m(1|3)_d(1[0-5]|[5-9])$')
_DISCONTINUED_TARIFF_CB_RE = re.compile(
    r'^(?:'
    r'r_m(?:6|12)_d(?:3|5|10)|'
    r'r_5000(?:sale)?|'
    r'r_120_d(?:3|10)|'
    r'gift_r_m(?:6|12)_d(?:3|5|10)'
    r')$'
)
_GIFT_DEVICE_TARIFF_RE = re.compile(r'^gift_r_m(1|3)_d5$')
_SELF_DUR_RE = re.compile(r'^self_dur_m(1|3)$')
_SELF_DEV_ADJ_RE = re.compile(r'^self_(inc|dec|go_pay)_m(1|3)_d(1[0-5]|[5-9])$')
_LEGACY_BUY_TIER_RE = re.compile(r'^buy_tier_(3|10)$')
_PROMO_120_TARIFF_RE = re.compile(r'^r_120_d5$')


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
                await _add_user_with_profile(message.from_user, False, False)
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
        inserted = await _add_user_with_profile(
            message.from_user, False, False,
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

    if not had_row_before:
        from handlers.handlers_contest_funnel import schedule_contest_win_funnel

        schedule_contest_win_funnel(message.from_user.id)


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


async def _show_self_devices_step(callback: CallbackQuery, months: int, devices: int) -> None:
    price = subscription_price_rub(months, devices)
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        self_devices_step_caption(months, devices, price),
        keyboard_self_devices(months, devices),
    )


@router.callback_query(F.data == 'buy_vpn_self')
async def buy_vpn_self_cb(callback: CallbackQuery):
    await callback.answer()
    user_obj = await sql.get_user_object_by_user_id(callback.from_user.id)
    if user_obj and pro_subscription_end_active(user_obj.subscription_end_date):
        devices = effective_user_devices(user_obj.devices)
        await edit_or_send_photo(
            callback,
            "buy_subscription",
            f'<b>Продление подписки</b>\n\nКол-во устройств: {devices}',
            keyboard_self_duration_renew(devices),
        )
        return
    main_dev = effective_user_devices(user_obj.devices if user_obj else None)
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        self_new_duration_caption(main_dev),
        keyboard_self_duration_new(),
    )


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    await show_connect_screen(callback)


@router.callback_query(F.data == 'r_120')
async def promo_120_entry(callback: CallbackQuery):
    await callback.answer()
    tariff = 'r_120_d5'
    dk = tariff_desc_key_from_payment_callback(tariff)
    text = promo_120_payment_caption(dk)
    text += '\n\nВыберите способ оплаты:'
    await edit_or_send_photo(
        callback,
        'buy_subscription',
        text,
        keyboard_payment_method_promo_120(tariff),
    )


@router.callback_query(F.data.regexp(_DISCONTINUED_TARIFF_CB_RE))
async def discontinued_tariff_callback(callback: CallbackQuery):
    await callback.answer(lexicon['tariff_discontinued'], show_alert=True)


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
    tariff = callback.data
    dk = tariff_desc_key_from_payment_callback(tariff)
    blocked = bot_tariff_purchase_blocked_reason(dk)
    if blocked:
        await callback.answer(blocked, show_alert=True)
        return
    await callback.answer()
    m = re.fullmatch(r'm(\d+)_d(\d+)', dk)
    if m:
        months, devices = int(m.group(1)), int(m.group(2))
        price = subscription_price_rub(months, devices)
        text = self_payment_method_caption(months, devices, price)
    else:
        text = payment_tariff_summary_pro(dk)
        text += '\n\nВыберите метод оплаты:'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_payment_method(tariff),
    )


@router.callback_query(F.data.regexp(_SELF_DUR_RE))
async def self_duration_chosen(callback: CallbackQuery):
    match = _SELF_DUR_RE.fullmatch(callback.data or "")
    if not match:
        await callback.answer()
        return
    months = int(match.group(1))
    await callback.answer()
    await _show_self_devices_step(callback, months, SELF_DEVICES_MIN)


@router.callback_query(F.data.regexp(_SELF_DEV_ADJ_RE))
async def self_devices_adjust(callback: CallbackQuery):
    match = _SELF_DEV_ADJ_RE.fullmatch(callback.data or '')
    if not match:
        await callback.answer()
        return
    action, months_s, devices_s = match.group(1), match.group(2), match.group(3)
    months, devices = int(months_s), int(devices_s)
    if action == 'go_pay':
        tariff = f'r_m{months}_d{devices}'
        price = subscription_price_rub(months, devices)
        await edit_or_send_photo(
            callback,
            "buy_subscription",
            self_payment_method_caption(months, devices, price),
            keyboard_payment_method(tariff),
        )
        await callback.answer()
        return
    step = extra_device_step_rub(months)
    if action == 'inc':
        if devices >= SELF_DEVICES_MAX:
            await callback.answer()
            return
        devices += 1
    else:
        if devices <= SELF_DEVICES_MIN:
            await callback.answer()
            return
        devices -= 1
    await callback.answer()
    await _show_self_devices_step(callback, months, devices)


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_cb(callback: CallbackQuery):
    """Триал: 1 день, 5 устройств, 2 GB антиглушилки (как Zoomer)."""
    day = _TRIAL_DAYS
    uid = callback.from_user.id

    user_data = await sql.get_user(uid)
    in_panel = bool(user_data and len(user_data) > 4 and user_data[4])
    if in_panel:
        await callback.answer()
        await show_main_menu(callback)
        return

    if user_data is None:
        await sql.add_user(uid, False)
    await sql.update_field_bool_3(uid, True)

    user_id_str = panel_username(uid, white=False, device_slots=_TRIAL_DEVICE_SLOTS)
    ok = await x3.addClient(
        day,
        user_id_str,
        uid,
        hwid_device_limit=_TRIAL_DEVICE_SLOTS,
    )
    if not ok:
        await sql.update_field_bool_3(uid, False)
        if not await sync_panel_user_to_db(uid):
            await callback.answer(
                "Не удалось активировать тест. Попробуйте позже или напишите в поддержку.",
                show_alert=True,
            )
            return

        user_data = await sql.get_user(uid)
        sub_url = await x3.sublink(user_id_str)
        if user_data and pro_subscription_end_active(user_data[9]):
            end_time = await subscription_end_display(uid)
            caption = trial_existing_active_caption(end_time, sub_url or "—")
            kb = keyboard_subscription_manage(show_add_devices=False)
        else:
            status = end_date_status_text(user_data[9] if user_data else None)
            expired_date = status.replace("Истекла ", "")
            caption = trial_existing_expired_caption(expired_date)
            kb = keyboard_trial_existing_expired()
        await callback.answer()
        await edit_or_send_photo(callback, "subscription_manage", caption, kb)
        return

    if await sql.get_user(uid) is not None:
        await sql.update_in_panel(uid)
    else:
        await _add_user_with_profile(callback.from_user, True)

    result_active = await x3.activ(user_id_str)
    subscription_time = result_active.get("time", "-")
    if subscription_time != "-":
        try:
            subscription_end_date = datetime.strptime(subscription_time, "%d-%m-%Y %H:%M МСК")
            await sql.update_subscription_end_date(uid, subscription_end_date)
        except ValueError as e:
            logger.error(f"free_vpn: ошибка парсинга даты для {uid}: {e}")

    await sql.init_wl_trial_limits(uid)
    await sql.update_user_devices(uid, _TRIAL_DEVICE_SLOTS)
    trafic_wl, limit_wl = await sql.get_wl_limits(uid)
    used_gb = await get_wl_used_gb_for_user(x3, uid, trafic_wl)
    await restore_pro_squads_if_under_limit(x3, uid, used_gb, limit_wl)

    sub_url = await x3.sublink(user_id_str)
    end_time = await subscription_end_display(uid)
    await callback.answer()
    await edit_or_send_photo(
        callback,
        "subscription_manage",
        trial_success_caption(end_time, sub_url or "—", _TRIAL_DEVICE_SLOTS),
        keyboard_sub_after_free(sub_url) if sub_url else keyboard_trial_existing_expired(),
    )
    logger.info(f"free_vpn: триал активирован user={uid} days={day} devices={_TRIAL_DEVICE_SLOTS}")


@router.callback_query(F.data.regexp(_LEGACY_BUY_TIER_RE))
async def buy_tier_legacy_disabled(callback: CallbackQuery):
    await callback.answer(lexicon['tariff_legacy_slots'], show_alert=True)


@router.callback_query(F.data == 'buy_tier_5')
async def buy_tier_5_legacy(callback: CallbackQuery):
    await callback.answer()
    await buy_vpn_self_cb(callback)


@router.callback_query(F.data == 'back_buy_tier')
async def buy_back_to_tier(callback: CallbackQuery):
    await callback.answer()
    await buy_vpn_self_cb(callback)


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
        await _add_user_with_profile(callback.from_user, True)

    result_active = await x3.activ(user_id_str)
    subscription_time = result_active.get("time", "-")
    if subscription_time != "-":
        try:
            subscription_end_date = datetime.strptime(subscription_time, "%d-%m-%Y %H:%M МСК")
            await sql.update_subscription_end_date(uid, subscription_end_date)
        except ValueError as e:
            logger.error(f"{log_prefix}: ошибка парсинга даты для {uid}: {e}")

    await sql.init_wl_trial_limits(uid)
    await sql.update_user_devices(uid, _TRIAL_DEVICE_SLOTS)
    trafic_wl, limit_wl = await sql.get_wl_limits(uid)
    used_gb = await get_wl_used_gb_for_user(x3, uid, trafic_wl)
    await restore_pro_squads_if_under_limit(x3, uid, used_gb, limit_wl)

    sub_link = await x3.sublink(user_id_str)
    text = lexicon["trial_success"].format(
        subscription_time, days, sub_link, ru_device_phrase(_TRIAL_DEVICE_SLOTS)
    )
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


@router.callback_query(F.data == 'ref_show_qr')
async def ref_show_qr_cb(callback: CallbackQuery):
    await callback.answer()
    uid = int(callback.from_user.id)
    message = callback.message
    if message is None or isinstance(message, InaccessibleMessage) or not message.photo:
        return

    base = (BOT_URL or "").rstrip("/")
    ref_url = f"{base}?start=ref{uid}"
    qr_bytes = referral_link_qr_png(ref_url)
    count = await sql.select_ref_count(uid)
    bonus_days = REFERRER_REF_BONUS_DAYS
    caption = emojify(lexicon['ref_info'].format(count, uid, bonus_days))

    try:
        await message.edit_media(
            media=InputMediaPhoto(
                media=BufferedInputFile(qr_bytes, filename="ref_qr.png"),
                caption=caption,
                parse_mode="HTML",
            ),
            reply_markup=ref_keyboard(uid, show_qr=False),
        )
    except TelegramBadRequest as e:
        logger.warning("ref_show_qr: edit_media failed uid={}: {}", uid, e)


async def _add_user_with_profile(from_user, in_panel: bool, is_connect: bool = False, **kwargs) -> bool:
    username, fullname = tg_profile_fields(from_user)
    return await sql.add_user(
        from_user.id,
        in_panel,
        is_connect,
        username=username,
        fullname=fullname,
        **kwargs,
    )


async def _ensure_user_exists(user_id: int, from_user=None) -> None:
    if await sql.get_user(user_id) is None:
        if from_user is not None:
            await _add_user_with_profile(from_user, False, False)
        else:
            await sql.add_user(user_id, False, False)


async def _partner_dashboard_caption(tg_id: int) -> str:
    user = await sql.get_user_object_by_user_id(tg_id)
    if user is None:
        await _ensure_user_exists(tg_id)
        user = await sql.get_user_object_by_user_id(tg_id)
    referrals = await sql.select_partner_count(tg_id)
    payments_sum = await sql.select_partner_referrals_payments_sum(tg_id)
    balance = (user.partner_balance or 0) if user else 0
    paid_out = (user.partner_pay or 0) if user else 0
    total_earned = balance + paid_out
    bot_link = partner_bot_link(tg_id)
    site_link = partner_site_link(tg_id)
    site_block = (
        f'🌐 <b>Сайт:</b>\n└ <code>{site_link}</code>\n\n'
        if site_link
        else ''
    )
    return lexicon['partner_dashboard'].format(
        bot_link=bot_link,
        site_block=site_block,
        procent=PARTNER_PROCENT,
        min_sum=PARTNER_MIN,
        referrals=referrals,
        payments_sum=payments_sum,
        total_earned=total_earned,
        paid_out=paid_out,
        balance=balance,
    )


async def _edit_message_qr_photo(
    callback: CallbackQuery,
    qr_url: str,
    caption: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    message = callback.message
    if message is None or isinstance(message, InaccessibleMessage) or not message.photo:
        return
    qr_bytes = referral_link_qr_png(qr_url)
    try:
        await message.edit_media(
            media=InputMediaPhoto(
                media=BufferedInputFile(qr_bytes, filename="partner_qr.png"),
                caption=caption,
                parse_mode="HTML",
            ),
            reply_markup=reply_markup,
        )
    except TelegramBadRequest as e:
        logger.warning("partner QR edit_media failed uid={}: {}", callback.from_user.id, e)


async def _send_partner_dashboard(callback: CallbackQuery) -> None:
    tg_id = callback.from_user.id
    caption = await _partner_dashboard_caption(tg_id)
    await edit_or_send_photo(
        callback,
        "earn_with_us",
        caption,
        keyboard_partner_dashboard(tg_id),
    )


@router.callback_query(F.data == 'partner_earn')
async def partner_program(callback: CallbackQuery):
    await callback.answer()
    await _ensure_user_exists(callback.from_user.id, callback.from_user)
    await _send_partner_dashboard(callback)


@router.callback_query(F.data == 'partner_qr_bot')
async def partner_qr_bot_cb(callback: CallbackQuery):
    await callback.answer()
    uid = int(callback.from_user.id)
    caption = emojify(await _partner_dashboard_caption(uid))
    has_site = bool(partner_site_link(uid))
    await _edit_message_qr_photo(
        callback,
        partner_bot_link(uid),
        caption,
        keyboard_partner_dashboard(uid, show_bot_qr=False, show_site_qr=has_site),
    )


@router.callback_query(F.data == 'partner_qr_site')
async def partner_qr_site_cb(callback: CallbackQuery):
    await callback.answer()
    uid = int(callback.from_user.id)
    site_url = partner_site_link(uid)
    if not site_url:
        await callback.answer("Ссылка на сайт не настроена", show_alert=True)
        return
    caption = emojify(await _partner_dashboard_caption(uid))
    await _edit_message_qr_photo(
        callback,
        site_url,
        caption,
        keyboard_partner_dashboard(uid, show_bot_qr=True, show_site_qr=False),
    )


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
    text = f'{lexicon["gift_start"]}\n\n{gift_duration_caption()}'
    await edit_or_send_photo(
        callback,
        "buy_subscription",
        text,
        keyboard_gift_duration_new(),
    )


@router.callback_query(F.data.regexp(r'^gift_tier_(3|10)$'))
async def gift_tier_legacy_disabled(callback: CallbackQuery):
    await callback.answer(lexicon['tariff_legacy_slots'], show_alert=True)


@router.callback_query(F.data == 'gift_tier_5')
async def gift_tier_5_legacy(callback: CallbackQuery):
    await gift_subscription_start(callback)


@router.callback_query(F.data == 'gift_back_tier')
async def gift_back_to_tier(callback: CallbackQuery):
    await gift_subscription_start(callback)


@router.callback_query(F.data.regexp(_GIFT_DEVICE_TARIFF_RE))
async def process_gift_payment_method(callback: CallbackQuery):
    tariff = callback.data
    dk = tariff_desc_key_from_payment_callback(tariff)
    blocked = bot_tariff_purchase_blocked_reason(dk)
    if blocked:
        await callback.answer(blocked, show_alert=True)
        return
    await callback.answer()
    m = re.fullmatch(r'm(\d+)_d5', dk)
    if m:
        months = int(m.group(1))
        price = subscription_price_rub(months, 5)
        text = self_payment_method_caption(months, 5, price)
    else:
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
            await _add_user_with_profile(message.from_user, False)
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
        await _add_user_with_profile(message.from_user, False, False, ref=ref_as_gift)

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
                    if device_slots >= SELF_DEVICES_MIN:
                        await sql.update_user_devices(user_id, device_slots)
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
            await _add_user_with_profile(message.from_user, False)
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
    text = f'{lexicon["gift_start"]}\n\n{gift_duration_caption()}'
    await callback.message.edit_text(
        text=text,
        reply_markup=keyboard_gift_duration_new(),
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