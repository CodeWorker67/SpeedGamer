import time
import requests

from bot import sql, x3, bot
from config import CHANEL_ID, ADMIN_IDS, BOT_URL
from lead_tracker import post_user_registered, tracker_source_from_ref_and_stamp
from keyboard import (keyboard_start, keyboard_start_bonus, keyboard_tariff,
                      keyboard_subscription, ref_keyboard, keyboard_gift_tariff,
                      keyboard_payment_method, keyboard_payment_method_stock,
                      keyboard_inline_ref, create_kb, STYLE_PRIMARY)
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, ChatMemberUpdated, InlineQuery, InlineQueryResultArticle, \
    InputTextMessageContent, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import ChatMemberUpdatedFilter, KICKED, MEMBER, Command
from lexicon import buy_text_for_pro_hwid, lexicon, payment_link_pro_for_hwid


router: Router = Router()

PRO_HWID_DEVICE_LIMIT = 5
REFERRER_REF_BONUS_DAYS = 7


# Этот хэндлер срабатывает на команду /start
@router.message(Command(commands="start"))
async def process_start_command(message: Message, command: Command):

    user_data = await sql.get_user(message.from_user.id)
    had_row_before = user_data is not None
    in_panel = False
    ref_login = ''
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
        if 'ref' in message.text:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с реферальной ссылкой')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по реферальной ссылкой')
                ref_login = message.text.split(' ')[1].replace('ref', '')

        elif 'gift_' in message.text:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} пытается активировать подарочную подписку')
            gift_id = message.text.split(' ')[1].replace('gift_', '')
            in_panel = await activate_gift(message, gift_id)
            await asyncio.sleep(2)
            existing = True
        elif 'ttclid_' in message.text:
            if user_data:
                logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} нажал старт повторно с меткой ttclid')
            else:
                logger.success(
                    f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз по метке ttclid')
                stamp = 'YuraTT'
                ttclid = message.text.split(' ')[1].replace('ttclid_', '').replace('_', '.')

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
                stamp = message.text.split(' ')[1]

    if not existing:
        inserted = await sql.add_user(message.from_user.id, False, False, ref=ref_login, stamp=stamp)
        if inserted:
            logger.info(f'Юзер {message.from_user.id} - {message.from_user.username} добавлен в БД')
            src = tracker_source_from_ref_and_stamp(ref_login, stamp)
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
        await message.answer(text=lexicon['start_bonus'],
                             reply_markup=keyboard_start_bonus(),
                             disable_web_page_preview=True)
    else:
        await message.answer(text=lexicon['start'],
                             reply_markup=keyboard_start(),
                             disable_web_page_preview=True)


@router.callback_query(F.data == 'buy_vpn')
async def buy_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    user_data = await sql.get_user(callback.from_user.id)

    buy_txt = buy_text_for_pro_hwid(PRO_HWID_DEVICE_LIMIT)

    await callback.message.answer(
        text=buy_txt,
        reply_markup=keyboard_tariff(),
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == 'connect_vpn')
async def direct_connect_vpn_cb(callback: CallbackQuery):
    await callback.answer()
    # await x3.test_connect()
    user_id = str(callback.from_user.id)
    sub_url = await x3.sublink(user_id)
    sub_url_white = None
    user_data = await sql.get_user(callback.from_user.id)
    if user_data and user_data[10]:
        user_id_white = user_id + '_white'
        sub_url_white = await x3.sublink(user_id_white)

    if not sub_url and not sub_url_white:
        await callback.message.answer(lexicon['no_sub'])
        return

    await callback.message.answer(
        text=lexicon['to_sub'],
        reply_markup=keyboard_subscription(sub_url, sub_url_white),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.in_({
    'r_3', 'r_7', 'r_30', 'r_90', 'r_180', 'r_white_30',
    'r_new_7', 'r_new_30', 'r_new_90', 'r_new_3000',
}))
async def process_payment_method(callback: CallbackQuery):
    await callback.answer()
    ud = await sql.get_user(callback.from_user.id)
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    else:
        text = payment_link_pro_for_hwid(PRO_HWID_DEVICE_LIMIT)
    text += '\n\nВыберите способ оплаты:'
    tariff = callback.data
    await callback.message.answer(text, reply_markup=keyboard_payment_method(tariff))


@router.callback_query(F.data == 'free_vpn')
async def free_vpn_legacy_cb(callback: CallbackQuery):
    """Старые кнопки «бесплатно» в рассылках: ведём на экран покупки подписки."""
    await callback.answer()
    user_data = await sql.get_user(callback.from_user.id)
    in_panel = False
    if user_data is not None and len(user_data) > 4:
        in_panel = user_data[4]
    if in_panel:
        await callback.message.answer(
            text=lexicon['free_vpn_no'],
            reply_markup=keyboard_start(),
        )
        return
    buy_txt = buy_text_for_pro_hwid(PRO_HWID_DEVICE_LIMIT)
    await callback.message.answer(
        text=buy_txt,
        reply_markup=keyboard_tariff(),
        disable_web_page_preview=True,
    )


@router.callback_query(F.data.startswith("trial_gift_"))
async def trial_gift_broadcast_callback(callback: CallbackQuery):
    tail = (callback.data or "")[len("trial_gift_") :]
    if not tail.isdigit():
        await callback.answer("Некорректные данные кнопки.", show_alert=True)
        return
    days = int(tail)
    uid = callback.from_user.id

    ud = await sql.get_user(uid)

    if ud is not None and len(ud) > 26 and ud[26]:
        await callback.answer("Вы уже взяли свой триал!", show_alert=True)
        return

    if ud is None:
        await sql.add_user(uid, False)
        ud = await sql.get_user(uid)

    user_id_str = str(uid)
    hwid_lim = PRO_HWID_DEVICE_LIMIT
    existing_user = await x3.get_user_by_username(user_id_str)
    if existing_user and "response" in existing_user and existing_user["response"]:
        ok = await x3.updateClient(days, user_id_str, uid)
    else:
        ok = await x3.addClient(
            days,
            user_id_str,
            uid,
            hwid_device_limit=hwid_lim,
        )

    if not ok:
        await callback.answer()
        await callback.message.answer(
            "Не удалось начислить дни. Попробуйте позже или напишите в поддержку."
        )
        return

    if await sql.get_user(uid) is not None:
        await sql.update_in_panel(uid)
    else:
        await sql.add_user(uid, True)

    await sql.update_field_bool_3(uid, True)
    await callback.answer()
    await callback.message.answer(
        f"🎉 Поздравляем! Вы получили {days} дней триального доступа к ВПН! ✨🔐",
        reply_markup=create_kb(
            1,
            styles={"connect_vpn": STYLE_PRIMARY},
            connect_vpn="🔗 Подключить VPN",
        ),
    )


@router.callback_query(F.data == "info")
async def info_legacy(callback: CallbackQuery):
    """Старая кнопка «Информация» снята с меню; отвечаем на callback со старых сообщений."""
    await callback.answer()


@router.callback_query(F.data == 'ref')
async def referral_program(callback: CallbackQuery):
    await callback.answer()
    ud = await sql.get_user(callback.from_user.id)
    count = await sql.select_ref_count(int(callback.from_user.id))
    bonus_days = REFERRER_REF_BONUS_DAYS
    await callback.message.answer(
        text=lexicon['ref_info'].format(count, callback.from_user.id, bonus_days),
        reply_markup=ref_keyboard(callback.from_user.id),
        disable_web_page_preview=True
    )


@router.callback_query(F.data == 'buy_gift')
async def gift_subscription_start(callback: CallbackQuery):
    """Начало процесса подарка подписки."""
    await callback.answer()
    ud = await sql.get_user(callback.from_user.id)
    await callback.message.answer(
        lexicon['gift_start'],
        reply_markup=keyboard_gift_tariff(),
    )


@router.callback_query(F.data.startswith('gift_'))
async def process_gift_payment_method(callback: CallbackQuery):
    await callback.answer()
    ud = await sql.get_user(callback.from_user.id)
    if 'white' in callback.data:
        await sql.add_white_counter_if_not_exists(callback.from_user.id)
        text = lexicon['payment_link_white']
    else:
        text = payment_link_pro_for_hwid(PRO_HWID_DEVICE_LIMIT)
    tariff = callback.data
    text += '\n\nВыберите способ оплаты <b>подарочной подписки</b>:'
    await callback.message.answer(text, reply_markup=keyboard_payment_method(tariff))


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

    # Активируем подписку для получателя
    # await x3.test_connect()
    user_id = message.from_user.id
    user_id_str = str(message.from_user.id)
    if white_flag:
        user_id_str += '_white'

    was_in_db = await sql.get_user(message.from_user.id) is not None
    if not was_in_db:
        ref_as_gift = ''
        if gift_giver_id and int(gift_giver_id) != int(user_id):
            ref_as_gift = str(int(gift_giver_id))
        await sql.add_user(message.from_user.id, False, False, ref=ref_as_gift)


    recipient_row = await sql.get_user(message.from_user.id)
    hw_lim = PRO_HWID_DEVICE_LIMIT

    # Проверяем существует ли пользователь
    existing_user = await x3.get_user_by_username(user_id_str)

    if existing_user and 'response' in existing_user and existing_user['response']:
        response = await x3.updateClient(duration, user_id_str, user_id)
    else:
        response = await x3.addClient(
            duration,
            user_id_str,
            user_id,
            hwid_device_limit=None if white_flag else hw_lim,
        )

    if response:
        # Получаем информацию о подписке
        result_active = await x3.activ(user_id_str)
        subscription_time = result_active.get('time', '-')

        # Обновляем базу данных
        await sql.update_in_panel(message.from_user.id)
        if was_in_db:
            logger.info(
                f'Юзер {message.from_user.id} - {message.from_user.username} получил в подарок подписку, уже был в БД')
        else:
            logger.success(
                f'Юзер {message.from_user.id} - {message.from_user.username} зашел в бота в первый раз и получил подарочную подписку')

        # Отправляем сообщение получателю
        await message.answer(lexicon['gift_yes'].format(duration, subscription_time))
        return True

    else:
        await message.answer("❌ Ошибка при активации подарка. Обратитесь в поддержку.")
        if await sql.get_user(message.from_user.id) is None:
            await sql.add_user(message.from_user.id, False)
        return False


@router.callback_query(F.data == 'video_faq')
async def video_faq(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer_video(video='BAACAgIAAxkBAAECpqNpqrDnzrb_G1rcEtZli39lGgVpMAACa5sAAl9MWUmxYN_rJYzPVToE',
                                        caption=lexicon['push_not_subscribed_3h'],
                                        reply_markup=create_kb(1, back_to_main='🔙 Назад'))


@router.callback_query(F.data == 'back_to_buy_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.answer()
    ud = await sql.get_user(callback.from_user.id)
    await callback.message.answer(
        text=buy_text_for_pro_hwid(PRO_HWID_DEVICE_LIMIT),
        reply_markup=keyboard_tariff(),
    )


@router.callback_query(F.data == 'back_to_main')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.answer()
    await callback.message.answer(text=lexicon['start'],
                                  reply_markup=keyboard_start(),
                                  disable_web_page_preview=True)


@router.callback_query(F.data == 'back_to_gift_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обработчик для возврата в главное меню из оплаты"""
    await callback.answer()
    await callback.message.edit_text(
        text=lexicon['gift_start'],
        reply_markup=keyboard_gift_tariff(),
    )


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=KICKED))
async def user_blocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, True)
    logger.warning(f'Юзер {event.from_user.id} заблокировал бота')


@router.my_chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def user_unblocked_bot(event: ChatMemberUpdated):
    await sql.update_delete(event.from_user.id, False)
    logger.success(f'Юзер {event.from_user.id} разблокировал бота')


@router.callback_query(F.data == 'r_120')
async def process_payment_method_bonus(callback: CallbackQuery):
    await callback.answer()
    user_data = await sql.get_user(callback.from_user.id)
    if not user_data:
        return
    if user_data[8]:
        await callback.message.answer('Акция действительна только при первой оплате.',
                                      reply_markup=create_kb(1, back_to_main='🔙 Назад'))
        return
    tariff = callback.data
    await callback.message.answer('Выберите метод оплаты акционной подписки:',
                                  reply_markup=keyboard_payment_method_stock(tariff))


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
    row = await sql.get_user(user_id)

    bonus_days = REFERRER_REF_BONUS_DAYS
    text = f'''
Привет! Подключись к ВПН ДЛЯ СВОИХ по моей ссылке — быстрый и надёжный ВПН для своих:

{BOT_URL}?start=ref{user_id}

За первый платёж приглашённого по ссылке мне начислится +{bonus_days} д. к PRO.

💥 Стабильный туннель для работы и личных задач
💫 Удобно для видео и сервисов
👌🏻 Делимся доступом только с близким кругом
    '''

    result = InlineQueryResultArticle(
        id="1",
        title='🤝🤝🤝 Приглашение',
        description="Друг, перешедший по этой кнопке станет Вашим рефералом.",
        input_message_content=InputTextMessageContent(
            message_text=text,
            parse_mode='HTML',
            disable_web_page_preview=False
        ),
        reply_markup=keyboard_inline_ref(user_id),
        thumb_url="https://img.freepik.com/premium-photo/glowing-blue-neon-wifi-signal-icon-dark-background_989822-6238.jpg?semt=ais_hybrid"  # опционально: иконка
    )

    # Отправляем результат обратно в Telegram
    await bot.answer_inline_query(
        inline_query.id,
        results=[result],
        cache_time=0
    )