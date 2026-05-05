from datetime import datetime

from bot import x3, sql, bot

from friends_vpn import pro_hwid_device_limit_for_user_row, referrer_ref_bonus_days
from keyboard import create_kb, keyboard_sub_after_buy
from lead_tracker import post_payment_success, post_user_trial
from lexicon import lexicon
from logging_config import logger

async def process_confirmed_payment(payload):
    """Обработка подтвержденного платежа"""
    try:
        # Парсим payload
        payload_parts = dict(item.split(':') for item in payload.split(','))
        user_id = int(payload_parts.get('user_id', 0))
        duration = int(payload_parts.get('duration', 0))
        white_flag = payload_parts.get('white', 'False') == 'True'
        is_gift = payload_parts.get('gift', 'False') == 'True'
        method = payload_parts.get('method', '')
        if method in ('sbp', 'stars', 'card', 'crypto', 'cryptobot', 'wata_sbp', 'wata_card'):
            amount = int(payload_parts.get('amount', 0))
        else:
            amount = float(payload_parts.get('amount', 0.0))

        logger.info(
            f"Обработка подтвержденного платежа для user={user_id}, duration={duration}, white={white_flag}, "
            f"gift={is_gift}, method={method}, amount={amount}")

        # Определяем валюту для сообщения
        if method in ['sbp', 'card', 'crypto', 'cryptobot', 'wata_sbp', 'wata_card']:
            currency = 'руб'
        elif method == 'stars':
            currency = '⭐️'
            # Записываем платёж Stars
            await sql.add_payment_stars(user_id, amount, is_gift, payload)
        elif method in ('ton', 'usdt'):
            currency = method.upper()
            # Платежи крипто уже записаны в payments_cryptobot при создании счета, здесь только обработка
        else:
            currency = ''

        if is_gift:
            # Обработка подарка
            gift_id = await sql.create_gift(user_id, duration, white_flag)
            await post_payment_success(user_id, method, amount)

            # Отправляем сообщение с ссылкой на подарок
            marker = ' (мобильный тариф)' if white_flag else ''
            gift_message = lexicon['payment_gift'].format(duration, marker, gift_id)

            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=gift_message,
                    disable_web_page_preview=True
                )

                # Второе сообщение с инструкцией
                await bot.send_message(
                    chat_id=user_id,
                    text=lexicon['payment_gift_faq'],
                    reply_markup=create_kb(1, back_to_main='🔙 Назад')
                )

                logger.info(f"✅ Сообщения о подарке отправлены пользователю {user_id}")

            except Exception as e:
                logger.error(f"❌ Ошибка отправки сообщения о подарке: {e}")

        else:
            # Обработка обычного платежа (не подарок)
            # await x3.test_connect()
            user_id_str = str(user_id)
            if white_flag:
                user_id_str += '_white'

            # Пробный «3 дня» — в payload всегда duration=3 (ключ тарифа «3»); сумма в поле amount
            # у Stars — это XTR, у крипты — иначе, поэтому триал не привязываем к рублям в payload.
            is_paid_trial = not white_flag and duration == 3

            existing_user = await x3.get_user_by_username(user_id_str)
            payer_row = await sql.get_user(user_id)
            hwid_lim = pro_hwid_device_limit_for_user_row(payer_row)

            if existing_user and 'response' in existing_user and existing_user['response']:
                logger.info(f"⏫ Обновляем {user_id_str} на {duration} дней")
                response = await x3.updateClient(duration, user_id_str, user_id)
            else:
                logger.info(f"➕ Добавляем {user_id_str} на {duration} дней")
                response = await x3.addClient(
                    duration,
                    user_id_str,
                    user_id,
                    hwid_device_limit=None if white_flag else hwid_lim,
                )

            if not response:
                logger.error(f"❌ Не удалось обновить клиента {user_id_str}")
                return

            result_active = await x3.activ(user_id_str)
            subscription_time = result_active.get('time', '-')

            # Обновляем дату окончания подписки в БД
            if subscription_time != '-':
                try:
                    subscription_end_date = datetime.strptime(subscription_time, '%d-%m-%Y %H:%M МСК')
                    if white_flag:
                        await sql.update_white_subscription_end_date(user_id, subscription_end_date)
                    else:
                        await sql.update_subscription_end_date(user_id, subscription_end_date)
                    logger.info(f"✅ Дата подписки обновлена: {subscription_end_date}")
                except ValueError as e:
                    logger.error(f"❌ Ошибка парсинга даты: {e}")

            # Реферальная система (не за пробный 3 дня / r_3)
            try:
                user_data = await sql.get_user(user_id)
                if user_data and len(user_data) > 4:
                    # reserve_field[8]: после первой «полной» оплаты True — не начислять рефералку повторно
                    referral_gate_done = user_data[8]
                    ref_id_str = user_data[2]

                    if is_paid_trial:
                        logger.info(f"Рефералка не начисляется: пробная оплата 3 дня user={user_id}")
                    if not referral_gate_done and ref_id_str and not is_paid_trial:
                        try:
                            ref_id = int(ref_id_str)
                            ref_data = await sql.get_user(ref_id)

                            if ref_data and len(ref_data) > 4:
                                referrer_in_panel = ref_data[4]

                                if referrer_in_panel:
                                    bonus_days = referrer_ref_bonus_days(ref_data)
                                    logger.info(f"🎁 Начисляем {bonus_days} дней рефереру {ref_id} за приглашение")

                                    # await x3.test_connect()
                                    ref_existing = await x3.get_user_by_username(str(ref_id))

                                    if ref_existing and 'response' in ref_existing and ref_existing['response']:
                                        await x3.updateClient(bonus_days, str(ref_id), ref_id)
                                        logger.info(f"✅ Обновлена подписка реферера {ref_id} на {bonus_days} дней")

                                    ref_result_active = await x3.activ(str(ref_id))
                                    ref_subscription_time = ref_result_active.get('time', '-')

                                    if ref_subscription_time != '-':
                                        try:
                                            ref_subscription_end_date = datetime.strptime(ref_subscription_time,
                                                                                          '%d-%m-%Y %H:%M МСК')
                                            await sql.update_subscription_end_date(ref_id, ref_subscription_end_date)
                                            logger.info(f"✅ Дата подписки реферера обновлена")
                                        except ValueError as e:
                                            logger.error(f"❌ Ошибка парсинга даты реферера: {e}")

                                    try:
                                        await bot.send_message(
                                            chat_id=ref_id,
                                            text=lexicon['ref_success'].format(user_id, bonus_days),
                                            reply_markup=create_kb(1, back_to_main='🔙 Назад')
                                        )
                                        logger.info(f"✅ Уведомление отправлено рефереру {ref_id}")
                                    except Exception as e:
                                        logger.error(f"❌ Ошибка отправки уведомления рефереру: {e}")

                        except (ValueError, Exception) as e:
                            logger.error(f"❌ Ошибка при обработке реферальной системы: {e}")
            except Exception as e:
                logger.error(f"❌ Ошибка при проверке реферальной системы: {e}")

            # Обновляем статус оплаты в БД users
            if await sql.get_user(user_id) is not None:
                await sql.update_in_panel(user_id)
            else:
                await sql.add_user(user_id, True)
            # Пробный 10 ₽ не помечаем reserve_field — иначе первая «нормальная» оплата не даст бонус рефереру
            if not is_paid_trial:
                await sql.update_reserve_field(user_id)

            if is_paid_trial:
                await post_user_trial(user_id)
            else:
                await post_payment_success(user_id, method, amount)

            # Отправляем уведомление пользователю
            try:
                sub_link = await x3.sublink(user_id_str)
                marker = 'продлена' if existing_user else 'активирована'
                message_text = lexicon['payment_success'].format(marker, subscription_time, amount, currency, duration, sub_link)

                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=keyboard_sub_after_buy(sub_link)
                )

                logger.info(f"✅ Уведомление отправлено пользователю {user_id}")

            except Exception as e:
                logger.error(f"❌ Ошибка отправки уведомления: {e}")

    except Exception as e:
        logger.error(f"❌ Ошибка обработки подтвержденного платежа: {e}")
