import asyncio
from datetime import datetime
from aiogram import Bot

from bot import sql
from keyboard import keyboard_tariff
from lexicon import lexicon
from logging_config import logger


async def send_message_cron(bot: Bot):
    all_users = await sql.select_all_users()
    sent_count_7 = 0
    sent_count_3 = 0
    sent_count_1 = 0
    sent_count_0 = 0
    sent_count_week = 0
    failed_count = 0
    for user_id in all_users:
        end_date = None  # Инициализация переменной перед блоком try
        try:
            # Если метод get_subscription_end_date не асинхронный, убираем await
            end_date = await sql.get_subscription_end_date(user_id)
            if end_date:
                if isinstance(end_date, datetime):
                    end_date = end_date.date()  # Приводим к типу date, если это datetime
                today = datetime.now().date()  # Приводим текущую дату и время к типу date
                days_left = (end_date - today).days
                if days_left == 7 and not await sql.notification_sent_today(user_id):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_7'], reply_markup=keyboard_tariff())
                    await asyncio.sleep(0.05)
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_7 += 1
                elif days_left == 3 and not await sql.notification_sent_today(user_id):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_3'], reply_markup=keyboard_tariff())
                    await asyncio.sleep(0.05)
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_3 += 1
                elif days_left == 1 and not await sql.notification_sent_today(user_id):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_1'], reply_markup=keyboard_tariff())
                    await asyncio.sleep(0.05)
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_1 += 1
                elif days_left == 0 and not await sql.notification_sent_today(user_id):
                    await bot.send_message(chat_id=user_id, text=lexicon['push_0'], reply_markup=keyboard_tariff())
                    await asyncio.sleep(0.05)
                    await sql.mark_notification_as_sent(user_id)
                    sent_count_0 += 1
                elif days_left < 0:
                    last_notification_date = await sql.get_last_notification_date(user_id)
                    if last_notification_date:
                        if isinstance(last_notification_date, datetime):
                            last_notification_date = last_notification_date.date()  # Приводим к типу date
                    # Проверяем, прошло ли 3 дней с момента последнего уведомления
                    if not last_notification_date or (today - last_notification_date).days >= 3:
                        await bot.send_message(chat_id=user_id, text=lexicon['push_off'], reply_markup=keyboard_tariff())
                        await asyncio.sleep(0.05)
                        await sql.mark_notification_as_sent(user_id)
                        sent_count_week += 1
        except Exception as e:
            failed_count += 1
            await sql.update_delete(user_id, True)

    await bot.send_message(1012882762, f'''
Рассылка об окончании подписки:
за 7 дней: {sent_count_7}
за 3 дня: {sent_count_3}
за 1 день: {sent_count_1}
за 0 дней: {sent_count_0}
после окончания каждые 3 дня: {sent_count_week}

Не получилось: {failed_count}
''')
    # Выводим обобщенную информацию в консоль
    logger.info(f"Уведомлений отправлено: {sent_count_7 + sent_count_3 + sent_count_1 + sent_count_0 +sent_count_week}")
    logger.info(f"Не удалось отправить уведомления: {failed_count}")
