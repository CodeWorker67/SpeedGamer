import random
from datetime import datetime

from bot import sql, x3
from config import ADMIN_IDS
from logging_config import logger
import asyncio
from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command

from sheduler.check_connect import check_connect

router = Router()


@router.message(F.video, F.from_user.id.in_(ADMIN_IDS))
async def get_video(message: Message):
    await message.answer(message.video.file_id)


@router.message(Command(commands=['user']))
async def user_info(message: Message):

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /user <telegram_id>\nНапример: /user 123456789")
            return

        user_id = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.get_user(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return
        text = []
        for i in range(len(user_data)):
            if isinstance(user_data[i], datetime):
                item = user_data[i].strftime('%Y-%m-%d %H:%M:%S')
                text.append(item)
            elif user_data[i] is None:
                text.append('None')
            else:
                text.append(str(user_data[i]))
        text = '\n'.join(text)
        await message.answer(text)
    except Exception as e:
        await message.answer(f'Ошибка при формировании сообщения: {str(e)}')


@router.message(Command(commands=['sub']))
async def set_subscription_date(message: Message):
    """Установка subscription_end_date или white_subscription_end_date в БД (только БД, не в панели)"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Эта команда доступна только администраторам.")
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 3:
            await message.answer(
                "❌ Использование:\n"
                "  /sub <telegram_id> <дата_время>               – обновить обычную подписку\n"
                "  /sub <telegram_id> white <дата_время>         – обновить белую подписку\n"
                "Примеры:\n"
                "  /sub 123456789 2026-02-01 17:14:27\n"
                "  /sub 123456789 white 2026-02-01 17:14:27\n"
                "Формат даты: YYYY-MM-DD HH:MM:SS"
            )
            return

        user_id = int(args[1].strip())

        # Определяем тип обновляемого поля
        if args[2].lower() == 'white':
            field_type = 'white'
            # Дата начинается с третьего аргумента
            date_str = " ".join(args[3:])
        else:
            field_type = 'regular'
            # Дата начинается со второго аргумента
            date_str = " ".join(args[2:])

        # Парсим дату и время
        try:
            date_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%d.%m.%Y %H:%M:%S",
                "%d.%m.%Y %H:%M"
            ]

            subscription_date = None
            for date_format in date_formats:
                try:
                    subscription_date = datetime.strptime(date_str, date_format)
                    break
                except ValueError:
                    continue

            if subscription_date is None:
                await message.answer(
                    f"❌ Неверный формат даты: {date_str}\n"
                    "Используйте формат: YYYY-MM-DD HH:MM:SS\n"
                    "Пример: 2026-02-01 17:14:27"
                )
                return

        except ValueError as e:
            await message.answer(f"❌ Ошибка парсинга даты: {e}")
            return

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.get_user(user_id)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе данных.")
            return

        # Обновляем соответствующую дату в БД
        try:
            if field_type == 'white':
                await sql.update_white_subscription_end_date(user_id, subscription_date)
                # Получаем обновлённое значение для проверки (white_subscription_end_date — индекс 10)
                updated_date = user_data[10] if len(user_data) > 10 else None
                field_name = "white_subscription_end_date"
            else:
                await sql.update_subscription_end_date(user_id, subscription_date)
                updated_date = await sql.get_subscription_end_date(user_id)
                field_name = "subscription_end_date"

            await message.answer(
                f"✅ Дата подписки ({field_name}) успешно обновлена!\n\n"
                f"👤 Пользователь: {user_id}\n"
                f"📅 Новая дата окончания: {subscription_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📝 Проверка из БД: {updated_date.strftime('%Y-%m-%d %H:%M:%S') if updated_date else 'Ошибка чтения'}\n\n"
                f"⚠️ Внимание: Изменена только дата в БД. Подписка в панели управления (X3) не изменена."
            )

            logger.info(
                f"Администратор {message.from_user.id} изменил {field_name} для пользователя {user_id} на {subscription_date}")
        except Exception as e:
            await message.answer(f"❌ Ошибка при обновлении даты в БД: {str(e)}")
            logger.error(f"Ошибка update_subscription_end_date: {e}")

    except ValueError:
        await message.answer(
            "❌ Неверный формат Telegram ID или даты.\n"
            "Используйте: /sub 123456789 2026-02-01 17:14:27\n"
            "Или: /sub 123456789 white 2026-02-01 17:14:27"
        )
    except Exception as e:
        logger.error(f"Ошибка в команде /sub: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")


@router.message(Command(commands=['delete']))
async def delete_user_command(message: Message):
    """Удаление пользователя из БД по Telegram ID"""

    # Проверка прав администратора
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        # Извлекаем аргументы команды
        args = message.text.split()

        if len(args) < 2:
            await message.answer("❌ Использование: /delete <telegram_id>\nНапример: /delete 123456789")
            return

        user_id_to_delete = int(args[1].strip())

        # Проверяем, существует ли пользователь в БД
        user_data = await sql.get_user(user_id_to_delete)

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {user_id_to_delete} не найден в базе данных.")
            return

        # Получаем информацию о пользователе для уведомления
        user_info = {
            "user_id": user_data[1],  # user_id
            "ref": user_data[2],  # ref
            "in_panel": user_data[4],  # in_panel
            "in_chanel": user_data[7] if len(user_data) > 7 else False  # in_chanel
        }

        # УДАЛЯЕМ ПОЛЬЗОВАТЕЛЯ ИЗ БД
        deletion_success = await sql.delete_from_db(user_id_to_delete)

        if deletion_success:
            # Логируем действие
            logger.info(f"Администратор {message.from_user.id} удалил пользователя {user_id_to_delete} из БД")

            # Формируем отчет об удалении
            report_message = (
                f"✅ Пользователь успешно удалён из базы данных\n\n"
                f"📋 Информация об удалённом пользователе:\n"
                f"├ ID: {user_info['user_id']}\n"
                f"├ Реферер: {user_info['ref'] if user_info['ref'] else 'нет'}\n"
                f"└ Брал ключ: {'✅ да' if user_info['in_panel'] else '❌ нет'}\n"
                f"⚠️ Пользователь удалён только из базы данных бота.\n"
                f"   Подписка в панели управления (X3) остаётся активной.\n"
                f"   Чтобы удалить полностью, используйте команду /gift на 0 дней."
            )

            await message.answer(report_message)

        else:
            await message.answer(f"❌ Ошибка при удалении пользователя {user_id_to_delete}.\n"
                                 "Возможно, пользователь уже был удалён или произошла ошибка базы данных.")

    except ValueError:
        await message.answer("❌ Неверный формат Telegram ID.\n"
                             "Используйте только цифры, например: /delete 123456789")
    except Exception as e:
        logger.error(f"Ошибка в команде /delete: {e}")
        await message.answer(f"❌ Произошла ошибка при выполнении команды: {str(e)}")


@router.message(Command("online"))
async def check_online(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()

    active_telegram_ids = []
    for user in users_x3:
        if user['userTraffic']['firstConnectedAt']:
            connected_str = user['userTraffic']['onlineAt']
            try:
                connected_dt = datetime.fromisoformat(connected_str.replace('Z', '+00:00'))
                connected_date = connected_dt.date()
                if connected_date == datetime.now().date():
                    telegram_id = user.get('telegramId')
                    if telegram_id is not None:
                        active_telegram_ids.append(int(telegram_id))
            except (ValueError, TypeError):
                continue

    count_pay = 0
    count_trial = 0
    for tg_id in active_telegram_ids:
        end_date = await sql.get_subscription_end_date(tg_id)
        if end_date is not None:
            days_left = (end_date.date() - datetime.now().date()).days
            if days_left > 5:
                count_pay += 1
            else:
                count_trial += 1
    await message.answer(
        f"Всего юзеров в панели: {len(users_x3)}\n"
        f"Юзеров, которые были онлайн сегодня: {len(active_telegram_ids)}\n"
        f"Юзеры с платной подпиской: {count_pay}\n"
        f"Юзеры на триале: {count_trial}"
    )


@router.message(Command("balance_panel"))
async def check_online(message: Message):
    squad_1 = ['494bf6ce-d62b-4929-a980-dfc14b8b5ddb']
    squad_2 = ['2e6f13b9-58a0-4f46-bd76-0d294f00ef18']
    success_count = 0
    fail_count = 0
    if message.from_user.id not in ADMIN_IDS:
        return
    users_x3 = await x3.get_all_users()
    for user in users_x3:
        await asyncio.sleep(0.3)
        random_squad = random.choice([squad_1, squad_2])
        username = user.get('username', '')
        if 'white' not in username and 'cascade-bridge-system' not in username:
            uuid = user.get('uuid')
            connect = user.get('firstConnectedAt')
            if uuid and connect:
                if await x3.update_user_squads(uuid, random_squad):
                    success_count += 1
                else:
                    fail_count += 1
    await message.answer(f"{len(users_x3)} - всего юзеров в панели\n{success_count + fail_count} - подключенных\n{success_count} - обновлено\n{fail_count} - ошибка")


@router.message(Command(commands=['check_connect']))
async def force_check_connect_command(message: Message):
    """Принудительная проверка подключённых пользователей и обновление Is_tarif в БД"""
    if message.from_user.id not in ADMIN_IDS:
        return

    await message.answer("🔄 Запускаю проверку подключений всех пользователей...")
    try:
        await check_connect()  # функция уже содержит логику обновления is_connect
        await message.answer("✅ Проверка завершена. Подробности в логах.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении force_check_connect: {e}")
        await message.answer(f"❌ Произошла ошибка: {e}")
