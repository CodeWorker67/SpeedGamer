from bot import bot, sql
from config import PLATEGA_API_KEY, PLATEGA_MERCHANT_ID
from logging_config import logger
from payments.process_payload import process_confirmed_payment
from keyboard import keyboard_payment_cancel
from lexicon import lexicon
from payments.pay_platega import PlategaPayment


async def check_platega():
    """Проверка статуса платежей Platega и их обработка"""

    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)

    try:
        # Получаем все платежи со статусом 'pending'
        pending_payments = await sql.get_pending_platega_payments()

        if not pending_payments:
            logger.info("✅ Нет платежей со статусом 'pending' для проверки")
            return

        logger.info(f"🔍 Найдено {len(pending_payments)} платежей со статусом 'pending'")

        processed_count = 0
        confirmed_count = 0
        canceled_count = 0

        for payment in pending_payments:
            try:
                transaction_id = payment.transaction_id

                # Проверяем статус платежа через Platega API
                result = await platega.check_payment(transaction_id)

                if result:
                    new_status = result.get('status', '').lower()

                    # Если статус изменился
                    if new_status != payment.status and new_status:
                        await sql.update_payment_status(transaction_id, new_status)

                        logger.info(f"🔄 Статус платежа {transaction_id} обновлен: {payment.status} → {new_status}")

                        # Если статус 'confirmed', обрабатываем платеж
                        if new_status == 'confirmed':
                            await process_confirmed_payment_platega(payment, result)
                            confirmed_count += 1
                        else:
                            canceled_count += 1
                            logger.debug(f"ℹ️ Статус платежа {transaction_id} изменился: {new_status}")
                            if new_status == 'canceled':
                                user_id = payment.user_id
                                cancel_text = lexicon['payment_cancel']
                                await bot.send_message(user_id, cancel_text, reply_markup=keyboard_payment_cancel())

                    else:
                        logger.debug(f"ℹ️ Статус платежа {transaction_id} не изменился: {new_status}")
                    processed_count += 1

            except Exception as e:
                logger.error(f"❌ Ошибка при проверке платежа {payment.transaction_id}: {e}")

        logger.info(f"✅ Проверено платежей: {processed_count}, подтверждено: {confirmed_count}, отменено: {canceled_count}")

    except Exception as e:
        logger.error(f"❌ Ошибка в функции check_platega: {e}")
    finally:
        await bot.session.close()


async def process_confirmed_payment_platega(payment, platega_data):
    """Обработка подтвержденного платежа (аналогично webhook_platega)"""
    # Проверяем, есть ли payload в платеже
    payload = platega_data.get('payload', '')

    if not payload:
        logger.error(f"❌ Нет payload в платеже {payment.transaction_id}")
        return

    await process_confirmed_payment(payload)
