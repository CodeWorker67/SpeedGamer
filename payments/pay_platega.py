import aiohttp
from typing import Dict, Optional
from bot import sql
from config import PLATEGA_API_KEY, PLATEGA_MERCHANT_ID, BOT_URL
from logging_config import logger


class PlategaPayment:
    """Класс для работы с Platega.io API"""

    def __init__(self, api_key: str, merchant_id: str):
        self.api_key = api_key
        self.merchant_id = merchant_id
        self.base_url = "https://app.platega.io"
        self.headers = {
            "X-Secret": api_key,
            "X-MerchantId": merchant_id,
            "Content-Type": "application/json"
        }

    async def create_payment(
            self,
            amount: float,
            description: str,
            payment_method: int = 2,
            return_url: str = BOT_URL,
            failed_url: str = BOT_URL,
            payload: Optional[str] = None
    ) -> Dict:
        """Создание платежа через Platega.io"""
        url = f"{self.base_url}/transaction/process"

        data = {
            "paymentMethod": payment_method,
            "paymentDetails": {
                "amount": float(amount),
                "currency": "RUB"
            },
            "description": description,
            "return": return_url,
            "failedUrl": failed_url
        }

        if payload:
            data["payload"] = payload

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=self.headers) as response:
                    response_text = await response.text()

                    if response.status == 200:
                        result = await response.json()

                        return {
                            'status': result.get('status', 'PENDING').lower(),
                            'url': result.get('redirect', ''),
                            'id': result.get('transactionId', ''),
                            'payment_method': result.get('paymentMethod', 'UNKNOWN')
                        }
                    else:
                        logger.error(f"Platega API error {response.status}: {response_text}")
                        raise Exception(f"Ошибка создания платежа: {response.status}")

        except Exception as e:
            logger.error(f"Error creating Platega payment: {e}")
            raise

    async def check_payment(self, transaction_id: str) -> Dict:
        url = f"{self.base_url}/transaction/{transaction_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Platega API check error {response.status}: {response_text}")
                        raise Exception(f"Ошибка проверки платежа: {response.status}")
        except Exception as e:
            logger.error(f"Error checking Platega payment: {e}")
            raise


async def pay(val: str, des: str, user_id: str, duration: str, white: bool, payment_method: int = 2) -> Dict:
    """Создание платежа для совместимости с pay_yoo.py"""

    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:False,method:sbp,amount:{int(val)}"

    try:
        result = await platega.create_payment(
            amount=float(val),
            description=des,
            payment_method=payment_method,
            payload=payload
        )

        # Асинхронная запись платежа
        await sql.add_platega_payment(int(user_id), int(val), result['status'], result['id'], is_gift=False, payload=payload)

        logger.info(f"✅ Platega payment created: {result['status']}")
        logger.info(f"🔗 Payment URL: {result['url']}")
        logger.info(f"🆔 Transaction ID: {result['id']}")

        return result

    except Exception as e:
        logger.error(f"❌ Error creating Platega payment: {e}")
        return {
            'status': 'error',
            'url': '',
            'id': ''
        }


async def pay_for_gift(val: str, des: str, user_id: str, duration: str, white: bool, payment_method: int = 2) -> Dict:
    """Создание платежа для совместимости с pay_yoo.py"""

    platega = PlategaPayment(PLATEGA_API_KEY, PLATEGA_MERCHANT_ID)
    payload = f"user_id:{user_id},duration:{duration},white:{white},gift:True,method:sbp,amount:{int(val)}"

    try:
        result = await platega.create_payment(
            amount=float(val),
            description=des,
            payment_method=payment_method,
            payload=payload
        )

        # Асинхронная запись платежа с флагом подарка
        await sql.add_platega_payment(int(user_id), int(val), result['status'], result['id'], is_gift=True, payload=payload)

        logger.info(f"✅ Platega payment for gift created: {result['status']}")
        logger.info(f"🔗 Payment URL for gift: {result['url']}")
        logger.info(f"🆔 Transaction ID for gift: {result['id']}")

        return result

    except Exception as e:
        logger.error(f"❌ Error creating Platega payment: {e}")
        return {
            'status': 'error',
            'url': '',
            'id': ''
        }
