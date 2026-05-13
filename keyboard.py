import urllib.parse
from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import CHANEL_URL, BOT_URL
from lexicon import dct_desc

BTN_BACK = "🔙 Назад"

STYLE_PRIMARY = "primary"
STYLE_SUCCESS = "success"
STYLE_DANGER = "danger"


def create_kb(
    width: int,
    *,
    styles: Optional[dict[str, str]] = None,
    **kwargs: str,
) -> InlineKeyboardMarkup:
    """
    Создает инлайн-клавиатуру. kwargs: callback_data -> текст кнопки.
    styles: callback_data -> 'primary' | 'success' | 'danger' (цвет кнопки в клиентах Telegram).
    """
    kb_builder = InlineKeyboardBuilder()
    buttons: List[InlineKeyboardButton] = []
    style_map = styles or {}

    for button_data, button_text in kwargs.items():
        st = style_map.get(button_data)
        if st:
            buttons.append(
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=button_data,
                    style=st,
                )
            )
        else:
            buttons.append(
                InlineKeyboardButton(
                    text=button_text,
                    callback_data=button_data,
                )
            )

    kb_builder.row(*buttons, width=width)
    return kb_builder.as_markup()


def chanel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="👉Подписаться на канал",
                url=CHANEL_URL,
                style=STYLE_PRIMARY,
            )
        ]
    ])
    return keyboard


def keyboard_start_bonus():
    return create_kb(
        1,
        styles={"buy_vpn": STYLE_SUCCESS},
        buy_vpn="Купить подписку",
    )


def keyboard_start():
    return create_kb(
        1,
        styles={
            "buy_vpn": STYLE_SUCCESS,
            "connect_vpn": STYLE_PRIMARY,
            "ref": STYLE_PRIMARY,
            "buy_gift": STYLE_SUCCESS,
        },
        buy_vpn="🛒 Купить подписку",
        connect_vpn="🔗 Подключить ВПН",
        ref="👥 Рефералка",
        buy_gift="🎁 Подарить подписку",
    )


def keyboard_buy_device_tier():
    return create_kb(
        1,
        styles={
            "buy_tier_3": STYLE_PRIMARY,
            "buy_tier_5": STYLE_PRIMARY,
            "buy_tier_10": STYLE_SUCCESS,
        },
        buy_tier_3="🔹 Тарифы на 3️⃣ устройства",
        buy_tier_5="🔸 Тарифы на 5️⃣ устройств",
        buy_tier_10="🏆 Тарифы на 🔟 устройств",
        back_to_main=BTN_BACK,
    )


def _styles_buy_duration(devices: int) -> dict[str, str]:
    st: dict[str, str] = {"back_buy_tier": STYLE_PRIMARY}
    for months in (1, 3, 6, 12):
        key = f"r_m{months}_d{devices}"
        st[key] = STYLE_SUCCESS if months >= 6 else STYLE_PRIMARY
    return st


def keyboard_buy_duration(devices: int) -> InlineKeyboardMarkup:
    """Срок подписки после выбора числа устройств (callback вида r_m1_d3)."""
    kwargs: dict[str, str] = {}
    for months in (1, 3, 6, 12):
        ck = f"r_m{months}_d{devices}"
        dk = f"m{months}_d{devices}"
        kwargs[ck] = dct_desc[dk]
    kwargs["back_buy_tier"] = BTN_BACK
    return create_kb(1, styles=_styles_buy_duration(devices), **kwargs)


def keyboard_gift_device_tier():
    return create_kb(
        1,
        styles={
            "gift_tier_3": STYLE_PRIMARY,
            "gift_tier_5": STYLE_PRIMARY,
            "gift_tier_10": STYLE_SUCCESS,
        },
        gift_tier_3="🔹 Тарифы на 3️⃣ устройства",
        gift_tier_5="🔸 Тарифы на 5️⃣ устройств",
        gift_tier_10="🏆 Тарифы на 🔟 устройств",
        back_to_main=BTN_BACK,
    )


def _styles_gift_duration(devices: int) -> dict[str, str]:
    st: dict[str, str] = {"gift_back_tier": STYLE_PRIMARY}
    for months in (1, 3, 6, 12):
        key = f"gift_r_m{months}_d{devices}"
        st[key] = STYLE_SUCCESS if months >= 6 else STYLE_PRIMARY
    return st


def keyboard_gift_duration(devices: int) -> InlineKeyboardMarkup:
    kwargs: dict[str, str] = {}
    for months in (1, 3, 6, 12):
        ck = f"gift_r_m{months}_d{devices}"
        dk = f"m{months}_d{devices}"
        kwargs[ck] = dct_desc[dk]
    kwargs["gift_back_tier"] = BTN_BACK
    return create_kb(1, styles=_styles_gift_duration(devices), **kwargs)


def keyboard_subscription(links: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    """
    links: (текст кнопки, https-ссылка на подписку). Только по активным слотам из панели.
    """
    buttons = []
    for text, url in links:
        if not url:
            continue
        buttons.append(
            [
                InlineKeyboardButton(
                    text=text[:64],
                    url=url,
                    style=STYLE_PRIMARY,
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(
                text="⚠️ Если страница не загружается",
                callback_data="import",
                style=STYLE_DANGER,
            )
        ]
    )
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_import_os():
    return create_kb(
        1,
        styles={
            "import_android": STYLE_PRIMARY,
            "import_ios": STYLE_PRIMARY,
            "import_windows": STYLE_PRIMARY,
            "import_macos": STYLE_PRIMARY,
        },
        import_android="🤖 Android",
        import_ios="🍎 iOS",
        import_windows="🖥️ Windows",
        import_macos="🍏 MacOS",
        back_to_main="🔙 Назад",
    )


def keyboard_import_app(os_callback: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⭐️ Happ",
                    callback_data=f"{os_callback}_happ",
                    style=STYLE_PRIMARY,
                )
            ],
            [
                InlineKeyboardButton(
                    text="📡 V2raytun",
                    callback_data=f"{os_callback}_v2",
                    style=STYLE_PRIMARY,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )


def keyboard_import_sub(app_callback: str, has_casual: bool, has_white: bool):
    buttons = []
    if has_casual:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="💫 Ваша подписка ВПН",
                    callback_data=f"{app_callback}_casual",
                    style=STYLE_PRIMARY,
                )
            ]
        )
    if has_white:
        buttons.append(
            [
                InlineKeyboardButton(
                    text="🦾 Включи мобильную связь",
                    callback_data=f"{app_callback}_white",
                    style=STYLE_PRIMARY,
                )
            ]
        )
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_sub_after_buy(sub_url):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 В личный кабинет",
                    url=sub_url,
                    style=STYLE_PRIMARY,
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚠️ Если страница не загружается",
                    callback_data="import",
                    style=STYLE_DANGER,
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎁 Подарить подписку",
                    callback_data="buy_gift",
                    style=STYLE_SUCCESS,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_sub_after_free(sub_url):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📋 В личный кабинет",
                    url=sub_url,
                    style=STYLE_PRIMARY,
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚠️ Если страница не загружается",
                    callback_data="import",
                    style=STYLE_DANGER,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_payment_cancel():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🛒 Купить подписку",
                    callback_data="buy_vpn",
                    style=STYLE_PRIMARY,
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎁 Подарить подписку",
                    callback_data="start_gift",
                    style=STYLE_SUCCESS,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )
    return keyboard


def _payment_rows_without_trial_card(tarif: str) -> list[list[InlineKeyboardButton]]:
    """Строки клавиатуры способов оплаты."""
    return [
        [
            InlineKeyboardButton(
                text="⚡ СБП",
                callback_data=f"wata_sbp_{tarif}",
                style=STYLE_SUCCESS,
            )
        ],
        [
            InlineKeyboardButton(
                text="💳 Карта РФ",
                callback_data=f"wata_card_{tarif}",
                style=STYLE_PRIMARY,
            )
        ],
        [
            InlineKeyboardButton(
                text="⭐️ Telegram Stars",
                callback_data=f"stars_{tarif}",
                style=STYLE_PRIMARY,
            )
        ],
        [
            InlineKeyboardButton(
                text="💎 Crypto bot",
                callback_data=f"crypto_{tarif}",
                style=STYLE_PRIMARY,
            )
        ],
    ]


def keyboard_payment_method(tarif):
    rows = _payment_rows_without_trial_card(tarif)
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_payment_method_stock(tarif):
    return InlineKeyboardMarkup(inline_keyboard=_payment_rows_without_trial_card(tarif))


def keyboard_payment_sbp(text, pay_url):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=text,
                    url=pay_url,
                    style=STYLE_SUCCESS,
                )
            ]
        ]
    )


def keyboard_payment_stars(stars_amount):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"Оплатить {stars_amount} ⭐️",
                    pay=True,
                    style=STYLE_SUCCESS,
                )
            ]
        ]
    )


def ref_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Пригласить друзей🫶",
                    url=f"https://t.me/share/url?url={BOT_URL}?start=ref{user_id}&text={urllib.parse.quote('Вот ссылка на быстрый ВПН для своих!')}",
                    style=STYLE_SUCCESS,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_inline_ref(user_id):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔗 Подключить ВПН",
                    url=f"{BOT_URL}?start=ref{user_id}",
                    style=STYLE_PRIMARY,
                )
            ]
        ]
    )


def keyboard_import_end(url_app: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📥 Скачать приложение",
                    url=url_app,
                    style=STYLE_PRIMARY,
                )
            ],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
        ]
    )
