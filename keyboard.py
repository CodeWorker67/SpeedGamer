import urllib.parse
from typing import List, Optional

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import CHANEL_URL, BOT_URL, DOCUMENT_URL_1, DOCUMENT_URL_2, PUBLIC_SITE_URL, SITE_URL, SUPPORT_URL
from lexicon import dct_desc
from utils.custom_emoji import emoji_button
from wl_traffic.constants import BUY_VPN_CB, WL_TRAFFIC_BUY_CB, WL_TRAFFIC_BUY_SUB_CB, WL_TRAFFIC_TARIFFS

STYLE_PRIMARY = "primary"
STYLE_SUCCESS = "success"
STYLE_DANGER = "danger"

BTN_BACK = "◀️ Назад"
ABOUT_SERVICE_CB = "about_service"
OPEN_SITE_CB = "open_site"


def create_kb(
    width: int,
    *,
    styles: Optional[dict[str, str]] = None,
    **kwargs: str,
) -> InlineKeyboardMarkup:
    """
    Создаёт инлайн-клавиатуру. kwargs: callback_data -> текст кнопки.
    styles: callback_data -> 'primary' | 'success' | 'danger'.
    """
    kb_builder = InlineKeyboardBuilder()
    buttons: List[InlineKeyboardButton] = []
    style_map = styles or {}

    for button_data, button_text in kwargs.items():
        st = style_map.get(button_data)
        if st:
            buttons.append(
                emoji_button(
                    text=button_text,
                    callback_data=button_data,
                    style=st,
                )
            )
        else:
            buttons.append(
                emoji_button(
                    text=button_text,
                    callback_data=button_data,
                )
            )

    kb_builder.row(*buttons, width=width)
    return kb_builder.as_markup()


def chanel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="👉Подписаться на канал",
                url=CHANEL_URL,
            )
        ]
    ])
    return keyboard


def keyboard_start(
    *,
    connect_buttons: Optional[list[tuple[str, str]]] = None,
    show_manage: bool = False,
    buy_primary: bool = True,
    show_trial: bool = False,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for text, url in connect_buttons or []:
        if not url:
            continue
        rows.append(
            [
                emoji_button(
                    text=text[:64],
                    url=url,
                    style=STYLE_PRIMARY,
                )
            ]
        )
    if show_manage or connect_buttons:
        rows.append(
            [
                emoji_button(
                    text="Управление подпиской",
                    callback_data="connect_vpn",
                    icon_custom_emoji_id="5877260593903177342",
                )
            ]
        )
    buy_kwargs = {"text": "💰 Купить подписку", "callback_data": "buy_vpn"}
    if buy_primary:
        buy_kwargs["style"] = STYLE_PRIMARY
    rows.append([emoji_button(**buy_kwargs)])
    if show_trial:
        rows.append(
            [
                emoji_button(
                    text="Попробовать бесплатно",
                    callback_data="free_vpn",
                    style=STYLE_SUCCESS,
                )
            ]
        )
    rows.append(
        [
            emoji_button(
                text="💸 Заработок",
                callback_data="earn_with_us",
            ),
            emoji_button(
                text="🌐 Наш сайт",
                callback_data=OPEN_SITE_CB,
            ),
        ]
    )
    support_url = SUPPORT_URL or "https://t.me/"
    rows.append(
        [
            emoji_button(
                text="О сервисе",
                callback_data=ABOUT_SERVICE_CB,
                icon_custom_emoji_id="5884510167986343350",
            ),
            emoji_button(
                text="Поддержка",
                url=support_url,
                icon_custom_emoji_id="5875465628285931233",
            ),
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_start_bonus():
    return keyboard_start(buy_primary=True)


def keyboard_push_buy_reviews() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="💰 Купить подписку",
                callback_data="buy_vpn",
                style=STYLE_PRIMARY,
            ),
        ],
        [
            emoji_button(
                text="Попробовать бесплатно",
                callback_data="free_vpn",
                style=STYLE_SUCCESS,
            ),
        ],
    ])


def keyboard_subscription_manage(*, show_add_devices: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if show_add_devices:
        rows.append(
            [
                emoji_button(
                    text="Добавить устройство",
                    callback_data="add_devices_start",
                    icon_custom_emoji_id="5775937998948404844",
                )
            ]
        )
    rows.extend([
        [
            emoji_button(
                text="📦 Купить трафик",
                callback_data=WL_TRAFFIC_BUY_CB,
            )
        ],
        [
            emoji_button(
                text="Управление устройствами",
                callback_data="manage_devices",
                icon_custom_emoji_id="6008258140108231117",
            ),
        ],
        [
            emoji_button(
                text="Если страница не загружается",
                callback_data="import",
                icon_custom_emoji_id="5996789164383669947",
            )
        ],
        [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_add_devices_choices(max_add: int, billable_months: int) -> InlineKeyboardMarkup:
    from device_addons import ADD_DEVICES_RUB_PER_DEVICE_MONTH

    rows: list[list[InlineKeyboardButton]] = []
    unit = ADD_DEVICES_RUB_PER_DEVICE_MONTH * billable_months
    for n in range(1, min(5, max_add) + 1):
        price = n * unit
        rows.append(
            [
                emoji_button(
                    text=f"+ {n} уст. (+{price} ₽)",
                    callback_data=f"add_dev_pick_{n}",
                )
            ]
        )
    rows.append([emoji_button(text=BTN_BACK, callback_data="connect_vpn")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_add_devices_payment(add_count: int, price: int) -> InlineKeyboardMarkup:
    n = int(add_count)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [emoji_button(text="⚡ СБП", callback_data=f"add_dev_sbp_{n}")],
            [emoji_button(text="💳 Карта РФ", callback_data=f"add_dev_card_{n}")],
            [emoji_button(text="⭐️ Telegram Stars", callback_data=f"add_dev_stars_{n}")],
            [emoji_button(text="💎 Crypto bot", callback_data=f"add_dev_crypto_{n}")],
            [emoji_button(text=BTN_BACK, callback_data="add_devices_start")],
        ]
    )


def keyboard_about_service() -> InlineKeyboardMarkup:
    rows = []
    if DOCUMENT_URL_1:
        rows.append(
            [
                emoji_button(
                    text="Пользовательское соглашение",
                    url=DOCUMENT_URL_1,
                )
            ]
        )
    if DOCUMENT_URL_2:
        rows.append(
            [
                emoji_button(
                    text="Политика конфиденциальности",
                    url=DOCUMENT_URL_2,
                )
            ]
        )
    rows.append([emoji_button(text=BTN_BACK, callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_buy_menu() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        buy_vpn_self="👤 Для себя",
        buy_gift="🎁 Подарить подписку",
        back_to_main=BTN_BACK,
    )


def keyboard_earn_with_us() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        ref="👭 Бесплатный VPN за приглашения",
        partner_earn="🔗 Партнерская ссылка",
        back_to_main=BTN_BACK,
    )


def keyboard_buy_device_tier(*, with_trial: bool = False):
    """Клавиатура для push-рассылок: сразу в меню покупки."""
    return create_kb(
        1,
        buy_vpn="🛒 Купить подписку",
        back_to_main=BTN_BACK,
    )


def keyboard_tariff_bonus():
    return keyboard_buy_device_tier()


def keyboard_tariff():
    return keyboard_buy_device_tier()


def keyboard_tariff_trial():
    return keyboard_buy_device_tier()


def keyboard_buy_duration(devices: int) -> InlineKeyboardMarkup:
    """Устаревший callback: 1/3 мес. для 5 устройств (конкурсы и старые кнопки)."""
    from tariff_resolve import subscription_price_rub

    kwargs: dict[str, str] = {}
    for months in (1, 3):
        price = subscription_price_rub(months, devices)
        kwargs[f"r_m{months}_d{devices}"] = f"{'🔸' if months == 1 else '👌'} {months} мес — {price} ₽"
    kwargs["back_to_buy_menu"] = BTN_BACK
    return create_kb(1, **kwargs)


def keyboard_self_duration_new() -> InlineKeyboardMarkup:
    from tariff_resolve import subscription_price_rub

    return create_kb(
        1,
        self_dur_m1=f"1 месяц — {subscription_price_rub(1, 5)} ₽",
        self_dur_m3=f"3 месяца — {subscription_price_rub(3, 5)} ₽",
        back_to_buy_menu=BTN_BACK,
    )


def keyboard_self_duration_renew(devices: int) -> InlineKeyboardMarkup:
    from tariff_resolve import subscription_price_rub

    p1 = subscription_price_rub(1, devices)
    p3 = subscription_price_rub(3, devices)
    return create_kb(
        1,
        **{
            f"r_m1_d{devices}": f"1 месяц — {p1} ₽",
            f"r_m3_d{devices}": f"3 месяца — {p3} ₽",
            "back_to_buy_menu": BTN_BACK,
        },
    )


def keyboard_self_devices(months: int, devices: int) -> InlineKeyboardMarkup:
    from tariff_resolve import SELF_DEVICES_MAX, SELF_DEVICES_MIN, extra_device_step_rub

    rows = []
    step = extra_device_step_rub(months)
    if devices < SELF_DEVICES_MAX:
        rows.append(
            [
                emoji_button(
                    text=f"Добавить устройство (+{step} ₽)",
                    callback_data=f"self_inc_m{months}_d{devices}",
                )
            ]
        )
    if devices > SELF_DEVICES_MIN:
        rows.append(
            [
                emoji_button(
                    text=f"Убрать устройство (-{step} ₽)",
                    callback_data=f"self_dec_m{months}_d{devices}",
                )
            ]
        )
    rows.append(
        [
            emoji_button(
                text="Далее",
                callback_data=f"self_go_pay_m{months}_d{devices}",
            )
        ]
    )
    rows.append(
        [
            emoji_button(
                text=BTN_BACK,
                callback_data="buy_vpn_self",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_gift_device_tier():
    return keyboard_gift_duration_new()


def keyboard_gift_duration(devices: int) -> InlineKeyboardMarkup:
    return keyboard_gift_duration_new()


def keyboard_gift_duration_new() -> InlineKeyboardMarkup:
    from tariff_resolve import subscription_price_rub

    return create_kb(
        1,
        gift_r_m1_d5=f"1 месяц — {subscription_price_rub(1, 5)} ₽",
        gift_r_m3_d5=f"3 месяца — {subscription_price_rub(3, 5)} ₽",
        back_to_buy_menu=BTN_BACK,
    )


def keyboard_gift_tariff():
    return keyboard_gift_device_tier()


def keyboard_subscription(links: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    """links: (текст кнопки, https-ссылка на подписку)."""
    buttons = []
    for text, url in links:
        if not url:
            continue
        buttons.append(
            [
                emoji_button(
                    text=text[:64],
                    url=url,
                )
            ]
        )
    buttons.append(
        [
            emoji_button(
                text="Если страница не загружается",
                callback_data="import",
            )
        ]
    )
    buttons.append([emoji_button(text=BTN_BACK, callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_devices_subscriptions(slots: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    """slots: (ключ слота, текст кнопки)."""
    buttons = []
    for slot_key, label in slots:
        buttons.append(
            [
                emoji_button(
                    text=label[:64],
                    callback_data=f"dev_sub_{slot_key}",
                )
            ]
        )
    buttons.append([emoji_button(text=BTN_BACK, callback_data="connect_vpn")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_devices_list(
    slot_key: str,
    devices: list[tuple[int, str]],
) -> InlineKeyboardMarkup:
    """devices: (индекс, текст кнопки)."""
    buttons = []
    for idx, btn_text in devices:
        buttons.append(
            [
                emoji_button(
                    text=btn_text[:64],
                    callback_data=f"dev_pick_{slot_key}_{idx}",
                )
            ]
        )
    buttons.append([emoji_button(text=BTN_BACK, callback_data="manage_devices")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_device_delete_confirm(slot_key: str, device_idx: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="✅ Да, удалить",
                    callback_data=f"dev_rm_{slot_key}_{device_idx}",
                ),
                emoji_button(
                    text="❌ Нет",
                    callback_data=f"dev_cancel_{slot_key}",
                ),
            ],
        ]
    )


def keyboard_sub_after_buy(sub_url):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="📋 В личный кабинет",
                    url=sub_url,
                )
            ],
            [
                emoji_button(
                    text="Если страница не загружается",
                    callback_data="import",
                )
            ],
            [
                emoji_button(
                    text="🎁 Подарить подписку",
                    callback_data="buy_gift",
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_trial_existing_expired() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )


def keyboard_sub_after_free(sub_url):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="📋 В личный кабинет",
                    url=sub_url,
                )
            ],
            [
                emoji_button(
                    text="Если страница не загружается",
                    callback_data="import",
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_import_os(slot: Optional[str] = None, *, back_callback: str = "import"):
    suffix = f"_{slot}" if slot else ""
    kwargs = {
        f"import_android{suffix}": "🤖 Android",
        f"import_ios{suffix}": "🍎 iOS",
        f"import_windows{suffix}": "🖥️ Windows",
        f"import_macos{suffix}": "🍏 MacOS",
        back_callback: BTN_BACK,
    }
    return create_kb(1, **kwargs)


def keyboard_import_app(
    os_callback: str,
    slot: Optional[str] = None,
    *,
    back_callback: str = "import",
):
    suffix = f"_{slot}" if slot else ""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="🔥 INCY",
                    callback_data=f"{os_callback}_incy{suffix}",
                )
            ],
            [
                emoji_button(
                    text="⭐️ Happ",
                    callback_data=f"{os_callback}_happ{suffix}",
                )
            ],
            [
                emoji_button(
                    text="📡 V2raytun",
                    callback_data=f"{os_callback}_v2{suffix}",
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data=back_callback)],
        ]
    )


def keyboard_import_sub(app_callback: str, subscriptions: list[tuple[str, str]]):
    """subscriptions: (slot_key, текст кнопки)."""
    buttons = []
    for slot, label in subscriptions:
        buttons.append(
            [
                emoji_button(
                    text=label[:64],
                    callback_data=f"{app_callback}_{slot}",
                )
            ]
        )
    buttons.append([emoji_button(text=BTN_BACK, callback_data="import")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_import_slots(slots: list[tuple[str, str]], *, back_callback: str = "connect_vpn"):
    buttons = []
    for slot, label in slots:
        buttons.append(
            [
                emoji_button(
                    text=label[:64],
                    callback_data=f"import_slot_{slot}",
                )
            ]
        )
    buttons.append([emoji_button(text=BTN_BACK, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_import_after_album() -> InlineKeyboardMarkup:
    return create_kb(
        1,
        connect_vpn="🔙 Назад к подписке",
    )


def keyboard_import_end(url_app: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="📥 Скачать приложение",
                    url=url_app,
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )


def keyboard_payment_cancel():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="💰 Купить подписку",
                    callback_data="buy_vpn",
                    style=STYLE_PRIMARY,
                )
            ],
            [
                emoji_button(
                    text="🎁 Подарить подписку",
                    callback_data="start_gift",
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )
    return keyboard


def keyboard_payment_method_trial(tarif):
    """ЮKassa только для пробного тарифа."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="⚡ СБП (ЮKassa)",
                    callback_data=f"yk_sbp_{tarif}",
                )
            ],
            [
                emoji_button(
                    text="💳 Карта (ЮKassa)",
                    callback_data=f"yk_card_{tarif}",
                )
            ],
            [emoji_button(text=BTN_BACK, callback_data="back_to_main")],
        ]
    )


def keyboard_payment_method(tarif):
    gift = str(tarif).startswith("gift_")
    pay_prefix = "fk" if gift else "wata"
    rows = [
        [
            emoji_button(
                text="⚡ СБП",
                callback_data=f"{pay_prefix}_sbp_{tarif}",
            )
        ],
        [
            emoji_button(
                text="💳 Карта РФ",
                callback_data=f"{pay_prefix}_card_{tarif}",
            )
        ],
        [
            emoji_button(
                text="⭐️ Telegram Stars",
                callback_data=f"stars_{tarif}",
            )
        ],
        [
            emoji_button(
                text="💎 Crypto bot",
                callback_data=f"crypto_{tarif}",
            )
        ],
        [emoji_button(text=BTN_BACK, callback_data="back_to_buy_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_promo_120_device_tier() -> InlineKeyboardMarkup:
    """Устаревшее имя: акция только на 5 устройств."""
    return create_kb(1, r_120="🎁 Акция 3+1 (5 устройств)")


def keyboard_payment_method_promo_120(tarif: str) -> InlineKeyboardMarkup:
    """Способы оплаты акции 120 дней; «Назад» — снова выбор числа устройств."""
    gift = str(tarif).startswith("gift_")
    pay_prefix = "fk" if gift else "wata"
    rows = [
        [
            emoji_button(
                text="⚡ СБП",
                callback_data=f"{pay_prefix}_sbp_{tarif}",
            )
        ],
        [
            emoji_button(
                text="💳 Карта РФ",
                callback_data=f"{pay_prefix}_card_{tarif}",
            )
        ],
        [
            emoji_button(
                text="⭐️ Telegram Stars",
                callback_data=f"stars_{tarif}",
            )
        ],
        [
            emoji_button(
                text="💎 Crypto bot",
                callback_data=f"crypto_{tarif}",
            )
        ],
        [emoji_button(text=BTN_BACK, callback_data="r_120")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_payment_method_stock(tarif):
    gift = str(tarif).startswith("gift_")
    pay_prefix = "fk" if gift else "wata"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="⚡ СБП",
                    callback_data=f"{pay_prefix}_sbp_{tarif}",
                )
            ],
            [
                emoji_button(
                    text="💳 Карта РФ",
                    callback_data=f"{pay_prefix}_card_{tarif}",
                )
            ],
            [
                emoji_button(
                    text="⭐️ Telegram Stars",
                    callback_data=f"stars_{tarif}",
                )
            ],
            [
                emoji_button(
                    text="💎 Crypto bot",
                    callback_data=f"crypto_{tarif}",
                )
            ],
        ]
    )


def keyboard_payment_sbp(text, pay_url):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text=text,
                    url=pay_url,
                )
            ]
        ]
    )


def keyboard_payment_stars(stars_amount):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text=f"Оплатить {stars_amount} ⭐️",
                    pay=True,
                )
            ]
        ]
    )


def ref_keyboard(user_id, *, show_qr: bool = True):
    rows = [
        [
            emoji_button(
                text="Пригласить друзей🫶",
                url=f"https://t.me/share/url?url={BOT_URL}?start=ref{user_id}&text={urllib.parse.quote('Вот ссылка на быстрый ВПН для своих!')}",
            )
        ],
    ]
    if show_qr:
        rows.append(
            [emoji_button(text="Показать QR-код", callback_data="ref_show_qr")]
        )
    rows.append([emoji_button(text=BTN_BACK, callback_data="back_to_earn")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_inline_ref(user_id):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                emoji_button(
                    text="🔗 Подключить ВПН",
                    url=f"{BOT_URL}?start=ref{user_id}",
                    style=STYLE_PRIMARY,
                )
            ]
        ]
    )


def _site_base_url() -> str:
    return (PUBLIC_SITE_URL or SITE_URL or "").strip().rstrip("/")


def partner_bot_link(user_id: int) -> str:
    base = (BOT_URL or "").rstrip("/")
    return f"{base}?start=partner_{user_id}"


def partner_site_link(user_id: int) -> Optional[str]:
    site = _site_base_url()
    if not site:
        return None
    return f"{site}?start=partner_{user_id}"


def partner_invite_share_text(user_id: int) -> str:
    lines = [
        "💸 ВПН ДЛЯ СВОИХ — подключайся по моей партнёрской ссылке "
        "к быстрому и надёжному ВПН для своих!",
        "",
        f"🤖 Бот: {partner_bot_link(user_id)}",
    ]
    site = partner_site_link(user_id)
    if site:
        lines.append(f"🌐 Сайт: {site}")
    return "\n".join(lines)


def partner_invite_share_url(user_id: int) -> str:
    bot_link = partner_bot_link(user_id)
    inner = urllib.parse.quote(bot_link, safe="")
    text = urllib.parse.quote(partner_invite_share_text(user_id))
    return f"https://t.me/share/url?url={inner}&text={text}"


def keyboard_inline_partner(user_id: int):
    rows = [
        [
            emoji_button(
                text="🔗 Подключить ВПН",
                url=partner_bot_link(user_id),
                style=STYLE_PRIMARY,
            )
        ]
    ]
    site = partner_site_link(user_id)
    if site:
        rows.append([
            emoji_button(
                text="🌐 Открыть сайт",
                url=site,
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_partner_intro():
    return create_kb(
        1,
        partner_create_link="🔗 Создать партнёрскую ссылку",
        back_to_earn=BTN_BACK,
    )


def keyboard_partner_dashboard(
    user_id: int,
    *,
    show_bot_qr: bool = True,
    show_site_qr: bool = True,
):
    rows = [
        [
            emoji_button(
                text="Пригласить друзей🫶",
                url=partner_invite_share_url(user_id),
            )
        ],
    ]
    if show_bot_qr:
        rows.append(
            [
                emoji_button(
                    text="Показать QR-код на тг-бота",
                    callback_data="partner_qr_bot",
                )
            ]
        )
    if show_site_qr and partner_site_link(user_id):
        rows.append(
            [
                emoji_button(
                    text="Показать QR-код на сайт",
                    callback_data="partner_qr_site",
                )
            ]
        )
    rows.append(
        [
            emoji_button(
                text="💰 Создать заявку на вывод",
                callback_data="partner_withdraw",
            )
        ]
    )
    rows.append([emoji_button(text=BTN_BACK, callback_data="back_to_earn")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_partner_withdraw(support_url: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="💬 Вывести деньги",
                url=support_url,
            )
        ],
        [
            emoji_button(
                text=BTN_BACK,
                callback_data="partner_earn",
            )
        ],
    ])


def keyboard_contest_win_reveal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="🎁 Узнать свой приз",
                callback_data="cwin_reveal",
            )
        ],
    ])


def keyboard_contest_win_take() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="🛍 Забрать скидку",
                callback_data="cwin_take",
            )
        ],
    ])


def keyboard_contest_win_urgency_buy() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="🕒 Успеть оформить со скидкой",
                callback_data="cwin_buy",
            )
        ],
    ])


def keyboard_contest_duration_new() -> InlineKeyboardMarkup:
    from tariff_resolve import subscription_price_rub

    return create_kb(
        1,
        cwin_dur_m1=f"1 месяц — {subscription_price_rub(1, 5)} ₽",
        cwin_dur_m3=f"3 месяца — {subscription_price_rub(3, 5)} ₽",
        cwin_back_prize=BTN_BACK,
    )


def keyboard_contest_devices(months: int, devices: int) -> InlineKeyboardMarkup:
    from tariff_resolve import SELF_DEVICES_MAX, SELF_DEVICES_MIN, extra_device_step_rub

    rows = []
    step = extra_device_step_rub(months)
    if devices < SELF_DEVICES_MAX:
        rows.append(
            [
                emoji_button(
                    text=f"Добавить устройство (+{step} ₽)",
                    callback_data=f"cwin_inc_m{months}_d{devices}",
                )
            ]
        )
    if devices > SELF_DEVICES_MIN:
        rows.append(
            [
                emoji_button(
                    text=f"Убрать устройство (-{step} ₽)",
                    callback_data=f"cwin_dec_m{months}_d{devices}",
                )
            ]
        )
    rows.append(
        [
            emoji_button(
                text="Далее",
                callback_data=f"cwin_go_pay_m{months}_d{devices}",
            )
        ]
    )
    rows.append(
        [
            emoji_button(
                text=BTN_BACK,
                callback_data="cwin_back_dur",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def keyboard_discount_push_reveal() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="🎁 Узнать награду",
                callback_data="dpush_reveal",
            )
        ],
    ])


def keyboard_discount_push_buy() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="⚡ Купить со скидкой",
                callback_data="dpush_buy",
            )
        ],
    ])


def keyboard_discount_push_device_tier() -> InlineKeyboardMarkup:
    return keyboard_discount_push_duration(5)


def keyboard_discount_push_duration(devices: int) -> InlineKeyboardMarkup:
    from lexicon import discount_duration_button_text

    kwargs: dict[str, str] = {}
    for months in (1, 3, 6, 12):
        ck = f"dpush_tariff_m{months}_d{devices}"
        kwargs[ck] = discount_duration_button_text(months, devices)
    kwargs["dpush_back_tier"] = BTN_BACK
    return create_kb(1, **kwargs)


def keyboard_discount_push_payment(desc_key: str) -> InlineKeyboardMarkup:
    devices = int(desc_key.rsplit("_d", 1)[-1])
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="⚡ СБП",
                callback_data=f"dpush_fk_sbp_{desc_key}",
            )
        ],
        [
            emoji_button(
                text="💳 Карта РФ",
                callback_data=f"dpush_fk_card_{desc_key}",
            )
        ],
        [
            emoji_button(
                text="⭐️ Telegram Stars",
                callback_data=f"dpush_stars_{desc_key}",
            )
        ],
        [
            emoji_button(
                text="💎 Crypto bot",
                callback_data=f"dpush_crypto_{desc_key}",
            )
        ],
        [
            emoji_button(
                text=BTN_BACK,
                callback_data=f"dpush_back_dur_{devices}",
            )
        ],
    ])


def keyboard_wl_traffic_tariffs(*, back_callback: str = "back_to_main") -> InlineKeyboardMarkup:
    from_sub = back_callback in (BUY_VPN_CB, "buy_vpn_self")
    buttons = []
    for gb, price in sorted(WL_TRAFFIC_TARIFFS.items(), key=lambda item: int(item[0]), reverse=True):
        cb = f"wl_traffic_sub_{gb}" if from_sub else f"wl_traffic_{gb}"
        buttons.append([
            emoji_button(
                text=f"{gb} GB — {price} ₽",
                callback_data=cb,
            )
        ])
    buttons.append([emoji_button(text=BTN_BACK, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def keyboard_wl_traffic_payment_method(mb: str, *, back_callback: str = WL_TRAFFIC_BUY_CB) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            emoji_button(
                text="⚡ СБП",
                callback_data=f"wl_traffic_sbp_{mb}",
            )
        ],
        [
            emoji_button(
                text="💳 Карта РФ",
                callback_data=f"wl_traffic_card_{mb}",
            )
        ],
        [
            emoji_button(
                text="⭐️ Telegram Stars",
                callback_data=f"wl_traffic_stars_{mb}",
            )
        ],
        [
            emoji_button(
                text="💎 Crypto bot",
                callback_data=f"wl_traffic_crypto_{mb}",
            )
        ],
        [emoji_button(text=BTN_BACK, callback_data=back_callback)],
    ])
