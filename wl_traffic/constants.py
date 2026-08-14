"""Константы лимита трафика Антиглушилка (белая нода YANDEX-RU-001)."""

from zoneinfo import ZoneInfo

WL_NODE_NAME = "YANDEX-RU-001"
WL_TIMEZONE = ZoneInfo("Europe/Moscow")
# Сутки WL-трафика: с 03:00 до 02:59 МСК (накопление в 02:57, проверка после 03:05).
WL_DAY_RESET_HOUR = 3
WL_ACCUMULATE_HOUR = 2
WL_ACCUMULATE_MINUTE = 57
WL_CHECK_SKIP_UNTIL_HOUR = 3
WL_CHECK_SKIP_UNTIL_MINUTE = 5
WL_LEGACY_RETRIES = 3

WL_SQUAD_ACTIVE = (
    "494bf6ce-d62b-4929-a980-dfc14b8b5ddb",
    "2e6f13b9-58a0-4f46-bd76-0d294f00ef18",
)

WL_SQUAD_LIMITED = (
    "e8090039-6aa0-4ffb-bc67-e064f02e472f",
    "eae3acd8-642d-43fb-b8a7-11cd71d27229",
)

WL_GB_PER_MONTH = 10
WL_TRIAL_LIMIT_GB = 2.0
WL_LOW_TRAFFIC_WARNING_GB = 1.0

# gb -> price (₽)
WL_TRAFFIC_TARIFFS: dict[str, int] = {
    "10": 50,
    "20": 79,
    "50": 149,
    "100": 259,
    "250": 629,
    "500": 1249,
}

# duration days -> months for +10 GB/month bonus on subscription payment
WL_SUBSCRIPTION_MONTHS: dict[int, int] = {
    7: 0,
    30: 1,
    90: 3,
    180: 6,
    365: 12,
}

PROFILE_CB = "user_profile"
WL_TRAFFIC_BUY_CB = "wl_traffic_buy"
WL_TRAFFIC_BUY_SUB_CB = "wl_traffic_buy_sub"
BUY_VPN_CB = "buy_vpn"
