"""Telegram file_id for menu screens — set depends on bot username."""
from __future__ import annotations

from typing import Optional

from aiogram import Bot

from logging_config import logger

FASTGAMER_BOT_USERNAME = "fastgamerbot"

PHOTO_KEYS = (
    "profile",
    "subscription_manage",
    "buy_subscription",
    "buy_traffic",
    "manage_devices",
    "our_site",
    "earn_with_us",
    "about_service",
    "faq",
)

_MENU_PHOTOS_FASTGAMER = {
    "buy_subscription": "AgACAgQAAxkBAAGDGFxqkTkFGlmXBHGHGNQ8DseZ0PTwUAACjg5rGxqpiVCHWmVYr8MtigEAAwIAA3kAAz0E",
    "earn_with_us": "AgACAgQAAxkBAAGDGGJqkTkbkwFBYLg9ICepkchYVXaaogACiw5rGxqpiVCBuPds1-9JxgEAAwIAA3kAAz0E",
    "our_site": "AgACAgQAAxkBAAGDGGBqkTkU29wEEMH24HwI6aPbR_q3vgACjA5rGxqpiVCN08nLAWQJcQEAAwIAA3kAAz0E",
    "about_service": "AgACAgQAAxkBAAGDGF5qkTkLD27mhkj8MoHpgtQ2KwuYxgACjQ5rGxqpiVCmP5HPmKYxtgEAAwIAA3kAAz0E",
    "buy_traffic": "AgACAgQAAxkBAAGDGFpqkTkBj0HTimcyh8RK0_GzzCqHFgACjw5rGxqpiVByFTZtD7vRgQEAAwIAA3kAAz0E",
    "profile": "AgACAgQAAxkBAAGDGFhqkTj7CDE1I0BbzP7dqAgE_TPIMAACkA5rGxqpiVCkYJtlzjT4RwEAAwIAA3kAAz0E",
    "manage_devices": "AgACAgQAAxkBAAGDGFRqkTjtUqEpQ-LekodqlvfZ7tLHtwACkg5rGxqpiVBF_bseIhSStwEAAwIAA3kAAz0E",
    "subscription_manage": "AgACAgQAAxkBAAGDGFZqkTjyGBIuC1eaUBNPojoAAeux5iYAApMOaxsaqYlQj4dwLQ8_JBEBAAMCAAN5AAM9BA",
    "faq": "AgACAgQAAxkBAAGDGGRqkTkhpPEvR1Q0yCoH3qZUfDNzQgACig5rGxqpiVCpswACF9qcrwEAAwIAA3kAAz0E",
}

_MENU_PHOTOS_DEFAULT = {
    "buy_subscription": "AgACAgQAAxkBAAILD2qROGNRecjw18aFhPX0BoCXqZ_8AAKODmsbGqmJUNXte_gNaZn-AQADAgADeQADPQQ",
    "earn_with_us": "AgACAgQAAxkBAAILCWqROFIQv3m-mR7OoD2CEbZMuD9zAAKLDmsbGqmJUAFVNTYLP7gCAQADAgADeQADPQQ",
    "our_site": "AgACAgQAAxkBAAILC2qROFnC8wsa2d4j3sUt2BcJVrL2AAKMDmsbGqmJUL5X70Kvc7sJAQADAgADeQADPQQ",
    "about_service": "AgACAgQAAxkBAAILDWqROF5YS0XzH1YsXrRDZ7aAeQ5XAAKNDmsbGqmJULJMcCBDx5PqAQADAgADeQADPQQ",
    "buy_traffic": "AgACAgQAAxkBAAILEWqROGm3AAEIE2YcmcPoIyr6OEoZ0AACjw5rGxqpiVA_c0Ppqko1iAEAAwIAA3kAAz0E",
    "profile": "AgACAgQAAxkBAAILE2qROG9FXw0rU8xo1Z3I3g4QWwQTAAKQDmsbGqmJUJOkmHx5LA7zAQADAgADeQADPQQ",
    "manage_devices": "AgACAgQAAxkBAAILF2qROHw4y-qT0IQ04uKwUiq5kaRiAAKSDmsbGqmJUL-GuTVVJlx7AQADAgADeQADPQQ",
    "subscription_manage": "AgACAgQAAxkBAAILGWqROIMMIudes97TkObuMsGoD_jRAAKTDmsbGqmJUEZPc9wZdsxHAQADAgADeQADPQQ",
    "faq": "AgACAgQAAxkBAAILB2qROEwGdezBZ7BGK92toQSXsEvxAAKKDmsbGqmJUMvXkGAJZHuXAQADAgADeQADPQQ",
}

_IMPORT_PHOTOS_FASTGAMER = {
    "incy": [
        "AgACAgQAAxkBAAFc_vJqQlS3lOcGNBGiNRJCcxdzwuf8swACAg9rG-RnGFIv88tguls85wEAAwIAA3gAAzwE",
        "AgACAgQAAxkBAAFc_0JqQlS_sBd8CPp_bsiFrlDd3pBRsQACAw9rG-RnGFIarCYVPhCt8gEAAwIAA3gAAzwE",
    ],
    "happ": [
        "AgACAgIAAxkBAAEPMfNpuuFYB037vCUdedfpqS5ypOaVZwAC4xhrG0h32Emm-Cx1F38P2AEAAwIAA3kAAzoE",
        "AgACAgIAAxkBAAEPMfVpuuGdCuGgeeOBc1e4cQthdWA3OAAC6BhrG0h32EkCxf1P9qKWzwEAAwIAA3kAAzoE",
    ],
    "v2": [
        "AgACAgIAAxkBAAEPMfhpuuGu0Rg_-nKG-PcvViCGfoN4AQAC6RhrG0h32El2wcWMdvvLrgEAAwIAA3kAAzoE",
        "AgACAgIAAxkBAAEPMfppuuHSQmUSRF9AtlPh8S_vYZpICgAC6hhrG0h32Elfi-ITfrYC6QEAAwIAA3kAAzoE",
        "AgACAgIAAxkBAAEPMfxpuuHnZOPTsCVK3JKqaYR_2TzIUAAC7RhrG0h32EmvaFothcL4KAEAAwIAA3kAAzoE",
    ],
}

_IMPORT_PHOTOS_DEFAULT = {
    "incy": [
        "AgACAgQAAxkBAAIK-WqRNxpnwY5vA49KrLQyciJrs1OQAAJsDmsbVN-RUK4xqPNbvQydAQADAgADeAADPQQ",
        "AgACAgQAAxkBAAIK-2qRNyAQj556isdX0TlYt-QYgSA4AAKADmsbGqmJUKG_6WW40Oo7AQADAgADeAADPQQ",
    ],
    "happ": [
        "AgACAgQAAxkBAAIK_WqRNyUqLTw5suSymWD72hvmevAnAAKBDmsbGqmJUGqgCjyzorWzAQADAgADeQADPQQ",
        "AgACAgQAAxkBAAIK_2qRNymgoyXyZawwQV-GMxyTo3_0AAKCDmsbGqmJUGzXmPVUviK5AQADAgADeQADPQQ",
    ],
    "v2": [
        "AgACAgQAAxkBAAILAWqRNy5ejegQIaqY54koRppsdZdLAAKDDmsbGqmJUIIOrm0NLir9AQADAgADeQADPQQ",
        "AgACAgQAAxkBAAILA2qRNzM1Rx8ofyA6A9-tjmy_iit8AAKEDmsbGqmJUMScW4gpCW22AQADAgADeQADPQQ",
        "AgACAgQAAxkBAAILBWqRNzmggx_zm8AreE7qsC4vBnPKAAKFDmsbGqmJUH4Dn86_3f-cAQADAgADeQADPQQ",
    ],
}

_cached_username: Optional[str] = None


def _username_from_bot_url() -> str:
    from config import BOT_URL

    slug = (BOT_URL or "").rstrip("/").split("/")[-1]
    return slug.lstrip("@").lower()


def _active_bot_username() -> str:
    return (_cached_username or _username_from_bot_url()).lower()


def is_fastgamer_bot() -> bool:
    return _active_bot_username() == FASTGAMER_BOT_USERNAME


def _menu_photos_map() -> dict[str, str]:
    if is_fastgamer_bot():
        return _MENU_PHOTOS_FASTGAMER
    return _MENU_PHOTOS_DEFAULT


def _import_photos_map() -> dict[str, list[str]]:
    if is_fastgamer_bot():
        return _IMPORT_PHOTOS_FASTGAMER
    return _IMPORT_PHOTOS_DEFAULT


def menu_photo(key: str) -> str:
    photos = _menu_photos_map()
    if key not in photos:
        raise KeyError(f"Unknown menu photo key: {key}")
    return photos[key]


def import_photos(app_key: str) -> list[str]:
    photos = _import_photos_map()
    if app_key not in photos:
        raise KeyError(f"Unknown import photo app key: {app_key}")
    return photos[app_key]


async def init_menu_photos(bot: Bot) -> None:
    global _cached_username
    me = await bot.get_me()
    if me and me.username:
        _cached_username = me.username.lower()
        profile = "fastgamer" if is_fastgamer_bot() else "default"
        logger.info("Menu photos: @{} ({})", _cached_username, profile)
