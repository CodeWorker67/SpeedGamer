"""Синхронизация username / fullname из Telegram в БД для роутера пользователя."""
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineQuery,
    Message,
    TelegramObject,
    User,
)

from bot import sql


def tg_profile_fields(from_user: User) -> tuple[Optional[str], Optional[str]]:
    username = (from_user.username or "").strip().lstrip("@") or None
    fullname = (from_user.full_name or "").strip() or None
    return username, fullname


async def sync_tg_user_profile(from_user: User) -> None:
    username, fullname = tg_profile_fields(from_user)
    if not username and not fullname:
        return
    await sql.sync_telegram_profile_if_missing(
        from_user.id,
        username=username,
        fullname=fullname,
    )


def tg_user_from_event(event: TelegramObject) -> Optional[User]:
    if isinstance(event, ChatMemberUpdated):
        member = event.new_chat_member
        if member is not None and member.user is not None:
            return member.user
        return event.from_user
    if isinstance(event, (Message, CallbackQuery, InlineQuery)):
        return event.from_user
    return None


class SyncTelegramProfileMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        tg_user = tg_user_from_event(event)
        if tg_user is not None:
            await sync_tg_user_profile(tg_user)
        return await handler(event, data)
