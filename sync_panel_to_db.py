"""
Синхронизация пользователей панели (username: цифры / цифры_3 / цифры_10) с БД.

Запуск из корня проекта:
    python sync_panel_to_db.py
"""
import asyncio
import re
import sys
from datetime import datetime, timezone
from typing import Literal, Optional, Tuple

from X3 import X3
from config_bd.models import Users
from config_bd.utils import AsyncSQL, _naive_utc
from logging_config import logger

Tier = Literal["base", "3", "10"]

HOURS_THRESHOLD = 5

_RE_BASE = re.compile(r"^\d+$")
_RE_3 = re.compile(r"^(\d+)_3$")
_RE_10 = re.compile(r"^(\d+)_10$")


def _configure_stdout_utf8() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


def _parse_panel_username(username: str) -> Optional[Tuple[int, Tier]]:
    """
    Допустимы только username из цифр и '_':
    — только цифры (базовый тариф);
    — цифры + '_3';
    — цифры + '_10'.
    """
    username = (username or "").strip()
    if not username or not all(c.isdigit() or c == "_" for c in username):
        return None
    if _RE_BASE.fullmatch(username):
        return int(username), "base"
    m3 = _RE_3.fullmatch(username)
    if m3:
        return int(m3.group(1)), "3"
    m10 = _RE_10.fullmatch(username)
    if m10:
        return int(m10.group(1)), "10"
    return None


def _panel_expire_to_utc(expire_str: Optional[str]) -> Optional[datetime]:
    if not expire_str:
        return None
    dt = datetime.fromisoformat(expire_str.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).astimezone(timezone.utc).replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _normalize_db_dt(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return _naive_utc(dt)


def _dates_differ_more_than_hours(
    panel_dt: Optional[datetime],
    db_dt: Optional[datetime],
    hours: float = HOURS_THRESHOLD,
) -> bool:
    panel_n = _normalize_db_dt(panel_dt)
    db_n = _normalize_db_dt(db_dt)
    if panel_n is None and db_n is None:
        return False
    if panel_n is None or db_n is None:
        return True
    diff_sec = abs((panel_n - db_n).total_seconds())
    return diff_sec > hours * 3600


def _subscribtion_differs(db_value: Optional[str], short_uuid: Optional[str]) -> bool:
    db_norm = (db_value or "").strip()
    panel_norm = (short_uuid or "").strip()
    if not panel_norm:
        return False
    return db_norm != panel_norm


def _tier_db_subscribtion(user: Users, tier: Tier) -> Optional[str]:
    if tier == "base":
        return user.subscribtion
    if tier == "3":
        return user.subscribtion_3
    return user.subscribtion_10


def _tier_db_end_date(user: Users, tier: Tier) -> Optional[datetime]:
    if tier == "base":
        return user.subscription_end_date
    if tier == "3":
        return user.subscription_3_end_date
    return user.subscription_10_end_date


async def _update_tier_subscribtion(sql: AsyncSQL, telegram_id: int, tier: Tier, short_uuid: str) -> None:
    if tier == "base":
        await sql.update_subscribtion(telegram_id, short_uuid)
    elif tier == "3":
        await sql.update_subscribtion_3(telegram_id, short_uuid)
    else:
        await sql.update_subscribtion_10(telegram_id, short_uuid)


async def _update_tier_end_date(sql: AsyncSQL, telegram_id: int, tier: Tier, end_date: datetime) -> None:
    if tier == "base":
        await sql.update_subscription_end_date(telegram_id, end_date)
    elif tier == "3":
        await sql.update_subscription_3_end_date(telegram_id, end_date)
    else:
        await sql.update_subscription_10_end_date(telegram_id, end_date)


async def _apply_tier_on_create(
    sql: AsyncSQL,
    telegram_id: int,
    tier: Tier,
    short_uuid: str,
    expire_at_utc: datetime,
) -> None:
    await _update_tier_subscribtion(sql, telegram_id, tier, short_uuid)
    await _update_tier_end_date(sql, telegram_id, tier, expire_at_utc)


async def _sync_existing_user(
    sql: AsyncSQL,
    user: Users,
    telegram_id: int,
    tier: Tier,
    short_uuid: Optional[str],
    expire_at_utc: Optional[datetime],
    stats: dict,
) -> None:
    changed_sub = False
    changed_date = False
    tier_label = {"base": "", "3": "_3", "10": "_10"}[tier]

    if _subscribtion_differs(_tier_db_subscribtion(user, tier), short_uuid):
        if not short_uuid:
            stats["skipped_no_short_uuid"] += 1
        else:
            await _update_tier_subscribtion(sql, telegram_id, tier, short_uuid)
            stats["updated_subscribtion"] += 1
            changed_sub = True
            logger.info(
                f"subscribtion{tier_label}: user_id={telegram_id} "
                f"'{_tier_db_subscribtion(user, tier)}' -> '{short_uuid}'"
            )

    if expire_at_utc is None:
        stats["skipped_no_expire"] += 1
    elif _dates_differ_more_than_hours(expire_at_utc, _tier_db_end_date(user, tier)):
        await _update_tier_end_date(sql, telegram_id, tier, expire_at_utc)
        stats["updated_subscription_end_date"] += 1
        changed_date = True
        logger.info(
            f"subscription{tier_label}_end_date: user_id={telegram_id} "
            f"db={_tier_db_end_date(user, tier)} panel={expire_at_utc}"
        )

    if not changed_sub and not changed_date:
        stats["unchanged_in_db"] += 1


async def sync_panel_to_db() -> dict:
    x3 = X3()
    sql = AsyncSQL()
    stats = {
        "panel_total": 0,
        "skipped_non_matching_username": 0,
        "processed_matching_username": 0,
        "unchanged_in_db": 0,
        "updated_subscribtion": 0,
        "updated_subscription_end_date": 0,
        "created_users": 0,
        "create_errors": 0,
        "skipped_no_short_uuid": 0,
        "skipped_no_expire": 0,
    }

    try:
        panel_users = await x3.get_all_panel()
        stats["panel_total"] = len(panel_users)
        logger.info(f"Загружено пользователей из панели: {stats['panel_total']}")

        for panel_user in panel_users:
            username = str(panel_user.get("username") or "").strip()
            parsed = _parse_panel_username(username)
            if parsed is None:
                stats["skipped_non_matching_username"] += 1
                continue

            stats["processed_matching_username"] += 1
            telegram_id, tier = parsed
            short_uuid = panel_user.get("shortUuid")
            expire_at_utc = _panel_expire_to_utc(panel_user.get("expireAt"))
            traffic = panel_user.get("userTraffic") or {}
            first_connected = traffic.get("firstConnectedAt")
            is_connected = first_connected is not None

            user = await sql.get_user_object_by_user_id(telegram_id)

            if user is not None:
                await _sync_existing_user(
                    sql, user, telegram_id, tier, short_uuid, expire_at_utc, stats
                )
                continue

            if not short_uuid:
                stats["skipped_no_short_uuid"] += 1
                logger.warning(
                    f"Пропуск создания {telegram_id} (tier={tier}): нет shortUuid в панели"
                )
                stats["create_errors"] += 1
                continue
            if expire_at_utc is None:
                stats["skipped_no_expire"] += 1
                logger.warning(
                    f"Пропуск создания {telegram_id} (tier={tier}): нет expireAt в панели"
                )
                stats["create_errors"] += 1
                continue

            inserted = await sql.add_user(telegram_id, in_panel=True, is_connect=is_connected)
            await _apply_tier_on_create(sql, telegram_id, tier, short_uuid, expire_at_utc)

            if await sql.get_user_object_by_user_id(telegram_id) is None:
                stats["create_errors"] += 1
                logger.error(f"Не удалось создать пользователя {telegram_id} в БД")
                continue

            if inserted:
                stats["created_users"] += 1
                logger.info(
                    f"Создан user_id={telegram_id} in_panel=True is_connect={is_connected} "
                    f"tier={tier} subscribtion={short_uuid} end_date={expire_at_utc}"
                )
            else:
                logger.info(
                    f"Пользователь {telegram_id} уже был в БД (гонка/другая запись панели), "
                    f"применены поля tier={tier}"
                )

    finally:
        await x3.close()

    return stats


def _print_report(stats: dict) -> None:
    print("\n" + "=" * 60)
    print("ОТЧЁТ: sync_panel_to_db")
    print("=" * 60)
    print(f"Всего в панели:                         {stats['panel_total']}")
    print(
        f"Пропущено (username не цифры/_3/_10):   "
        f"{stats['skipped_non_matching_username']}"
    )
    print(f"Обработано (подходящий username):       {stats['processed_matching_username']}")
    print("-" * 60)
    print(f"1. В БД, данные не менялись:            {stats['unchanged_in_db']}")
    print(
        f"2. Обновлено subscribtion / _3 / _10:   {stats['updated_subscribtion']}"
    )
    print(
        f"3. Обновлено subscription_end_date "
        f"/ _3 / _10:  {stats['updated_subscription_end_date']}"
    )
    print(f"4. Создано пользователей:               {stats['created_users']}")
    if stats["create_errors"]:
        print(f"Ошибок/пропусков при создании:          {stats['create_errors']}")
    if stats["skipped_no_short_uuid"] or stats["skipped_no_expire"]:
        print(
            f"Пропуски (нет shortUuid/expireAt):      "
            f"shortUuid={stats['skipped_no_short_uuid']}, "
            f"expireAt={stats['skipped_no_expire']}"
        )
    print("=" * 60 + "\n")


async def main() -> None:
    _configure_stdout_utf8()
    logger.info("Старт sync_panel_to_db")
    stats = await sync_panel_to_db()
    _print_report(stats)
    logger.info(f"Завершено sync_panel_to_db: {stats}")


if __name__ == "__main__":
    asyncio.run(main())
