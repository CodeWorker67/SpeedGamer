"""Wheel DB helpers (SQLite)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from config_bd.models import (
    WheelDiscountCheckout,
    WheelDiscountRedemption,
    WheelDiscountReservation,
    WheelFakeRecentWin,
    WheelFortuna,
)


class WheelSqlMixin:
    async def get_wheel_fortuna(self, user_id: int) -> Optional[WheelFortuna]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(WheelFortuna).where(WheelFortuna.user_id == user_id)
            )
            return result.scalar_one_or_none()

    async def ensure_wheel_fortuna(self, user_id: int) -> WheelFortuna:
        async with self.session_factory() as session:
            result = await session.execute(
                select(WheelFortuna).where(WheelFortuna.user_id == user_id)
            )
            row = result.scalar_one_or_none()
            if row is not None:
                return row
            row = WheelFortuna(user_id=user_id)
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def wheel_touch_profile(
        self,
        user_id: int,
        *,
        username: Optional[str] = None,
        full_name: Optional[str] = None,
    ) -> None:
        uname = (username or "").strip().lstrip("@") or None
        if uname and len(uname) > 255:
            uname = uname[:255]
        fname = (full_name or "").strip() or None
        if fname and len(fname) > 512:
            fname = fname[:512]
        if not uname and not fname:
            return
        values: dict[str, Any] = {"user_id": user_id}
        if uname:
            values["username"] = uname
        if fname:
            values["full_name"] = fname
        set_cols: dict[str, Any] = {"updated_at": datetime.now()}
        if uname:
            set_cols["username"] = uname
        if fname:
            set_cols["full_name"] = fname
        async with self.session_factory() as session:
            await session.execute(
                sqlite_insert(WheelFortuna)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[WheelFortuna.user_id],
                    set_=set_cols,
                )
            )
            await session.commit()

    async def wheel_add_attempts(self, user_id: int, count: int) -> None:
        if count == 0:
            return
        async with self.session_factory() as session:
            if count > 0:
                await session.execute(
                    sqlite_insert(WheelFortuna)
                    .values(user_id=user_id, attempt=count, rotation_number=0)
                    .on_conflict_do_update(
                        index_elements=[WheelFortuna.user_id],
                        set_={
                            "attempt": WheelFortuna.attempt + count,
                            "updated_at": datetime.now(),
                        },
                    )
                )
            else:
                row = (
                    await session.execute(
                        select(WheelFortuna).where(WheelFortuna.user_id == user_id)
                    )
                ).scalar_one_or_none()
                if row is None:
                    return
                rotation = int(row.rotation_number or 0)
                current = int(row.attempt or 0)
                new_attempt = max(rotation, current + count)
                row.attempt = new_attempt
                row.updated_at = datetime.now()
            await session.commit()

    async def wheel_collect_recent_history(
        self,
        *,
        limit_users: int = 500,
        limit_entries: int = 24,
    ) -> list[dict[str, Any]]:
        import json

        async with self.session_factory() as session:
            stmt = (
                select(
                    WheelFortuna.user_id,
                    WheelFortuna.history,
                    WheelFortuna.username,
                    WheelFortuna.full_name,
                )
                .where(WheelFortuna.history.isnot(None), WheelFortuna.history != "")
                .order_by(WheelFortuna.updated_at.desc())
                .limit(limit_users)
            )
            rows = (await session.execute(stmt)).all()

        flat: list[dict[str, Any]] = []
        for uid, hist_raw, username, full_name in rows:
            try:
                items = json.loads(hist_raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                prize_id = item.get("prize_id")
                if not prize_id:
                    continue
                flat.append(
                    {
                        "user_id": int(uid),
                        "prize_id": str(prize_id),
                        "at": item.get("at") or "",
                        "username": username,
                        "full_name": full_name,
                    }
                )
        flat.sort(key=lambda x: x.get("at") or "", reverse=True)
        return flat[:limit_entries]

    async def wheel_discount_checkout_expire(self) -> None:
        now = datetime.now()
        async with self.session_factory() as session:
            await session.execute(
                delete(WheelDiscountCheckout).where(WheelDiscountCheckout.expires_at < now)
            )
            await session.commit()

    async def wheel_discount_checkout_clear(self, user_id: int) -> None:
        async with self.session_factory() as session:
            await session.execute(
                delete(WheelDiscountCheckout).where(WheelDiscountCheckout.user_id == user_id)
            )
            await session.commit()

    async def wheel_discount_checkout_get(self, user_id: int) -> Optional[WheelDiscountCheckout]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(WheelDiscountCheckout).where(WheelDiscountCheckout.user_id == user_id)
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            if row.expires_at < datetime.now():
                await session.execute(
                    delete(WheelDiscountCheckout).where(WheelDiscountCheckout.user_id == user_id)
                )
                await session.commit()
                return None
            return row

    async def wheel_discount_checkout_activate(
        self,
        user_id: int,
        *,
        kind: str,
        product_key: str,
        percent: int,
        base_rub: int,
        final_rub: int,
        base_stars: int,
        final_stars: int,
        expires_at: datetime,
    ) -> bool:
        async with self.session_factory() as session:
            wf = (
                await session.execute(
                    select(WheelFortuna)
                    .where(WheelFortuna.user_id == user_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if wf is None:
                wf = WheelFortuna(user_id=user_id)
                session.add(wf)
                await session.flush()

            pct = int(percent)
            if pct == 10:
                if int(wf.discount_10 or 0) <= 0:
                    return False
                wf.discount_10 = int(wf.discount_10) - 1
            elif pct == 30:
                if int(wf.discount_30 or 0) <= 0:
                    return False
                wf.discount_30 = int(wf.discount_30) - 1
            elif pct == 50:
                if int(wf.discount_50 or 0) <= 0:
                    return False
                wf.discount_50 = int(wf.discount_50) - 1
            else:
                return False

            wf.updated_at = datetime.now()
            await session.execute(
                delete(WheelDiscountCheckout).where(WheelDiscountCheckout.user_id == user_id)
            )
            session.add(
                WheelDiscountCheckout(
                    user_id=user_id,
                    kind=kind,
                    product_key=product_key,
                    percent=pct,
                    base_rub=int(base_rub),
                    final_rub=int(final_rub),
                    base_stars=int(base_stars),
                    final_stars=int(final_stars),
                    expires_at=expires_at,
                )
            )
            await session.commit()
            return True

    async def wheel_discount_checkout_complete(
        self,
        user_id: int,
        *,
        payment_ref: str,
        payload: str,
        kind: str,
        product_key: str,
        percent: int,
    ) -> bool:
        async with self.session_factory() as session:
            if (
                await session.execute(
                    select(WheelDiscountRedemption.id).where(
                        WheelDiscountRedemption.payment_ref == payment_ref
                    )
                )
            ).scalar_one_or_none() is not None:
                return True

            chk = (
                await session.execute(
                    select(WheelDiscountCheckout)
                    .where(WheelDiscountCheckout.user_id == user_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if chk is None:
                return False
            if (
                str(chk.kind) != kind
                or str(chk.product_key) != product_key
                or int(chk.percent) != int(percent)
            ):
                return False

            await session.execute(
                delete(WheelDiscountCheckout).where(WheelDiscountCheckout.user_id == user_id)
            )
            session.add(
                WheelDiscountRedemption(
                    reservation_id=None,
                    user_id=user_id,
                    percent=int(percent),
                    kind=kind,
                    product_key=product_key,
                    payment_ref=payment_ref,
                    payload=payload[:2000] if payload else None,
                )
            )
            await session.commit()
            return True

    async def wheel_discount_expire_pending(self) -> None:
        now = datetime.now()
        async with self.session_factory() as session:
            await session.execute(
                update(WheelDiscountReservation)
                .where(
                    WheelDiscountReservation.status == "pending",
                    WheelDiscountReservation.expires_at < now,
                )
                .values(status="expired")
            )
            await session.commit()

    async def wheel_discount_cancel_pending_for_user(self, user_id: int) -> None:
        async with self.session_factory() as session:
            await session.execute(
                update(WheelDiscountReservation)
                .where(
                    WheelDiscountReservation.user_id == user_id,
                    WheelDiscountReservation.status == "pending",
                )
                .values(status="cancelled")
            )
            await session.commit()

    async def wheel_discount_get_by_token(self, token: str) -> Optional[WheelDiscountReservation]:
        async with self.session_factory() as session:
            result = await session.execute(
                select(WheelDiscountReservation).where(WheelDiscountReservation.token == token)
            )
            return result.scalar_one_or_none()

    async def wheel_discount_get_pending_for_user(
        self,
        user_id: int,
        *,
        kind: str,
        product_key: str,
    ) -> Optional[WheelDiscountReservation]:
        now = datetime.now()
        async with self.session_factory() as session:
            result = await session.execute(
                select(WheelDiscountReservation)
                .where(
                    WheelDiscountReservation.user_id == user_id,
                    WheelDiscountReservation.kind == kind,
                    WheelDiscountReservation.product_key == product_key,
                    WheelDiscountReservation.status == "pending",
                    WheelDiscountReservation.expires_at >= now,
                )
                .order_by(WheelDiscountReservation.created_at.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()

    async def wheel_discount_insert_reservation(
        self,
        *,
        token: str,
        user_id: int,
        percent: int,
        kind: str,
        product_key: str,
        base_rub: int,
        final_rub: int,
        base_stars: int,
        final_stars: int,
        expires_at: datetime,
    ) -> None:
        async with self.session_factory() as session:
            row = WheelDiscountReservation(
                token=token,
                user_id=user_id,
                percent=percent,
                kind=kind,
                product_key=product_key,
                base_rub=base_rub,
                final_rub=final_rub,
                base_stars=base_stars,
                final_stars=final_stars,
                status="pending",
                expires_at=expires_at,
            )
            session.add(row)
            await session.commit()

    async def wheel_discount_redemption_exists(self, payment_ref: str) -> bool:
        async with self.session_factory() as session:
            row = await session.execute(
                select(WheelDiscountRedemption.id).where(
                    WheelDiscountRedemption.payment_ref == payment_ref
                )
            )
            return row.scalar_one_or_none() is not None

    async def wheel_discount_commit(
        self,
        *,
        token: str,
        user_id: int,
        payment_ref: str,
        payload: str,
    ) -> bool:
        """Списывает счётчик скидки и помечает резерв consumed. Идемпотентно по payment_ref."""
        async with self.session_factory() as session:
            existing = await session.execute(
                select(WheelDiscountRedemption.id).where(
                    WheelDiscountRedemption.payment_ref == payment_ref
                )
            )
            if existing.scalar_one_or_none() is not None:
                return True

            res_row = (
                await session.execute(
                    select(WheelDiscountReservation)
                    .where(WheelDiscountReservation.token == token)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if res_row is None:
                return False
            if res_row.status == "consumed":
                if res_row.payment_ref and str(res_row.payment_ref) == payment_ref:
                    return True
                return False
            if res_row.status != "pending":
                return False
            if int(res_row.user_id) != int(user_id):
                return False
            if res_row.expires_at < datetime.now():
                res_row.status = "expired"
                await session.commit()
                return False

            pct = int(res_row.percent)
            wf = (
                await session.execute(
                    select(WheelFortuna)
                    .where(WheelFortuna.user_id == user_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if wf is None:
                wf = WheelFortuna(user_id=user_id)
                session.add(wf)
                await session.flush()
            if pct == 10:
                if int(wf.discount_10 or 0) <= 0:
                    return False
                wf.discount_10 = int(wf.discount_10) - 1
            elif pct == 30:
                if int(wf.discount_30 or 0) <= 0:
                    return False
                wf.discount_30 = int(wf.discount_30) - 1
            elif pct == 50:
                if int(wf.discount_50 or 0) <= 0:
                    return False
                wf.discount_50 = int(wf.discount_50) - 1
            else:
                return False

            wf.updated_at = datetime.now()
            res_row.status = "consumed"
            res_row.consumed_at = datetime.now()
            res_row.payment_ref = payment_ref
            session.add(
                WheelDiscountRedemption(
                    reservation_id=int(res_row.id),
                    user_id=user_id,
                    percent=pct,
                    kind=str(res_row.kind),
                    product_key=str(res_row.product_key),
                    payment_ref=payment_ref,
                    payload=payload[:2000] if payload else None,
                )
            )
            await session.commit()
            return True

    async def wheel_fake_recent_win_count(self) -> int:
        async with self.session_factory() as session:
            result = await session.execute(select(func.count(WheelFakeRecentWin.id)))
            return int(result.scalar() or 0)

    async def wheel_fake_recent_win_insert(
        self,
        *,
        prize_id: str,
        name_initial: str,
        mask_stars: int,
        won_at: datetime,
    ) -> int:
        async with self.session_factory() as session:
            row = WheelFakeRecentWin(
                prize_id=prize_id,
                name_initial=name_initial,
                mask_stars=int(mask_stars),
                won_at=won_at,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return int(row.id)

    async def wheel_fake_recent_wins_list(self, *, limit: int = 50) -> list[dict[str, Any]]:
        async with self.session_factory() as session:
            stmt = (
                select(
                    WheelFakeRecentWin.id,
                    WheelFakeRecentWin.prize_id,
                    WheelFakeRecentWin.name_initial,
                    WheelFakeRecentWin.mask_stars,
                    WheelFakeRecentWin.won_at,
                )
                .order_by(WheelFakeRecentWin.won_at.desc())
                .limit(limit)
            )
            rows = (await session.execute(stmt)).all()
        out: list[dict[str, Any]] = []
        for row_id, prize_id, initial, stars, won_at in rows:
            at = ""
            if won_at is not None:
                dt = won_at
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                at = dt.astimezone(timezone.utc).isoformat()
            out.append(
                {
                    "fake_id": int(row_id),
                    "prize_id": str(prize_id),
                    "name_initial": str(initial or "У"),
                    "mask_stars": int(stars or 4),
                    "at": at,
                }
            )
        return out

    async def wheel_fake_recent_win_trim(self, max_rows: int) -> None:
        if max_rows < 1:
            return
        async with self.session_factory() as session:
            total = await session.scalar(select(func.count(WheelFakeRecentWin.id)))
            total = int(total or 0)
            if total <= max_rows:
                return
            excess = total - max_rows
            ids_subq = (
                select(WheelFakeRecentWin.id)
                .order_by(WheelFakeRecentWin.won_at.asc())
                .limit(excess)
            )
            await session.execute(delete(WheelFakeRecentWin).where(WheelFakeRecentWin.id.in_(ids_subq)))
            await session.commit()

    async def wheel_fake_recent_win_delete_oldest(self, count: int) -> None:
        if count <= 0:
            return
        async with self.session_factory() as session:
            ids_subq = (
                select(WheelFakeRecentWin.id)
                .order_by(WheelFakeRecentWin.won_at.asc())
                .limit(count)
            )
            await session.execute(delete(WheelFakeRecentWin).where(WheelFakeRecentWin.id.in_(ids_subq)))
            await session.commit()

    async def wheel_fake_recent_win_delete_not_cash_1k(self) -> int:
        async with self.session_factory() as session:
            result = await session.execute(
                delete(WheelFakeRecentWin).where(WheelFakeRecentWin.prize_id != "cash_1k")
            )
            await session.commit()
            return int(result.rowcount or 0)

    async def wheel_count_history_wins(self) -> int:
        import json

        async with self.session_factory() as session:
            stmt = select(WheelFortuna.history).where(
                WheelFortuna.history.isnot(None),
                WheelFortuna.history != "",
            )
            rows = (await session.execute(stmt)).all()

        total = 0
        for (hist_raw,) in rows:
            try:
                items = json.loads(hist_raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict) and item.get("prize_id"):
                    total += 1
        return total

