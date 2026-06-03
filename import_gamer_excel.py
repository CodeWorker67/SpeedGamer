"""
Импорт базы из Excel (формат экспорта /export).
Запуск: python import_gamer_excel.py [путь_к_xlsx]
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "config_bd" / "speedgamer.db"
DEFAULT_XLSX = Path(r"c:\Users\nusht\OneDrive\Desktop\gamer_2605.xlsx")

# (лист Excel, поле в Excel отсутствует → значение по умолчанию)
DEFAULTS_APPLIED: List[str] = []

CHUNK = 5000

TABLES_CLEAR_ORDER = [
    "payments_cryptobot",
    "payments_stars",
    "payments_fk_sbp",
    "payments_wata_card",
    "payments_wata_sbp",
    "payments_platega_crypto",
    "payments_cards",
    "payments",
    "gifts",
    "white_counter",
    "online",
    "users",
]


def _log_default(sheet: str, field: str, default: Any) -> None:
    msg = f"  [{sheet}] нет колонки «{field}» → по умолчанию: {default!r}"
    if msg not in DEFAULTS_APPLIED:
        DEFAULTS_APPLIED.append(msg)


def _is_na(v: Any) -> bool:
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def _bool(v: Any, default: bool = False) -> bool:
    if _is_na(v):
        return default
    if isinstance(v, (bool,)):
        return v
    if isinstance(v, (int, float)):
        return bool(int(v))
    return str(v).strip().lower() in ("1", "true", "yes", "да")


def _int_opt(v: Any) -> Optional[int]:
    if _is_na(v):
        return None
    return int(float(v))


def _bigint(v: Any) -> int:
    if _is_na(v):
        raise ValueError("bigint required")
    return int(float(v))


def _bigint_opt(v: Any) -> Optional[int]:
    if _is_na(v):
        return None
    return int(float(v))


def _str_opt(v: Any, default: Optional[str] = None) -> Optional[str]:
    if _is_na(v):
        return default
    s = str(v).strip()
    return s if s else default


def _str_req(v: Any, default: str = "") -> str:
    if _is_na(v):
        return default
    return str(v).strip()


def _dt_opt(v: Any) -> Optional[datetime]:
    if _is_na(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None)
    if isinstance(v, datetime):
        return v.replace(tzinfo=None) if v.tzinfo else v
    return v


def _date_opt(v: Any) -> Optional[date]:
    if _is_na(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _executemany(conn: sqlite3.Connection, sql: str, rows: Iterable[Sequence]) -> int:
    data = list(rows)
    if not data:
        return 0
    conn.executemany(sql, data)
    return len(data)


def _run_migrations() -> None:
    scripts = [
        "migrate_add_user_fields.py",
        "migrate_add_subscription_3_10_columns.py",
        "migrate_add_gift_device_slots.py",
        "config_bd/migrate_users_auth_fields.py",
        "config_bd/migrate_users_partner_fields.py",
    ]
    for name in scripts:
        path = ROOT / name
        if not path.is_file():
            continue
        import subprocess

        subprocess.run([sys.executable, str(path)], cwd=str(ROOT), check=False)


async def _ensure_schema() -> None:
    sys.path.insert(0, str(ROOT))
    from config_bd.models import create_tables

    await create_tables()


def _clear_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=OFF")
    for table in TABLES_CLEAR_ORDER:
        conn.execute(f'DELETE FROM "{table}"')
    conn.commit()


def _import_users(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    sheet = "users"
    _log_default(sheet, "in_chanel", False)
    _log_default(sheet, "subscribtion", None)
    _log_default(sheet, "white_subscription", None)
    _log_default(sheet, "email / password / activation_pass / field_str_*", None)
    _log_default(sheet, "field_bool_1/2/3", False)
    _log_default(sheet, "password_hash / linked_telegram_id", None)
    _log_default(sheet, "partner", None)
    _log_default(sheet, "partner_balance / partner_pay", 0)
    _log_default(sheet, "partner_flag", False)
    _log_default(sheet, "stamp (пусто в Excel)", "")

    sql = """
        INSERT INTO users (
            id, user_id, ref, is_delete, in_panel, is_connect, create_user, in_chanel,
            reserve_field, subscription_end_date, white_subscription_end_date,
            last_notification_date, last_broadcast_status, last_broadcast_date,
            stamp, ttclid, subscribtion, subscription_3_end_date, subscription_10_end_date,
            subscribtion_3, subscribtion_10, white_subscription,
            email, password, activation_pass, field_str_1, field_str_2, field_str_3,
            field_bool_1, field_bool_2, field_bool_3,
            password_hash, linked_telegram_id, partner, partner_balance, partner_pay, partner_flag
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    total = 0
    rows_buf: List[Tuple] = []

    for _, r in df.iterrows():
        rows_buf.append(
            (
                _int_opt(r["id"]),
                _bigint(r["user_id"]),
                _str_opt(r.get("ref")),
                _bool(r.get("is_delete")),
                _bool(r.get("in_panel")),
                _bool(r.get("is_connect")),
                _dt_opt(r.get("create_user")) or datetime.now(),
                False,
                _bool(r.get("reserve_field")),
                _dt_opt(r.get("subscription_end_date")),
                _dt_opt(r.get("white_subscription_end_date")),
                _date_opt(r.get("last_notification_date")),
                _str_opt(r.get("last_broadcast_status")),
                _dt_opt(r.get("last_broadcast_date")),
                _str_req(r.get("stamp"), ""),
                _str_opt(r.get("ttclid")),
                None,
                _dt_opt(r.get("subscription_3_end_date")),
                _dt_opt(r.get("subscription_10_end_date")),
                _str_opt(r.get("subscribtion_3")),
                _str_opt(r.get("subscribtion_10")),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
                None,
                None,
                0,
                0,
                0,
            )
        )
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    _reset_seq(conn, "users")
    return total


def _import_payments_generic(
    conn: sqlite3.Connection,
    df: pd.DataFrame,
    table: str,
    *,
    with_payload: bool,
) -> int:
    sheet = table
    if with_payload:
        payload_default = None
    else:
        _log_default(sheet, "payload", None)
        payload_default = None

    cols = ["id", "user_id", "amount", "time_created", "is_gift", "status", "transaction_id"]
    if with_payload:
        cols.append("payload")
    placeholders = ",".join("?" * len(cols))
    sql = f'INSERT INTO "{table}" ({",".join(cols)}) VALUES ({placeholders})'

    rows_buf: List[Tuple] = []
    total = 0
    uid_col = _col(df, "User ID", "user_id") or "User ID"
    id_col = _col(df, "ID", "id") or "ID"
    amt_col = _col(df, "Amount", "amount") or "Amount"
    tc_col = _col(df, "Time Created", "time_created") or "Time Created"
    gift_col = _col(df, "Is Gift", "is_gift") or "Is Gift"
    st_col = _col(df, "Status", "status") or "Status"
    tr_col = _col(df, "Transaction_Id", "transaction_id") or "Transaction_Id"
    pl_col = _col(df, "Payload", "payload") if with_payload else None

    for _, r in df.iterrows():
        row = [
            _int_opt(r[id_col]),
            _bigint(r[uid_col]),
            int(float(r[amt_col])),
            _dt_opt(r[tc_col]) or datetime.now(),
            _bool(r[gift_col]),
            _str_opt(r[st_col]),
            _str_opt(r[tr_col]),
        ]
        if with_payload:
            row.append(_str_opt(r[pl_col]) if pl_col else payload_default)
        rows_buf.append(tuple(row))
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    _reset_seq(conn, table)
    return total


def _import_payments_stars(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    _log_default("payments_stars", "payload", None)
    sql = """
        INSERT INTO payments_stars (id, user_id, amount, time_created, is_gift, status, payload)
        VALUES (?,?,?,?,?,?,?)
    """
    amt_col = _col(df, "Amount (Stars)", "amount") or "Amount (Stars)"
    total = 0
    rows_buf: List[Tuple] = []
    for _, r in df.iterrows():
        rows_buf.append(
            (
                _int_opt(r["ID"]),
                _bigint(r["User ID"]),
                int(float(r[amt_col])),
                _dt_opt(r["Time Created"]) or datetime.now(),
                _bool(r["Is Gift"]),
                _str_opt(r["Status"], "confirmed"),
                None,
            )
        )
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    _reset_seq(conn, "payments_stars")
    return total


def _import_payments_fk(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    sql = """
        INSERT INTO payments_fk_sbp (
            id, user_id, amount, time_created, is_gift, status, transaction_id,
            fk_order_id, nonce, signature, method, payload
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """
    _log_default("payments_fk_sbp", "nonce (пусто)", 0)
    total = 0
    rows_buf: List[Tuple] = []
    for _, r in df.iterrows():
        nonce = r.get("Nonce")
        if _is_na(nonce):
            nonce_val = 0
        else:
            nonce_val = int(float(nonce))
        rows_buf.append(
            (
                _int_opt(r["ID"]),
                _bigint(r["User ID"]),
                int(float(r["Amount"])),
                _dt_opt(r["Time Created"]) or datetime.now(),
                _bool(r["Is Gift"]),
                _str_opt(r["Status"]),
                _str_opt(r["Transaction_Id"]),
                _int_opt(r.get("FK_Order_Id")),
                nonce_val,
                _str_opt(r.get("Signature")),
                _str_req(r.get("Method"), "fksbp"),
                _str_opt(r.get("Payload")),
            )
        )
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    _reset_seq(conn, "payments_fk_sbp")
    return total


def _import_payments_cryptobot(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    sql = """
        INSERT INTO payments_cryptobot (
            id, user_id, amount, currency, time_created, is_gift, status, invoice_id, payload
        ) VALUES (?,?,?,?,?,?,?,?,?)
    """
    total = 0
    rows_buf: List[Tuple] = []
    inv_col = _col(df, "Invoice ID", "invoice_id") or "Invoice ID"
    for _, r in df.iterrows():
        rows_buf.append(
            (
                _int_opt(r["ID"]),
                _bigint(r["User ID"]),
                float(r["Amount"]),
                _str_req(r["Currency"], "USDT"),
                _dt_opt(r["Time Created"]) or datetime.now(),
                _bool(r["Is Gift"]),
                _str_opt(r["Status"], "pending"),
                _str_opt(r[inv_col]),
                _str_opt(r.get("Payload")),
            )
        )
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    _reset_seq(conn, "payments_cryptobot")
    return total


def _import_gifts(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    _log_default("gifts", "device_slots", 5)
    sql = """
        INSERT INTO gifts (gift_id, giver_id, duration, recepient_id, white_flag, device_slots, flag)
        VALUES (?,?,?,?,?,?,?)
    """
    total = 0
    rows_buf: List[Tuple] = []
    for _, r in df.iterrows():
        rows_buf.append(
            (
                _str_req(r["gift_id"]),
                _bigint(r["giver_id"]),
                int(float(r["duration"])),
                _bigint_opt(r.get("recepient_id")),
                _bool(r.get("white_flag")),
                5,
                _bool(r.get("flag")),
            )
        )
        if len(rows_buf) >= CHUNK:
            total += _executemany(conn, sql, rows_buf)
            rows_buf.clear()
    if rows_buf:
        total += _executemany(conn, sql, rows_buf)
    return total


def _import_online(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    sql = """
        INSERT INTO online (online_id, online_date, users_panel, users_active, users_pay, users_trial)
        VALUES (?,?,?,?,?,?)
    """
    id_col = _col(df, "ID", "online_id") or "ID"
    date_col = _col(df, "Дата сбора", "online_date") or "Дата сбора"
    panel_col = _col(df, "Всего в панели", "users_panel") or "Всего в панели"
    active_col = _col(df, "Активны сегодня", "users_active") or "Активны сегодня"
    pay_col = _col(df, "Платных", "users_pay") or "Платных"
    trial_col = _col(df, "Триальных", "users_trial") or "Триальных"

    rows_buf: List[Tuple] = []
    for _, r in df.iterrows():
        rows_buf.append(
            (
                _int_opt(r[id_col]),
                _dt_opt(r[date_col]) or datetime.now(),
                int(float(r[panel_col])),
                int(float(r[active_col])),
                int(float(r[pay_col])),
                int(float(r[trial_col])),
            )
        )
    total = _executemany(conn, sql, rows_buf)
    _reset_seq(conn, "online", pk="online_id")
    return total


def _import_white_counter(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    sql = "INSERT INTO white_counter (id, user_id, time_created) VALUES (?,?,?)"
    rows_buf: List[Tuple] = []
    for _, r in df.iterrows():
        rows_buf.append(
            (
                _int_opt(r["ID"]),
                _bigint(r["User ID"]),
                _dt_opt(r["Time Created"]) or datetime.now(),
            )
        )
    total = 0
    for i in range(0, len(rows_buf), CHUNK):
        total += _executemany(conn, sql, rows_buf[i : i + CHUNK])
    _reset_seq(conn, "white_counter")
    return total


def _has_sqlite_sequence(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
    ).fetchone()
    return row is not None


def _reset_seq(conn: sqlite3.Connection, table: str, pk: str = "id") -> None:
    row = conn.execute(f'SELECT MAX("{pk}") FROM "{table}"').fetchone()
    if not row or not row[0] or not _has_sqlite_sequence(conn):
        return
    conn.execute("DELETE FROM sqlite_sequence WHERE name=?", (table,))
    conn.execute("INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)", (table, row[0]))


def import_workbook(xlsx_path: Path) -> None:
    if not xlsx_path.is_file():
        raise SystemExit(f"Файл не найден: {xlsx_path}")

    print(f"Схема БД: {DB_PATH}")
    asyncio.run(_ensure_schema())
    _run_migrations()

    xl = pd.ExcelFile(xlsx_path)
    print(f"Листы: {xl.sheet_names}")

    conn = sqlite3.connect(DB_PATH)
    try:
        _clear_tables(conn)
        conn.commit()
        print("Старые данные очищены.")

        stats: List[Tuple[str, int]] = []

        df_users = pd.read_excel(xl, sheet_name="users")
        n = _import_users(conn, df_users)
        stats.append(("users", n))
        conn.commit()
        print(f"users: {n}")

        sheet_table_payload = [
            ("payments_sbp", "payments", False),
            ("payments_cards", "payments_cards", True),
            ("payments_platega_crypto", "payments_platega_crypto", True),
            ("payments_wata_sbp", "payments_wata_sbp", True),
            ("payments_wata_card", "payments_wata_card", True),
        ]
        for sheet, table, with_payload in sheet_table_payload:
            if sheet not in xl.sheet_names:
                print(f"пропуск (нет листа): {sheet}")
                continue
            df = pd.read_excel(xl, sheet_name=sheet)
            n = _import_payments_generic(conn, df, table, with_payload=with_payload)
            stats.append((table, n))
            conn.commit()
            print(f"{table}: {n}")

        if "payments_stars" in xl.sheet_names:
            n = _import_payments_stars(conn, pd.read_excel(xl, "payments_stars"))
            stats.append(("payments_stars", n))
            conn.commit()
            print(f"payments_stars: {n}")

        if "payments_fk_sbp" in xl.sheet_names:
            n = _import_payments_fk(conn, pd.read_excel(xl, "payments_fk_sbp"))
            stats.append(("payments_fk_sbp", n))
            conn.commit()
            print(f"payments_fk_sbp: {n}")

        if "payments_cryptobot" in xl.sheet_names:
            n = _import_payments_cryptobot(conn, pd.read_excel(xl, "payments_cryptobot"))
            stats.append(("payments_cryptobot", n))
            conn.commit()
            print(f"payments_cryptobot: {n}")

        if "gifts" in xl.sheet_names:
            n = _import_gifts(conn, pd.read_excel(xl, "gifts"))
            stats.append(("gifts", n))
            conn.commit()
            print(f"gifts: {n}")

        if "online" in xl.sheet_names:
            n = _import_online(conn, pd.read_excel(xl, "online"))
            stats.append(("online", n))
            conn.commit()
            print(f"online: {n}")

        if "white_counter" in xl.sheet_names:
            n = _import_white_counter(conn, pd.read_excel(xl, "white_counter"))
            stats.append(("white_counter", n))
            conn.commit()
            print(f"white_counter: {n}")

        conn.commit()
        print("\n--- Поля без колонок в Excel (подставлены значения по умолчанию) ---")
        for line in DEFAULTS_APPLIED:
            print(line)
        print("\n--- Итого ---")
        for name, cnt in stats:
            print(f"  {name}: {cnt}")
        verify = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        print(f"\nПроверка users в БД: {verify}")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Импорт gamer Excel в speedgamer.db")
    parser.add_argument("xlsx", nargs="?", default=str(DEFAULT_XLSX), help="Путь к .xlsx")
    args = parser.parse_args()
    import_workbook(Path(args.xlsx))


if __name__ == "__main__":
    main()
