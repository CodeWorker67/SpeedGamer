"""
Adds users.devices (integer, default 0).
Run from project root: python config_bd/migrate_users_devices.py
"""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "config_bd" / "speedgamer.db"


def main() -> None:
    if not DB_PATH.is_file():
        raise SystemExit(f"Database not found: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute("PRAGMA table_info(users)")
        existing = {row[1] for row in cur.fetchall()}
        if "devices" in existing:
            print("skip (exists): devices")
        else:
            conn.execute("ALTER TABLE users ADD COLUMN devices INTEGER NOT NULL DEFAULT 5")
            print("ok: devices")
        conn.execute("UPDATE users SET devices = 5 WHERE devices IS NULL OR devices < 5")
        print("ok: backfill devices=5")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
