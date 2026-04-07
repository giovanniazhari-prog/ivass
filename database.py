import sqlite3
import os
import logging

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    db_path = os.getenv("DB_PATH", "bot_data.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS phone_numbers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                number TEXT UNIQUE NOT NULL,
                quality TEXT DEFAULT 'standard',
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_otps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone_number TEXT NOT NULL,
                otp_message TEXT NOT NULL,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(phone_number, otp_message)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            conn.execute("ALTER TABLE phone_numbers ADD COLUMN quality TEXT DEFAULT 'standard'")
        except Exception:
            pass
        conn.commit()
    logger.info("Database initialized")


def get_setting(key: str) -> str | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else None


def set_setting(key: str, value: str):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=CURRENT_TIMESTAMP",
            (key, value),
        )
        conn.commit()


def add_numbers(numbers: list[str], quality: str = "standard") -> tuple[int, int]:
    added = 0
    skipped = 0
    with get_connection() as conn:
        for number in numbers:
            number = number.strip()
            if not number:
                continue
            try:
                conn.execute(
                    "INSERT INTO phone_numbers (number, quality) VALUES (?, ?)",
                    (number, quality)
                )
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
        conn.commit()
    return added, skipped


def add_numbers_with_quality(entries: list[tuple[str, str]]) -> tuple[int, int]:
    added = 0
    skipped = 0
    with get_connection() as conn:
        for number, quality in entries:
            number = number.strip()
            if not number:
                continue
            try:
                conn.execute(
                    "INSERT INTO phone_numbers (number, quality) VALUES (?, ?)",
                    (number, quality)
                )
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
        conn.commit()
    return added, skipped


def get_random_numbers(count: int = 5, filter_quality: str = "all") -> list[tuple[str, str]]:
    with get_connection() as conn:
        if filter_quality == "bio_lmb":
            rows = conn.execute(
                "SELECT number, quality FROM phone_numbers WHERE quality IN ('bio_lmb', 'bio', 'lmb') ORDER BY RANDOM() LIMIT ?",
                (count,)
            ).fetchall()
        elif filter_quality == "lmb":
            rows = conn.execute(
                "SELECT number, quality FROM phone_numbers WHERE quality IN ('bio_lmb', 'lmb') ORDER BY RANDOM() LIMIT ?",
                (count,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT number, quality FROM phone_numbers ORDER BY RANDOM() LIMIT ?",
                (count,)
            ).fetchall()
    return [(row["number"], row["quality"]) for row in rows]


def count_numbers() -> int:
    with get_connection() as conn:
        row = conn.execute("SELECT COUNT(*) as total FROM phone_numbers").fetchone()
    return row["total"] if row else 0


def count_by_quality() -> dict:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT quality, COUNT(*) as total FROM phone_numbers GROUP BY quality"
        ).fetchall()
    result = {"bio_lmb": 0, "bio": 0, "lmb": 0, "standard": 0}
    for row in rows:
        q = row["quality"] or "standard"
        result[q] = row["total"]
    return result


def clear_numbers() -> int:
    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM phone_numbers")
        conn.commit()
    return cursor.rowcount


def delete_number(number: str) -> bool:
    number = number.strip()
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM phone_numbers WHERE number = ?", (number,)
        )
        conn.commit()
    return cursor.rowcount > 0


def get_all_numbers_for_export() -> list[tuple[str, str]]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT number, quality FROM phone_numbers ORDER BY quality, number"
        ).fetchall()
    return [(row["number"], row["quality"]) for row in rows]


def is_otp_seen(phone_number: str, otp_message: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM seen_otps WHERE phone_number = ? AND otp_message = ?",
            (phone_number, otp_message),
        ).fetchone()
    return row is not None


def mark_otp_seen(phone_number: str, otp_message: str):
    with get_connection() as conn:
        try:
            conn.execute(
                "INSERT INTO seen_otps (phone_number, otp_message) VALUES (?, ?)",
                (phone_number, otp_message),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            pass


def get_today_otps() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT phone_number, otp_message, seen_at FROM seen_otps "
            "WHERE date(seen_at) = date('now') "
            "ORDER BY seen_at DESC"
        ).fetchall()
    return [
        {
            "phone_number": r["phone_number"],
            "otp_message": r["otp_message"],
            "seen_at": r["seen_at"],
        }
        for r in rows
    ]
