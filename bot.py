import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.executor import start_webhook

# =========================
# CONFIG
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://kitchme-bot.onrender.com
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.environ.get("PORT", "8000"))

# Optional: restrict /stats to you only
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")
ADMIN_USER_ID = int(ADMIN_USER_ID) if ADMIN_USER_ID and ADMIN_USER_ID.isdigit() else None

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")
if not WEBHOOK_HOST:
    raise ValueError("Не задан WEBHOOK_HOST (например https://kitchme-bot.onrender.com)")


bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Fixes context errors in webhook mode sometimes
Bot.set_current(bot)
Dispatcher.set_current(dp)


# =========================
# DB HELPERS
# =========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def db_exec(sql: str, params=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS(
              SELECT 1 FROM information_schema.tables
              WHERE table_schema='public' AND table_name=%s
            );
            """,
            (table,),
        )
        return bool(cur.fetchone()[0])


def column_exists(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS(
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='public' AND table_name=%s AND column_name=%s
            );
            """,
            (table, column),
        )
        return bool(cur.fetchone()[0])


def ensure_db():
    """
    Creates/migrates tables safely (no data loss).
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # users table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_seen_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )

            # Add "first source" columns if missing
            alter_users = []
            if not column_exists(conn, "users", "start_param_first"):
                alter_users.append("ADD COLUMN start_param_first TEXT")
            if not column_exists(conn, "users", "source_first"):
                alter_users.append("ADD COLUMN source_first TEXT")
            if not column_exists(conn, "users", "source_variant_first"):
                alter_users.append("ADD COLUMN source_variant_first TEXT")
            if alter_users:
                cur.execute(f"ALTER TABLE users {', '.join(alter_users)};")

            # events table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT,
                    event_type TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                """
            )

            # Add analytics columns to events if missing
            alter_events = []
            if not column_exists(conn, "events", "start_param"):
                alter_events.append("ADD COLUMN start_param TEXT")
            if not column_exists(conn, "events", "source"):
                alter_events.append("ADD COLUMN source TEXT")
            if not column_exists(conn, "events", "source_variant"):
                alter_events.append("ADD COLUMN source_variant TEXT")
            if alter_events:
                cur.execute(f"ALTER TABLE events {', '.join(alter_events)};")

        conn.commit()
    finally:
        conn.close()


def parse_start_param(sp: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    sp example: youtube1, youtube2, vk, tg, bonus, instagram3
    returns: (start_param, source, source_variant)
    """
    if not sp:
        return None, None, None

    sp = sp.strip()
    if not sp:
        return None, None, None

    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", sp)
    if not m:
        # keep raw as start_param, source unknown
        return sp, None, None

    source = m.group(1).lower()
    variant = m.group(2)
    return sp, source, variant


def save_user(user: types.User, start_param: Optional[str]):
    """
    - Upserts user basic info
    - Fixes first source only once (doesn't overwrite)
    - Updates last_seen_at always
    """
    sp, source, variant = parse_start_param(start_param)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (telegram_id, username, first_name, last_name, created_at, last_seen_at,
                                   start_param_first, source_first, source_variant_first)
                VALUES (%s, %s, %s, %s, NOW(), NOW(), %s, %s, %s)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    last_seen_at = NOW(),
                    start_param_first = COALESCE(users.start_param_first, EXCLUDED.start_param_first),
                    source_first = COALESCE(users.source_first, EXCLUDED.source_first),
                    source_variant_first = COALESCE(users.source_variant_first, EXCLUDED.source_variant_first);
                """,
                (user.id, user.username, user.first_name, user.last_name, sp, source, variant),
            )
        conn.commit()
    finally:
        conn.close()


def log_event(telegram_id: int, event_type: str, start_param: Optional[str] = None):
    sp, source, variant = parse_start_param(start_param)

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (telegram_id, event_type, created_at, start_param, source, source_variant)
                VALUES (%s, %s, NOW(), %s, %s, %s);
                """,
                (telegram_id, event_type, sp, source, variant),
            )
        conn.commit()
    finally:
        conn.close()


# =========================
# UI
# =========================
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# =========================
# HANDLERS
# =========================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # read deep link param
    start_param = None
    try:
        # /start xyz
        parts = (message.text or "").split(maxsplit=1)
        if len(parts) == 2:
            start_param = parts[1].strip()
    except Exception:
        start_param = None

    save_user(message.from_user, start_param)
    log_event(message.from_user.id, "start", start_param=start_param)

    text = (
        "Привет! Я бот студии корпусной мебели kitchME.\n\n"
        "Помогу с кухней или шкафом на заказ: подскажу по планировке, "
        "ошибкам и полезным материалам.\n\n"
        "Выбери, что актуальнее:"
    )
    await message.answer(text, reply_markup=main_menu())


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.answer("Я помогу с кухней или шкафом на заказ. Нажмите /start чтобы открыть меню.")


@dp.message_handler(commands=["about"])
async def cmd_about(message: types.Message):
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и помогаю связаться с дизайнером.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    log_event(message.from_user.id, "bonus")
    text = (
        "🎁 Ваши бонусы готовы!\n\n"
        "Скачивайте по ссылке ниже ⤵️\n\n"
        f"{BONUS_LINK}\n\n"
        "Есть вопросы по вашей кухне?\n"
        "Наши дизайнеры готовы помочь — бесплатно."
    )
    await message.answer(text)


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def handle_consult(message: types.Message):
    log_event(message.from_user.id, "consult")
    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


def _utc_now():
    return datetime.now(timezone.utc)


def stats_between(start_utc: datetime, end_utc: datetime):
    """
    Returns:
      new_users, starts, bonus, consult, sources_dict
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # new users by created_at
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM users
                WHERE created_at >= %s AND created_at < %s;
                """,
                (start_utc, end_utc),
            )
            new_users = int(cur.fetchone()["c"])

            def count_events(event_type: str) -> int:
                cur.execute(
                    """
                    SELECT COUNT(*)::int AS c
                    FROM events
                    WHERE event_type = %s
                      AND created_at >= %s AND created_at < %s;
                    """,
                    (event_type, start_utc, end_utc),
                )
                return int(cur.fetchone()["c"])

            starts = count_events("start")
            bonus = count_events("bonus")
            consult = count_events("consult")

            # sources by first source from users (more stable)
            cur.execute(
                """
                SELECT COALESCE(source_first, 'unknown') AS source,
                       COALESCE(source_variant_first, '0') AS variant,
                       COUNT(*)::int AS c
                FROM users
                WHERE created_at >= %s AND created_at < %s
                GROUP BY 1,2
                ORDER BY c DESC;
                """,
                (start_utc, end_utc),
            )
            rows = cur.fetchall()

            sources: Dict[str, Dict[str, int]] = {}
            for r in rows:
                s = r["source"] or "unknown"
                v = r["variant"] or "0"
                sources.setdefault(s, {})
                sources[s][v] = int(r["c"])

            return new_users, starts, bonus, consult, sources
    finally:
        conn.close()


def format_stats(title: str, start_utc: datetime, end_utc: datetime) -> str:
    new_users, starts, bonus, consult, sources = stats_between(start_utc, end_utc)

    lines = [
        f"📊 {title}",
        "",
        f"👤 Новых пользователей: {new_users}",
        f"▶️ /start: {starts}",
        f"🎁 Бонусы: {bonus}",
        f"📞 Консультация: {consult}",
        "",
        "📌 Источники (первый заход):",
    ]

    if not sources:
        lines.append("— пока нет данных")
    else:
        for src, variants in sources.items():
            # print like youtube: v1=10, v2=3
            parts = []
            for v, c in sorted(variants.items(), key=lambda x: (-x[1], x[0])):
                if v == "0":
                    parts.append(f"{c}")
                else:
                    parts.append(f"{v}:{c}")
            lines.append(f"• {src} — " + ", ".join(parts))

    return "\n".join(lines)


def _is_admin(user_id: int) -> bool:
    if ADMIN_USER_ID is None:
        return True  # if not set - allow (you can restrict later)
    return user_id == ADMIN_USER_ID


@dp.message_handler(commands=["stats"])
async def cmd_stats(m: types.Message):
    if not _is_admin(m.from_user.id):
        return

    # default = today (UTC day)
    now = _utc_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Сегодня", start, end))


@dp.message_handler(commands=["stats_today"])
async def cmd_stats_today(m: types.Message):
    if not _is_admin(m.from_user.id):
        return

    now = _utc_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Сегодня", start, end))


@dp.message_handler(commands=["stats_7d"])
async def cmd_stats_7d(m: types.Message):
    if not _is_admin(m.from_user.id):
        return

    end = _utc_now()
    start = end - timedelta(days=7)

    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Последние 7 дней", start, end))


@dp.message_handler(commands=["stats_30d"])
async def cmd_stats_30d(m: types.Message):
    if not _is_admin(m.from_user.id):
        return

    end = _utc_now()
    start = end - timedelta(days=30)

    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Последние 30 дней", start, end))


# =========================
# AIOHTTP ROUTES
# =========================
async def health(request: web.Request):
    return web.json_response({"status": "ok"})


async def root(request: web.Request):
    return web.Response(text="ok")


# =========================
# WEBHOOK STARTUP/SHUTDOWN
# =========================
async def on_startup(dispatcher: Dispatcher):
    ensure_db()
    log.info("=== kitchME BOT STARTED ===")
    log.info("БД и таблицы готовы + миграция выполнена (если нужна)")

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_shutdown(dispatcher: Dispatcher):
    # ВАЖНО: не трогай webhook на shutdown, иначе он будет слетать при каждом рестарте/деплое
    # Render иногда шлёт SIGTERM при деплое/перезапуске => delete_webhook здесь = "бот молчит"
    log.info("Shutdown: завершаем работу (webhook не удаляем).")


def setup_app():
    app = web.Application()
    # allow_head=True => HEAD работает автоматически (не надо add_head, иначе как у тебя было: "HEAD already registered")
    app.router.add_get("/", root, allow_head=True)
    app.router.add_get("/health", health, allow_head=True)
    return app


if __name__ == "__main__":
    app = setup_app()

    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
        web_app=app,  # aiogram 2.25.1 поддерживает web_app
    )
