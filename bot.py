import os
import re
import logging
import asyncio
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

# =========================
# LOGGING
# =========================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# =========================
# ENV
# =========================
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # optional
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # https://kitchme-bot.onrender.com
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

PORT = int(os.environ.get("PORT", "8000"))
HOST = "0.0.0.0"

ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")
ADMIN_USER_ID = int(ADMIN_USER_ID) if ADMIN_USER_ID and ADMIN_USER_ID.isdigit() else None

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# Ресурсы (замени на свои)
RES_TELEGRAM = "https://t.me/Kit4Me"
RES_YOUTUBE = "https://youtube.com/@kitchmedesign"
RES_VK = "https://vk.com/your_page"
RES_SITE = "https://kitchme.ru/"

DB_DOWN_TEXT = (
    "⚠️ База данных временно недоступна.\n"
    "Статистика за этот период не собиралась."
)

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not WEBHOOK_HOST:
    raise ValueError("Не задан WEBHOOK_HOST (например https://kitchme-bot.onrender.com)")
if not DATABASE_URL:
    log.warning("DATABASE_URL не задан — бот запущен в DB Optional режиме (без статистики).")

# =========================
# AIROGRAM
# =========================
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# важные фиксы контекста в webhook-режиме
Bot.set_current(bot)
Dispatcher.set_current(dp)

# =========================
# DB OPTIONAL MODE
# =========================
DB_AVAILABLE: bool = False
DB_LAST_CHECK_UTC: Optional[datetime] = None

DB_CHECK_COOLDOWN_SEC = 30          # как часто разрешать активные переподключения из handlers
DB_WATCHDOG_INTERVAL_SEC = 20       # период фоновой проверки БД (без рестарта)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def mark_db_down(reason):
    global DB_AVAILABLE, DB_LAST_CHECK_UTC
    if DB_AVAILABLE:
        log.warning("DB switched to DOWN: %s", reason)
    DB_AVAILABLE = False
    DB_LAST_CHECK_UTC = _utcnow()


def should_recheck_db() -> bool:
    if DB_LAST_CHECK_UTC is None:
        return True
    return (_utcnow() - DB_LAST_CHECK_UTC).total_seconds() >= DB_CHECK_COOLDOWN_SEC


def check_db_once() -> bool:
    """
    Пытается выполнить SELECT 1. Никаких исключений наружу.
    """
    global DB_AVAILABLE, DB_LAST_CHECK_UTC

    if not DATABASE_URL:
        DB_AVAILABLE = False
        return False

    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
            if not DB_AVAILABLE:
                log.info("DB switched to UP")
            DB_AVAILABLE = True
            DB_LAST_CHECK_UTC = _utcnow()
            return True
        finally:
            conn.close()
    except Exception as e:
        DB_AVAILABLE = False
        DB_LAST_CHECK_UTC = _utcnow()
        log.warning("DB check failed: %s", e)
        return False


def get_conn() -> Optional[psycopg2.extensions.connection]:
    """
    Возвращает connection или None. Никогда не бросает исключения наружу.
    """
    if not DATABASE_URL:
        return None

    # если ранее падали — не долбить БД на каждом событии
    if not DB_AVAILABLE and not should_recheck_db():
        return None

    # если надо — перепроверяем (активно) по cooldown
    if not DB_AVAILABLE:
        if not check_db_once():
            return None

    try:
        return psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=5)
    except Exception as e:
        mark_db_down(e)
        return None


def column_exists(conn, table: str, column: str) -> bool:
    try:
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
    except Exception as e:
        mark_db_down(e)
        return False


def ensure_db():
    """
    Создаёт таблицы и аккуратно добавляет недостающие колонки.
    Ничего не удаляет и не теряет данные.
    """
    conn = get_conn()
    if conn is None:
        log.warning("ensure_db skipped: DB unavailable")
        return

    try:
        with conn.cursor() as cur:
            # users
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

            alters = []
            if not column_exists(conn, "users", "start_param_first"):
                alters.append("ADD COLUMN start_param_first TEXT")
            if not column_exists(conn, "users", "source_first"):
                alters.append("ADD COLUMN source_first TEXT")
            if not column_exists(conn, "users", "source_variant_first"):
                alters.append("ADD COLUMN source_variant_first TEXT")
            if alters:
                cur.execute(f"ALTER TABLE users {', '.join(alters)};")

            # events
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

            alters = []
            if not column_exists(conn, "events", "start_param"):
                alters.append("ADD COLUMN start_param TEXT")
            if not column_exists(conn, "events", "source"):
                alters.append("ADD COLUMN source TEXT")
            if not column_exists(conn, "events", "source_variant"):
                alters.append("ADD COLUMN source_variant TEXT")
            if alters:
                cur.execute(f"ALTER TABLE events {', '.join(alters)};")

        conn.commit()
        log.info("БД и таблицы готовы + миграция выполнена (если нужна)")
    except Exception as e:
        mark_db_down(e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def parse_start_param(sp: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    youtube2 -> (youtube2, youtube, 2)
    vk -> (vk, vk, None)
    unknown-format -> (raw, None, None)
    """
    if not sp:
        return None, None, None
    sp = sp.strip()
    if not sp:
        return None, None, None

    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", sp)
    if not m:
        return sp, None, None

    source = m.group(1).lower()
    variant = m.group(2)
    return sp, source, variant


def save_user(user: types.User, start_param: Optional[str]):
    conn = get_conn()
    if conn is None:
        return

    sp, source, variant = parse_start_param(start_param)

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
    except Exception as e:
        mark_db_down(e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def log_event(telegram_id: int, event_type: str, start_param: Optional[str] = None):
    conn = get_conn()
    if conn is None:
        return

    sp, source, variant = parse_start_param(start_param)

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
    except Exception as e:
        mark_db_down(e)
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =========================
# UI
# =========================
BTN_BONUS = "🎁 Забрать бонусы"
BTN_CONSULT = "📞 Получить консультацию дизайнера"
BTN_RESOURCES = "📚 Ресурсы"


def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton(BTN_BONUS))
    kb.add(KeyboardButton(BTN_CONSULT))
    kb.add(KeyboardButton(BTN_RESOURCES))
    return kb


def resources_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("Telegram", url=RES_TELEGRAM),
        InlineKeyboardButton("YouTube", url=RES_YOUTUBE),
        InlineKeyboardButton("VK", url=RES_VK),
        InlineKeyboardButton("Сайт", url=RES_SITE),
    )
    return kb


# =========================
# BOT HANDLERS
# =========================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    start_param = None
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2:
        start_param = parts[1].strip()

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
    await message.answer("Нажмите /start чтобы открыть меню. Я помогу с кухней или шкафом на заказ.")


@dp.message_handler(commands=["about"])
async def cmd_about(message: types.Message):
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и связываю с дизайнером.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult(message: types.Message):
    await handle_consult(message)


@dp.message_handler(commands=["resources"])
async def cmd_resources(message: types.Message):
    await handle_resources(message)


@dp.message_handler(lambda m: m.text == BTN_BONUS)
async def handle_bonuses(message: types.Message):
    log_event(message.from_user.id, "bonus")
    text = (
        "🎁 Ваши бонусы готовы!\n\n"
        "Скачивайте по ссылке ниже ⤵️\n\n"
        f"{BONUS_LINK}\n\n"
        "Если хотите — можно бесплатно проконсультироваться с дизайнером."
    )
    await message.answer(text)


@dp.message_handler(lambda m: m.text == BTN_CONSULT)
async def handle_consult(message: types.Message):
    log_event(message.from_user.id, "consult")
    text = (
        "Ок, свяжем вас с дизайнером.\n\n"
        "Нажмите кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


@dp.message_handler(lambda m: m.text == BTN_RESOURCES)
async def handle_resources(message: types.Message):
    log_event(message.from_user.id, "resources")
    text = "📌 Ресурсы kitchME — выберите, куда перейти:"
    await message.answer(text, reply_markup=resources_kb())


# =========================
# STATS (admin)
# =========================
def _utc_now():
    return datetime.now(timezone.utc)


def _is_admin(user_id: int) -> bool:
    if ADMIN_USER_ID is None:
        return True
    return user_id == ADMIN_USER_ID


def stats_between(start_utc: datetime, end_utc: datetime):
    # строго: если DB недоступна — вообще не делаем SQL
    if not DB_AVAILABLE:
        return None

    conn = get_conn()
    if conn is None:
        return None

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
    except Exception as e:
        mark_db_down(e)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def format_stats(title: str, start_utc: datetime, end_utc: datetime) -> str:
    data = stats_between(start_utc, end_utc)
    if data is None:
        return f"📊 {title}\n\n{DB_DOWN_TEXT}"

    new_users, starts, bonus, consult, sources = data

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
            parts = []
            for v, c in sorted(variants.items(), key=lambda x: (-x[1], x[0])):
                if v == "0":
                    parts.append(f"{c}")
                else:
                    parts.append(f"{v}:{c}")
            lines.append(f"• {src} — " + ", ".join(parts))
    return "\n".join(lines)


@dp.message_handler(commands=["stats"])
async def cmd_stats(m: types.Message):
    if not _is_admin(m.from_user.id):
        return
    if not DB_AVAILABLE:
        await m.answer(DB_DOWN_TEXT)
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
    if not DB_AVAILABLE:
        await m.answer(DB_DOWN_TEXT)
        return

    end = _utc_now()
    start = end - timedelta(days=7)
    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Последние 7 дней", start, end))


@dp.message_handler(commands=["stats_30d"])
async def cmd_stats_30d(m: types.Message):
    if not _is_admin(m.from_user.id):
        return
    if not DB_AVAILABLE:
        await m.answer(DB_DOWN_TEXT)
        return

    end = _utc_now()
    start = end - timedelta(days=30)
    log_event(m.from_user.id, "stats")
    await m.answer(format_stats("Последние 30 дней", start, end))


# =========================
# AIOHTTP APP
# =========================
async def handle_root(request: web.Request):
    return web.Response(text="ok")


async def handle_health(request: web.Request):
    # НЕ обращаемся к БД
    return web.json_response({"status": "ok", "db_available": DB_AVAILABLE})


async def handle_webhook(request: web.Request):
    try:
        data = await request.json()
        update = types.Update(**data)

        Bot.set_current(bot)
        Dispatcher.set_current(dp)

        await dp.process_update(update)
        return web.Response(text="ok")
    except Exception as e:
        log.exception("Ошибка обработки webhook: %s", e)
        # Telegram нужен 200, иначе будут ретраи
        return web.Response(text="ok")


async def db_watchdog(app: web.Application):
    """
    Фоновая проверка DB. Переключает DB_AVAILABLE без рестарта.
    """
    while True:
        try:
            # если DATABASE_URL нет — просто живём в optional режиме
            if DATABASE_URL:
                check_db_once()
                # миграции делаем только когда DB поднялась
                if DB_AVAILABLE and not app.get("db_migrated_once", False):
                    ensure_db()
                    app["db_migrated_once"] = True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # на всякий случай: watchdog тоже не должен ронять процесс
            log.warning("DB watchdog error: %s", e)
        await asyncio.sleep(DB_WATCHDOG_INTERVAL_SEC)


async def on_startup(app: web.Application):
    log.info("=== kitchME BOT STARTED ===")

    # первичная проверка: без фатала
    check_db_once()
    if DB_AVAILABLE:
        ensure_db()
        app["db_migrated_once"] = True
    else:
        app["db_migrated_once"] = False
        log.warning("DB unavailable on startup — running in optional mode")

    # старт watchdog
    app["db_watchdog_task"] = asyncio.create_task(db_watchdog(app))

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_cleanup(app: web.Application):
    log.info("Cleanup: завершаем работу (webhook не удаляем).")

    task = app.get("db_watchdog_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def create_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    app.router.add_get("/", handle_root, allow_head=True)
    app.router.add_get("/health", handle_health, allow_head=True)
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get(WEBHOOK_PATH, lambda r: web.Response(text="ok"), allow_head=True)

    return app


if __name__ == "__main__":
    web.run_app(create_app(), host=HOST, port=PORT)
