import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

import psycopg2
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiohttp import web

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # fallback ниже


# -------------------- CONFIG --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # https://kitchme-bot.onrender.com
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", "/webhook")  # оставь по умолчанию
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

PUBLISH_TOKEN = os.environ.get("PUBLISH_TOKEN")  # секрет для /publish
CHANNEL_ID = os.environ.get("CHANNEL_ID")        # куда публиковать по умолчанию
REPORT_CHAT_ID = os.environ.get("REPORT_CHAT_ID")  # куда слать отчёт в 21:00

TZ_NAME = os.environ.get("TZ", "Europe/Moscow")

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.environ.get("PORT", "8000"))


if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")

# Для публикации и отчётов эти два параметра крайне желательны
if not PUBLISH_TOKEN:
    log.warning("PUBLISH_TOKEN не задан — эндпоинт /publish будет недоступен.")
if not CHANNEL_ID:
    log.warning("CHANNEL_ID не задан — /publish без channel_id не сможет публиковать.")
if not REPORT_CHAT_ID:
    log.warning("REPORT_CHAT_ID не задан — ежедневный отчёт отправляться не будет.")


bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"


# -------------------- DB --------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # Мягкая миграция (добавляем колонки, если их нет)
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_param TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source_variant TEXT;")

    # events
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            event_type TEXT NOT NULL,
            source TEXT,
            source_variant TEXT,
            meta JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    log.info("Таблица users проверена/создана и мигрирована (если нужно)")


def parse_start_param(param: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    youtube2 -> (youtube2, youtube, 2)
    vk -> (vk, vk, None)
    """
    if not param:
        return None, None, None

    p = param.strip().lower()
    if not p:
        return None, None, None

    # отделим trailing digits
    i = len(p)
    while i > 0 and p[i - 1].isdigit():
        i -= 1

    source = p[:i] if i < len(p) else p
    variant = p[i:] if i < len(p) and p[i:].isdigit() else None
    return p, source, variant


def upsert_user(user: types.User, start_param: Optional[str] = None):
    now = datetime.utcnow()
    sp, src, var = parse_start_param(start_param)

    conn = get_conn()
    cur = conn.cursor()

    # создаём или обновляем базовые данные
    cur.execute(
        """
        INSERT INTO users (telegram_id, username, first_name, last_name, first_seen_at, last_seen_at, start_param, source, source_variant)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen_at = EXCLUDED.last_seen_at
        RETURNING first_seen_at, start_param, source, source_variant;
        """,
        (user.id, user.username, user.first_name, user.last_name, now, now, sp, src, var),
    )

    # важно: не перетирать "первый источник", если уже был
    row = cur.fetchone()
    existing_start_param = row[1] if row else None
    existing_source = row[2] if row else None
    existing_variant = row[3] if row else None

    if (existing_start_param is None) and sp is not None:
        cur.execute(
            """
            UPDATE users
            SET start_param=%s, source=%s, source_variant=%s
            WHERE telegram_id=%s;
            """,
            (sp, src, var, user.id),
        )

    conn.commit()
    cur.close()
    conn.close()


def log_event(telegram_id: int, event_type: str, source: Optional[str], source_variant: Optional[str], meta: Optional[dict] = None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO events (telegram_id, event_type, source, source_variant, meta)
        VALUES (%s, %s, %s, %s, %s);
        """,
        (telegram_id, event_type, source, source_variant, json.dumps(meta or {})),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_user_source(telegram_id: int) -> Tuple[Optional[str], Optional[str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT source, source_variant FROM users WHERE telegram_id=%s;", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None, None
    return row[0], row[1]


# -------------------- UI --------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# -------------------- HANDLERS --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # start-param из deep link: t.me/xxx?start=youtube2
    start_param = message.get_args()  # aiogram v2
    upsert_user(message.from_user, start_param=start_param)

    src, var = get_user_source(message.from_user.id)
    log_event(message.from_user.id, "start", src, var, meta={"start_param": start_param})

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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и помогаю получить консультацию дизайнера.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    src, var = get_user_source(message.from_user.id)
    log_event(message.from_user.id, "bonus", src, var)

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
    src, var = get_user_source(message.from_user.id)
    log_event(message.from_user.id, "consult", src, var)

    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


# -------------------- AIOHTTP APP (webhook + health + publish) --------------------
async def health_handler(request: web.Request):
    return web.Response(text="OK")


async def webhook_handler(request: web.Request):
    # Telegram шлёт POST с update
    if request.method == "POST":
        try:
            data = await request.json()
            update = types.Update.to_object(data)
            await dp.process_update(update)
        except Exception as e:
            log.exception("Ошибка обработки webhook: %s", e)
        return web.Response(text="OK")

    # Для UptimeRobot/браузера
    return web.Response(text="OK")


async def publish_handler(request: web.Request):
    """
    POST /publish?token=...  или header: X-Publish-Token
    JSON:
    {
      "channel_id": "@mychannel" (optional, иначе CHANNEL_ID),
      "text": "....",
      "parse_mode": "HTML" (optional),
      "disable_web_page_preview": true (optional),
      "photo": "https://..." (optional)
    }
    """
    if not PUBLISH_TOKEN:
        return web.json_response({"ok": False, "error": "PUBLISH_TOKEN not set"}, status=503)

    token = request.query.get("token") or request.headers.get("X-Publish-Token")
    if token != PUBLISH_TOKEN:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

    channel_id = payload.get("channel_id") or CHANNEL_ID
    text = payload.get("text")
    parse_mode = payload.get("parse_mode")
    disable_preview = bool(payload.get("disable_web_page_preview", True))
    photo = payload.get("photo")

    if not channel_id:
        return web.json_response({"ok": False, "error": "channel_id not provided and CHANNEL_ID not set"}, status=400)
    if not text and not photo:
        return web.json_response({"ok": False, "error": "text or photo required"}, status=400)

    try:
        if photo:
            await bot.send_photo(chat_id=channel_id, photo=photo, caption=text or "", parse_mode=parse_mode)
        else:
            await bot.send_message(chat_id=channel_id, text=text, parse_mode=parse_mode, disable_web_page_preview=disable_preview)

        # логируем событие publish (без telegram_id клиента — это сервисная публикация)
        log_event(telegram_id=0, event_type="publish", source=None, source_variant=None, meta={"channel_id": str(channel_id)})
        return web.json_response({"ok": True})
    except Exception as e:
        log.exception("Ошибка publish: %s", e)
        return web.json_response({"ok": False, "error": str(e)}, status=500)


def make_app() -> web.Application:
    app = web.Application()
    app.router.add_route("GET", "/", health_handler)
    app.router.add_route("HEAD", "/", health_handler)
    app.router.add_route("GET", "/health", health_handler)
    app.router.add_route("HEAD", "/health", health_handler)

    app.router.add_route("POST", WEBHOOK_PATH, webhook_handler)
    app.router.add_route("GET", WEBHOOK_PATH, webhook_handler)
    app.router.add_route("HEAD", WEBHOOK_PATH, webhook_handler)

    app.router.add_route("POST", "/publish", publish_handler)
    return app


# -------------------- DAILY REPORT --------------------
def _get_tz():
    if ZoneInfo:
        try:
            return ZoneInfo(TZ_NAME)
        except Exception:
            pass
    # fallback: Москва = UTC+3
    class _FixedTZ:
        def utcoffset(self, dt): return timedelta(hours=3)
        def tzname(self, dt): return "UTC+3"
        def dst(self, dt): return timedelta(0)
    return _FixedTZ()


def _moscow_now():
    tz = _get_tz()
    return datetime.now(tz)


def _utc_from_local(dt_local: datetime) -> datetime:
    # dt_local aware
    return dt_local.astimezone(ZoneInfo("UTC")) if ZoneInfo else dt_local - timedelta(hours=3)


def build_daily_report(date_local: datetime) -> str:
    """
    date_local: локальная дата (Москва)
    Отчёт за текущий день 00:00-23:59 (Мск)
    """
    tz = _get_tz()
    start_local = date_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)

    # переведём границы в UTC для сравнения с created_at (у нас UTC-naive в БД обычно)
    # В Postgres created_at DEFAULT CURRENT_TIMESTAMP — обычно в UTC на Render.
    start_utc = _utc_from_local(start_local).replace(tzinfo=None)
    end_utc = _utc_from_local(end_local).replace(tzinfo=None)

    conn = get_conn()
    cur = conn.cursor()

    # всего уникальных пользователей, кто нажал /start сегодня
    cur.execute(
        """
        SELECT COUNT(DISTINCT telegram_id)
        FROM events
        WHERE event_type='start'
          AND telegram_id <> 0
          AND created_at >= %s AND created_at < %s;
        """,
        (start_utc, end_utc),
    )
    uniq_starts = cur.fetchone()[0] or 0

    # события
    def count_event(ev: str) -> int:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM events
            WHERE event_type=%s
              AND created_at >= %s AND created_at < %s;
            """,
            (ev, start_utc, end_utc),
        )
        return cur.fetchone()[0] or 0

    bonus_cnt = count_event("bonus")
    consult_cnt = count_event("consult")

    # по источникам (по /start)
    cur.execute(
        """
        SELECT COALESCE(source, 'unknown') AS src, COALESCE(source_variant, '-') AS var, COUNT(*) AS cnt
        FROM events
        WHERE event_type='start'
          AND telegram_id <> 0
          AND created_at >= %s AND created_at < %s
        GROUP BY src, var
        ORDER BY cnt DESC;
        """,
        (start_utc, end_utc),
    )
    rows = cur.fetchall()

    cur.close()
    conn.close()

    date_str = start_local.strftime("%d.%m.%Y")

    lines = [
        f"📊 Отчёт kitchME за {date_str} (Мск)",
        "",
        f"👤 Новых/активных по /start: {uniq_starts}",
        f"🎁 Запросили бонусы: {bonus_cnt}",
        f"📞 Запросили консультацию: {consult_cnt}",
        "",
        "📌 Источники (по /start):",
    ]

    if not rows:
        lines.append("— нет данных")
    else:
        for src, var, cnt in rows:
            # пример: youtube / 2 — 5
            if var == "-" or var is None:
                lines.append(f"— {src}: {cnt}")
            else:
                lines.append(f"— {src}{var}: {cnt}")

    return "\n".join(lines)


async def report_scheduler():
    if not REPORT_CHAT_ID:
        return

    tz = _get_tz()

    while True:
        now = _moscow_now()

        # следующий запуск сегодня 21:00, или завтра 21:00
        target = now.replace(hour=21, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)

        seconds = (target - now).total_seconds()
        await asyncio.sleep(max(1, int(seconds)))

        try:
            # отчёт за текущий день (по Москве)
            today_local = _moscow_now()
            report = build_daily_report(today_local)
            await bot.send_message(chat_id=REPORT_CHAT_ID, text=report)
        except Exception as e:
            log.exception("Ошибка отправки отчёта: %s", e)

        # небольшая пауза чтобы не словить дубль
        await asyncio.sleep(5)


# -------------------- STARTUP / SHUTDOWN --------------------
async def on_startup(app: web.Application):
    log.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    init_db()

    if not WEBHOOK_HOST:
        log.warning("WEBHOOK_HOST не задан — webhook не будет установлен!")
        return

    # Ставим webhook. Важно: НЕ делать delete_webhook на shutdown (иначе url будет пустой).
    try:
        await bot.set_webhook(WEBHOOK_URL)
        log.info(f"Webhook установлен: {WEBHOOK_URL}")
    except Exception as e:
        log.exception("Не удалось установить webhook: %s", e)

    # запускаем ежедневный отчёт
    app["report_task"] = asyncio.create_task(report_scheduler())


async def on_shutdown(app: web.Application):
    log.info("Остановка сервиса. Закрываем сессию бота и фоновые задачи...")

    task = app.get("report_task")
    if task:
        task.cancel()
        try:
            await task
        except Exception:
            pass

    # ВАЖНО: webhook не удаляем, чтобы Telegram не сбрасывал URL в пустой.
    # Просто закрываем сессию.
    await bot.session.close()
    log.info("Остановлено.")


# -------------------- MAIN --------------------
def main():
    app = make_app()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host=WEBAPP_HOST, port=WEBAPP_PORT)


if __name__ == "__main__":
    main()
