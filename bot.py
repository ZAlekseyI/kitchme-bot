import logging
import os
import re
from datetime import datetime, timezone

import psycopg2
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.utils.executor import start_webhook
from aiohttp import web

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ===================== ENV =====================
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # https://kitchme-bot.onrender.com (БЕЗ /webhook)
PORT = int(os.environ.get("PORT", "10000"))

if not API_TOKEN:
    raise ValueError("API_TOKEN не задан")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан")

WEBHOOK_PATH = "/webhook"
HEALTH_PATH = "/health"
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

# ===================== BOT =====================
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# КРИТИЧНО: фикс контекста для webhook
Bot.set_current(bot)
Dispatcher.set_current(dp)

# ===================== LINKS =====================
DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# ===================== DB =====================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # users
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            start_param TEXT,
            source TEXT,
            source_variant TEXT
        );
    """)

    # events
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            event_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            start_param TEXT,
            source TEXT,
            source_variant TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    log.info("БД и таблицы готовы")


def parse_start_param(start_param: str | None):
    if not start_param:
        return None, None, None
    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", start_param)
    if not m:
        return start_param, start_param.lower(), None
    return start_param, m.group(1).lower(), m.group(2)


def add_or_update_user(user: types.User, start_param: str | None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    sp, src, var = parse_start_param(start_param)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO users (
            telegram_id, username, first_name, last_name,
            first_seen_at, last_seen_at, start_param, source, source_variant
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen_at = EXCLUDED.last_seen_at
    """, (
        user.id, user.username, user.first_name, user.last_name,
        now, now, sp, src, var
    ))

    conn.commit()
    cur.close()
    conn.close()


def log_event(telegram_id: int, event_type: str, start_param: str | None):
    sp, src, var = parse_start_param(start_param)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO events (telegram_id, event_type, start_param, source, source_variant)
        VALUES (%s,%s,%s,%s,%s)
    """, (telegram_id, event_type, sp, src, var))
    conn.commit()
    cur.close()
    conn.close()


def get_user_start_param(telegram_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT start_param FROM users WHERE telegram_id=%s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

# ===================== UI =====================
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb

# ===================== HANDLERS =====================
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    start_param = message.get_args() or None
    add_or_update_user(message.from_user, start_param)
    log_event(message.from_user.id, "start", start_param)

    await message.answer(
        "Привет! Я бот студии корпусной мебели kitchME.\n\n"
        "Помогу с кухней или шкафом на заказ.\n\n"
        "Выберите, что нужно:",
        reply_markup=main_menu()
    )


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    sp = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "bonus", sp)

    await message.answer(
        "🎁 Ваши бонусы готовы!\n\n"
        f"{BONUS_LINK}\n\n"
        "Если хотите — поможем бесплатно."
    )


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def handle_consult(message: types.Message):
    sp = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "consult", sp)

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))

    await message.answer(
        "Нажмите кнопку ниже, чтобы написать дизайнеру:",
        reply_markup=kb
    )

# ===================== WEB =====================
async def health_handler(request):
    return web.json_response({"status": "ok"})


async def webhook_handler(request):
    try:
        data = await request.json()
        update = types.Update.to_object(data)
        await dp.process_update(update)
    except Exception as e:
        log.exception("Ошибка webhook: %s", e)
    return web.Response(text="ok")


def setup_app():
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="ok"))
    app.router.add_get(HEALTH_PATH, health_handler)
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    return app

# ===================== START =====================
async def on_startup(dp):
    log.info("=== kitchME BOT STARTED ===")
    init_db()
    if WEBHOOK_HOST:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(WEBHOOK_URL)
        log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_shutdown(dp):
    await bot.session.close()


if __name__ == "__main__":
    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=PORT,
        web_app=setup_app(),
    )
