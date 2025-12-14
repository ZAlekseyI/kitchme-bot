import logging
import os
import re
from datetime import datetime, timezone

import psycopg2
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ===================== ENV =====================
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # например: https://kitchme-bot.onrender.com  (БЕЗ /webhook)
PORT = int(os.environ.get("PORT", "10000"))

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")

WEBHOOK_PATH = "/webhook"
HEALTH_PATH = "/health"
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

# ===================== BOT =====================
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# фикс контекста (важно для webhook режима)
Bot.set_current(bot)
Dispatcher.set_current(dp)

# ===================== LINKS =====================
DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# ===================== DB =====================
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_columns(cur, table: str, columns: list[tuple[str, str]]):
    """
    columns: [("col_name", "SQL_TYPE"), ...]
    Добавляет отсутствующие колонки без потери данных.
    """
    for col, col_type in columns:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type};")


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

    # мягкая миграция, если таблицы были старыми
    ensure_columns(cur, "users", [
        ("first_seen_at", "TIMESTAMP"),
        ("last_seen_at", "TIMESTAMP"),
        ("start_param", "TEXT"),
        ("source", "TEXT"),
        ("source_variant", "TEXT"),
    ])
    ensure_columns(cur, "events", [
        ("start_param", "TEXT"),
        ("source", "TEXT"),
        ("source_variant", "TEXT"),
    ])

    conn.commit()
    cur.close()
    conn.close()
    log.info("Таблицы users/events проверены/созданы и мигрированы (если нужно)")


def parse_start_param(start_param: str | None):
    """
    Примеры:
      youtube2 -> start_param=youtube2, source=youtube, source_variant=2
      vk1 -> vk, 1
      bonus -> bonus, None
      tg -> tg, None
    """
    if not start_param:
        return None, None, None

    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", start_param.strip())
    if not m:
        sp = start_param.strip()
        return sp, sp.lower(), None

    return start_param.strip(), m.group(1).lower(), m.group(2)


def add_or_update_user(user: types.User, start_param: str | None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    sp, src, var = parse_start_param(start_param)

    conn = get_conn()
    cur = conn.cursor()

    # вставляем или обновляем "последний визит"; первый источник фиксируем только при первом входе
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


def get_user_start_param(telegram_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT start_param FROM users WHERE telegram_id=%s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None


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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и собираю статистику источников трафика.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    sp = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "bonus", sp)

    text = (
        "🎁 Ваши бонусы готовы!\n\n"
        "Скачивайте по ссылке ниже ⤵️\n\n"
        f"{BONUS_LINK}\n\n"
        "Есть вопросы по кухне/шкафу?\n"
        "Наши дизайнеры готовы помочь — бесплатно."
    )
    await message.answer(text)


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def handle_consult(message: types.Message):
    sp = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "consult", sp)

    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)

# ===================== AIOHTTP (WEBHOOK + HEALTH) =====================
async def root_handler(request):
    return web.Response(text="ok")


async def health_handler(request):
    # HEAD для этого же маршрута aiohttp отдаст автоматически
    return web.json_response({"status": "ok"})


async def webhook_handler(request: web.Request):
    try:
        data = await request.json()
        update = types.Update.to_object(data)
        await dp.process_update(update)
    except Exception as e:
        log.exception("Ошибка обработки webhook: %s", e)
    return web.Response(text="ok")


async def on_app_startup(app: web.Application):
    log.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    init_db()

    if not WEBHOOK_HOST:
        log.warning("WEBHOOK_HOST не задан — webhook НЕ будет установлен.")
        return

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_app_cleanup(app: web.Application):
    # ВАЖНО: не удаляем webhook при рестарте/деплое, иначе получишь url:""
    # просто закрываем сессию
    await bot.session.close()
    log.info("Bot session закрыта")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", root_handler)
    app.router.add_get(HEALTH_PATH, health_handler)
    app.router.add_post(WEBHOOK_PATH, webhook_handler)

    app.on_startup.append(on_app_startup)
    app.on_cleanup.append(on_app_cleanup)
    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
