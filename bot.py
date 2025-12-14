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

# -------------------- LOGGING --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# -------------------- ENV --------------------
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # e.g. https://kitchme-bot.onrender.com  (NO /webhook)
PORT = int(os.environ.get("PORT", "10000"))

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")
if not WEBHOOK_HOST:
    # можно жить и без него (например локально), но на Render он нужен
    log.warning("WEBHOOK_HOST не задан — webhook не установится автоматически")

WEBHOOK_PATH = "/webhook"
HEALTH_PATH = "/health"
WEBHOOK_URL = (WEBHOOK_HOST or "").rstrip("/") + WEBHOOK_PATH

# -------------------- BOT --------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# важно для стабильности в webhook-обработке (чтобы message.answer() не падал контекстом)
Bot.set_current(bot)
Dispatcher.set_current(dp)

# -------------------- LINKS / TEXTS --------------------
DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# -------------------- DB HELPERS --------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _col_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def init_db():
    """Создаёт таблицы и мягко добавляет недостающие колонки, не ломая существующие данные."""
    conn = get_conn()
    cur = conn.cursor()

    # --- users base ---
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

    # --- events base ---
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            event_type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # --- users миграции ---
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_param TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source_variant TEXT;")

    # --- events миграции ---
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS start_param TEXT;")
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source_variant TEXT;")

    # индексы (не обязательны, но полезны)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source);")

    conn.commit()
    cur.close()
    conn.close()

    log.info("Таблица users/events проверена/создана и мигрирована (если нужно)")


def parse_start_param(start_param: str | None):
    """
    start_param:
      youtube2 -> source=youtube, variant=2
      vk1 -> source=vk, variant=1
      bonus -> source=bonus, variant=None
    """
    if not start_param:
        return None, None, None

    sp = start_param.strip()
    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", sp)
    if not m:
        # если странный формат — сохраним как есть
        return sp, sp.lower(), None

    src = (m.group(1) or "").lower()
    var = m.group(2)
    return sp, src, var


def add_or_update_user(user: types.User, start_param: str | None):
    """Фиксируем first_seen и первый источник (не перетираем), но обновляем last_seen."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    sp, src, var = parse_start_param(start_param)

    conn = get_conn()
    cur = conn.cursor()

    # upsert + first_seen_at/last_seen_at логика
    cur.execute(
        """
        INSERT INTO users (telegram_id, username, first_name, last_name, first_seen_at, last_seen_at, start_param, source, source_variant)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen_at = EXCLUDED.last_seen_at
        """,
        (user.id, user.username, user.first_name, user.last_name, now, now, sp, src, var),
    )

    # если пользователь уже был — не затираем "первый источник" пустотой
    # и не затираем вообще, если уже заполнено
    cur.execute(
        """
        UPDATE users
        SET
            start_param = COALESCE(start_param, %s),
            source = COALESCE(source, %s),
            source_variant = COALESCE(source_variant, %s),
            first_seen_at = COALESCE(first_seen_at, %s)
        WHERE telegram_id = %s
        """,
        (sp, src, var, now, user.id),
    )

    conn.commit()
    cur.close()
    conn.close()


def log_event(telegram_id: int, event_type: str, start_param: str | None):
    sp, src, var = parse_start_param(start_param)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO events (telegram_id, event_type, start_param, source, source_variant)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (telegram_id, event_type, sp, src, var),
    )
    conn.commit()
    cur.close()
    conn.close()


# -------------------- UI --------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# -------------------- HANDLERS --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # source из deep-link: https://t.me/kitchme_bot?start=youtube2
    start_param = None
    try:
        # message.get_args() в aiogram2 возвращает текст после /start
        start_param = message.get_args() or None
    except Exception:
        start_param = None

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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и собираю статистику по источникам трафика.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    # при нажатии у нас нет start_param, но мы возьмём сохранённый из users
    start_param = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "bonus", start_param)

    text = (
        "🎁 Ваши бонусы готовы!\n\n"
        "Скачивайте по ссылке ниже ⤵️\n\n"
        f"{BONUS_LINK}\n\n"
        "Если хотите — можно бесплатно подсказать по вашей планировке кухни/шкафа."
    )
    await message.answer(text)


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def handle_consult(message: types.Message):
    start_param = get_user_start_param(message.from_user.id)
    log_event(message.from_user.id, "consult", start_param)

    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


def get_user_start_param(telegram_id: int) -> str | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT start_param FROM users WHERE telegram_id = %s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row and row[0] else None


# -------------------- WEBHOOK / HEALTH SERVER --------------------
async def health_handler(request: web.Request):
    return web.json_response({"status": "ok"})


async def root_handler(request: web.Request):
    # чтобы Render/браузер не путались
    return web.Response(text="kitchME bot is running", content_type="text/plain")


async def webhook_handler(request: web.Request):
    """
    Telegram присылает сюда POST updates.
    UptimeRobot сюда слать НЕ надо (пусть ходит на /health).
    """
    try:
        data = await request.json()
        update = types.Update.to_object(data)
        await dp.process_update(update)
        return web.Response(text="ok")
    except Exception as e:
        log.exception("Ошибка обработки webhook: %s", e)
        # Telegramу всё равно лучше отвечать 200, чтобы не долбил ретраями бесконечно
        return web.Response(text="ok")


def setup_aiohttp_app() -> web.Application:
    app = web.Application()
    # GET + HEAD для uptime
    app.router.add_get("/", root_handler)
    app.router.add_head("/", root_handler)

    app.router.add_get(HEALTH_PATH, health_handler)
    app.router.add_head(HEALTH_PATH, health_handler)

    # webhook
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    # иногда мониторинги шлют HEAD — не страшно
    app.router.add_head(WEBHOOK_PATH, lambda r: web.Response(text="ok"))
    app.router.add_get(WEBHOOK_PATH, lambda r: web.Response(text="ok"))
    return app


async def on_startup(dispatcher: Dispatcher):
    log.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    init_db()

    if not WEBHOOK_HOST:
        log.warning("WEBHOOK_HOST не задан, webhook не будет установлен")
        return

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_shutdown(dispatcher: Dispatcher):
    # ВАЖНО: не удаляем webhook при каждом рестарте Render,
    # иначе будет ситуация: url пустой и бот молчит.
    log.info("Shutdown: webhook НЕ удаляем (чтобы не сбрасывался).")
    await bot.session.close()


if __name__ == "__main__":
    # aiohttp app для Render
    app = setup_aiohttp_app()

    start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=PORT,
        web_app=app,  # важно: передаём НАШЕ aiohttp приложение
    )
