import logging
import os
from datetime import datetime, timedelta, timezone

import psycopg2
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

# -------------------- LOGGING --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# -------------------- ENV --------------------
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # https://kitchme-bot.onrender.com
PORT = int(os.environ.get("PORT", "8000"))

ADMIN_IDS = {
    int(x.strip())
    for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

if not API_TOKEN or not DATABASE_URL or not WEBHOOK_HOST:
    raise RuntimeError("Проверь API_TOKEN, DATABASE_URL, WEBHOOK_HOST в Render")

# -------------------- CONSTANTS --------------------
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

TZ_MSK = timezone(timedelta(hours=3))

# -------------------- BOT --------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# важно для aiogram 2 в кастомном aiohttp приложении
Bot.set_current(bot)
Dispatcher.set_current(dp)

# -------------------- DB --------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_tables_and_migrate():
    """
    Делает:
    - create table если таблиц нет
    - add column если таблицы уже есть старые
    Никаких данных не теряем.
    """
    conn = get_conn()
    cur = conn.cursor()

    # 1) users
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # мягкая миграция users
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_param_first TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source_first TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source_variant_first TEXT;")

    # 2) events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT,
        event_type TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # мягкая миграция events
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS start_param TEXT;")
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE events ADD COLUMN IF NOT EXISTS source_variant TEXT;")

    conn.commit()
    cur.close()
    conn.close()
    log.info("БД и таблицы готовы + миграция выполнена (если нужна)")


def parse_source(start_param: str):
    """
    youtube2 -> (youtube, 2)
    vk -> (vk, None)
    """
    if not start_param:
        return None, None

    s = start_param.strip().lower()
    i = len(s)
    while i > 0 and s[i - 1].isdigit():
        i -= 1

    source = s[:i] or s
    variant = s[i:] or None
    return source, variant


def save_user(user: types.User, start_param: str | None):
    source, variant = parse_source(start_param)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO users (
            telegram_id, username, first_name, last_name,
            start_param_first, source_first, source_variant_first
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username=EXCLUDED.username,
            first_name=EXCLUDED.first_name,
            last_name=EXCLUDED.last_name,
            last_seen_at=CURRENT_TIMESTAMP,
            start_param_first=COALESCE(users.start_param_first, EXCLUDED.start_param_first),
            source_first=COALESCE(users.source_first, EXCLUDED.source_first),
            source_variant_first=COALESCE(users.source_variant_first, EXCLUDED.source_variant_first);
        """,
        (
            user.id, user.username, user.first_name, user.last_name,
            start_param, source, variant
        ),
    )

    conn.commit()
    cur.close()
    conn.close()


def log_event(telegram_id: int, event_type: str, start_param: str | None = None):
    source, variant = parse_source(start_param)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO events (telegram_id, event_type, start_param, source, source_variant)
        VALUES (%s,%s,%s,%s,%s)
        """,
        (telegram_id, event_type, start_param, source, variant),
    )
    conn.commit()
    cur.close()
    conn.close()


# -------------------- STATS --------------------
def stats_between(start: datetime, end: datetime):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*) FROM users
        WHERE first_seen_at >= %s AND first_seen_at < %s
        """,
        (start, end),
    )
    new_users = cur.fetchone()[0]

    def cnt(event_name: str):
        cur.execute(
            """
            SELECT COUNT(*) FROM events
            WHERE event_type=%s AND created_at >= %s AND created_at < %s
            """,
            (event_name, start, end),
        )
        return cur.fetchone()[0]

    cur.execute(
        """
        SELECT COALESCE(source,'unknown'), COALESCE(source_variant,''), COUNT(*)
        FROM events
        WHERE event_type='start'
          AND created_at >= %s AND created_at < %s
        GROUP BY 1,2
        ORDER BY 3 DESC
        """,
        (start, end),
    )
    sources = cur.fetchall()

    cur.close()
    conn.close()
    return new_users, cnt("start"), cnt("bonus"), cnt("consult"), sources


def format_stats(title: str, start: datetime, end: datetime):
    new_users, starts, bonus, consult, sources = stats_between(start, end)

    lines = [
        f"📊 {title} (МСК)",
        "",
        f"👤 Новых пользователей: {new_users}",
        f"▶️ /start: {starts}",
        f"🎁 Бонусы: {bonus}",
        f"📞 Консультации: {consult}",
        "",
        "🚦 Источники (по /start):",
    ]

    if not sources:
        lines.append("— нет данных")
    else:
        for s, v, c in sources:
            lines.append(f"• {s}{v}: {c}")

    return "\n".join(lines)


# -------------------- UI --------------------
def menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# -------------------- COMMANDS --------------------
@dp.message_handler(commands=["myid"])
async def cmd_myid(m: types.Message):
    await m.answer(f"Ваш Telegram ID: {m.from_user.id}")


@dp.message_handler(commands=["stats", "stats_week", "stats_all"])
async def cmd_stats(m: types.Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return await m.answer("⛔ Эта команда доступна только администратору")

    now = datetime.now(TZ_MSK)

    if m.text.startswith("/stats_week"):
        end = now
        start = now - timedelta(days=7)
        title = "За 7 дней"
    elif m.text.startswith("/stats_all"):
        end = now
        start = datetime(2000, 1, 1, tzinfo=TZ_MSK)
        title = "За всё время"
    else:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        title = "Сегодня"

    await m.answer(format_stats(title, start, end))


# -------------------- BOT LOGIC --------------------
@dp.message_handler(commands=["start"])
async def cmd_start(m: types.Message):
    start_param = m.get_args() or None

    save_user(m.from_user, start_param)
    log_event(m.from_user.id, "start", start_param)

    text = (
        "Привет! Я бот студии корпусной мебели kitchME.\n\n"
        "Помогу с кухней или шкафом на заказ.\n\n"
        "Выбирай, что актуально:"
    )
    await m.answer(text, reply_markup=menu())


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonus(m: types.Message):
    log_event(m.from_user.id, "bonus")
    await m.answer(f"🎁 Ваши бонусы:\n{BONUS_LINK}")


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def handle_consult(m: types.Message):
    log_event(m.from_user.id, "consult")
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK)
    )
    await m.answer("Ок, вот кнопка для связи с дизайнером:", reply_markup=kb)


# -------------------- WEB HANDLERS --------------------
async def health_handler(_request):
    return web.Response(text="OK")


async def webhook_handler(request: web.Request):
    try:
        data = await request.json()
        update = types.Update.to_object(data)
        await dp.process_update(update)
        return web.Response(text="OK")
    except Exception as e:
        log.exception("Ошибка обработки webhook: %s", e)
        # Telegram не должен получать 500, иначе будет ретраить бесконечно
        return web.Response(text="OK")


async def on_startup(app: web.Application):
    ensure_tables_and_migrate()
    await bot.set_webhook(WEBHOOK_URL)
    log.info("Webhook установлен: %s", WEBHOOK_URL)


def create_app():
    app = web.Application()
    # UptimeRobot обычно шлёт HEAD — aiohttp сам обработает HEAD для GET маршрута
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_post(WEBHOOK_PATH, webhook_handler)
    app.on_startup.append(on_startup)
    return app


# -------------------- RUN --------------------
if __name__ == "__main__":
    log.info("=== kitchME BOT STARTED ===")
    web.run_app(create_app(), host="0.0.0.0", port=PORT)
