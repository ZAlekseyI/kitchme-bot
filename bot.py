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
    raise RuntimeError("Проверь API_TOKEN, DATABASE_URL, WEBHOOK_HOST")

# -------------------- CONSTANTS --------------------
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

TZ_MSK = timezone(timedelta(hours=3))

# -------------------- BOT --------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

Bot.set_current(bot)
Dispatcher.set_current(dp)

# -------------------- DB --------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_tables():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        start_param_first TEXT,
        source_first TEXT,
        source_variant_first TEXT
    );
    """)

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


def parse_source(start_param):
    if not start_param:
        return None, None

    s = start_param.lower()
    i = len(s)
    while i > 0 and s[i - 1].isdigit():
        i -= 1

    return s[:i], s[i:] or None


def save_user(user, start_param):
    source, variant = parse_source(start_param)
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
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
        start_param_first=COALESCE(users.start_param_first,EXCLUDED.start_param_first),
        source_first=COALESCE(users.source_first,EXCLUDED.source_first),
        source_variant_first=COALESCE(users.source_variant_first,EXCLUDED.source_variant_first)
    """, (
        user.id, user.username, user.first_name, user.last_name,
        start_param, source, variant
    ))

    conn.commit()
    cur.close()
    conn.close()


def log_event(uid, event, start_param=None):
    source, variant = parse_source(start_param)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO events (telegram_id,event_type,start_param,source,source_variant)
    VALUES (%s,%s,%s,%s,%s)
    """, (uid, event, start_param, source, variant))
    conn.commit()
    cur.close()
    conn.close()

# -------------------- STATS --------------------
def stats_between(start, end):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT COUNT(*) FROM users
    WHERE first_seen_at >= %s AND first_seen_at < %s
    """, (start, end))
    users = cur.fetchone()[0]

    def cnt(e):
        cur.execute("""
        SELECT COUNT(*) FROM events
        WHERE event_type=%s AND created_at >= %s AND created_at < %s
        """, (e, start, end))
        return cur.fetchone()[0]

    cur.execute("""
    SELECT COALESCE(source,'unknown'), COALESCE(source_variant,''), COUNT(*)
    FROM events
    WHERE event_type='start'
      AND created_at >= %s AND created_at < %s
    GROUP BY 1,2 ORDER BY 3 DESC
    """, (start, end))

    sources = cur.fetchall()
    cur.close()
    conn.close()

    return users, cnt("start"), cnt("bonus"), cnt("consult"), sources


def format_stats(title, start, end):
    users, starts, bonus, consult, sources = stats_between(start, end)

    lines = [
        f"📊 {title}",
        "",
        f"👤 Новых пользователей: {users}",
        f"▶️ /start: {starts}",
        f"🎁 Бонусы: {bonus}",
        f"📞 Консультации: {consult}",
        "",
        "🚦 Источники:"
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
async def myid(m: types.Message):
    await m.answer(f"Ваш Telegram ID: {m.from_user.id}")


@dp.message_handler(commands=["stats","stats_week","stats_all"])
async def stats(m: types.Message):
    if ADMIN_IDS and m.from_user.id not in ADMIN_IDS:
        return await m.answer("⛔ Только для администратора")

    now = datetime.now(TZ_MSK)
    if m.text == "/stats":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        title = "Сегодня"
    elif m.text == "/stats_week":
        end = now
        start = now - timedelta(days=7)
        title = "За 7 дней"
    else:
        start = datetime(2000,1,1)
        end = now
        title = "За всё время"

    await m.answer(format_stats(title, start, end))

# -------------------- BOT LOGIC --------------------
@dp.message_handler(commands=["start"])
async def start(m: types.Message):
    sp = m.get_args() or None
    save_user(m.from_user, sp)
    log_event(m.from_user.id, "start", sp)

    await m.answer(
        "Привет! Я бот студии корпусной мебели kitchME.\n\n"
        "Выбери, что актуально:",
        reply_markup=menu()
    )


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def bonus(m: types.Message):
    log_event(m.from_user.id, "bonus")
    await m.answer(f"🎁 Забирайте бонусы:\n{BONUS_LINK}")


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def consult(m: types.Message):
    log_event(m.from_user.id, "consult")
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK)
    )
    await m.answer("Связь с дизайнером:", reply_markup=kb)

# -------------------- WEB --------------------
async def health(_):
    return web.Response(text="OK")


async def webhook(req):
    data = await req.json()
    update = types.Update.to_object(data)
    await dp.process_update(update)
    return web.Response(text="OK")


async def on_startup(app):
    ensure_tables()
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


def app():
    a = web.Application()
    a.router.add_get("/", health)
    a.router.add_get("/health", health)
    a.router.add_post(WEBHOOK_PATH, webhook)
    a.on_startup.append(on_startup)
    return a

# -------------------- RUN --------------------
if __name__ == "__main__":
    log.info("=== kitchME BOT STARTED ===")
    web.run_app(app(), host="0.0.0.0", port=PORT)
