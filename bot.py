import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from aiogram import Bot, Dispatcher, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiohttp import web


# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ----------------------------
# ENV
# ----------------------------
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

# Render: WEBHOOK_HOST должен быть вида https://kitchme-bot.onrender.com (без /webhook)
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST", "").rstrip("/")
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", "/webhook")
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else ""

# Render port
PORT = int(os.environ.get("PORT", "10000"))
HOST = os.environ.get("HOST", "0.0.0.0")

# Куда слать ежедневную статистику (ID чата/канала)
# - личка: твой user_id
# - канал: отрицательный id вида -100xxxxxxxxxx
REPORT_CHAT_ID = os.environ.get("REPORT_CHAT_ID")  # строкой, потом приведём к int

# Время отчёта
# По умолчанию: 21:00 по Москве (UTC+3). Можно поменять переменными окружения.
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "21"))
REPORT_MINUTE = int(os.environ.get("REPORT_MINUTE", "0"))
REPORT_TZ_OFFSET_HOURS = int(os.environ.get("REPORT_TZ_OFFSET_HOURS", "3"))  # MSK = +3

# Твои ссылки
DESIGNER_LINK = os.environ.get("DESIGNER_LINK", "https://t.me/kitchme_design")
BONUS_LINK = os.environ.get("BONUS_LINK", "https://disk.yandex.ru/d/TeEMNTquvbJMjg")


if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")


# ----------------------------
# AIORAM BOT + DISPATCHER
# ----------------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# Критично для ручного webhook-обработчика:
Bot.set_current(bot)
Dispatcher.set_current(dp)


# ----------------------------
# DB HELPERS
# ----------------------------
def get_conn():
    # sslmode=require — норм для Render Postgres/Managed Postgres
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Создаём таблицы и делаем мягкую миграцию (добавляем колонки, если их нет)."""
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP,
            start_param TEXT,
            source TEXT,
            source_variant TEXT
        );
        """
    )

    # events (для аналитики действий)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT,
            event_type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            start_param TEXT,
            source TEXT,
            source_variant TEXT
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()
    log.info("Таблица users/events проверена/создана")


def _parse_start_param(param: str | None):
    """
    Примеры:
      youtube1 -> source=youtube, variant=1
      vk -> source=vk, variant=None
      instagram2 -> source=instagram, variant=2
    """
    if not param:
        return None, None, None

    p = param.strip()
    m = re.match(r"^([a-zA-Z_]+)(\d+)?$", p)
    if not m:
        return p, None, None

    source = m.group(1).lower()
    variant = m.group(2) if m.group(2) else None
    return p, source, variant


def upsert_user(user: types.User, start_param: str | None):
    """
    Важно:
    - first_seen_at и "первый источник" фиксируем один раз
    - last_seen_at обновляем всегда
    - start_param/source/source_variant записываем только если пусто (чтобы не перетирало первый источник)
    """
    sp, src, var = _parse_start_param(start_param)
    now = datetime.utcnow()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT telegram_id, first_seen_at, start_param, source, source_variant FROM users WHERE telegram_id=%s",
                (user.id,))
    row = cur.fetchone()

    if row is None:
        cur.execute(
            """
            INSERT INTO users (telegram_id, username, first_name, last_name, first_seen_at, last_seen_at, start_param, source, source_variant)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (user.id, user.username, user.first_name, user.last_name, now, now, sp, src, var),
        )
    else:
        # Обновляем базовые поля и last_seen_at
        cur.execute(
            """
            UPDATE users
            SET username=%s, first_name=%s, last_name=%s, last_seen_at=%s
            WHERE telegram_id=%s
            """,
            (user.username, user.first_name, user.last_name, now, user.id),
        )
        # Если источник ещё не зафиксирован — фиксируем
        existing_start_param = row[2]
        if (existing_start_param is None) and sp:
            cur.execute(
                """
                UPDATE users
                SET start_param=%s, source=%s, source_variant=%s
                WHERE telegram_id=%s
                """,
                (sp, src, var, user.id),
            )

    conn.commit()
    cur.close()
    conn.close()


def log_event(telegram_id: int, event_type: str, start_param: str | None = None):
    sp, src, var = _parse_start_param(start_param)
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


def get_user_first_source(telegram_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT start_param, source, source_variant FROM users WHERE telegram_id=%s", (telegram_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


# ----------------------------
# UI / MENUS
# ----------------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# ----------------------------
# HANDLERS
# ----------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # start_param приходит как /start youtube2
    start_param = message.get_args() if hasattr(message, "get_args") else None
    upsert_user(message.from_user, start_param)

    # событие start логируем с тем start_param, который пришёл в этот запуск
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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и собираю аналитику по источникам трафика.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    # логируем бонус по "первому источнику" пользователя
    sp, _, _ = get_user_first_source(message.from_user.id)
    log_event(message.from_user.id, "bonus", start_param=sp)

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
    sp, _, _ = get_user_first_source(message.from_user.id)
    log_event(message.from_user.id, "consult", start_param=sp)

    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


# ----------------------------
# HEALTH ENDPOINT (для UptimeRobot)
# ----------------------------
async def health_handler(request: web.Request) -> web.Response:
    # Должно отвечать и на GET, и на HEAD
    return web.Response(text="ok")


# ----------------------------
# WEBHOOK ENDPOINT (Telegram -> POST)
# ----------------------------
async def webhook_handler(request: web.Request) -> web.Response:
    # Telegram шлёт POST JSON
    try:
        if request.method in ("GET", "HEAD"):
            # Нормально, но это не для Telegram.
            return web.Response(text="ok")

        data = await request.json()

        # ВАЖНО: контекст на каждый апдейт (фикс твоей ошибки)
        Bot.set_current(bot)
        Dispatcher.set_current(dp)

        update = types.Update.to_object(data)
        await dp.process_update(update)

        return web.Response(text="ok")
    except Exception as e:
        log.exception(f"Ошибка обработки webhook: {e}")
        # 200 чтобы Telegram не долбил бесконечно при твоих внутренних ошибках
        return web.Response(text="error", status=200)


# ----------------------------
# DAILY REPORT (21:00 MSK по умолчанию)
# ----------------------------
def _tz_now():
    tz = timezone(timedelta(hours=REPORT_TZ_OFFSET_HOURS))
    return datetime.now(tz)


def _range_for_today_utc():
    """
    Возвращаем (start_utc, end_utc) для "сегодня" в REPORT TZ.
    """
    tz = timezone(timedelta(hours=REPORT_TZ_OFFSET_HOURS))
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc).replace(tzinfo=None), end_local.astimezone(timezone.utc).replace(tzinfo=None)


def _fetch_daily_stats():
    start_utc, end_utc = _range_for_today_utc()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Общие
    cur.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE event_type='start')  AS starts,
          COUNT(*) FILTER (WHERE event_type='bonus')  AS bonuses,
          COUNT(*) FILTER (WHERE event_type='consult') AS consults
        FROM events
        WHERE created_at >= %s AND created_at < %s
        """,
        (start_utc, end_utc),
    )
    totals = cur.fetchone() or {"starts": 0, "bonuses": 0, "consults": 0}

    # По источникам (берём source из события)
    cur.execute(
        """
        SELECT COALESCE(source, 'unknown') AS source, COUNT(*) AS cnt
        FROM events
        WHERE event_type='start' AND created_at >= %s AND created_at < %s
        GROUP BY COALESCE(source, 'unknown')
        ORDER BY cnt DESC
        """,
        (start_utc, end_utc),
    )
    by_source = cur.fetchall() or []

    # По source+variant (start_param)
    cur.execute(
        """
        SELECT COALESCE(start_param, 'unknown') AS start_param, COUNT(*) AS cnt
        FROM events
        WHERE event_type='start' AND created_at >= %s AND created_at < %s
        GROUP BY COALESCE(start_param, 'unknown')
        ORDER BY cnt DESC
        LIMIT 30
        """,
        (start_utc, end_utc),
    )
    by_param = cur.fetchall() or []

    cur.close()
    conn.close()

    return totals, by_source, by_param


async def daily_report_loop():
    if not REPORT_CHAT_ID:
        log.warning("REPORT_CHAT_ID не задан — ежедневная статистика отключена")
        return

    chat_id = int(REPORT_CHAT_ID)

    while True:
        try:
            now = _tz_now()
            target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            sleep_seconds = (target - now).total_seconds()
            await asyncio.sleep(sleep_seconds)

            totals, by_source, by_param = _fetch_daily_stats()

            lines = []
            lines.append("📊 kitchME — отчёт за сегодня")
            lines.append("")
            lines.append(f"👤 Стартов: {totals.get('starts', 0)}")
            lines.append(f"🎁 Бонусы: {totals.get('bonuses', 0)}")
            lines.append(f"📞 Консультации: {totals.get('consults', 0)}")
            lines.append("")
            lines.append("Источники (start):")
            if by_source:
                for r in by_source:
                    lines.append(f"• {r['source']}: {r['cnt']}")
            else:
                lines.append("• нет данных")
            lines.append("")
            lines.append("Параметры (start_param):")
            if by_param:
                for r in by_param:
                    lines.append(f"• {r['start_param']}: {r['cnt']}")
            else:
                lines.append("• нет данных")

            await bot.send_message(chat_id=chat_id, text="\n".join(lines))
            log.info("Ежедневный отчёт отправлен")
        except Exception as e:
            log.exception(f"Ошибка ежедневного отчёта: {e}")
            await asyncio.sleep(30)


# ----------------------------
# STARTUP / SHUTDOWN
# ----------------------------
async def on_startup(app: web.Application):
    init_db()

    # Ставим webhook только если задан WEBHOOK_HOST
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_HOST не задан — webhook не будет установлен")
    else:
        # drop_pending_updates=True, чтобы не ловить хвост старых апдейтов
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(WEBHOOK_URL)
        log.info(f"Webhook установлен: {WEBHOOK_URL}")

    # запуск ежедневного отчёта
    app["daily_report_task"] = asyncio.create_task(daily_report_loop())


async def on_shutdown(app: web.Application):
    # ВАЖНО: НЕ удаляем webhook на shutdown (иначе url станет пустым и бот отвалится)
    log.info("Shutdown: останавливаем фоновые задачи...")
    task = app.get("daily_report_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    log.info("Shutdown завершён.")


# ----------------------------
# AIOHTTP APP
# ----------------------------
def create_app() -> web.Application:
    app = web.Application()

    # /health — для UptimeRobot (GET/HEAD)
    app.router.add_route("GET", "/health", health_handler)
    app.router.add_route("HEAD", "/health", health_handler)

    # webhook — Telegram будет слать POST сюда
    app.router.add_route("POST", WEBHOOK_PATH, webhook_handler)
    # можно отвечать и на HEAD/GET, чтобы UptimeRobot не ругался если ткнули сюда
    app.router.add_route("GET", WEBHOOK_PATH, webhook_handler)
    app.router.add_route("HEAD", WEBHOOK_PATH, webhook_handler)

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app


if __name__ == "__main__":
    log.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    web.run_app(create_app(), host=HOST, port=PORT)
