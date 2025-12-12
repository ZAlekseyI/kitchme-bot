import logging
import os
import re
from datetime import datetime, timezone

import psycopg2
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

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

# WEBHOOK_HOST should be like: https://kitchme-bot.onrender.com  (NO /webhook)
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST", "").rstrip("/")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else ""

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.environ.get("PORT", "10000"))

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")

# ----------------------------
# BOT
# ----------------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

# ----------------------------
# CONSTANTS
# ----------------------------
DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# ----------------------------
# DB HELPERS
# ----------------------------
def get_conn():
    # sslmode=require нужен для Render Postgres
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """
    Создаём таблицу + мягкая миграция:
    добавляем поля, если их не было в старой версии.
    """
    conn = get_conn()
    cur = conn.cursor()

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

    # Мягкая миграция: добавим нужные поля, если нет
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS start_param TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS source_variant TEXT;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP;")
    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP;")

    conn.commit()
    cur.close()
    conn.close()
    log.info("Таблица users проверена/создана и мигрирована (если нужно)")


def parse_start_param(start_param: str | None):
    """
    Примеры:
      youtube1 -> source=youtube, source_variant=1
      vk -> source=vk, source_variant=NULL
      bonus -> source=bonus, source_variant=NULL
    """
    if not start_param:
        return None, None

    s = start_param.strip().lower()
    m = re.match(r"^([a-z_]+)(\d+)?$", s)
    if not m:
        # если прилетит что-то нестандартное — сохраним как source целиком
        return s, None

    source = m.group(1)
    variant = m.group(2)
    return source, variant


def upsert_user(user: types.User, start_param: str | None):
    """
    - При первом входе фиксируем start_param/source/source_variant + first_seen_at
    - При повторных входах НЕ перетираем первый источник (если уже есть),
      но обновляем last_seen_at и свежие username/имя.
    """
    source, source_variant = parse_start_param(start_param)
    now = datetime.now(timezone.utc)

    conn = get_conn()
    cur = conn.cursor()

    # Пробуем вставить новую запись
    cur.execute(
        """
        INSERT INTO users (
            telegram_id, username, first_name, last_name,
            start_param, source, source_variant,
            first_seen_at, last_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen_at = EXCLUDED.last_seen_at;
        """,
        (
            user.id,
            user.username,
            user.first_name,
            user.last_name,
            start_param,
            source,
            source_variant,
            now,
            now,
        ),
    )

    # Если пользователь уже был — проверим, пустые ли поля источника.
    # Если пустые — заполним, иначе оставим как "первый источник".
    cur.execute(
        """
        UPDATE users
        SET
            start_param = COALESCE(start_param, %s),
            source = COALESCE(source, %s),
            source_variant = COALESCE(source_variant, %s),
            first_seen_at = COALESCE(first_seen_at, %s)
        WHERE telegram_id = %s;
        """,
        (start_param, source, source_variant, now, user.id),
    )

    conn.commit()
    cur.close()
    conn.close()


# ----------------------------
# UI
# ----------------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# ----------------------------
# COMMANDS
# ----------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    # start_param приходит как /start something
    start_param = message.get_args()  # "" если нет параметра
    start_param = start_param if start_param else None

    upsert_user(message.from_user, start_param)

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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и соединяю с дизайнером.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


# ----------------------------
# BUTTON HANDLERS
# ----------------------------
@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
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
    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


# ----------------------------
# WEBHOOK LIFECYCLE
# ----------------------------
async def on_startup(dispatcher: Dispatcher):
    log.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    init_db()

    if not WEBHOOK_HOST:
        log.warning("WEBHOOK_HOST не задан — webhook не будет установлен. Проверь env в Render.")
        return

    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    log.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_shutdown(dispatcher: Dispatcher):
    log.info("Отключаем webhook...")
    await bot.delete_webhook()
    log.info("Webhook удалён. Остановка бота.")


# ----------------------------
# ENTRYPOINT
# ----------------------------
if __name__ == "__main__":
    executor.start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=WEBAPP_HOST,
        port=WEBAPP_PORT,
    )
