import logging
import os
import psycopg2

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from aiohttp import web

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# ENV VARIABLES
# -------------------------------------------------
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # https://kitchme-bot.onrender.com
PORT = int(os.environ.get("PORT", 10000))

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL")

WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

# -------------------------------------------------
# BOT
# -------------------------------------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# -------------------------------------------------
# DATABASE
# -------------------------------------------------
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
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
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Таблица users проверена/создана")


def save_user(user: types.User):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (telegram_id, username, first_name, last_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (telegram_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name;
        """,
        (user.id, user.username, user.first_name, user.last_name),
    )
    conn.commit()
    cur.close()
    conn.close()

# -------------------------------------------------
# KEYBOARDS
# -------------------------------------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb

# -------------------------------------------------
# HANDLERS
# -------------------------------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    save_user(message.from_user)

    text = (
        "Привет! Я бот студии корпусной мебели kitchME.\n\n"
        "Помогаю избежать ошибок при заказе кухни или шкафа.\n\n"
        "Выбери, что тебе нужно:"
    )
    await message.answer(text, reply_markup=main_menu())


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    await message.answer("Нажмите /start, чтобы открыть меню бота.")


@dp.message_handler(commands=["about"])
async def cmd_about(message: types.Message):
    await message.answer(
        "Я бот студии корпусной мебели kitchME.\n"
        "Выдаю полезные материалы и помогаю связаться с дизайнером."
    )


@dp.message_handler(commands=["bonus"])
async def cmd_bonus(message: types.Message):
    await send_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult(message: types.Message):
    await send_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def send_bonuses(message: types.Message):
    text = (
        "🎁 Ваши бонусы готовы!\n\n"
        "Скачивайте по ссылке ⤵️\n\n"
        f"{BONUS_LINK}\n\n"
        "Если есть вопросы — дизайнер поможет бесплатно."
    )
    await message.answer(text)


@dp.message_handler(lambda m: m.text == "📞 Получить консультацию дизайнера")
async def send_consult(message: types.Message):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(
            "Написать дизайнеру", url=DESIGNER_LINK
        )
    )
    await message.answer(
        "Нажмите кнопку ниже, чтобы написать дизайнеру:",
        reply_markup=kb
    )

# -------------------------------------------------
# WEBHOOK / HEALTH
# -------------------------------------------------
async def health(request):
    return web.Response(text="OK")


async def on_startup(dispatcher):
    logger.info("=== kitchME BOT STARTED ===")
    init_db()

    if WEBHOOK_URL:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(WEBHOOK_URL)
        logger.info(f"Webhook установлен: {WEBHOOK_URL}")
    else:
        logger.warning("WEBHOOK_HOST не задан — webhook не установлен")


async def on_shutdown(dispatcher):
    logger.info("Отключаем webhook")
    await bot.delete_webhook()

# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":
    app = web.Application()
    app.router.add_get("/health", health)

    executor.start_webhook(
        dispatcher=dp,
        webhook_path=WEBHOOK_PATH,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host="0.0.0.0",
        port=PORT,
        app=app,
    )
