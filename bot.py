import logging
import os

import psycopg2
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# ENV
# -----------------------
API_TOKEN = os.environ.get("API_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_HOST = os.environ.get("WEBHOOK_HOST")  # например: https://kitchme-bot.onrender.com

if not API_TOKEN:
    raise ValueError("Не задан API_TOKEN в переменных окружения")
if not DATABASE_URL:
    raise ValueError("Не задан DATABASE_URL в переменных окружения")
if not WEBHOOK_HOST:
    raise ValueError("Не задан WEBHOOK_HOST в переменных окружения (пример: https://kitchme-bot.onrender.com)")

# -----------------------
# LINKS (твои)
# -----------------------
DESIGNER_LINK = "https://t.me/kitchme_design"
BONUS_LINK = "https://disk.yandex.ru/d/TeEMNTquvbJMjg"

# -----------------------
# WEBHOOK SETTINGS
# -----------------------
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = WEBHOOK_HOST.rstrip("/") + WEBHOOK_PATH

WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.environ.get("PORT", "10000"))

# -----------------------
# BOT
# -----------------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)


# -----------------------
# DB
# -----------------------
def get_conn():
    # sslmode=require важно для Render Postgres
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


def add_or_update_user(user: types.User):
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


# -----------------------
# UI
# -----------------------
def main_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("🎁 Забрать бонусы"))
    kb.add(KeyboardButton("📞 Получить консультацию дизайнера"))
    return kb


# -----------------------
# HANDLERS
# -----------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    add_or_update_user(message.from_user)

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
    await message.answer("Я бот студии корпусной мебели kitchME. Выдаю бонусы и помогаю связаться с дизайнером.")


@dp.message_handler(commands=["bonus"])
async def cmd_bonus_cmd(message: types.Message):
    await handle_bonuses(message)


@dp.message_handler(commands=["consult"])
async def cmd_consult_cmd(message: types.Message):
    await handle_consult(message)


@dp.message_handler(lambda m: m.text == "🎁 Забрать бонусы")
async def handle_bonuses(message: types.Message):
    add_or_update_user(message.from_user)

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
    add_or_update_user(message.from_user)

    text = (
        "Ок, давай свяжем тебя с дизайнером.\n\n"
        "Нажми на кнопку ниже, чтобы написать в личные сообщения:"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Написать дизайнеру", url=DESIGNER_LINK))
    await message.answer(text, reply_markup=kb)


# -----------------------
# STARTUP/SHUTDOWN
# -----------------------
async def on_startup(dispatcher: Dispatcher):
    logger.info("=== kitchME BOT STARTED IN WEBHOOK MODE ===")
    logger.info("Запуск бота, инициализация БД...")
    init_db()
    logger.info("Таблица users проверена/создана")

    # Важно: на webhook не должно быть параллельно polling
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(WEBHOOK_URL)
    logger.info(f"Webhook установлен: {WEBHOOK_URL}")


async def on_shutdown(dispatcher: Dispatcher):
    logger.info("Отключаем webhook...")
    await bot.delete_webhook()
    logger.info("Webhook удалён. Остановка бота.")


# -----------------------
# MAIN
# -----------------------
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
