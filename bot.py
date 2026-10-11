"""
Бот для Telegram Mini App «Трекер привычек».

Что делает:
  * раздаёт index.html (саму Mini App) по адресу /
  * принимает от приложения список напоминаний: POST /api/sync (подпись initData проверяется)
  * в нужное время присылает напоминание, если привычка ещё не отмечена

Запуск:
  BOT_TOKEN=123:ABC WEBAPP_URL=https://твой-домен/ python bot.py
"""
import asyncio
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiohttp import web
from aiogram import Bot, Dispatcher, Router
from aiogram.exceptions import TelegramAPIError
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    Message,
    WebAppInfo,
)
from aiogram.utils.web_app import safe_parse_webapp_init_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("habits")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")
PORT = int(os.environ.get("PORT", "8080"))
DB_PATH = os.environ.get("DB_PATH", "habits.db")
BASE_DIR = Path(__file__).resolve().parent

TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REMIND_WINDOW_MIN = 5  # если бот «проспал» минуту, напоминание всё равно уйдёт в течение 5 минут

DB: sqlite3.Connection | None = None


# ---------- база ----------
def init_db() -> None:
    global DB
    DB = sqlite3.connect(DB_PATH)
    DB.row_factory = sqlite3.Row
    DB.executescript(
        """
        CREATE TABLE IF NOT EXISTS users(
            user_id   INTEGER PRIMARY KEY,
            tz_offset INTEGER NOT NULL DEFAULT 180,
            reminders TEXT    NOT NULL DEFAULT '[]',
            done_date TEXT,
            done      TEXT    NOT NULL DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS sent(
            user_id  INTEGER NOT NULL,
            habit_id TEXT    NOT NULL,
            day      TEXT    NOT NULL,
            PRIMARY KEY(user_id, habit_id, day)
        );
        """
    )
    DB.commit()


def clean_reminders(raw) -> list[dict]:
    """Оставляем только корректные напоминания, ничего лишнего от клиента не храним."""
    out = []
    if not isinstance(raw, list):
        return out
    for r in raw[:50]:
        if not isinstance(r, dict):
            continue
        t = str(r.get("time", ""))
        days_raw = r.get("days") if isinstance(r.get("days"), list) else []
        days = sorted({d for d in days_raw if isinstance(d, int) and 1 <= d <= 7})
        if not TIME_RE.match(t) or not days:
            continue
        out.append({
            "id": str(r.get("id", ""))[:32],
            "name": str(r.get("name", ""))[:64] or "Привычка",
            "emoji": str(r.get("emoji", ""))[:8],
            "time": t,
            "days": days,
        })
    return out


# ---------- клавиатура ----------
def open_kb(text: str = "Открыть трекер") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))]])


# ---------- команды бота ----------
router = Router()


@router.message(CommandStart())
async def cmd_start(m: Message) -> None:
    await m.answer(
        "Привет! Это трекер привычек 🌱\n\n"
        "Отмечай привычки каждый день, следи за сериями и статистикой. "
        "Если включишь напоминание у привычки, я напишу в выбранное время.",
        reply_markup=open_kb(),
    )


@router.message(Command("help"))
async def cmd_help(m: Message) -> None:
    await m.answer(
        "Как пользоваться:\n"
        "1. Открой трекер кнопкой ниже или через меню слева от поля ввода.\n"
        "2. Добавь привычку: да/нет, количество (например, 8 стаканов) или отказ (дни без срыва).\n"
        "3. Включи у привычки напоминание, и я напомню, если она не отмечена.\n\n"
        "Данные хранятся в облаке Telegram и доступны с любого устройства.",
        reply_markup=open_kb(),
    )


@router.message()
async def any_message(m: Message) -> None:
    await m.answer("Открой трекер кнопкой ниже 👇", reply_markup=open_kb())


# ---------- HTTP ----------
@web.middleware
async def cors(request: web.Request, handler):
    resp = web.Response() if request.method == "OPTIONS" else await handler(request)
    if request.path.startswith("/api/"):
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(BASE_DIR / "index.html", headers={"Cache-Control": "no-cache"})


async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def options(request: web.Request) -> web.Response:
    return web.Response()


async def api_sync(request: web.Request) -> web.Response:
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "bad json"}, status=400)
    if not isinstance(body, dict):
        return web.json_response({"ok": False, "error": "bad body"}, status=400)

    # initData подписан Telegram — так мы точно знаем, какой пользователь прислал запрос
    try:
        init = safe_parse_webapp_init_data(BOT_TOKEN, str(body.get("initData", "")))
    except Exception:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    if not init.user:
        return web.json_response({"ok": False, "error": "no user"}, status=401)

    try:
        tz = int(body.get("tz", 180))
    except (TypeError, ValueError):
        tz = 180
    tz = max(-720, min(840, tz))
    reminders = clean_reminders(body.get("reminders"))
    today = str(body.get("today", ""))
    today = today if DATE_RE.match(today) else None
    done_raw = body.get("done") if isinstance(body.get("done"), list) else []
    done = [str(x)[:32] for x in done_raw[:100]]

    DB.execute(
        """INSERT INTO users(user_id, tz_offset, reminders, done_date, done) VALUES(?,?,?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET
             tz_offset=excluded.tz_offset, reminders=excluded.reminders,
             done_date=excluded.done_date, done=excluded.done""",
        (init.user.id, tz, json.dumps(reminders, ensure_ascii=False), today, json.dumps(done)),
    )
    DB.commit()
    return web.json_response({"ok": True, "reminders": len(reminders)})


# ---------- напоминания ----------
async def send_reminder(bot: Bot, user_id: int, r: dict) -> None:
    text = f"{r['emoji']} Пора: {r['name']}\nОтметь в трекере, когда сделаешь."
    try:
        await bot.send_message(user_id, text, reply_markup=open_kb())
        log.info("reminder sent to %s: %s", user_id, r["name"])
    except TelegramAPIError as e:  # пользователь заблокировал бота и т.п.
        log.warning("reminder to %s failed: %s", user_id, e)


async def check_reminders(bot: Bot, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    rows = DB.execute("SELECT user_id, tz_offset, reminders, done_date, done FROM users").fetchall()
    for row in rows:
        local = now + timedelta(minutes=row["tz_offset"])
        day = local.strftime("%Y-%m-%d")
        now_min = local.hour * 60 + local.minute
        start_min = max(0, now_min - REMIND_WINDOW_MIN)
        weekday = local.isoweekday()
        done = set(json.loads(row["done"])) if row["done_date"] == day else set()
        for r in json.loads(row["reminders"]):
            h, m = map(int, r["time"].split(":"))
            if not (start_min <= h * 60 + m <= now_min):
                continue
            if weekday not in r["days"] or r["id"] in done:
                continue
            cur = DB.execute(
                "INSERT OR IGNORE INTO sent(user_id, habit_id, day) VALUES(?,?,?)",
                (row["user_id"], r["id"], day),
            )
            DB.commit()
            if cur.rowcount:  # ещё не отправляли сегодня
                await send_reminder(bot, row["user_id"], r)


async def reminder_loop(bot: Bot) -> None:
    ticks = 0
    while True:
        try:
            await check_reminders(bot)
            ticks += 1
            if ticks % 120 == 0:  # раз в час чистим старые отметки об отправке
                DB.execute("DELETE FROM sent WHERE day < date('now', '-3 day')")
                DB.commit()
        except Exception:
            log.exception("reminder loop error")
        await asyncio.sleep(30)


# ---------- запуск ----------
def build_app() -> web.Application:
    app = web.Application(middlewares=[cors])
    app.router.add_get("/", index)
    app.router.add_get("/index.html", index)
    app.router.add_get("/health", health)
    app.router.add_post("/api/sync", api_sync)
    app.router.add_route("OPTIONS", "/api/sync", options)
    return app


async def main() -> None:
    if not BOT_TOKEN or not WEBAPP_URL.startswith("https://"):
        raise SystemExit("Задай переменные окружения BOT_TOKEN и WEBAPP_URL (адрес должен начинаться с https://)")
    init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    runner = web.AppRunner(build_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("web server on port %s", PORT)

    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть трекер"),
        BotCommand(command="help", description="Как пользоваться"),
    ])
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Трекер", web_app=WebAppInfo(url=WEBAPP_URL)))

    reminders = asyncio.create_task(reminder_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        reminders.cancel()
        await runner.cleanup()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
