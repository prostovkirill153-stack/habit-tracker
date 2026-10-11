"""
Бот для Telegram Mini App «Трекер привычек».

Что делает:
  * раздаёт index.html (саму Mini App) по адресу /
  * принимает от приложения привычки и настройки напоминаний: POST /api/sync (подпись initData проверяется)
  * присылает напоминания:
      - по привычкам (в заданное время или каждые N часов), если привычка ещё не отмечена
      - «План на день» — утром список сегодняшних привычек
      - «Спасти серию» — вечером, если день ещё не закрыт
      - «Итоги недели» — в воскресенье

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
DEFAULT_CFG = {
    "plan": {"on": True, "time": "09:00"},
    "save": {"on": True, "time": "23:00"},
    "week": {"on": False, "time": "20:00"},
}

DB: sqlite3.Connection | None = None
BOT_USERNAME = ""


# ---------- база ----------
def init_db() -> None:
    global DB
    DB = sqlite3.connect(DB_PATH)
    DB.row_factory = sqlite3.Row
    DB.executescript(
        """
        CREATE TABLE IF NOT EXISTS profiles(
            user_id   INTEGER PRIMARY KEY,
            tz_offset INTEGER NOT NULL DEFAULT 180,
            data      TEXT    NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS sent(
            user_id INTEGER NOT NULL,
            item    TEXT    NOT NULL,
            day     TEXT    NOT NULL,
            PRIMARY KEY(user_id, item, day)
        );
        """
    )
    DB.commit()


# ---------- очистка данных от клиента ----------
def _str(v, n: int) -> str:
    return str(v if v is not None else "")[:n]


def clean_habits(raw) -> list[dict]:
    out = []
    if not isinstance(raw, list):
        return out
    for h in raw[:60]:
        if not isinstance(h, dict):
            continue
        days_raw = h.get("days") if isinstance(h.get("days"), list) else []
        days = sorted({d for d in days_raw if isinstance(d, int) and 1 <= d <= 7})
        times_raw = h.get("times") if isinstance(h.get("times"), list) else []
        times = sorted({str(t) for t in times_raw[:12] if TIME_RE.match(str(t))})
        created = _str(h.get("created"), 10)
        typ = h.get("type") if h.get("type") in ("check", "count", "quit") else "check"
        try:
            target = max(1, min(999, int(h.get("target", 1))))
        except (TypeError, ValueError):
            target = 1
        if not days or not DATE_RE.match(created):
            continue
        out.append({
            "id": _str(h.get("id"), 32), "name": _str(h.get("name"), 64) or "Привычка", "type": typ,
            "target": target, "unit": _str(h.get("unit"), 16), "days": days, "times": times, "created": created,
        })
    return out


def clean_cfg(raw) -> dict:
    cfg = json.loads(json.dumps(DEFAULT_CFG))
    if isinstance(raw, dict):
        for k in cfg:
            v = raw.get(k)
            if isinstance(v, dict):
                cfg[k]["on"] = bool(v.get("on", cfg[k]["on"]))
                if TIME_RE.match(str(v.get("time", ""))):
                    cfg[k]["time"] = str(v["time"])
    return cfg


# ---------- тексты ----------
def plural(n: int, a: str, b: str, c: str) -> str:
    m10, m100 = n % 10, n % 100
    if m10 == 1 and m100 != 11:
        return a
    if 2 <= m10 <= 4 and not 12 <= m100 <= 14:
        return b
    return c


def days_word(n: int) -> str:
    return f"{n} {plural(n, 'день', 'дня', 'дней')}"


def habit_text(h: dict) -> str:
    goal = f"\nЦель: {h['target']} {h['unit']}".rstrip() if h["type"] == "count" else ""
    return f"⏰ Пора: {h['name']}{goal}\nОтметь в трекере, когда сделаешь."


def plan_text(habits: list[dict], streak) -> str:
    lines = []
    for h in habits:
        if h["type"] == "count":
            lines.append(f"• {h['name']} — {h['target']} {h['unit']}".rstrip())
        elif h["type"] == "quit":
            lines.append(f"• {h['name']} (держимся)")
        else:
            lines.append(f"• {h['name']}")
    tail = f"\n\n🔥 Серия: {days_word(streak)}" if streak else "\n\nЗакрой все — и огонёк загорится 🔥"
    return "☀️ План на сегодня\n\n" + "\n".join(lines) + tail


def save_text(left: int, streak) -> str:
    word = plural(left, "привычка", "привычки", "привычек")
    if streak:
        return f"🔥 Огонёк скоро погаснет\nОсталось {left} {word}, а серия — {days_word(streak)}. До полуночи ещё есть время."
    return f"🔥 Осталось {left} {word} на сегодня.\nЗакрой день — и огонёк загорится."


def week_text(week: dict | None, streak) -> str:
    parts = ["📊 Итоги недели"]
    if week and week.get("all"):
        pct = round(week["ok"] / week["all"] * 100)
        parts.append(f"Выполнено {pct}% — {week['ok']} из {week['all']} отметок.")
    if streak:
        parts.append(f"🔥 Серия: {days_word(streak)}")
    parts.append("Новая неделя — новый огонёк. Так держать!")
    return "\n".join(parts)


# ---------- клавиатура ----------
def open_kb(text: str = "Открыть трекер") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))]])


# ---------- команды бота ----------
router = Router()


@router.message(CommandStart())
async def cmd_start(m: Message) -> None:
    await m.answer(
        "Привет! Это трекер привычек 🔥\n\n"
        "Выполняй все привычки за день — и огонёк загорится, а серия вырастет. "
        "Пропустишь день — выручит заморозка. А я напомню, если день ещё не закрыт.",
        reply_markup=open_kb(),
    )


@router.message(Command("help"))
async def cmd_help(m: Message) -> None:
    await m.answer(
        "Как пользоваться:\n"
        "1. Открой трекер кнопкой ниже или через меню слева от поля ввода.\n"
        "2. Добавь привычки: да/нет, количество (например, 8 стаканов) или отказ (дни без срыва).\n"
        "3. Закрывай все привычки за день — огонёк 🔥 горит, серия растёт. Заморозки ❄️ спасают пропуски.\n"
        "4. Напоминания настраиваются в приложении — значок колокольчика.\n\n"
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
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(BASE_DIR / "index.html", headers={"Cache-Control": "no-cache"})


async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def options(request: web.Request) -> web.Response:
    return web.Response()


async def api_me(request: web.Request) -> web.Response:
    return web.json_response({"username": BOT_USERNAME})


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
        tz = max(-720, min(840, int(body.get("tz", 180))))
    except (TypeError, ValueError):
        tz = 180
    today = _str(body.get("today"), 10)
    week = body.get("week") if isinstance(body.get("week"), dict) else {}
    try:
        week = {"ok": max(0, int(week.get("ok", 0))), "all": max(0, int(week.get("all", 0)))}
    except (TypeError, ValueError):
        week = {"ok": 0, "all": 0}
    try:
        streak = max(0, min(100000, int(body.get("streak", 0))))
    except (TypeError, ValueError):
        streak = 0
    done_raw = body.get("done") if isinstance(body.get("done"), list) else []
    data = {
        "today": today if DATE_RE.match(today) else None,
        "habits": clean_habits(body.get("habits")),
        "done": [_str(x, 32) for x in done_raw[:100]],
        "streak": streak,
        "todayDone": bool(body.get("todayDone")),
        "cfg": clean_cfg(body.get("cfg")),
        "week": week,
    }
    DB.execute(
        """INSERT INTO profiles(user_id, tz_offset, data) VALUES(?,?,?)
           ON CONFLICT(user_id) DO UPDATE SET tz_offset=excluded.tz_offset, data=excluded.data""",
        (init.user.id, tz, json.dumps(data, ensure_ascii=False)),
    )
    DB.commit()
    return web.json_response({"ok": True, "habits": len(data["habits"])})


# ---------- напоминания ----------
def known_streak(data: dict, day: str, yesterday: str):
    """Серия, которую можно назвать сегодня, или None, если данные устарели."""
    if data.get("today") == day:
        return data.get("streak", 0)
    if data.get("today") == yesterday and data.get("todayDone"):
        return data.get("streak", 0)
    return None


def due_items(data: dict, local: datetime) -> list[tuple[str, str, str]]:
    """Все напоминания пользователя на сегодня: (ключ, время, текст)."""
    day = local.strftime("%Y-%m-%d")
    yesterday = (local - timedelta(days=1)).strftime("%Y-%m-%d")
    weekday = local.isoweekday()
    done = set(data.get("done", [])) if data.get("today") == day else set()
    habits = [h for h in data.get("habits", []) if h["created"] <= day and weekday in h["days"]]
    remaining = [h for h in habits if h["type"] != "quit" and h["id"] not in done]
    streak = known_streak(data, day, yesterday)
    cfg = data.get("cfg") or DEFAULT_CFG
    items = []
    for h in habits:
        if h["type"] != "quit" and h["id"] in done:
            continue
        for t in h["times"]:
            items.append((f"h:{h['id']}:{t}", t, habit_text(h)))
    if cfg["plan"]["on"] and habits:
        items.append(("plan", cfg["plan"]["time"], plan_text(habits, streak)))
    if cfg["save"]["on"] and remaining:
        items.append(("save", cfg["save"]["time"], save_text(len(remaining), streak)))
    if cfg["week"]["on"] and weekday == 7:
        monday = (local - timedelta(days=6)).strftime("%Y-%m-%d")
        fresh = data.get("today") and data["today"] >= monday
        items.append(("week", cfg["week"]["time"], week_text(data.get("week") if fresh else None, streak)))
    return items


async def send(bot: Bot, user_id: int, text: str) -> None:
    try:
        await bot.send_message(user_id, text, reply_markup=open_kb())
        log.info("reminder sent to %s: %s", user_id, text.splitlines()[0])
    except TelegramAPIError as e:  # пользователь заблокировал бота и т.п.
        log.warning("reminder to %s failed: %s", user_id, e)


async def check_reminders(bot: Bot, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    rows = DB.execute("SELECT user_id, tz_offset, data FROM profiles").fetchall()
    for row in rows:
        local = now + timedelta(minutes=row["tz_offset"])
        day = local.strftime("%Y-%m-%d")
        now_min = local.hour * 60 + local.minute
        start_min = max(0, now_min - REMIND_WINDOW_MIN)
        try:
            data = json.loads(row["data"])
        except ValueError:
            continue
        for item, t, text in due_items(data, local):
            h, m = map(int, t.split(":"))
            if not (start_min <= h * 60 + m <= now_min):
                continue
            cur = DB.execute("INSERT OR IGNORE INTO sent(user_id, item, day) VALUES(?,?,?)", (row["user_id"], item, day))
            DB.commit()
            if cur.rowcount:  # ещё не отправляли сегодня
                await send(bot, row["user_id"], text)


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
    app.router.add_get("/api/me", api_me)
    app.router.add_post("/api/sync", api_sync)
    app.router.add_route("OPTIONS", "/api/sync", options)
    return app


async def main() -> None:
    global BOT_USERNAME
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

    try:
        BOT_USERNAME = (await bot.get_me()).username or ""
    except TelegramAPIError:
        BOT_USERNAME = ""
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
