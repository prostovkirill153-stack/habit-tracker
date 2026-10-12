"""
Бот для Telegram Mini App «Трекер привычек».

Что делает:
  * раздаёт index.html (саму Mini App) по адресу /
  * принимает от приложения привычки и настройки напоминаний: POST /api/sync (подпись initData проверяется)
  * хранит данные в Postgres (Neon), если задан DATABASE_URL, иначе в локальной SQLite
  * считает пользователей; владелец видит статистику командой /stats
  * ведёт еженедельную лигу по XP (5 лиг, группы до 30 человек, топ-5 поднимаются выше)
  * для владельца: ежедневный отчёт, панель с графиками, рассылка, отзывы, «Что нового», подарки,
    управление лигой, оповещения об ошибках и о лимитах бесплатных сервисов (команда /admin)
  * присылает напоминания:
      - по привычкам (в заданное время или каждые N часов), если привычка ещё не отмечена
      - «План на день» — утром список сегодняшних привычек
      - «Спасти серию» — вечером, если день ещё не закрыт
      - «Итоги недели» — в воскресенье: закрытые дни, XP, место в лиге, лучшая привычка
  * кнопки «✅ Сделал» и «+1» прямо в напоминаниях: отметка попадает в приложение при следующем открытии
  * резервная копия: приложение присылает файл в чат, а если переслать его боту — предложит восстановить
  * магазин за Telegram Stars: заморозки, починка серии, темы, поддержка автора; владелец может выдавать
    всё это бесплатно и делать возвраты из панели 👑

Запуск:
  BOT_TOKEN=123:ABC WEBAPP_URL=https://твой-домен/ [DATABASE_URL=postgresql://...] python bot.py
"""
import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    MenuButtonWebApp,
    Message,
    PreCheckoutQuery,
    WebAppInfo,
)
from aiogram.utils.web_app import safe_parse_webapp_init_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("habits")

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
WEBAPP_URL = os.environ.get("WEBAPP_URL", "")
PORT = int(os.environ.get("PORT", "8080"))
DB_PATH = os.environ.get("DB_PATH", "habits.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)
NEON_API_KEY = os.environ.get("NEON_API_KEY", "").strip()        # необязательно: для учёта часов Neon
NEON_PROJECT_ID = os.environ.get("NEON_PROJECT_ID", "").strip()
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "9"))           # ежедневный отчёт владельцу, час по МСК
LIMITS = {"db_mb": 512, "neon_cu_h": 100, "render_h": 750}      # бесплатные лимиты Neon и Render
BASE_DIR = Path(__file__).resolve().parent

TIME_RE = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REMIND_WINDOW_MIN = 5  # если бот «проспал» минуту, напоминание всё равно уйдёт в течение 5 минут
LEAGUES = ["Искра", "Огонёк", "Пламя", "Костёр", "Феникс"]
PROMOTE, DEMOTE, GROUP_MAX, DEMOTE_MIN_GROUP = 5, 5, 30, 10
LEAGUE_TZ = timedelta(hours=3)  # неделя лиги идёт по московскому времени: с понедельника 00:00


def league_week(now: datetime | None = None) -> tuple[str, int]:
    """Текущая неделя лиги: (дата понедельника 'ГГГГ-ММ-ДД' по МСК, unix-время окончания недели)."""
    now = now or datetime.now(timezone.utc)
    msk = now + LEAGUE_TZ
    monday = (msk - timedelta(days=msk.weekday())).date()
    end = datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc) + timedelta(days=7) - LEAGUE_TZ
    return monday.isoformat(), int(end.timestamp())


def msk_day(ts: float | None = None) -> str:
    """Дата по Москве для статистики и отчётов."""
    dt = datetime.fromtimestamp(ts, timezone.utc) if ts is not None else datetime.now(timezone.utc)
    return (dt + LEAGUE_TZ).strftime("%Y-%m-%d")


SOURCE_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

DEFAULT_CFG = {
    "plan": {"on": True, "time": "09:00"},
    "save": {"on": True, "time": "23:00"},
    "week": {"on": True, "time": "20:00"},
}

BOT_USERNAME = ""
BOT = None  # экземпляр Bot: нужен HTTP-обработчикам (отправка резервной копии)
BACKUP_KEY_RE = re.compile(r"^(h_[A-Za-z0-9_-]{1,32}|l_[A-Za-z0-9_-]{1,32}_\d{6}|cfg|meta|td)$")
BACKUP_MAX = 2 * 1024 * 1024

# магазин за Telegram Stars (валюта XTR). kind: что выдаём после оплаты
THEMES = {"midnight": "Полночь", "sunset": "Закат", "emerald": "Изумруд"}
ITEMS = {
    "freeze": {"title": "Заморозка", "stars": 25, "kind": "freeze", "n": 1,
               "desc": "Одна заморозка: сохранит серию, если пропустишь день."},
    "freeze3": {"title": "3 заморозки", "stars": 60, "kind": "freeze", "n": 3,
                "desc": "Набор из трёх заморозок — выгоднее, чем по одной."},
    "repair": {"title": "Починка серии", "stars": 75, "kind": "repair",
               "desc": "Вернёт серию, которая сгорела за последние два дня."},
    **{f"theme_{k}": {"title": f"Тема «{v}»", "stars": 50, "kind": "theme", "theme": k,
                      "desc": f"Тема оформления «{v}» — навсегда."} for k, v in THEMES.items()},
    **{f"tip{n}": {"title": "Поддержать автора", "stars": n, "kind": "tip",
                   "desc": f"Спасибо за поддержку! {n} ⭐ помогут развивать трекер."} for n in (50, 100, 250)},
}

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS profiles(
        user_id   BIGINT  PRIMARY KEY,
        tz_offset INTEGER NOT NULL DEFAULT 180,
        data      TEXT    NOT NULL DEFAULT '{}')""",
    """CREATE TABLE IF NOT EXISTS sent(
        user_id BIGINT NOT NULL,
        item    TEXT   NOT NULL,
        day     TEXT   NOT NULL,
        PRIMARY KEY(user_id, item, day))""",
    """CREATE TABLE IF NOT EXISTS users(
        user_id    BIGINT PRIMARY KEY,
        first_seen BIGINT NOT NULL,
        last_seen  BIGINT NOT NULL,
        last_open  BIGINT)""",
    """CREATE TABLE IF NOT EXISTS meta(
        k TEXT PRIMARY KEY,
        v TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS players(
        user_id BIGINT  PRIMARY KEY,
        tier    INTEGER NOT NULL DEFAULT 1,
        name    TEXT    NOT NULL DEFAULT '')""",
    """CREATE TABLE IF NOT EXISTS league(
        week    TEXT    NOT NULL,
        user_id BIGINT  NOT NULL,
        tier    INTEGER NOT NULL,
        grp     INTEGER NOT NULL,
        xp      INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(week, user_id))""",
    """CREATE TABLE IF NOT EXISTS activity(
        day     TEXT   NOT NULL,
        user_id BIGINT NOT NULL,
        PRIMARY KEY(day, user_id))""",
    """CREATE TABLE IF NOT EXISTS marks(
        user_id BIGINT  NOT NULL,
        created BIGINT  NOT NULL,
        habit   TEXT    NOT NULL,
        day     TEXT    NOT NULL,
        v       INTEGER NOT NULL,
        PRIMARY KEY(user_id, created))""",
    """CREATE TABLE IF NOT EXISTS restores(
        user_id BIGINT PRIMARY KEY,
        created BIGINT NOT NULL,
        data    TEXT   NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS purchases(
        user_id BIGINT  NOT NULL,
        created BIGINT  NOT NULL,
        item    TEXT    NOT NULL,
        stars   INTEGER NOT NULL,
        charge  TEXT    NOT NULL,
        info    TEXT    NOT NULL DEFAULT '',
        status  TEXT    NOT NULL DEFAULT 'paid',
        PRIMARY KEY(user_id, created))""",
    """CREATE TABLE IF NOT EXISTS perks(
        user_id BIGINT NOT NULL,
        created BIGINT NOT NULL,
        kind    TEXT   NOT NULL,
        value   TEXT   NOT NULL DEFAULT '',
        src     TEXT   NOT NULL DEFAULT '',
        PRIMARY KEY(user_id, created))""",
    """CREATE TABLE IF NOT EXISTS gifts(
        user_id BIGINT  NOT NULL,
        created BIGINT  NOT NULL,
        day     TEXT    NOT NULL,
        n       INTEGER NOT NULL,
        PRIMARY KEY(user_id, created))""",
]
# новые колонки в старых таблицах (ошибка «колонка уже есть» игнорируется)
MIGRATIONS = [
    "ALTER TABLE users ADD COLUMN source TEXT",
    "ALTER TABLE users ADD COLUMN username TEXT",
    "ALTER TABLE users ADD COLUMN name TEXT",
    "ALTER TABLE users ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE players ADD COLUMN banned INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE gifts ADD COLUMN src TEXT",
]


def clean_dsn(url: str) -> str:
    """Neon добавляет в адрес параметры, которые asyncpg не понимает (например channel_binding)."""
    parts = urlsplit(url)
    keep = [(k, v) for k, v in parse_qsl(parts.query) if k in ("sslmode", "sslrootcert", "options")]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(keep), ""))


class Store:
    """Одинаковый интерфейс для Postgres (asyncpg) и SQLite. В SQL пишем параметры как $1, $2…

    Профили и отметки об отправке держим ещё и в памяти: цикл напоминаний читает только память,
    а в базу ходим лишь при событиях (открыли приложение, написали боту, ушло напоминание).
    Так бесплатный Neon успевает «засыпать» и не тратит лимит вычислений."""

    def __init__(self, url: str, path: str):
        self.url, self.path = url, path
        self.pool = None
        self.lite: sqlite3.Connection | None = None
        self.profiles: dict[int, tuple[int, str]] = {}
        self.sent: set[tuple[int, str, str]] = set()
        self.players: dict[int, dict] = {}   # user_id -> {tier, name}
        self.week = ""                       # неделя лиги, которая сейчас в памяти
        self.league: dict[int, dict] = {}    # user_id -> {tier, grp, xp} за self.week
        self.league_done: str | None = None  # последняя неделя, по которой подведены итоги
        self.league_checked = ""             # неделя, на которой последний раз проверяли итоги
        self.admin: int | None = None        # владелец (кэш)
        self.admin_loaded = False
        self.active: set[tuple[str, int]] = set()  # кто уже отмечен активным сегодня
        self.report_day = ""                 # день, за который отправлен отчёт владельцу
        self.news: dict | None = None        # плашка «Что нового» от владельца
        self.awaiting_fb: set[int] = set()   # кто сейчас пишет отзыв
        self.reply_to: dict[int, int] = {}   # владелец отвечает пользователю
        self.pending_bc: dict[int, str] = {} # рассылка ждёт подтверждения

    @property
    def kind(self) -> str:
        return "postgres" if self.pool else "sqlite"

    async def open(self) -> None:
        if self.url:
            import asyncpg  # noqa: WPS433 — нужен только с Postgres
            self.pool = await asyncpg.create_pool(
                clean_dsn(self.url), min_size=1, max_size=4, statement_cache_size=0, command_timeout=20)
        else:
            self.lite = sqlite3.connect(self.path)
            self.lite.row_factory = sqlite3.Row
        for stmt in SCHEMA:
            await self.execute(stmt)
        for stmt in MIGRATIONS:
            try:
                await self.execute(stmt.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS") if self.pool else stmt)
            except Exception:  # SQLite: колонка уже есть
                pass
        # пользователи, которые уже настраивали напоминания, тоже считаются
        now = int(time.time())
        await self.execute(
            "INSERT INTO users(user_id, first_seen, last_seen, last_open) "
            "SELECT user_id, $1, $1, $1 FROM profiles WHERE user_id > 0 ON CONFLICT(user_id) DO NOTHING", now)
        await self.prune_sent(db=True)
        self.profiles = {r["user_id"]: (r["tz_offset"], r["data"])
                         for r in await self.fetch("SELECT user_id, tz_offset, data FROM profiles")}
        self.sent = {(r["user_id"], r["item"], r["day"])
                     for r in await self.fetch("SELECT user_id, item, day FROM sent")}
        self.players = {r["user_id"]: {"tier": r["tier"], "name": r["name"], "banned": bool(r["banned"])}
                        for r in await self.fetch("SELECT user_id, tier, name, banned FROM players")}
        today = msk_day()
        self.active = {(today, r["user_id"]) for r in await self.fetch("SELECT user_id FROM activity WHERE day = $1", today)}
        meta = {r["k"]: r["v"] for r in await self.fetch("SELECT k, v FROM meta")}
        self.report_day = meta.get("report_day", "")
        try:
            self.news = json.loads(meta["news"]) if meta.get("news") else None
        except ValueError:
            self.news = None
        self.week = league_week()[0]
        self.league = {r["user_id"]: {"tier": r["tier"], "grp": r["grp"], "xp": r["xp"]}
                       for r in await self.fetch("SELECT user_id, tier, grp, xp FROM league WHERE week = $1", self.week)}
        log.info("storage: %s, profiles: %s", self.kind, len(self.profiles))

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
        if self.lite:
            self.lite.close()

    @staticmethod
    def _lite_sql(sql: str) -> str:
        return re.sub(r"\$(\d+)", r"?\1", sql)

    async def execute(self, sql: str, *args) -> int:
        """Выполняет запрос и возвращает число затронутых строк."""
        if self.pool:
            status = await self.pool.execute(sql, *args)
            try:
                return int(status.split()[-1])
            except (ValueError, IndexError):
                return 0
        cur = self.lite.execute(self._lite_sql(sql), args)
        self.lite.commit()
        return cur.rowcount

    async def fetch(self, sql: str, *args) -> list[dict]:
        if self.pool:
            return [dict(r) for r in await self.pool.fetch(sql, *args)]
        return [dict(r) for r in self.lite.execute(self._lite_sql(sql), args).fetchall()]

    async def fetchval(self, sql: str, *args):
        rows = await self.fetch(sql, *args)
        return next(iter(rows[0].values())) if rows else None

    # --- профили и напоминания ---
    async def save_profile(self, user_id: int, tz: int, data: str) -> None:
        await self.execute(
            """INSERT INTO profiles(user_id, tz_offset, data) VALUES($1, $2, $3)
               ON CONFLICT(user_id) DO UPDATE SET tz_offset = excluded.tz_offset, data = excluded.data""",
            user_id, tz, data)
        self.profiles[user_id] = (tz, data)

    async def mark_sent(self, user_id: int, item: str, day: str) -> bool:
        """True, если это напоминание сегодня ещё не отправляли (и помечает его отправленным)."""
        k = (user_id, item, day)
        if k in self.sent:
            return False
        self.sent.add(k)
        try:
            return bool(await self.execute(
                "INSERT INTO sent(user_id, item, day) VALUES($1, $2, $3) ON CONFLICT DO NOTHING", user_id, item, day))
        except Exception:
            log.exception("mark_sent failed")  # база недоступна — всё равно отправим, в памяти уже отмечено
            return True

    async def prune_sent(self, db: bool = False) -> None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d")
        self.sent = {k for k in self.sent if k[2] >= cutoff}
        if db:
            await self.execute("DELETE FROM sent WHERE day < $1", cutoff)
            await self.execute("DELETE FROM marks WHERE created < $1", int((time.time() - 14 * 86400) * 1000))

    async def insert_ts(self, table: str, cols: tuple[str, ...], user_id: int, *values) -> int:
        """Вставка строки с уникальным (user_id, created). created — миллисекунды; при совпадении сдвигаем."""
        created = int(time.time() * 1000)
        names = ", ".join(("user_id", "created") + cols)
        marks = ", ".join(f"${i}" for i in range(1, len(cols) + 3))
        for _ in range(20):
            if await self.execute(f"INSERT INTO {table}({names}) VALUES({marks}) ON CONFLICT DO NOTHING",
                                  user_id, created, *values):
                return created
            created += 1
        raise RuntimeError("insert_ts: no free timestamp")

    async def add_mark(self, user_id: int, habit: str, day: str, v: int) -> int:
        """Отметка из чата (кнопка в напоминании). Приложение заберёт её при открытии."""
        created = int(time.time() * 1000)
        for _ in range(10):
            if await self.execute("INSERT INTO marks(user_id, created, habit, day, v) VALUES($1, $2, $3, $4, $5) "
                                  "ON CONFLICT DO NOTHING", user_id, created, habit, day, v):
                return created
            created += 1
        return created

    # --- лига ---
    async def set_player(self, user_id: int, name: str) -> dict:
        p = self.players.get(user_id)
        if p is None:
            p = self.players[user_id] = {"tier": 1, "name": name, "banned": False}
            await self.execute("INSERT INTO players(user_id, tier, name) VALUES($1, 1, $2) ON CONFLICT(user_id) DO NOTHING",
                               user_id, name)
        elif name and p["name"] != name:
            p["name"] = name
            await self.execute("UPDATE players SET name = $1 WHERE user_id = $2", name, user_id)
        return p

    async def league_update(self, user_id: int, name: str, xp: int) -> dict | None:
        """Вступает в лигу текущей недели (если ещё нет) и обновляет недельный XP."""
        week = league_week()[0]
        if week != self.week:  # неделя сменилась, итоги подведёт league_tick
            self.week, self.league = week, {}
        p = await self.set_player(user_id, name)
        if p.get("banned"):
            return None
        row = self.league.get(user_id)
        if row is None:
            counts: dict[int, int] = {}
            for r in self.league.values():
                if r["tier"] == p["tier"]:
                    counts[r["grp"]] = counts.get(r["grp"], 0) + 1
            grp = next((g for g in sorted(counts) if counts[g] < GROUP_MAX), max(counts, default=-1) + 1)
            row = self.league[user_id] = {"tier": p["tier"], "grp": grp, "xp": xp}
            await self.execute("INSERT INTO league(week, user_id, tier, grp, xp) VALUES($1, $2, $3, $4, $5) "
                               "ON CONFLICT(week, user_id) DO UPDATE SET xp = excluded.xp",
                               week, user_id, p["tier"], grp, xp)
        elif row["xp"] != xp:
            row["xp"] = xp
            await self.execute("UPDATE league SET xp = $1 WHERE week = $2 AND user_id = $3", xp, week, user_id)
        return row

    def standings(self, tier: int, grp: int, rows: dict | None = None) -> list[tuple[int, dict]]:
        rows = self.league if rows is None else rows
        group = [(uid, r) for uid, r in rows.items() if r["tier"] == tier and r["grp"] == grp]
        return sorted(group, key=lambda x: (-x[1]["xp"], x[0]))

    # --- пользователи и статистика ---
    async def touch_user(self, user_id: int, opened: bool = False, username: str | None = None,
                         name: str | None = None, source: str | None = None) -> None:
        now = int(time.time())
        await self.execute(
            """INSERT INTO users(user_id, first_seen, last_seen, last_open, username, name, source, blocked)
               VALUES($1, $2, $2, $3, $4, $5, $6, 0)
               ON CONFLICT(user_id) DO UPDATE SET last_seen = excluded.last_seen,
               last_open = COALESCE(excluded.last_open, users.last_open),
               username = COALESCE(excluded.username, users.username),
               name = COALESCE(excluded.name, users.name),
               source = COALESCE(users.source, excluded.source), blocked = 0""",
            user_id, now, now if opened else None, username, name, source)
        day = msk_day(now)
        if (day, user_id) not in self.active:
            if any(d != day for d, _ in list(self.active)[:1]):
                self.active = set()
            self.active.add((day, user_id))
            await self.execute("INSERT INTO activity(day, user_id) VALUES($1, $2) ON CONFLICT DO NOTHING", day, user_id)

    async def set_blocked(self, user_id: int) -> None:
        await self.execute("UPDATE users SET blocked = 1 WHERE user_id = $1", user_id)

    async def admin_id(self) -> int | None:
        if ADMIN_ID:
            return ADMIN_ID
        if not self.admin_loaded:
            v = await self.fetchval("SELECT v FROM meta WHERE k = 'admin_id'")
            self.admin, self.admin_loaded = (int(v) if v else None), True
        return self.admin

    async def claim_admin(self, user_id: int) -> int | None:
        """Первый, кто отправит /stats, становится владельцем (если ADMIN_ID не задан)."""
        if not ADMIN_ID:
            await self.execute("INSERT INTO meta(k, v) VALUES('admin_id', $1) ON CONFLICT(k) DO NOTHING", str(user_id))
            self.admin_loaded = False
        return await self.admin_id()

    async def set_meta(self, k: str, v: str | None) -> None:
        if v is None:
            await self.execute("DELETE FROM meta WHERE k = $1", k)
        else:
            await self.execute("INSERT INTO meta(k, v) VALUES($1, $2) ON CONFLICT(k) DO UPDATE SET v = excluded.v", k, v)


DB = Store(DATABASE_URL, DB_PATH)


async def collect_stats() -> dict:
    now = int(time.time())
    d1, d7, d30 = now - 86400, now - 7 * 86400, now - 30 * 86400
    row = (await DB.fetch(
        """SELECT COUNT(*) AS total,
             SUM(CASE WHEN first_seen >= $1 THEN 1 ELSE 0 END) AS new1,
             SUM(CASE WHEN first_seen >= $2 THEN 1 ELSE 0 END) AS new7,
             SUM(CASE WHEN last_seen  >= $1 THEN 1 ELSE 0 END) AS act1,
             SUM(CASE WHEN last_seen  >= $2 THEN 1 ELSE 0 END) AS act7,
             SUM(CASE WHEN last_seen  >= $3 THEN 1 ELSE 0 END) AS act30,
             SUM(CASE WHEN last_open IS NOT NULL THEN 1 ELSE 0 END) AS opened,
             SUM(CASE WHEN last_open >= $2 THEN 1 ELSE 0 END) AS opened7,
             SUM(CASE WHEN blocked = 1 THEN 1 ELSE 0 END) AS blocked
           FROM users""", d1, d7, d30))[0]
    st = {k: int(v or 0) for k, v in row.items()}
    habits_total = with_habits = with_reminders = 0
    for _tz, raw in DB.profiles.values():
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        hs = data.get("habits") or []
        if hs:
            with_habits += 1
            habits_total += len(hs)
        if any(h.get("times") for h in hs):
            with_reminders += 1
    st.update(league=len(DB.league), with_habits=with_habits, with_reminders=with_reminders,
              avg_habits=round(habits_total / with_habits, 1) if with_habits else 0)
    return st


def stats_text(st: dict) -> str:
    storage = "Neon (Postgres) — данные сохраняются" if DB.kind == "postgres" else "SQLite — ⚠️ сбрасывается при обновлении бота"
    return (
        "📊 Статистика бота\n\n"
        f"👥 Всего пользователей: {st['total']}\n"
        f"🆕 Новые: за 24 ч — {st['new1']}, за 7 дней — {st['new7']}\n"
        f"🔥 Активные: за 24 ч — {st['act1']}, за 7 дней — {st['act7']}, за 30 дней — {st['act30']}\n"
        f"📱 Открывали трекер: всего — {st['opened']}, за 7 дней — {st['opened7']}\n"
        f"📋 С привычками: {st['with_habits']} (в среднем {st['avg_habits']} на человека)\n"
        f"🔔 С напоминаниями по привычкам: {st['with_reminders']}\n"
        f"🏆 В лиге на этой неделе: {st['league']}\n"
        f"🚫 Заблокировали бота: {st['blocked']}\n\n"
        f"🗄 База: {storage}\n"
        "Все команды владельца — /admin"
    )


# ---------- аналитика для владельца ----------
async def series(days: int = 30) -> dict:
    """Новые и активные пользователи по дням (МСК) за последние N дней."""
    today = datetime.strptime(msk_day(), "%Y-%m-%d")
    labels = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]
    new = {d: 0 for d in labels}
    for r in await DB.fetch("SELECT first_seen FROM users WHERE first_seen >= $1", int(time.time()) - (days + 1) * 86400):
        d = msk_day(r["first_seen"])
        if d in new:
            new[d] += 1
    act = {d: 0 for d in labels}
    for r in await DB.fetch("SELECT day, COUNT(*) AS n FROM activity WHERE day >= $1 GROUP BY day", labels[0]):
        if r["day"] in act:
            act[r["day"]] = int(r["n"])
    return {"days": labels, "new": [new[d] for d in labels], "active": [act[d] for d in labels]}


async def retention() -> dict:
    """D1/D7/D30: доля пользователей, вернувшихся ровно через 1, 7, 30 дней после первого визита."""
    today = datetime.strptime(msk_day(), "%Y-%m-%d")
    users = {r["user_id"]: msk_day(r["first_seen"]) for r in await DB.fetch("SELECT user_id, first_seen FROM users")}
    seen = {(r["day"], r["user_id"]) for r in await DB.fetch("SELECT day, user_id FROM activity")}
    out = {}
    for n in (1, 7, 30):
        cohort = ok = 0
        for uid, d0 in users.items():
            first = datetime.strptime(d0, "%Y-%m-%d")
            if first + timedelta(days=n) > today:
                continue
            cohort += 1
            if ((first + timedelta(days=n)).strftime("%Y-%m-%d"), uid) in seen:
                ok += 1
        out[f"d{n}"] = {"cohort": cohort, "ok": ok, "pct": round(ok / cohort * 100) if cohort else None}
    return out


def popular_habits(min_users: int = 3) -> list[list]:
    """Самые частые названия привычек. Показываем только те, что есть хотя бы у 3 человек, — чтобы не раскрывать чьи-то личные."""
    counts: dict[str, int] = {}
    for _tz, raw in DB.profiles.values():
        try:
            names = {h.get("name", "").strip().lower() for h in json.loads(raw).get("habits", [])}
        except ValueError:
            continue
        for n in names:
            if n:
                counts[n] = counts.get(n, 0) + 1
    top = sorted(((n.capitalize(), c) for n, c in counts.items() if c >= min_users), key=lambda x: -x[1])
    return [list(x) for x in top[:10]]


async def sources() -> list[list]:
    rows = await DB.fetch("SELECT COALESCE(source, '') AS s, COUNT(*) AS n FROM users GROUP BY COALESCE(source, '') ORDER BY n DESC")
    return [[r["s"] or "без метки", int(r["n"])] for r in rows]


def league_summary() -> list[dict]:
    out = []
    for t, name in enumerate(LEAGUES, 1):
        rows = [r for r in DB.league.values() if r["tier"] == t]
        out.append({"tier": t, "name": name, "players": len(rows), "groups": len({r["grp"] for r in rows}),
                    "xp": sum(r["xp"] for r in rows)})
    return out


async def limits() -> dict:
    """Сколько потрачено из бесплатных лимитов."""
    out = {}
    try:
        if DB.pool:
            size = await DB.fetchval("SELECT pg_database_size(current_database())")
        else:
            size = os.path.getsize(DB.path) if os.path.exists(DB.path) else 0
        out["db_mb"] = round(int(size) / 1024 / 1024, 1)
    except Exception:
        out["db_mb"] = None
    now = datetime.now(timezone.utc)
    out["render_h"] = int((now - now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)).total_seconds() // 3600)
    out["neon_cu_h"] = None
    if NEON_API_KEY and NEON_PROJECT_ID:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as cs:
                async with cs.get(f"https://console.neon.tech/api/v2/projects/{NEON_PROJECT_ID}",
                                  headers={"Authorization": f"Bearer {NEON_API_KEY}", "Accept": "application/json"}) as r:
                    prj = (await r.json()).get("project", {})
            secs = prj.get("compute_time_seconds", prj.get("cpu_used_sec"))
            if secs is not None:
                out["neon_cu_h"] = round(float(secs) / 3600, 1)
        except Exception:
            log.warning("neon api unavailable")
    out["limits"] = LIMITS
    return out


def limits_text(lm: dict) -> str:
    parts = []
    if lm.get("db_mb") is not None:
        pct = lm["db_mb"] / LIMITS["db_mb"] * 100
        parts.append(f"{'⚠️ ' if pct >= 80 else ''}база {lm['db_mb']} из {LIMITS['db_mb']} МБ")
    if lm.get("neon_cu_h") is not None:
        pct = lm["neon_cu_h"] / LIMITS["neon_cu_h"] * 100
        parts.append(f"{'⚠️ ' if pct >= 80 else ''}Neon ≈{lm['neon_cu_h']} из {LIMITS['neon_cu_h']} ч")
    parts.append(f"Render ≈{lm['render_h']} из {LIMITS['render_h']} ч")
    return " · ".join(parts)


async def report_text() -> str:
    """Отчёт за вчера (по МСК) + как идёт сегодня."""
    today = msk_day()
    yday = msk_day(time.time() - 86400)
    sr = await series(8)
    i_y, i_t = sr["days"].index(yday), sr["days"].index(today)
    st = await collect_stats()
    srcs = await sources()
    lm = await limits()
    d = datetime.strptime(yday, "%Y-%m-%d")
    months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"]
    week_act = len({uid for (dd, uid) in [(r["day"], r["user_id"]) for r in await DB.fetch(
        "SELECT day, user_id FROM activity WHERE day >= $1", sr["days"][1])]})
    lines = [
        f"☀️ Отчёт за {d.day} {months[d.month - 1]}\n",
        f"👥 Всего пользователей: {st['total']} (новых вчера: +{sr['new'][i_y]})",
        f"🔥 Активных вчера: {sr['active'][i_y]} · за 7 дней: {week_act}",
        f"🏆 В лиге на этой неделе: {st['league']}",
        f"🚫 Заблокировали бота: {st['blocked']}",
        f"📅 Сегодня пока: новых {sr['new'][i_t]}, активных {sr['active'][i_t]}",
    ]
    tagged = [f"{s} — {n}" for s, n in srcs if s != "без метки"][:5]
    if tagged:
        lines.append("🔗 Источники (всего): " + ", ".join(tagged))
    sh = await shop_stats()
    if sh["count"]:
        lines.append(f"⭐ Магазин: вчера {sh['yday']} ⭐, за 30 дней {sh['d30']} ⭐, всего {sh['total']} ⭐")
    lines.append(f"🗄 Лимиты: {limits_text(lm)}")
    lines.append("\nПодробнее — панель 👑 в приложении или /stats")
    return "\n".join(lines)


# ---------- магазин ----------
async def add_gift(user_id: int, n: int, src: str) -> int:
    return await DB.insert_ts("gifts", ("day", "n", "src"), user_id, msk_day(), n, src)


def repair_days_ok(days: list, user_id: int) -> bool:
    """Чинить можно только вчера и позавчера (по времени пользователя)."""
    tz = DB.profiles.get(user_id, (180, ""))[0]
    local = datetime.now(timezone.utc) + timedelta(minutes=tz)
    allowed = {(local - timedelta(days=i)).strftime("%Y-%m-%d") for i in (1, 2)}
    return 0 < len(days) <= 2 and all(d in allowed for d in days)


async def owned_themes(user_id: int) -> set[str]:
    return {r["value"] for r in await DB.fetch("SELECT value FROM perks WHERE user_id = $1 AND kind = 'theme'", user_id)}


async def grant(user_id: int, item: str, src: str, days: list | None = None, n: int | None = None) -> str:
    """Выдаёт товар. Возвращает ссылку на выданное (таблица:created) — чтобы при возврате забрать обратно."""
    it = ITEMS[item]
    if it["kind"] == "freeze":
        return f"gifts:{await add_gift(user_id, n or it['n'], src)}"
    if it["kind"] == "repair":
        return f"perks:{await DB.insert_ts('perks', ('kind', 'value', 'src'), user_id, 'repair', ','.join(days or []), src)}"
    if it["kind"] == "theme":
        if it["theme"] in await owned_themes(user_id):
            return ""
        return f"perks:{await DB.insert_ts('perks', ('kind', 'value', 'src'), user_id, 'theme', it['theme'], src)}"
    return ""


async def revoke(user_id: int, ref: str) -> None:
    if ":" not in ref:
        return
    table, created = ref.split(":", 1)
    if table in ("gifts", "perks") and created.isdigit():
        await DB.execute(f"DELETE FROM {table} WHERE user_id = $1 AND created = $2", user_id, int(created))


def gift_text(item: str, n: int | None = None) -> str:
    it = ITEMS[item]
    if it["kind"] == "freeze":
        k = n or it["n"]
        return f"{k} {plural(k, 'заморозку', 'заморозки', 'заморозок')}"
    if it["kind"] == "repair":
        return "починку серии — применишь, когда серия сгорит"
    return it["title"].replace("Тема", "тему")


async def perks_for(user_id: int) -> dict:
    themes, repairs, credits = [], [], 0
    for r in await DB.fetch("SELECT kind, value FROM perks WHERE user_id = $1 ORDER BY created", user_id):
        if r["kind"] == "theme":
            themes.append(r["value"])
        elif r["kind"] == "repair":
            if r["value"]:
                repairs += r["value"].split(",")
            else:
                credits += 1
    return {"themes": themes, "repairs": repairs, "repc": credits}


async def shop_stats() -> dict:
    now = time.time()
    rows = await DB.fetch("SELECT user_id, created, item, stars, charge, status FROM purchases ORDER BY created DESC")
    paid = [r for r in rows if r["status"] == "paid"]
    yday = msk_day(now - 86400)
    out = {"count": len(paid), "total": sum(r["stars"] for r in paid),
           "d30": sum(r["stars"] for r in paid if r["created"] >= (now - 30 * 86400) * 1000),
           "yday": sum(r["stars"] for r in paid if msk_day(r["created"] / 1000) == yday), "recent": []}
    names = {r["user_id"]: r["name"] or (f"@{r['username']}" if r["username"] else str(r["user_id"]))
             for r in await DB.fetch("SELECT user_id, name, username FROM users")}
    for r in rows[:30]:
        out["recent"].append({"uid": int(r["user_id"]), "id": int(r["created"]), "name": names.get(r["user_id"], str(r["user_id"])),
                              "title": ITEMS.get(r["item"], {}).get("title", r["item"]), "stars": int(r["stars"]),
                              "day": msk_day(r["created"] / 1000), "status": r["status"]})
    return out


def parse_payload(payload: str) -> tuple[str, int, list] | None:
    try:
        item, uid, days = payload.split("|")
        return (item, int(uid), [d for d in days.split(",") if d]) if item in ITEMS else None
    except ValueError:
        return None


async def do_refund(bot: Bot, uid: int, created: int) -> str:
    row = await DB.fetch("SELECT item, stars, charge, info, status FROM purchases WHERE user_id = $1 AND created = $2", uid, created)
    if not row:
        return "Покупка не найдена."
    r = row[0]
    if r["status"] != "paid":
        return "Эта покупка уже возвращена."
    try:
        await bot.refund_star_payment(user_id=uid, telegram_payment_charge_id=r["charge"])
    except TelegramAPIError as e:
        return f"Telegram не дал вернуть звёзды: {e.message if hasattr(e, 'message') else e}"
    await revoke(uid, r["info"])
    await DB.execute("UPDATE purchases SET status = 'refunded' WHERE user_id = $1 AND created = $2", uid, created)
    try:
        await bot.send_message(uid, f"↩️ Вернул {r['stars']} ⭐ за «{ITEMS.get(r['item'], {}).get('title', r['item'])}».")
    except TelegramAPIError:
        pass
    return f"↩️ Возврат сделан: {r['stars']} ⭐"


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


def habit_text(h: dict, v: int = 0) -> str:
    if h["type"] == "count":
        now = f"\nСейчас: {v} из {h['target']} {h['unit']}".rstrip() if v else f"\nЦель: {h['target']} {h['unit']}".rstrip()
        return f"⏰ Пора: {h['name']}{now}\nОтмечай кнопками ниже — сразу засчитается в трекере."
    return f"⏰ Пора: {h['name']}\nСделал? Нажми кнопку — отметка сразу попадёт в трекер."


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


def league_line(user_id: int) -> str | None:
    """Место в лиге на текущей неделе — для итогов недели."""
    if not user_id or DB.week != league_week()[0] or user_id not in DB.league:
        return None
    me = DB.league[user_id]
    group = DB.standings(me["tier"], me["grp"])
    place = next((i for i, (u, _) in enumerate(group, 1) if u == user_id), 0)
    n, tier = len(group), me["tier"]
    line = f"🏆 Лига «{LEAGUES[tier - 1]}»: {place} место из {n}"
    if tier < len(LEAGUES) and place <= PROMOTE and me["xp"] > 0:
        line += " — ты в зоне повышения!"
    elif tier > 1 and n >= DEMOTE_MIN_GROUP and place > n - DEMOTE:
        line += " — до полуночи ещё можно выбраться из зоны вылета"
    return line


def week_text(data: dict, local: datetime, streak, user_id: int = 0) -> str:
    monday = (local - timedelta(days=local.isoweekday() - 1)).strftime("%Y-%m-%d")
    fresh = bool(data.get("today")) and data["today"] >= monday
    wk = data.get("wk") if isinstance(data.get("wk"), dict) and data["wk"].get("mon") == monday else None
    parts = ["📊 Итоги недели", ""]
    if wk and wk.get("days"):
        parts.append(f"🔥 Закрыто дней: {wk['closed']} из {wk['days']}")
    week = data.get("week") if fresh else None
    if week and week.get("all"):
        pct = round(week["ok"] / week["all"] * 100)
        parts.append(f"✅ Выполнено {pct}% — {week['ok']} из {week['all']} отметок")
    row = DB.league.get(user_id) if DB.week == league_week()[0] else None
    xp = row["xp"] if row else (wk or {}).get("xp", 0)
    if xp:
        parts.append(f"⭐ +{xp} XP за неделю")
    lg = league_line(user_id)
    if lg:
        parts.append(lg)
    if wk and wk.get("best"):
        b = wk["best"]
        parts.append(f"💪 Лучшая привычка: {b['name']} ({b['ok']} из {b['all']})")
    if streak:
        parts.append(f"🔥 Серия: {days_word(streak)}")
    if len(parts) == 2:
        parts.append("На этой неделе ты не заглядывал в трекер. Новая неделя — отличный повод начать!")
    else:
        parts += ["", "Новая неделя — новый огонёк. Так держать!"]
    return "\n".join(parts)


# ---------- итоги недели лиги ----------
def week_results(rows: dict) -> list[dict]:
    """Кто куда переходит по итогам недели. rows: user_id -> {tier, grp, xp}."""
    out = []
    for tier, grp in {(r["tier"], r["grp"]) for r in rows.values()}:
        group = DB.standings(tier, grp, rows)
        n = len(group)
        for place, (uid, r) in enumerate(group, 1):
            new = tier
            if place <= PROMOTE and r["xp"] > 0 and tier < len(LEAGUES):
                new = tier + 1
            elif tier > 1 and n >= DEMOTE_MIN_GROUP and place > n - DEMOTE:
                new = tier - 1
            out.append({"user_id": uid, "tier": tier, "new": new, "place": place, "xp": r["xp"], "size": n})
    return out


def result_text(r: dict) -> str:
    old, new = LEAGUES[r["tier"] - 1], LEAGUES[r["new"] - 1]
    head = f"🏆 Неделя в лиге «{old}» закончилась: ты на {r['place']} месте ({r['xp']} XP)."
    if r["new"] > r["tier"]:
        return f"{head}\nПоздравляю — ты поднимаешься в лигу «{new}»! 🔥"
    if r["new"] < r["tier"]:
        return f"{head}\nВ этот раз — лига «{new}». На новой неделе всё вернём 💪"
    return f"{head}\nОстаёшься в лиге «{old}». Новая неделя уже началась!"


async def close_week(bot, week: str, rows: dict, notify: bool) -> None:
    results = week_results(rows)
    for r in results:
        p = DB.players.setdefault(r["user_id"], {"tier": 1, "name": ""})
        if r["new"] != p["tier"]:
            p["tier"] = r["new"]
            await DB.execute("UPDATE players SET tier = $1 WHERE user_id = $2", r["new"], r["user_id"])
    await DB.execute("INSERT INTO meta(k, v) VALUES('league_done', $1) ON CONFLICT(k) DO UPDATE SET v = excluded.v", week)
    log.info("league week %s closed: %s players", week, len(results))
    if notify and bot is not None:
        for r in results:
            if r["xp"] > 0:
                await send(bot, r["user_id"], result_text(r))


async def league_tick(bot) -> None:
    """Подводит итоги всех завершённых недель, которые ещё не закрыты."""
    current = league_week()[0]
    done = DB.league_done
    if done is None:
        done = await DB.fetchval("SELECT v FROM meta WHERE k = 'league_done'") or ""
    weeks = [r["week"] for r in await DB.fetch(
        "SELECT DISTINCT week FROM league WHERE week < $1 AND week > $2 ORDER BY week", current, done or "")]
    for i, week in enumerate(weeks):
        rows = {r["user_id"]: {"tier": r["tier"], "grp": r["grp"], "xp": r["xp"]}
                for r in await DB.fetch("SELECT user_id, tier, grp, xp FROM league WHERE week = $1", week)}
        await close_week(bot, week, rows, notify=(i == len(weeks) - 1))
        done = week
    DB.league_done = done
    DB.league_checked = current
    if DB.week != current:
        DB.week, DB.league = current, {}


# ---------- клавиатура ----------
def open_kb(text: str = "Открыть трекер") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL))]])


def open_btn() -> list[InlineKeyboardButton]:
    return [InlineKeyboardButton(text="Открыть трекер", web_app=WebAppInfo(url=WEBAPP_URL))]


def habit_kb(h: dict, day: str) -> InlineKeyboardMarkup:
    """Кнопки под напоминанием о привычке. callback: d:<habit>:<день>:<действие>."""
    pre = f"d:{h['id']}:{day}:"
    if h["type"] == "count":
        row = [InlineKeyboardButton(text="+1", callback_data=pre + "+"),
               InlineKeyboardButton(text=f"✅ Все {h['target']}", callback_data=pre + "a")]
    else:
        row = [InlineKeyboardButton(text="✅ Сделал", callback_data=pre + "1")]
    return InlineKeyboardMarkup(inline_keyboard=[row, open_btn()])


def save_kb(habits: list[dict], day: str) -> InlineKeyboardMarkup:
    """Под «Спасти серию»: по кнопке на каждую незакрытую привычку."""
    rows = []
    for h in habits[:6]:
        label = f"✅ {h['name']}"[:40]
        rows.append([InlineKeyboardButton(text=label, callback_data=f"d:{h['id']}:{day}:{'a' if h['type'] == 'count' else '1'}")])
    return InlineKeyboardMarkup(inline_keyboard=rows + [open_btn()])


# ---------- команды бота ----------
router = Router()


# --- оплата звёздами ---
@router.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery) -> None:
    """Telegram спрашивает перед списанием звёзд — проверяем, что заказ в порядке."""
    p = parse_payload(q.invoice_payload)
    err = None
    if not p or p[1] != q.from_user.id or q.currency != "XTR" or q.total_amount != ITEMS[p[0]]["stars"]:
        err = "Заказ устарел — открой магазин в трекере ещё раз."
    elif ITEMS[p[0]]["kind"] == "theme" and ITEMS[p[0]]["theme"] in await owned_themes(p[1]):
        err = "Эта тема у тебя уже есть 🙂"
    elif ITEMS[p[0]]["kind"] == "repair" and not repair_days_ok(p[2], p[1]):
        err = "Починить можно только серию, сгоревшую вчера или позавчера."
    await q.answer(ok=err is None, error_message=err)


@router.message(F.successful_payment)
async def on_paid(m: Message) -> None:
    sp = m.successful_payment
    p = parse_payload(sp.invoice_payload)
    uid = m.from_user.id
    if await DB.fetchval("SELECT user_id FROM purchases WHERE charge = $1", sp.telegram_payment_charge_id):
        return  # уже обработали
    item = p[0] if p else "unknown"
    ref = await grant(uid, item, "buy", p[2] if p else None) if p else ""
    await DB.insert_ts("purchases", ("item", "stars", "charge", "info", "status"), uid,
                       item, int(sp.total_amount), sp.telegram_payment_charge_id, ref, "paid")
    it = ITEMS.get(item, {"title": item, "kind": "tip"})
    thanks = ("Спасибо огромное за поддержку! 💜 Это правда помогает развивать трекер." if it["kind"] == "tip"
              else f"✅ Готово: {it['title']}. Открой трекер — покупка уже там.")
    await m.answer(thanks, reply_markup=open_kb())
    admin = await DB.admin_id()
    if admin and admin != uid:
        try:
            await m.bot.send_message(admin, f"⭐ Покупка: {display_name(m.from_user)}"
                                            f"{' (@' + m.from_user.username + ')' if m.from_user.username else ''} — "
                                            f"{it['title']} за {sp.total_amount} ⭐")
        except TelegramAPIError:
            pass
    log.info("purchase %s by %s: %s stars", item, uid, sp.total_amount)



class TrackUsers(BaseMiddleware):
    """Запоминает каждого, кто пишет боту, — для /stats."""

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user and not user.is_bot:
            source = None
            text = getattr(event, "text", None) or ""
            if text.startswith("/start "):
                payload = text.split(maxsplit=1)[1].strip()
                if SOURCE_RE.match(payload) and payload != "feedback":
                    source = payload
            try:
                await DB.touch_user(user.id, username=(user.username or None), name=display_name(user), source=source)
            except Exception:
                log.exception("touch_user failed")
        return await handler(event, data)


@router.message(CommandStart())
async def cmd_start(m: Message, command: CommandObject) -> None:
    if (command.args or "").strip() == "feedback":
        await ask_feedback(m)
        return
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
        "4. Напоминания настраиваются в приложении — значок колокольчика.\n"
        "5. За отметки копятся XP и растут уровни, а в лиге можно соревноваться с другими за неделю 🏆\n\n"
        "Есть идея или что-то сломалось? Напиши /feedback — сообщение придёт автору.\n"
        "Данные хранятся в облаке Telegram и доступны с любого устройства.",
        reply_markup=open_kb(),
    )


@router.message(Command("stats"))
async def cmd_stats(m: Message) -> None:
    admin = await DB.admin_id()
    first_time = False
    if admin is None:
        admin = await DB.claim_admin(m.from_user.id)
        first_time = admin == m.from_user.id
    if admin != m.from_user.id:  # для остальных команды как будто нет
        await any_message(m)
        return
    text = stats_text(await collect_stats())
    if first_time:
        text = "👑 Ты назначен владельцем бота — эта команда работает только у тебя.\n\n" + text
        if getattr(m, "bot", None):
            await set_admin_commands(m.bot, m.from_user.id)
    await m.answer(text)


async def is_admin(m) -> bool:
    return m.from_user is not None and await DB.admin_id() == m.from_user.id


ADMIN_HELP = (
    "👑 Команды владельца\n\n"
    "/stats — сводка по пользователям\n"
    "/report — отчёт прямо сейчас (сам приходит каждый день в {h}:00 МСК)\n"
    "/broadcast текст — рассылка всем (с превью и подтверждением)\n"
    "/news Заголовок + пункты с новой строки — плашка «Что нового» в приложении; /news off — убрать\n"
    "/league — все группы лиги с ID участников\n"
    "/ban ID — исключить из лиги · /unban ID — вернуть\n"
    "/gift @username или ID [кол-во] — подарить заморозки (по умолчанию 1)\n"
    "/refund — последние покупки и возврат звёзд\n"
    "/cancel — отменить ответ или рассылку\n\n"
    "🔗 Ссылки с меткой источника: t.me/{bot}?start=insta, t.me/{bot}?start=vk и т.п. — "
    "метки видны в /stats, отчёте и панели.\n"
    "📊 Графики, удержание, лимиты, покупки и выдача товаров — кнопка 👑 в приложении.\n"
    "⚠️ Если в боте случится ошибка, я пришлю её сюда."
)


@router.message(Command("admin"))
async def cmd_admin(m: Message) -> None:
    if not await is_admin(m):
        return await any_message(m)
    await m.answer(ADMIN_HELP.format(h=REPORT_HOUR, bot=BOT_USERNAME or "твой_бот"))


@router.message(Command("report"))
async def cmd_report(m: Message) -> None:
    if not await is_admin(m):
        return await any_message(m)
    await m.answer(await report_text())


# --- рассылка ---
def bc_kb(n: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"✅ Отправить ({n})", callback_data="bc:go"),
        InlineKeyboardButton(text="✖ Отмена", callback_data="bc:no")]])


@router.message(Command("broadcast"))
async def cmd_broadcast(m: Message, command: CommandObject) -> None:
    if not await is_admin(m):
        return await any_message(m)
    text = (command.args or "").strip()
    if not text:
        await m.answer("Напиши так: /broadcast Текст сообщения\nПеред отправкой я покажу превью.")
        return
    DB.pending_bc[m.from_user.id] = text[:3500]
    n = int(await DB.fetchval("SELECT COUNT(*) FROM users WHERE blocked = 0") or 0)
    await m.answer("👀 Так сообщение увидят пользователи:")
    await m.answer(text[:3500], reply_markup=open_kb())
    await m.answer(f"Отправить {n} {plural(n, 'пользователю', 'пользователям', 'пользователям')}?", reply_markup=bc_kb(n))


async def run_broadcast(bot: Bot, admin: int, text: str) -> None:
    ids = [r["user_id"] for r in await DB.fetch("SELECT user_id FROM users WHERE blocked = 0")]
    ok = blocked = failed = 0
    for uid in ids:
        for attempt in (1, 2):
            try:
                await bot.send_message(uid, text, reply_markup=open_kb())
                ok += 1
            except TelegramRetryAfter as e:
                if attempt == 1:
                    await asyncio.sleep(e.retry_after + 1)
                    continue
                failed += 1
            except TelegramForbiddenError:
                blocked += 1
                await DB.set_blocked(uid)
            except TelegramAPIError:
                failed += 1
            break
        await asyncio.sleep(0.05)  # не больше ~20 сообщений в секунду
    await bot.send_message(admin, f"📤 Рассылка завершена\n✅ Доставлено: {ok}\n🚫 Заблокировали бота: {blocked}\n⚠️ Ошибки: {failed}")


@router.callback_query(F.data.startswith("bc:"))
async def cb_broadcast(c: CallbackQuery) -> None:
    if not await is_admin(c):
        await c.answer()
        return
    text = DB.pending_bc.pop(c.from_user.id, None)
    if c.data == "bc:no" or not text:
        await c.message.edit_text("Рассылка отменена." if c.data == "bc:no" else "Эта рассылка уже неактуальна.")
        await c.answer()
        return
    await c.message.edit_text("📤 Отправляю… Сообщу, когда закончу.")
    await c.answer()
    asyncio.create_task(run_broadcast(c.bot, c.from_user.id, text))


# --- обратная связь ---
async def ask_feedback(m: Message) -> None:
    DB.awaiting_fb.add(m.from_user.id)
    await m.answer("✍️ Напиши одним сообщением идею, вопрос или что сломалось — я передам автору.\n/cancel — отмена")


@router.message(Command("feedback"))
async def cmd_feedback(m: Message, command: CommandObject) -> None:
    text = (command.args or "").strip()
    if text:
        await deliver_feedback(m, text)
    else:
        await ask_feedback(m)


async def deliver_feedback(m: Message, text: str) -> None:
    DB.awaiting_fb.discard(m.from_user.id)
    admin = await DB.admin_id()
    u = m.from_user
    if admin and admin != u.id:
        who = display_name(u) + (f" (@{u.username})" if u.username else "") + f", id {u.id}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="↩️ Ответить", callback_data=f"fb:{u.id}")]])
        await m.bot.send_message(admin, f"💬 Отзыв от {who}:\n\n{text[:3500]}", reply_markup=kb)
    log.info("feedback from %s", u.id)
    await m.answer("Спасибо! Передал автору 🙌")


@router.callback_query(F.data.startswith("fb:"))
async def cb_feedback_reply(c: CallbackQuery) -> None:
    if not await is_admin(c):
        await c.answer()
        return
    DB.reply_to[c.from_user.id] = int(c.data.split(":", 1)[1])
    await c.message.answer("Напиши ответ одним сообщением — я отправлю его пользователю.\n/cancel — отмена")
    await c.answer()


@router.message(Command("paysupport"))
async def cmd_paysupport(m: Message) -> None:
    DB.awaiting_fb.add(m.from_user.id)
    await m.answer("💳 Вопрос по оплате\n\nОпиши одним сообщением, что случилось с покупкой, — автор ответит здесь же. "
                   "Если товар не пришёл, звёзды вернём.\n/cancel — отмена")


@router.message(Command("terms"))
async def cmd_terms(m: Message) -> None:
    await m.answer("📄 Условия\n\nПокупки в трекере — цифровые товары внутри приложения: заморозки, починка серии и темы "
                   "оформления. Они не дают XP и не влияют на места в лиге. Оплата — звёздами Telegram. "
                   "Если товар не пришёл или что-то пошло не так, напиши /paysupport — вернём звёзды.")


@router.message(Command("refund"))
async def cmd_refund(m: Message, command: CommandObject) -> None:
    if not await is_admin(m):
        return await any_message(m)
    parts = (command.args or "").split()
    if len(parts) != 2 or not all(x.isdigit() for x in parts):
        sh = await shop_stats()
        lines = [f"• {r['name']} — {r['title']}, {r['stars']} ⭐, {r['day']}{' (возвращено)' if r['status'] != 'paid' else ''}"
                 f"\n  <code>/refund {r['uid']} {r['id']}</code>" for r in sh["recent"][:10]]
        await m.answer("Возврат: /refund ID_пользователя ID_покупки\n\nПоследние покупки:\n" + ("\n".join(lines) or "пока нет"),
                       parse_mode="HTML")
        return
    await m.answer(await do_refund(m.bot, int(parts[0]), int(parts[1])))


@router.message(Command("cancel"))
async def cmd_cancel(m: Message) -> None:
    uid = m.from_user.id
    had = uid in DB.awaiting_fb or uid in DB.reply_to or uid in DB.pending_bc
    DB.awaiting_fb.discard(uid)
    DB.reply_to.pop(uid, None)
    DB.pending_bc.pop(uid, None)
    await m.answer("Отменено." if had else "Нечего отменять 🙂")


# --- «Что нового» ---
@router.message(Command("news"))
async def cmd_news(m: Message, command: CommandObject) -> None:
    if not await is_admin(m):
        return await any_message(m)
    args = (command.args or "").strip()
    if not args:
        cur = DB.news
        now = f"Сейчас: «{cur['title']}» ({len(cur['items'])} пунктов)" if cur else "Сейчас показывается встроенное описание версии."
        await m.answer(f"{now}\n\nЧтобы изменить:\n/news Заголовок\nпункт 1\nпункт 2\n\nУбрать свою плашку: /news off")
        return
    if args.lower() == "off":
        DB.news = None
        await DB.set_meta("news", None)
        await m.answer("Плашка убрана — в приложении снова встроенное описание версии.")
        return
    lines = [ln.strip() for ln in args.split("\n") if ln.strip()]
    DB.news = {"ver": str(int(time.time())), "title": lines[0][:80],
               "items": [ln.lstrip("-•* ").strip()[:200] for ln in lines[1:]][:8]}
    await DB.set_meta("news", json.dumps(DB.news, ensure_ascii=False))
    items = "\n".join(f"• {x}" for x in DB.news["items"]) or "(без пунктов)"
    await m.answer(f"✅ Плашка обновлена. Пользователи увидят её при следующем открытии:\n\n✨ {DB.news['title']}\n{items}")


# --- лига ---
@router.message(Command("league"))
async def cmd_league(m: Message) -> None:
    if not await is_admin(m):
        return await any_message(m)
    if not DB.league:
        await m.answer("На этой неделе в лиге пока никого нет.")
        return
    out = [f"🏆 Лига, неделя с {DB.week}"]
    for tier, grp in sorted({(r["tier"], r["grp"]) for r in DB.league.values()}):
        group = DB.standings(tier, grp)
        out.append(f"\n{LEAGUES[tier - 1]} · группа {grp + 1} ({len(group)})")
        for i, (uid, r) in enumerate(group, 1):
            out.append(f"{i}. {DB.players.get(uid, {}).get('name') or 'Игрок'} — {r['xp']} XP · {uid}")
    text = "\n".join(out)
    await m.answer(text[:3900] + ("\n…" if len(text) > 3900 else ""))


async def set_ban(m: Message, command: CommandObject, banned: bool) -> None:
    if not await is_admin(m):
        return await any_message(m)
    try:
        uid = int((command.args or "").strip())
    except ValueError:
        await m.answer(f"Напиши так: /{'ban' if banned else 'unban'} ID (ID есть в /league)")
        return
    p = DB.players.setdefault(uid, {"tier": 1, "name": "", "banned": False})
    p["banned"] = banned
    await DB.execute("INSERT INTO players(user_id, tier, name, banned) VALUES($1, 1, '', $2) "
                     "ON CONFLICT(user_id) DO UPDATE SET banned = excluded.banned", uid, int(banned))
    if banned:
        DB.league.pop(uid, None)
        await DB.execute("DELETE FROM league WHERE week = $1 AND user_id = $2", DB.week, uid)
    await m.answer(f"{'⛔ Исключён из лиги' if banned else '✅ Снова может участвовать в лиге'}: {p.get('name') or uid}")


@router.message(Command("ban"))
async def cmd_ban(m: Message, command: CommandObject) -> None:
    await set_ban(m, command, True)


@router.message(Command("unban"))
async def cmd_unban(m: Message, command: CommandObject) -> None:
    await set_ban(m, command, False)


# --- подарки ---
async def refresh_usernames(bot: Bot, limit: int = 100) -> None:
    """Дотягиваем @username и имя у тех, кого запомнили раньше без них (Telegram отдаёт их через getChat)."""
    rows = await DB.fetch("SELECT user_id FROM users WHERE (username IS NULL OR name IS NULL) AND blocked = 0 "
                          "ORDER BY last_seen DESC LIMIT $1", limit)
    for r in rows:
        try:
            ch = await bot.get_chat(int(r["user_id"]))
        except Exception:
            continue
        name = " ".join(x for x in (ch.first_name, ch.last_name) if x) or None
        await DB.execute("UPDATE users SET username = COALESCE($1, username), name = COALESCE($2, name) WHERE user_id = $3",
                         ch.username or None, name, int(r["user_id"]))


async def find_user(who: str) -> list[int]:
    """Ищем по ID, @username, ссылке t.me/… или имени. Возвращает подходящих пользователей."""
    w = who.strip()
    for pre in ("https://t.me/", "http://t.me/", "t.me/", "@"):
        if w.lower().startswith(pre):
            w = w[len(pre):]
    if not w:
        return []
    if w.isdigit():
        return [int(w)] if await DB.fetchval("SELECT user_id FROM users WHERE user_id = $1", int(w)) else []
    rows = await DB.fetch("SELECT user_id FROM users WHERE LOWER(username) = $1", w.lower())
    if rows:
        return [int(r["user_id"]) for r in rows]
    ids = {}
    for r in await DB.fetch("SELECT user_id, name FROM users"):
        ids[int(r["user_id"])] = (r["name"] or "").lower()
    for uid, p in DB.players.items():
        if p.get("name") and uid in ids:
            ids[uid] = ids[uid] + " " + p["name"].lower()
    q = w.lower()
    return [uid for uid, n in ids.items() if n and any(part.startswith(q) for part in n.replace(".", " ").split())]


async def users_hint(limit: int = 15) -> str:
    rows = await DB.fetch("SELECT user_id, username, name FROM users ORDER BY last_seen DESC LIMIT $1", limit)
    if not rows:
        return "Пока никого нет."
    out = []
    for r in rows:
        nm = html.escape(r["name"] or "без имени")
        un = f" @{html.escape(r['username'])}" if r["username"] else ""
        out.append(f"• {nm}{un} — <code>/gift {r['user_id']} 1</code>")
    return "\n".join(out)


@router.message(Command("gift"))
async def cmd_gift(m: Message, command: CommandObject) -> None:
    if not await is_admin(m):
        return await any_message(m)
    parts = (command.args or "").split()
    if not parts:
        await m.answer("Кому подарить? Напиши /gift и ID, @username или имя, а потом сколько (1–5).\n"
                       "Себе: <code>/gift me 2</code>\n\nПоследние пользователи (нажми на команду, чтобы скопировать):\n"
                       + await users_hint(), parse_mode="HTML")
        return
    n = 1
    if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) <= 2:
        n = int(parts.pop())
    elif len(parts) == 1 and parts[0].isdigit() and len(parts[0]) <= 2:   # «/gift 2» — себе
        n, parts = int(parts[0]), ["me"]
    n = max(1, min(5, n))
    who = " ".join(parts)
    if who.lower() in ("me", "я", "мне", "себе"):
        found = [m.from_user.id]
    else:
        found = await find_user(who)
        if not found:
            await refresh_usernames(m.bot)
            found = await find_user(who)
    if len(found) > 1:
        await m.answer("Нашлось несколько человек, выбери по ID:\n" + "\n".join(f"• <code>/gift {u} {n}</code>" for u in found[:15]), parse_mode="HTML")
        return
    if not found:
        await m.answer(f"Не нашёл «{html.escape(who)}». Вот кто есть в боте:\n" + await users_hint(), parse_mode="HTML")
        return
    uid = found[0]
    await DB.execute("INSERT INTO gifts(user_id, created, day, n) VALUES($1, $2, $3, $4)",
                     int(uid), int(time.time() * 1000), msk_day(), n)
    word = plural(n, "заморозку", "заморозки", "заморозок")
    name = await DB.fetchval("SELECT COALESCE(name, username) FROM users WHERE user_id = $1", uid) or str(uid)
    try:
        await m.bot.send_message(int(uid), f"🎁 Тебе подарили {n} {word}! Они уже ждут в трекере ❄️", reply_markup=open_kb())
    except TelegramAPIError:
        pass
    await m.answer(f"🎁 Готово: {n} {word} для {name}. Появятся в трекере при следующем открытии.")


# --- отметка кнопкой из напоминания ---
@router.callback_query(F.data.startswith("d:"))
async def cb_done(c: CallbackQuery) -> None:
    try:
        hid, day, op = c.data[2:].rsplit(":", 2)
    except ValueError:
        await c.answer()
        return
    uid = c.from_user.id
    prof = DB.profiles.get(uid)
    if not prof:
        await c.answer("Открой трекер, чтобы отметить", show_alert=True)
        return
    tz, raw = prof
    try:
        data = json.loads(raw)
    except ValueError:
        data = {}
    local = datetime.now(timezone.utc) + timedelta(minutes=tz)
    today = local.strftime("%Y-%m-%d")
    if day != today:
        await c.answer("Этот день уже закончился — отмечать можно только в тот же день 🙂", show_alert=True)
        return
    h = next((x for x in data.get("habits", []) if x["id"] == hid), None)
    if h is None or h["type"] == "quit":
        await c.answer("Этой привычки больше нет в трекере", show_alert=True)
        return
    cur = eff_val(data, today, h)
    if op == "+":
        v = min(999, cur + 1)
    elif op == "a":
        v = max(cur, h["target"])
    else:
        v = max(cur, 1)
    if v != cur:
        created = await DB.add_mark(uid, hid, today, v)
        srv = data.get("srv") if (data.get("srv") or {}).get("day") == today else None
        srv = srv or {"day": today, "vals": {}}
        srv["vals"][hid] = [v, created]
        data["srv"] = srv
        await DB.save_profile(uid, tz, json.dumps(data, ensure_ascii=False))
    done = is_done(h, v)
    habits = [x for x in today_habits(data, local) if x["type"] != "quit"]
    remaining = [x for x in habits if not is_done(x, eff_val(data, today, x))]
    if h["type"] == "count" and not done:
        await c.answer(f"{v} из {h['target']}")
    else:
        await c.answer("Отмечено ✅" if v != cur else "Уже отмечено ✅")
    # обновляем само сообщение
    if not remaining:
        yesterday = (local - timedelta(days=1)).strftime("%Y-%m-%d")
        st = known_streak(data, today, yesterday)
        if st is not None and not (data.get("today") == today and data.get("todayDone")):
            st += 1
        tail = f"\n🔥 День закрыт! Серия: {days_word(st)}" if st else "\n🔥 День закрыт — огонёк горит!"
    else:
        tail = f"\nОсталось на сегодня: {len(remaining)} {plural(len(remaining), 'привычка', 'привычки', 'привычек')}"
    kb_rows = c.message.reply_markup.inline_keyboard if c.message and c.message.reply_markup else []
    multi = len({(b.callback_data or "")[2:].rsplit(":", 2)[0] for row in kb_rows for b in row
                 if (b.callback_data or "").startswith("d:")}) > 1  # это «Спасти серию» с несколькими привычками
    try:
        if multi and remaining:
            await c.message.edit_text(f"✅ {h['name']} — готово!{tail}", reply_markup=save_kb(remaining, today))
        elif h["type"] == "count" and not done:
            await c.message.edit_text(habit_text(h, v), reply_markup=habit_kb(h, today))
        else:
            await c.message.edit_text(f"✅ {h['name']} — готово!{tail}", reply_markup=open_kb())
    except TelegramAPIError:
        pass


# --- резервная копия: пользователь пересылает файл боту ---
@router.message(F.document)
async def on_document(m: Message) -> None:
    doc = m.document
    if not (doc.file_name or "").lower().endswith(".json") or (doc.file_size or 0) > BACKUP_MAX:
        return await any_message(m)
    try:
        buf = await m.bot.download(doc)
        backup = json.loads(buf.read().decode("utf-8"))
        items = backup.get("items") if isinstance(backup, dict) and backup.get("app") == "habit-tracker" else None
        if not isinstance(items, dict):
            raise ValueError("not a backup")
        items = {k: v for k, v in items.items() if isinstance(k, str) and BACKUP_KEY_RE.match(k) and isinstance(v, str)}
        if not any(k.startswith("h_") for k in items):
            raise ValueError("empty")
    except Exception:
        await m.answer("Это не похоже на резервную копию трекера 🤔 Пришли файл, который бот присылал раньше.")
        return
    created = int(backup.get("created") or 0)
    payload = json.dumps({"created": created, "items": items}, ensure_ascii=False)
    await DB.execute("INSERT INTO restores(user_id, created, data) VALUES($1, $2, $3) "
                     "ON CONFLICT(user_id) DO UPDATE SET created = excluded.created, data = excluded.data",
                     m.from_user.id, created, payload)
    n = sum(1 for k in items if k.startswith("h_"))
    when = datetime.fromtimestamp(created / 1000, timezone.utc).strftime("%d.%m.%Y") if created else "без даты"
    await m.answer(f"💾 Нашёл копию от {when}: {n} {plural(n, 'привычка', 'привычки', 'привычек')}.\n"
                   "Открой трекер — там подтвердишь восстановление.", reply_markup=open_kb())


@router.message()
async def any_message(m: Message) -> None:
    uid = m.from_user.id if m.from_user else 0
    text = (m.text or m.caption or "").strip()
    if uid in DB.reply_to and await is_admin(m):
        if not text:
            await m.answer("Пришли ответ текстом.")
            return
        target = DB.reply_to.pop(uid)
        try:
            await m.bot.send_message(target, f"💬 Ответ автора бота:\n\n{text}")
            await m.answer("Отправлено ✅")
        except TelegramAPIError:
            await m.answer("Не получилось отправить — возможно, пользователь заблокировал бота.")
        return
    if uid in DB.awaiting_fb:
        if not text:
            await m.answer("Пришли, пожалуйста, текстом 🙏")
            return
        await deliver_feedback(m, text)
        return
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
    vals = {}
    if isinstance(body.get("vals"), dict):
        for k, v in list(body["vals"].items())[:100]:
            if isinstance(v, int) and v > 0:
                vals[_str(k, 32)] = min(999, v)
    try:
        mk = max(0, int(body.get("mk", 0) or 0))
    except (TypeError, ValueError):
        mk = 0
    data = {
        "today": today if DATE_RE.match(today) else None,
        "habits": clean_habits(body.get("habits")),
        "done": [_str(x, 32) for x in done_raw[:100]],
        "streak": streak,
        "todayDone": bool(body.get("todayDone")),
        "cfg": clean_cfg(body.get("cfg")),
        "week": week,
        "vals": vals,
        "mk": mk,
        "wk": clean_wk(body.get("wk")),
    }
    # отметки из чата, которые приложение ещё не забрало, не теряем
    try:
        old = json.loads(DB.profiles.get(init.user.id, (0, "{}"))[1])
    except ValueError:
        old = {}
    srv = old.get("srv") or {}
    keep = {k: v for k, v in (srv.get("vals") or {}).items() if isinstance(v, list) and len(v) == 2 and v[1] > mk}
    if keep:
        data["srv"] = {"day": srv.get("day"), "vals": keep}
    await DB.save_profile(init.user.id, tz, json.dumps(data, ensure_ascii=False))
    await DB.touch_user(init.user.id, opened=True, username=init.user.username or None, name=display_name(init.user))
    xp = week_xp(body.get("xp"))
    if xp is not None:
        await DB.league_update(init.user.id, display_name(init.user), xp)
    return web.json_response({"ok": True, "habits": len(data["habits"])})


def clean_wk(raw) -> dict | None:
    """Итоги недели, посчитанные приложением."""
    if not isinstance(raw, dict) or not DATE_RE.match(str(raw.get("mon", ""))):
        return None
    try:
        out = {"mon": str(raw["mon"]), "closed": max(0, min(7, int(raw.get("closed", 0)))),
               "days": max(0, min(7, int(raw.get("days", 0)))), "xp": max(0, min(20000, int(raw.get("xp", 0))))}
        b = raw.get("best")
        if isinstance(b, dict) and b.get("name"):
            out["best"] = {"name": _str(b["name"], 64), "ok": max(0, min(99, int(b.get("ok", 0)))),
                           "all": max(0, min(99, int(b.get("all", 0))))}
    except (TypeError, ValueError):
        return None
    return out


def display_name(user) -> str:
    """Имя для таблицы лиги: имя и первая буква фамилии."""
    name = (user.first_name or "").strip() or "Игрок"
    if user.last_name:
        name += f" {user.last_name.strip()[:1]}."
    return name[:24]


def week_xp(raw) -> int | None:
    """XP за текущую неделю лиги из присланных приложением XP по дням."""
    if not isinstance(raw, dict) or not isinstance(raw.get("days"), dict):
        return None
    monday = league_week()[0]
    total = 0
    for day, v in list(raw["days"].items())[:10]:
        if isinstance(day, str) and DATE_RE.match(day) and day >= monday and isinstance(v, int):
            total += max(0, min(2000, v))
    return total


async def auth(request: web.Request):
    try:
        body = await request.json()
        init = safe_parse_webapp_init_data(BOT_TOKEN, str(body.get("initData", "")))
    except Exception:
        return None
    return init if init.user else None


async def api_league(request: web.Request) -> web.Response:
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    uid = init.user.id
    week, end = league_week()
    me = DB.league.get(uid) if DB.week == week else None
    if me is None:
        me = await DB.league_update(uid, display_name(init.user), 0)
    if me is None:  # владелец исключил из лиги
        return web.json_response({"ok": True, "banned": True, "end": end, "leagues": LEAGUES})
    group = DB.standings(me["tier"], me["grp"])
    n = len(group)
    rows, place = [], 0
    for i, (u, r) in enumerate(group, 1):
        if u == uid:
            place = i
        name = DB.players.get(u, {}).get("name") or "Игрок"
        rows.append({"name": name, "xp": r["xp"], "me": u == uid})
    tier = me["tier"]
    return web.json_response({
        "ok": True, "week": week, "end": end, "tier": tier, "leagues": LEAGUES, "place": place, "size": n,
        "promote": PROMOTE if tier < len(LEAGUES) else 0,
        "demoteFrom": (n - DEMOTE) if tier > 1 and n >= DEMOTE_MIN_GROUP else None,
        "rows": rows,
    })


async def api_whoami(request: web.Request) -> web.Response:
    """При открытии приложения: владелец ли это, подарки, плашка «Что нового»."""
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    uid = init.user.id
    try:
        body = await request.json()
        mk = max(0, int(body.get("mk", 0) or 0))
    except Exception:
        mk = 0
    gifts = [{"day": r["day"], "n": int(r["n"]), "src": r["src"] or "gift"} for r in await DB.fetch(
        "SELECT day, n, src FROM gifts WHERE user_id = $1 ORDER BY created", uid)]
    since = max(mk, int((time.time() - 3 * 86400) * 1000))
    marks = [{"id": int(r["created"]), "h": r["habit"], "d": r["day"], "v": int(r["v"])} for r in await DB.fetch(
        "SELECT created, habit, day, v FROM marks WHERE user_id = $1 AND created > $2 ORDER BY created", uid, since)]
    restore = None
    row = await DB.fetch("SELECT created, data FROM restores WHERE user_id = $1", uid)
    if row:
        try:
            items = json.loads(row[0]["data"])["items"]
            restore = {"created": int(row[0]["created"]), "habits": sum(1 for k in items if k.startswith("h_"))}
        except (ValueError, KeyError):
            restore = None
    return web.json_response({"ok": True, "admin": await DB.admin_id() == uid, "gifts": gifts,
                              "news": DB.news, "bot": BOT_USERNAME, "marks": marks, "restore": restore,
                              "perks": await perks_for(uid), "shop": {k: v["stars"] for k, v in ITEMS.items()}})


async def api_buy(request: web.Request) -> web.Response:
    """Создаёт ссылку на оплату звёздами. Приложение открывает её через Telegram.WebApp.openInvoice."""
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    body = await request.json()
    item, uid = str(body.get("item", "")), init.user.id
    if item not in ITEMS:
        return web.json_response({"ok": False, "error": "no item"}, status=400)
    it = ITEMS[item]
    days = [d for d in (body.get("days") or []) if isinstance(d, str) and DATE_RE.match(d)][:2]
    if it["kind"] == "repair" and not repair_days_ok(days, uid):
        return web.json_response({"ok": False, "error": "Починить можно только вчера или позавчера"}, status=400)
    if it["kind"] == "theme" and it["theme"] in await owned_themes(uid):
        return web.json_response({"ok": False, "error": "Тема уже есть"}, status=400)
    if BOT is None:
        return web.json_response({"ok": False, "error": "bot offline"}, status=503)
    try:
        link = await BOT.create_invoice_link(
            title=it["title"], description=it["desc"], payload=f"{item}|{uid}|{','.join(days)}",
            currency="XTR", prices=[LabeledPrice(label=it["title"], amount=it["stars"])])
    except TelegramAPIError as e:
        log.warning("invoice failed: %s", e)
        return web.json_response({"ok": False, "error": "invoice failed"}, status=502)
    return web.json_response({"ok": True, "link": link})


async def api_repair(request: web.Request) -> web.Response:
    """Бесплатная починка от владельца: тратим подаренный «кредит» на конкретные дни."""
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    body = await request.json()
    uid = init.user.id
    days = [d for d in (body.get("days") or []) if isinstance(d, str) and DATE_RE.match(d)][:2]
    if not repair_days_ok(days, uid):
        return web.json_response({"ok": False, "error": "bad days"}, status=400)
    row = await DB.fetch("SELECT created FROM perks WHERE user_id = $1 AND kind = 'repair' AND value = '' ORDER BY created LIMIT 1", uid)
    if not row:
        return web.json_response({"ok": False, "error": "no credit"}, status=400)
    await DB.execute("UPDATE perks SET value = $1 WHERE user_id = $2 AND created = $3", ",".join(days), uid, row[0]["created"])
    return web.json_response({"ok": True, "perks": await perks_for(uid)})


async def api_admin_grant(request: web.Request) -> web.Response:
    """Владелец выдаёт товар из магазина бесплатно."""
    init = await auth(request)
    if init is None or await DB.admin_id() != init.user.id:
        return web.json_response({"ok": False, "error": "forbidden"}, status=403)
    body = await request.json()
    who, item = str(body.get("who", "")).strip(), str(body.get("item", ""))
    try:
        n = max(1, min(10, int(body.get("n", 1))))
    except (TypeError, ValueError):
        n = 1
    if item not in ITEMS or ITEMS[item]["kind"] == "tip":
        return web.json_response({"ok": False, "error": "Нет такого товара"})
    if who.lower() in ("me", "я", "мне", "себе"):
        found = [init.user.id]
    else:
        found = await find_user(who)
        if not found and BOT is not None:
            await refresh_usernames(BOT)
            found = await find_user(who)
    rows = {r["user_id"]: r for r in await DB.fetch("SELECT user_id, name, username FROM users")}
    label = lambda u: (rows.get(u) or {}).get("name") or (f"@{rows[u]['username']}" if u in rows and rows[u]["username"] else str(u))
    if not found:
        return web.json_response({"ok": False, "error": f"Не нашёл «{who}». Напиши ID, @username или имя."})
    if len(found) > 1:
        return web.json_response({"ok": False, "error": "Нашлось несколько — выбери:", "candidates": [
            {"id": u, "name": label(u), "username": (rows.get(u) or {}).get("username")} for u in found[:10]]})
    uid = found[0]
    it = ITEMS[item]
    if it["kind"] == "theme" and it["theme"] in await owned_themes(uid):
        return web.json_response({"ok": False, "error": f"У пользователя «{label(uid)}» эта тема уже есть"})
    await grant(uid, item, "admin", n=n if it["kind"] == "freeze" else None)
    what = gift_text(item, n if it["kind"] == "freeze" else None)
    if BOT is not None and uid != init.user.id:
        try:
            await BOT.send_message(uid, f"🎁 Тебе подарили {what}! Загляни в трекер ✨", reply_markup=open_kb())
        except TelegramAPIError:
            pass
    k = n if it["kind"] == "freeze" else 0
    name = f"{k} {plural(k, 'заморозка', 'заморозки', 'заморозок')}" if k else it["title"].lower() if it["kind"] == "repair" else it["title"]
    return web.json_response({"ok": True, "text": f"🎁 {label(uid)} получает: {name}"})


async def api_admin_refund(request: web.Request) -> web.Response:
    init = await auth(request)
    if init is None or await DB.admin_id() != init.user.id:
        return web.json_response({"ok": False, "error": "forbidden"}, status=403)
    body = await request.json()
    try:
        uid, created = int(body.get("uid")), int(body.get("id"))
    except (TypeError, ValueError):
        return web.json_response({"ok": False, "error": "bad id"})
    text = await do_refund(BOT, uid, created)
    return web.json_response({"ok": text.startswith("↩️"), "text": text, "shop": await shop_stats()})


async def api_backup(request: web.Request) -> web.Response:
    """Приложение присылает все свои данные — бот отправляет их файлом в чат."""
    if request.content_length and request.content_length > BACKUP_MAX:
        return web.json_response({"ok": False, "error": "too big"}, status=413)
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    body = await request.json()
    raw = body.get("items") if isinstance(body.get("items"), dict) else {}
    items = {k: v for k, v in raw.items() if isinstance(k, str) and BACKUP_KEY_RE.match(k) and isinstance(v, str) and len(v) <= 8192}
    habits = sum(1 for k in items if k.startswith("h_"))
    if not habits:
        return web.json_response({"ok": False, "error": "empty"}, status=400)
    now = int(time.time() * 1000)
    data = json.dumps({"app": "habit-tracker", "v": 1, "created": now, "user": init.user.id, "items": items},
                      ensure_ascii=False).encode("utf-8")
    local = datetime.now(timezone.utc) + timedelta(minutes=DB.profiles.get(init.user.id, (180, ""))[0])
    name = f"habits-backup-{local.strftime('%Y-%m-%d')}.json"
    months = sorted(k[-6:] for k in items if k.startswith("l_"))
    since = f", отметки с {months[0][4:]}.{months[0][:4]}" if months else ""
    if BOT is None:
        return web.json_response({"ok": False, "error": "bot offline"}, status=503)
    try:
        await BOT.send_document(init.user.id, BufferedInputFile(data, filename=name), caption=(
            f"💾 Резервная копия трекера: {habits} {plural(habits, 'привычка', 'привычки', 'привычек')}{since}.\n\n"
            "Чтобы восстановить — просто перешли этот файл мне в чат."))
    except TelegramAPIError as e:
        log.warning("backup send failed: %s", e)
        return web.json_response({"ok": False, "error": "send failed"}, status=502)
    return web.json_response({"ok": True, "habits": habits})


async def api_restore(request: web.Request) -> web.Response:
    """get — отдать копию, которую пользователь переслал боту; drop — забыть её."""
    init = await auth(request)
    if init is None:
        return web.json_response({"ok": False, "error": "bad initData"}, status=401)
    body = await request.json()
    uid = init.user.id
    if body.get("action") == "drop":
        await DB.execute("DELETE FROM restores WHERE user_id = $1", uid)
        return web.json_response({"ok": True})
    row = await DB.fetch("SELECT created, data FROM restores WHERE user_id = $1", uid)
    if not row:
        return web.json_response({"ok": False, "error": "none"}, status=404)
    return web.json_response({"ok": True, "created": int(row[0]["created"]), "items": json.loads(row[0]["data"])["items"]})


async def api_admin(request: web.Request) -> web.Response:
    init = await auth(request)
    if init is None or await DB.admin_id() != init.user.id:
        return web.json_response({"ok": False, "error": "forbidden"}, status=403)
    return web.json_response({
        "ok": True, "stats": await collect_stats(), "series": await series(30), "retention": await retention(),
        "sources": await sources(), "habits": popular_habits(), "league": league_summary(), "limits": await limits(),
        "storage": DB.kind, "bot": BOT_USERNAME, "reportHour": REPORT_HOUR, "shop": await shop_stats(),
        "items": {k: {"title": v["title"], "stars": v["stars"], "kind": v["kind"]} for k, v in ITEMS.items() if v["kind"] != "tip"},
    })


# ---------- оповещения владельцу об ошибках ----------
class OwnerAlerts(logging.Handler):
    """Пересылает владельцу ошибки бота. Сетевые сбои Telegram пропускает, одинаковые — не чаще раза в 10 минут."""
    SKIP = ("TelegramNetworkError", "TelegramConflictError", "TelegramRetryAfter", "ServerDisconnected",
            "ClientConnector", "ClientOSError", "Failed to fetch updates")

    def __init__(self, bot: Bot, loop: asyncio.AbstractEventLoop):
        super().__init__(logging.ERROR)
        self.bot, self.loop = bot, loop
        self.seen: dict[str, float] = {}
        self.recent: list[float] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            exc = "".join(traceback.format_exception(*record.exc_info)) if record.exc_info else ""
            if any(x in msg or x in exc for x in self.SKIP):
                return
            now, key = time.time(), f"{record.name}:{msg[:80]}"
            self.recent = [t for t in self.recent if now - t < 3600]
            if now - self.seen.get(key, 0) < 600 or len(self.recent) >= 20:
                return
            self.seen[key] = now
            self.recent.append(now)
            text = f"⚠️ Ошибка в боте\n{record.name}: {msg[:500]}" + (f"\n\n{exc[-1500:]}" if exc else "")
            self.loop.call_soon_threadsafe(lambda: asyncio.ensure_future(self._send(text)))
        except Exception:
            pass

    async def _send(self, text: str) -> None:
        try:
            admin = await DB.admin_id()
            if admin:
                await self.bot.send_message(admin, text[:4000])
        except Exception:
            pass


async def maybe_report(bot: Bot) -> None:
    """Раз в день в REPORT_HOUR по МСК присылает владельцу отчёт."""
    today = msk_day()
    if DB.report_day == today or (datetime.now(timezone.utc) + LEAGUE_TZ).hour < REPORT_HOUR:
        return
    admin = await DB.admin_id()
    DB.report_day = today
    await DB.set_meta("report_day", today)
    if admin:
        try:
            await bot.send_message(admin, await report_text())
        except TelegramAPIError as e:
            log.warning("report failed: %s", e)


async def set_admin_commands(bot: Bot, admin: int) -> None:
    try:
        await bot.set_my_commands([
            BotCommand(command="start", description="Открыть трекер"),
            BotCommand(command="admin", description="👑 Команды владельца"),
            BotCommand(command="stats", description="Статистика"),
            BotCommand(command="report", description="Отчёт сейчас"),
            BotCommand(command="broadcast", description="Рассылка всем"),
            BotCommand(command="news", description="Плашка «Что нового»"),
            BotCommand(command="league", description="Группы лиги"),
            BotCommand(command="gift", description="Подарить заморозки"),
            BotCommand(command="refund", description="Покупки и возвраты"),
            BotCommand(command="feedback", description="Написать автору"),
        ], scope=BotCommandScopeChat(chat_id=admin))
    except Exception as e:  # меню команд — не критично
        log.warning("admin commands: %s", e)


# ---------- напоминания ----------
def known_streak(data: dict, day: str, yesterday: str):
    """Серия, которую можно назвать сегодня, или None, если данные устарели."""
    if data.get("today") == day:
        return data.get("streak", 0)
    if data.get("today") == yesterday and data.get("todayDone"):
        return data.get("streak", 0)
    return None


def eff_val(data: dict, day: str, h: dict) -> int:
    """Значение привычки за день по данным сервера: что прислало приложение + отметки кнопками из чата."""
    v = 0
    if data.get("today") == day:
        try:
            v = int((data.get("vals") or {}).get(h["id"], 0) or 0)
        except (TypeError, ValueError):
            v = 0
        if h["id"] in set(data.get("done", [])):
            v = max(v, h["target"] if h["type"] == "count" else 1)
    srv = data.get("srv") or {}
    if srv.get("day") == day and h["id"] in (srv.get("vals") or {}):
        v = max(v, int(srv["vals"][h["id"]][0]))
    return v


def is_done(h: dict, v: int) -> bool:
    return v >= (h["target"] if h["type"] == "count" else 1)


def today_habits(data: dict, local: datetime) -> list[dict]:
    day, weekday = local.strftime("%Y-%m-%d"), local.isoweekday()
    return [h for h in data.get("habits", []) if h["created"] <= day and weekday in h["days"]]


def due_items(data: dict, local: datetime, user_id: int = 0) -> list[tuple]:
    """Все напоминания пользователя на сегодня: (ключ, время, текст, клавиатура)."""
    day = local.strftime("%Y-%m-%d")
    yesterday = (local - timedelta(days=1)).strftime("%Y-%m-%d")
    weekday = local.isoweekday()
    habits = today_habits(data, local)
    remaining = [h for h in habits if h["type"] != "quit" and not is_done(h, eff_val(data, day, h))]
    left = {h["id"] for h in remaining}
    streak = known_streak(data, day, yesterday)
    cfg = data.get("cfg") or DEFAULT_CFG
    items = []
    for h in habits:
        if h["type"] != "quit" and h["id"] not in left:
            continue
        kb = habit_kb(h, day) if h["type"] != "quit" else open_kb()
        for t in h["times"]:
            items.append((f"h:{h['id']}:{t}", t, habit_text(h, eff_val(data, day, h)), kb))
    if cfg["plan"]["on"] and habits:
        items.append(("plan", cfg["plan"]["time"], plan_text(habits, streak), open_kb()))
    if cfg["save"]["on"] and remaining:
        items.append(("save", cfg["save"]["time"], save_text(len(remaining), streak), save_kb(remaining, day)))
    if cfg["week"]["on"] and weekday == 7:
        items.append(("week", cfg["week"]["time"], week_text(data, local, streak, user_id), open_kb()))
    return items


async def send(bot: Bot, user_id: int, text: str, kb: InlineKeyboardMarkup | None = None) -> None:
    try:
        await bot.send_message(user_id, text, reply_markup=kb or open_kb())
        log.info("reminder sent to %s: %s", user_id, text.splitlines()[0])
    except TelegramForbiddenError:  # пользователь заблокировал бота
        await DB.set_blocked(user_id)
    except TelegramAPIError as e:
        log.warning("reminder to %s failed: %s", user_id, e)


async def check_reminders(bot: Bot, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    for user_id, (tz_offset, raw) in list(DB.profiles.items()):
        local = now + timedelta(minutes=tz_offset)
        day = local.strftime("%Y-%m-%d")
        now_min = local.hour * 60 + local.minute
        start_min = max(0, now_min - REMIND_WINDOW_MIN)
        try:
            data = json.loads(raw)
        except ValueError:
            continue
        for item, t, text, kb in due_items(data, local, user_id):
            h, m = map(int, t.split(":"))
            if not (start_min <= h * 60 + m <= now_min):
                continue
            if await DB.mark_sent(user_id, item, day):  # ещё не отправляли сегодня
                await send(bot, user_id, text, kb)


async def reminder_loop(bot: Bot) -> None:
    ticks = 0
    while True:
        try:
            if DB.league_checked != league_week()[0]:  # раз в неделю (и при старте)
                await league_tick(bot)
            await check_reminders(bot)
            await maybe_report(bot)
            ticks += 1
            if ticks % 120 == 0:  # раз в час чистим память, раз в сутки — базу
                await DB.prune_sent(db=ticks % 2880 == 0)
        except Exception:
            log.exception("reminder loop error")
        await asyncio.sleep(30)


# ---------- запуск ----------
def build_app() -> web.Application:
    app = web.Application(middlewares=[cors], client_max_size=BACKUP_MAX + 65536)
    app.router.add_get("/", index)
    app.router.add_get("/index.html", index)
    app.router.add_get("/health", health)
    app.router.add_get("/api/me", api_me)
    app.router.add_post("/api/sync", api_sync)
    app.router.add_post("/api/league", api_league)
    app.router.add_post("/api/whoami", api_whoami)
    app.router.add_post("/api/admin", api_admin)
    app.router.add_post("/api/backup", api_backup)
    app.router.add_post("/api/restore", api_restore)
    app.router.add_post("/api/buy", api_buy)
    app.router.add_post("/api/repair", api_repair)
    app.router.add_post("/api/admin/grant", api_admin_grant)
    app.router.add_post("/api/admin/refund", api_admin_refund)
    for path in ("/api/sync", "/api/league", "/api/whoami", "/api/admin", "/api/backup", "/api/restore",
                 "/api/buy", "/api/repair", "/api/admin/grant", "/api/admin/refund"):
        app.router.add_route("OPTIONS", path, options)
    return app


async def main() -> None:
    global BOT_USERNAME, BOT
    if not BOT_TOKEN or not WEBAPP_URL.startswith("https://"):
        raise SystemExit("Задай переменные окружения BOT_TOKEN и WEBAPP_URL (адрес должен начинаться с https://)")
    await DB.open()
    bot = BOT = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.message.outer_middleware(TrackUsers())
    dp.include_router(router)

    runner = web.AppRunner(build_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("web server on port %s", PORT)

    try:
        BOT_USERNAME = (await bot.get_me()).username or ""
    except TelegramAPIError:
        BOT_USERNAME = ""
    logging.getLogger().addHandler(OwnerAlerts(bot, asyncio.get_running_loop()))
    await bot.set_my_commands([
        BotCommand(command="start", description="Открыть трекер"),
        BotCommand(command="help", description="Как пользоваться"),
        BotCommand(command="feedback", description="Написать автору"),
        BotCommand(command="paysupport", description="Вопрос по оплате"),
    ], scope=BotCommandScopeDefault())
    admin = await DB.admin_id()
    if admin:
        await set_admin_commands(bot, admin)
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Трекер", web_app=WebAppInfo(url=WEBAPP_URL)))

    reminders = asyncio.create_task(reminder_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        reminders.cancel()
        await runner.cleanup()
        await bot.session.close()
        await DB.close()


if __name__ == "__main__":
    asyncio.run(main())
