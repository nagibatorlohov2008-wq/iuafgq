"""Бот авторассылки в Telegram (aiogram + Telethon). Запуск: python bot.py"""

# ==================== CONFIG ====================
import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "9100654902:AAFAl_ZuulPMODDzMx5zjpupZ15ZxjK3kco").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "717200582"))
API_ID = int(os.getenv("API_ID", "37813865") or 0)
API_HASH = os.getenv("API_HASH", "a22bdda5bc45d8344a77de0c0e4da804").strip()

# Postgres (Neon / Supabase) — чтобы аккаунты и настройки не пропадали на Render.
# Если не задан — всё хранится в файле (на бесплатном Render он стирается при рестарте).
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DATA_FILE = os.getenv("DATA_FILE", "data/state.json")
PORT = int(os.getenv("PORT", "10000"))

MAX_GROUPS = 100          # максимум групп в списке одного аккаунта
PAGE_SIZE = 20            # кнопок на странице
MAX_ACCOUNTS = 20         # максимум аккаунтов
MIN_INTERVAL_MIN = 1      # границы ручного ввода интервала (минуты)
MAX_INTERVAL_MIN = 1440


# ==================== STORAGE ====================
"""Хранилище: один JSON-документ. Postgres, если есть DATABASE_URL, иначе файл."""
import asyncio
import copy
import json
import os


DEFAULT = {
    "text": "",
    "interval_min": 10,   # раз в сколько минут повторять рассылку
    "delay_sec": 5,       # пауза между группами (секунды)
    "running": False,
    # uid -> {"session", "name", "phone", "groups": [{"id","kind","hash","title"}]}
    "accounts": {},
}


class Storage:
    def __init__(self):
        self.data = copy.deepcopy(DEFAULT)
        self._pool = None
        self._lock = asyncio.Lock()

    async def init(self):
        raw = None
        if DATABASE_URL:
            import asyncpg

            self._pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=2,
                statement_cache_size=0,
                max_inactive_connection_lifetime=60,
            )
            async with self._pool.acquire() as c:
                await c.execute(
                    "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                )
                row = await c.fetchrow("SELECT value FROM kv WHERE key = 'state'")
                raw = row["value"] if row else None
        else:
            os.makedirs(os.path.dirname(DATA_FILE) or ".", exist_ok=True)
            if os.path.exists(DATA_FILE):
                with open(DATA_FILE, encoding="utf-8") as f:
                    raw = f.read()
        if raw:
            self.data.update(json.loads(raw))

    async def save(self):
        raw = json.dumps(self.data, ensure_ascii=False)
        async with self._lock:
            if self._pool:
                async with self._pool.acquire() as c:
                    await c.execute(
                        "INSERT INTO kv (key, value) VALUES ('state', $1) "
                        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        raw,
                    )
            else:
                tmp = DATA_FILE + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(raw)
                os.replace(tmp, DATA_FILE)


db = Storage()


# ==================== TG ====================
"""Помощники для Telethon."""
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, InputPeerChannel, InputPeerChat



def new_client(session: str = "") -> TelegramClient:
    return TelegramClient(StringSession(session), API_ID, API_HASH)


def make_peer(g: dict):
    """Собираем peer из сохранённых id/access_hash — без загрузки всех диалогов."""
    if g["kind"] == "c":
        return InputPeerChannel(g["id"], g["hash"])
    return InputPeerChat(g["id"])


async def fetch_groups(session: str) -> list[dict]:
    """Все группы аккаунта (до MAX_GROUPS): название + число участников."""
    client = new_client(session)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise PermissionError("Сессия недействительна")

        found = []
        async for d in client.iter_dialogs():
            if not d.is_group:
                continue
            ent = d.entity
            if getattr(ent, "deactivated", False):
                continue
            found.append(ent)
            if len(found) >= MAX_GROUPS:
                break

        sem = asyncio.Semaphore(5)

        async def count(ent):
            n = getattr(ent, "participants_count", None)
            if n is not None:
                return n
            async with sem:
                try:
                    res = await client.get_participants(ent, limit=0)
                    return res.total
                except Exception:
                    return None

        counts = await asyncio.gather(*(count(e) for e in found))

        groups = []
        for ent, n in zip(found, counts):
            is_ch = isinstance(ent, Channel)
            groups.append(
                {
                    "id": ent.id,
                    "kind": "c" if is_ch else "g",
                    "hash": ent.access_hash if is_ch else 0,
                    "title": ent.title or "Без названия",
                    "members": n,
                }
            )
        return groups
    finally:
        await client.disconnect()


# ==================== BROADCASTER ====================
"""Рассылка: на каждый аккаунт — своя асинхронная задача, все работают параллельно."""
import asyncio
import logging
import random
import time
from collections import Counter

from aiogram import Bot
from telethon.errors import (
    AuthKeyUnregisteredError,
    ChannelInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    ChatWriteForbiddenError,
    FloodWaitError,
    PeerIdInvalidError,
    SessionRevokedError,
    SlowModeWaitError,
    UserBannedInChannelError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)


log = logging.getLogger("broadcaster")

DEAD_ERRORS = (
    AuthKeyUnregisteredError,
    SessionRevokedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)


class Broadcaster:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.tasks: dict[str, asyncio.Task] = {}
        self.stats: dict[str, dict] = {}

    # ---------- управление ----------
    def start_all(self):
        for uid in list(db.data["accounts"]):
            self.ensure(uid)

    def ensure(self, uid: str):
        t = self.tasks.get(uid)
        if t and not t.done():
            return
        self.tasks[uid] = asyncio.create_task(self._loop(uid))

    def drop(self, uid: str):
        t = self.tasks.pop(uid, None)
        if t:
            t.cancel()

    def stop_all(self):
        for uid in list(self.tasks):
            self.drop(uid)

    # ---------- цикл аккаунта ----------
    async def _loop(self, uid: str):
        await asyncio.sleep(random.uniform(0, 15))  # разносим старт аккаунтов
        last = 0.0
        while True:
            try:
                acc = db.data["accounts"].get(uid)
                if not acc:
                    return
                interval = db.data["interval_min"] * 60
                if time.time() - last >= interval:
                    last = time.time()
                    if await self._round(uid) == "dead":
                        try:
                            await self.bot.send_message(
                                ADMIN_ID,
                                f"⚠️ Аккаунт <b>{acc['name']}</b> разлогинен или заблокирован. "
                                "Удали его в «Регистрация» и добавь заново.",
                            )
                        except Exception:
                            pass
                        return
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Ошибка в цикле аккаунта %s", uid)
            await asyncio.sleep(5)

    async def _round(self, uid: str) -> str:
        acc = db.data["accounts"].get(uid)
        if not acc:
            return "skip"
        text = (db.data.get("text") or "").strip()
        groups = list(acc.get("groups", []))
        if not text or not groups:
            self.stats[uid] = {
                "at": time.time(), "ok": 0, "fail": 0, "errors": {},
                "note": "нет текста или выбранных групп",
            }
            return "skip"

        delay = db.data["delay_sec"]
        ok = fail = 0
        errors: Counter = Counter()
        client = new_client(acc["session"])
        result = "ok"
        try:
            await client.connect()
            if not await client.is_user_authorized():
                return "dead"
            for g in groups:
                err = await self._send(client, make_peer(g), text)
                if err:
                    fail += 1
                    errors[err] += 1
                else:
                    ok += 1
                await asyncio.sleep(delay + random.uniform(0, 2))
        except DEAD_ERRORS:
            result = "dead"
        except (ConnectionError, OSError) as e:
            errors[f"сеть: {type(e).__name__}"] += 1
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            self.stats[uid] = {
                "at": time.time(), "ok": ok, "fail": fail, "errors": dict(errors), "note": "",
            }
        return result

    async def _send(self, client, peer, text: str):
        """None — успех, строка — причина ошибки."""
        for attempt in (1, 2):
            try:
                await client.send_message(peer, text)
                return None
            except FloodWaitError as e:
                if attempt == 1 and e.seconds <= 300:
                    await asyncio.sleep(e.seconds + 1)
                    continue
                return f"FloodWait {e.seconds}с"
            except SlowModeWaitError:
                return "слоумод в группе"
            except (ChatWriteForbiddenError, UserBannedInChannelError, ChatAdminRequiredError):
                return "нет прав писать"
            except (ChannelPrivateError, ChannelInvalidError, PeerIdInvalidError):
                return "нет доступа к группе"
            except DEAD_ERRORS:
                raise
            except Exception as e:
                return type(e).__name__
        return "ошибка"


# ==================== HANDLERS ====================
"""Меню, регистрация аккаунтов (Telethon), настройки рассылки."""
import html
import math
import re
import time

from aiogram import BaseMiddleware, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)


router = Router()
groups_cache: dict[str, list[dict]] = {}   # uid -> список групп аккаунта
pending: dict[str, dict] = {}              # "login" -> идущая авторизация


class Reg(StatesGroup):
    phone = State()
    code = State()
    password = State()


class Edit(StatesGroup):
    text = State()
    interval = State()


class AdminOnly(BaseMiddleware):
    """Бот отвечает только админу."""

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user and user.id == ADMIN_ID:
            return await handler(event, data)
        return None


# ---------------------------------------------------------------- утилиты
def B(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=data)


def KB(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[list(r) for r in rows])


def esc(s) -> str:
    return html.escape(str(s))


def ago(ts: float) -> str:
    s = int(time.time() - ts)
    if s < 60:
        return f"{s} сек назад"
    if s < 3600:
        return f"{s // 60} мин назад"
    return f"{s // 3600} ч назад"


async def show(cb: CallbackQuery, text: str, kb=None):
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "not modified" not in str(e):
            await cb.message.answer(text, reply_markup=kb)


async def drop_pending():
    p = pending.pop("login", None)
    if p:
        try:
            await p["client"].disconnect()
        except Exception:
            pass


async def reset(state: FSMContext):
    await state.clear()
    await drop_pending()


async def try_delete(m: Message):
    try:
        await m.delete()
    except Exception:
        pass


# ---------------------------------------------------------------- главное меню
MAIN_TEXT = "👋 Привет! Я <b>авторассылка</b>.\nНачинай прямо сейчас 👇"


def kb_main():
    return KB([B("📲 Регистрация", "reg")], [B("⚙️ Настройки рассылки", "set")])


def kb_cancel(back: str = "menu"):
    return KB([B("❌ Отмена", back)])


@router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await reset(state)
    await m.answer(MAIN_TEXT, reply_markup=kb_main())


@router.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await cb.answer()
    await show(cb, MAIN_TEXT, kb_main())


# ---------------------------------------------------------------- регистрация
def reg_text() -> str:
    accs = db.data["accounts"]
    if accs:
        body = "\n".join(
            f"{i}. {esc(a['name'])} ({esc(a['phone'])})"
            for i, a in enumerate(accs.values(), 1)
        )
    else:
        body = "Пока нет ни одного аккаунта."
    return (
        f"📲 <b>Регистрация</b>\nАккаунты ({len(accs)}/{MAX_ACCOUNTS}):\n{body}\n\n"
        "Нажми 🗑, чтобы удалить аккаунт."
    )


def kb_reg():
    rows = [[B(f"🗑 {a['name']}"[:40], f"rdel:{uid}")] for uid, a in db.data["accounts"].items()]
    rows += [[B("➕ Добавить аккаунт", "radd")], [B("⬅️ Назад", "menu")]]
    return KB(*rows)


@router.callback_query(F.data == "reg")
async def cb_reg(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await cb.answer()
    await show(cb, reg_text(), kb_reg())


@router.callback_query(F.data.startswith("rdel:"))
async def cb_rdel(cb: CallbackQuery, bc: Broadcaster):
    uid = cb.data.split(":", 1)[1]
    db.data["accounts"].pop(uid, None)
    groups_cache.pop(uid, None)
    bc.drop(uid)
    await db.save()
    await cb.answer("Аккаунт удалён")
    await show(cb, reg_text(), kb_reg())


@router.callback_query(F.data == "radd")
async def cb_radd(cb: CallbackQuery, state: FSMContext):
    if len(db.data["accounts"]) >= MAX_ACCOUNTS:
        await cb.answer(f"Максимум {MAX_ACCOUNTS} аккаунтов", show_alert=True)
        return
    await reset(state)
    await state.set_state(Reg.phone)
    await cb.answer()
    await show(
        cb,
        "🔐 <b>Регистрация аккаунта</b>\n\nШаг 1 из 3: отправь номер телефона "
        "в международном формате, например <code>+79991234567</code>",
        kb_cancel("reg"),
    )


@router.message(Reg.phone)
async def reg_phone(m: Message, state: FSMContext):
    phone = re.sub(r"[^\d+]", "", m.text or "")
    if not phone.startswith("+"):
        phone = "+" + phone
    if len(phone) < 8:
        await m.answer("❌ Не похоже на номер. Пример: <code>+79991234567</code>", reply_markup=kb_cancel("reg"))
        return

    await drop_pending()
    client = new_client()
    try:
        await client.connect()
        sent = await client.send_code_request(phone)
    except PhoneNumberInvalidError:
        await client.disconnect()
        await m.answer("❌ Неверный номер телефона. Отправь ещё раз.", reply_markup=kb_cancel("reg"))
        return
    except FloodWaitError as e:
        await client.disconnect()
        await state.clear()
        await m.answer(f"⏳ Telegram просит подождать {e.seconds} сек. Попробуй позже.", reply_markup=kb_reg())
        return
    except ApiIdInvalidError:
        await client.disconnect()
        await state.clear()
        await m.answer("❌ API_ID / API_HASH неверные. Проверь переменные окружения.", reply_markup=kb_reg())
        return
    except Exception as e:
        await client.disconnect()
        await state.clear()
        await m.answer(f"❌ Ошибка: {esc(type(e).__name__)}", reply_markup=kb_reg())
        return

    pending["login"] = {"client": client, "phone": phone, "hash": sent.phone_code_hash}
    await state.set_state(Reg.code)
    await m.answer(
        "Шаг 2 из 3: Telegram отправил код на этот аккаунт.\n\n"
        "⚠️ Введи код <b>с разделителями</b>, например <code>1-2-3-4-5</code> — "
        "если отправить цифрами подряд, Telegram аннулирует код.",
        reply_markup=kb_cancel("reg"),
    )


async def finish_login(m: Message, state: FSMContext, bc: Broadcaster, p: dict):
    client = p["client"]
    me = await client.get_me()
    session = client.session.save()
    await client.disconnect()
    pending.pop("login", None)
    await state.clear()

    uid = str(me.id)
    old = db.data["accounts"].get(uid, {})
    name = me.first_name or me.username or uid
    db.data["accounts"][uid] = {
        "session": session,
        "name": name,
        "phone": p["phone"],
        "groups": old.get("groups", []),
    }
    await db.save()
    groups_cache.pop(uid, None)
    if db.data["running"]:
        bc.ensure(uid)
    await m.answer(f"✅ Аккаунт <b>{esc(name)}</b> добавлен!", reply_markup=kb_reg())


@router.message(Reg.code)
async def reg_code(m: Message, state: FSMContext, bc: Broadcaster):
    p = pending.get("login")
    await try_delete(m)
    if not p:
        await state.clear()
        await m.answer("Сессия входа устарела, начни заново.", reply_markup=kb_reg())
        return
    code = re.sub(r"\D", "", m.text or "")
    try:
        await p["client"].sign_in(phone=p["phone"], code=code, phone_code_hash=p["hash"])
    except SessionPasswordNeededError:
        await state.set_state(Reg.password)
        await m.answer("Шаг 3 из 3: на аккаунте включена двухфакторка. Введи пароль 2FA:", reply_markup=kb_cancel("reg"))
        return
    except PhoneCodeInvalidError:
        await m.answer("❌ Неверный код. Попробуй ещё раз (например <code>1-2-3-4-5</code>).", reply_markup=kb_cancel("reg"))
        return
    except PhoneCodeExpiredError:
        await drop_pending()
        await state.clear()
        await m.answer("⌛ Код истёк. Начни регистрацию заново.", reply_markup=kb_reg())
        return
    except Exception as e:
        await drop_pending()
        await state.clear()
        await m.answer(f"❌ Ошибка входа: {esc(type(e).__name__)}", reply_markup=kb_reg())
        return
    await finish_login(m, state, bc, p)


@router.message(Reg.password)
async def reg_password(m: Message, state: FSMContext, bc: Broadcaster):
    p = pending.get("login")
    pw = m.text or ""
    await try_delete(m)
    if not p:
        await state.clear()
        await m.answer("Сессия входа устарела, начни заново.", reply_markup=kb_reg())
        return
    try:
        await p["client"].sign_in(password=pw)
    except PasswordHashInvalidError:
        await m.answer("❌ Неверный пароль. Попробуй ещё раз.", reply_markup=kb_cancel("reg"))
        return
    except Exception as e:
        await drop_pending()
        await state.clear()
        await m.answer(f"❌ Ошибка входа: {esc(type(e).__name__)}", reply_markup=kb_reg())
        return
    await finish_login(m, state, bc, p)


# ---------------------------------------------------------------- настройки
def settings_text() -> str:
    d = db.data
    t = d["text"]
    preview = esc(t[:150] + ("…" if len(t) > 150 else "")) if t else "— не задан"
    n_groups = sum(len(a.get("groups", [])) for a in d["accounts"].values())
    status = "🟢 работает" if d["running"] else "🔴 остановлена"
    return (
        "⚙️ <b>Настройки рассылки</b>\n\n"
        f"📝 Текст: {preview}\n"
        f"👤 Аккаунтов: {len(d['accounts'])}, 👥 групп выбрано: {n_groups}\n"
        f"⏱ Интервал: каждые {d['interval_min']} мин\n"
        f"🕐 Пауза между группами: ~{d['delay_sec']} сек\n"
        f"Статус: {status}"
    )


def kb_settings():
    running = db.data["running"]
    return KB(
        [B("📝 Текст для отправки", "stext")],
        [B("👥 Группы для рассылки", "groups")],
        [B("⏱ Время рассылки", "time")],
        [B("⏹ Остановить рассылку" if running else "▶️ Запустить рассылку", "run")],
        [B("📊 Статус", "status")],
        [B("⬅️ Назад", "menu")],
    )


@router.callback_query(F.data == "set")
async def cb_set(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await cb.answer()
    await show(cb, settings_text(), kb_settings())


# ----- текст
@router.callback_query(F.data == "stext")
async def cb_stext(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await state.set_state(Edit.text)
    await cb.answer()
    await show(cb, "📝 Введи текст — я буду рассылать его в выбранные группы:", kb_cancel("set"))


@router.message(Edit.text)
async def msg_text(m: Message, state: FSMContext):
    if not m.text:
        await m.answer("Нужен обычный текст. Отправь ещё раз:", reply_markup=kb_cancel("set"))
        return
    db.data["text"] = m.text[:4096]
    await db.save()
    await state.clear()
    await m.answer("✅ Текст сохранён.\n\n" + settings_text(), reply_markup=kb_settings())


# ----- время
def time_text() -> str:
    d = db.data
    warn = "\n\n⚠️ Интервал меньше 10 минут — выше риск флуд-лимитов и бана." if d["interval_min"] < 10 else ""
    return (
        "⏱ <b>Время рассылки</b>\n\n"
        f"Интервал между рассылками: <b>{d['interval_min']} мин</b>\n"
        f"Пауза между группами: <b>~{d['delay_sec']} сек</b> (+ случайные 0–2 сек)\n\n"
        "Новые значения применяются со следующего круга." + warn
    )


def kb_time():
    ci, cd = db.data["interval_min"], db.data["delay_sec"]
    return KB(
        [B(("✅ " if m == ci else "") + f"{m} мин", f"int:{m}") for m in (10, 15, 30, 60)],
        [B("✍️ Свой интервал", "int_custom")],
        [B(("✅ " if s == cd else "") + f"{s} с", f"dly:{s}") for s in (3, 5, 10, 20, 30)],
        [B("⬅️ Назад", "set")],
    )


@router.callback_query(F.data == "time")
async def cb_time(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await cb.answer()
    await show(cb, time_text(), kb_time())


@router.callback_query(F.data.startswith("int:"))
async def cb_int(cb: CallbackQuery):
    db.data["interval_min"] = int(cb.data.split(":")[1])
    await db.save()
    await cb.answer("Сохранено")
    await show(cb, time_text(), kb_time())


@router.callback_query(F.data.startswith("dly:"))
async def cb_dly(cb: CallbackQuery):
    db.data["delay_sec"] = int(cb.data.split(":")[1])
    await db.save()
    await cb.answer("Сохранено")
    await show(cb, time_text(), kb_time())


@router.callback_query(F.data == "int_custom")
async def cb_int_custom(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    await state.set_state(Edit.interval)
    await cb.answer()
    await show(
        cb,
        f"✍️ Введи интервал в минутах (от {MIN_INTERVAL_MIN} до {MAX_INTERVAL_MIN}), например <code>45</code>:",
        kb_cancel("time"),
    )


@router.message(Edit.interval)
async def msg_interval(m: Message, state: FSMContext):
    raw = (m.text or "").strip()
    if not raw.isdigit() or not (MIN_INTERVAL_MIN <= int(raw) <= MAX_INTERVAL_MIN):
        await m.answer(
            f"❌ Нужно целое число от {MIN_INTERVAL_MIN} до {MAX_INTERVAL_MIN}. Попробуй ещё раз:",
            reply_markup=kb_cancel("time"),
        )
        return
    db.data["interval_min"] = int(raw)
    await db.save()
    await state.clear()
    await m.answer(time_text(), reply_markup=kb_time())


# ----- запуск / остановка
@router.callback_query(F.data == "run")
async def cb_run(cb: CallbackQuery, bc: Broadcaster):
    if db.data["running"]:
        db.data["running"] = False
        await db.save()
        bc.stop_all()
        await cb.answer("Рассылка остановлена")
    else:
        if not db.data["text"].strip():
            await cb.answer("Сначала задай текст рассылки", show_alert=True)
            return
        if not any(a.get("groups") for a in db.data["accounts"].values()):
            await cb.answer("Сначала добавь аккаунт и выбери группы", show_alert=True)
            return
        db.data["running"] = True
        await db.save()
        bc.start_all()
        await cb.answer("Рассылка запущена 🚀")
    await show(cb, settings_text(), kb_settings())


# ----- статус
@router.callback_query(F.data == "status")
async def cb_status(cb: CallbackQuery, bc: Broadcaster):
    d = db.data
    lines = [f"📊 <b>Статус:</b> {'🟢 работает' if d['running'] else '🔴 остановлена'}\n"]
    if not d["accounts"]:
        lines.append("Аккаунтов нет.")
    for uid, a in d["accounts"].items():
        st = bc.stats.get(uid)
        if st is None:
            res = "ещё не отправляли"
        elif st.get("note"):
            res = st["note"]
        else:
            res = f"✅ {st['ok']} / ❌ {st['fail']} · {ago(st['at'])}"
            if st["errors"]:
                res += "\n    " + ", ".join(f"{esc(k)}: {v}" for k, v in list(st["errors"].items())[:4])
        lines.append(f"• <b>{esc(a['name'])}</b> — групп: {len(a.get('groups', []))}\n    {res}")
    await cb.answer()
    await show(cb, "\n".join(lines), KB([B("🔄 Обновить", "status")], [B("⬅️ Назад", "set")]))


# ----- группы
def groups_header(uid: str, page: int) -> str:
    acc = db.data["accounts"][uid]
    total = max(1, math.ceil(len(groups_cache[uid]) / PAGE_SIZE))
    return (
        f"👥 <b>Группы аккаунта {esc(acc['name'])}</b>\n"
        f"Страница {page + 1}/{total} · выбрано: {len(acc.get('groups', []))}\n\n"
        "Нажимай на группу, чтобы включить/выключить рассылку в неё."
    )


def kb_groups(uid: str, page: int):
    groups = groups_cache[uid]
    total = max(1, math.ceil(len(groups) / PAGE_SIZE))
    sel = {(g["kind"], g["id"]) for g in db.data["accounts"][uid].get("groups", [])}
    rows = []
    for i in range(page * PAGE_SIZE, min(len(groups), (page + 1) * PAGE_SIZE)):
        g = groups[i]
        mark = "✅ " if (g["kind"], g["id"]) in sel else ""
        n = g["members"] if g["members"] is not None else "?"
        rows.append([B(f"{mark}{g['title'][:45]} ({n})", f"gt:{uid}:{i}:{page}")])
    nav = []
    if page > 0:
        nav.append(B("⬅️ Назад", f"gp:{uid}:{page - 1}"))
    if page < total - 1:
        nav.append(B("Вперёд ➡️", f"gp:{uid}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([B("🔄 Обновить список", f"gr:{uid}")])
    rows.append([B("⬅️ В настройки", "set")])
    return KB(*rows)


async def render_groups(cb: CallbackQuery, uid: str, page: int):
    if not groups_cache[uid]:
        await show(cb, "В этом аккаунте нет групп.", KB([B("🔄 Обновить список", f"gr:{uid}")], [B("⬅️ В настройки", "set")]))
        return
    await show(cb, groups_header(uid, page), kb_groups(uid, page))


async def open_groups(cb: CallbackQuery, uid: str, page: int = 0, reload: bool = False):
    acc = db.data["accounts"].get(uid)
    if not acc:
        await cb.answer("Аккаунт не найден", show_alert=True)
        return
    if reload or uid not in groups_cache:
        await show(cb, "⏳ Собираю группы аккаунта, подожди несколько секунд…")
        try:
            groups_cache[uid] = await fetch_groups(acc["session"])
        except Exception as e:
            await show(
                cb,
                f"❌ Не удалось получить группы: {esc(type(e).__name__)}",
                KB([B("🔄 Повторить", f"gr:{uid}")], [B("⬅️ В настройки", "set")]),
            )
            return
    await render_groups(cb, uid, page)


@router.callback_query(F.data == "groups")
async def cb_groups(cb: CallbackQuery, state: FSMContext):
    await reset(state)
    accs = db.data["accounts"]
    if not accs:
        await cb.answer("Сначала добавь аккаунт в «Регистрация»", show_alert=True)
        return
    await cb.answer()
    if len(accs) == 1:
        await open_groups(cb, next(iter(accs)))
        return
    rows = [
        [B(f"{a['name']} (выбрано: {len(a.get('groups', []))})"[:60], f"gacc:{uid}")]
        for uid, a in accs.items()
    ]
    rows.append([B("⬅️ Назад", "set")])
    await show(cb, "👤 Выбери аккаунт, для которого настраиваем группы:", KB(*rows))


@router.callback_query(F.data.startswith("gacc:"))
async def cb_gacc(cb: CallbackQuery):
    await cb.answer()
    await open_groups(cb, cb.data.split(":")[1])


@router.callback_query(F.data.startswith("gr:"))
async def cb_gr(cb: CallbackQuery):
    await cb.answer()
    await open_groups(cb, cb.data.split(":")[1], reload=True)


@router.callback_query(F.data.startswith("gp:"))
async def cb_gp(cb: CallbackQuery):
    _, uid, page = cb.data.split(":")
    await cb.answer()
    await open_groups(cb, uid, int(page))


@router.callback_query(F.data.startswith("gt:"))
async def cb_gt(cb: CallbackQuery):
    _, uid, i, page = cb.data.split(":")
    acc = db.data["accounts"].get(uid)
    groups = groups_cache.get(uid)
    if not acc or groups is None or int(i) >= len(groups):
        await cb.answer("Список устарел, открываю заново…")
        await open_groups(cb, uid, int(page), reload=True)
        return
    g = groups[int(i)]
    key = (g["kind"], g["id"])
    cur = acc.get("groups", [])
    new = [x for x in cur if (x["kind"], x["id"]) != key]
    if len(new) == len(cur):
        new.append({"id": g["id"], "kind": g["kind"], "hash": g["hash"], "title": g["title"]})
    acc["groups"] = new
    await db.save()
    await cb.answer()
    await render_groups(cb, uid, int(page))


# ----- всё остальное
@router.message()
async def fallback(m: Message):
    await m.answer(MAIN_TEXT, reply_markup=kb_main())


# ==================== RUN ====================
import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiohttp import web



async def health(_request):
    return web.Response(text="OK")


async def start_web():
    """Мини-сервер для Render + UptimeRobot (чтобы бесплатный сервис не засыпал)."""
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    missing = [n for n, v in (("BOT_TOKEN", BOT_TOKEN), ("API_ID", API_ID), ("API_HASH", API_HASH)) if not v]
    if missing:
        print("Не заданы переменные окружения:", ", ".join(missing))
        sys.exit(1)

    await start_web()
    await db.init()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    bc = Broadcaster(bot)
    dp = Dispatcher(storage=MemoryStorage())
    dp["bc"] = bc
    dp.message.outer_middleware(AdminOnly())
    dp.callback_query.outer_middleware(AdminOnly())
    dp.include_router(router)

    if db.data["running"]:      # после рестарта на Render рассылка продолжается сама
        bc.start_all()

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
