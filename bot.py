import asyncio, base64, html, io, json, logging, math, os, random, re, time, wave
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import aiohttp
from aiohttp import web
import psycopg2
import psycopg2.extras
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup, default_state
from aiogram.types import (Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile,
                           ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton)

# =====================================================================
#                              НАСТРОЙКИ
# =====================================================================
TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬ_СЮДА_НОВЫЙ_ТОКЕН")
# Впиши свой Telegram ID (цифры). Узнать можно у @userinfobot.
# Админ проверяет верификацию и получает жалобы. Если пусто, верификация одобряется автоматически (только для тестов!).
ADMIN_IDS = {7142098499}

DATABASE_URL = os.getenv("DATABASE_URL", "")   # строка подключения Postgres (Neon / Supabase)
TZ_HOURS = 3             # часовой пояс пользователей относительно UTC (3 = Москва, 5 = Екатеринбург, 7 = Красноярск)
NOW_WINDOW_H = 4         # сколько часов активен статус «Сейчас»
CUSTOM_WINDOW_H = 3      # сколько часов длится выбранное время (например 20:00 → до 23:00)
MAX_DAYS_AHEAD = 7       # на сколько дней вперёд можно планировать
RADIUS_KM = 15           # радиус поиска людей с таким же желанием
FALLBACK_KM = 50         # если таких нет, показываем всех в этом радиусе (с пометкой)
PAGE = 5                 # анкет на страницу
DAILY_INVITES = 10       # бесплатных приглашений в день
BAN_REPORTS = 3          # столько жалоб от разных людей = автобан

WELCOME_BONUS = 3        # баллы новичку после верификации
REF_BONUS = 10           # баллы за каждого друга, прошедшего верификацию
BOOST_COST = 5           # цена буста
BOOST_HOURS = 3          # длительность буста
INVITES_PACK_COST = 3    # цена пакета приглашений
INVITES_PACK_SIZE = 5

STATUSES = ["🎬 Кино", "🍻 Выпить", "🚶 Погулять", "🤝 Поддержка",
            "💃 Потанцевать", "☕ Кофе", "🏋️ Спорт", "🎲 Настолки",
            "🍕 Поесть", "🎤 Караоке"]
TIMES = ["Сейчас", "Сегодня днём", "Сегодня вечером"]
CUSTOM_BTN = "📅 Выбрать дату и время"
WD = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

REASONS = {"spam": "Спам / реклама", "abuse": "Оскорбления", "fake": "Фейк / чужое фото",
           "scam": "Мошенничество", "other": "Другое"}

# ---------- ИИ-собеседник ----------
# Ключи берутся из переменных окружения или вписываются сюда:
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "ВСТАВЬ_КЛЮЧ_GEMINI")   # aistudio.google.com/apikey
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
# Если основная модель перегружена или кончился лимит, бот пробует следующие по очереди:
GEMINI_CHAT_MODELS = [m for m in dict.fromkeys(
    [GEMINI_MODEL, "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-flash-latest"]) if m]
GEMINI_TTS_MODELS = [m for m in (os.getenv("GEMINI_TTS_MODEL"), "gemini-2.5-flash-preview-tts") if m]
VOICES = {"f": "Kore", "m": "Orus"}   # голоса Gemini: женский Kore, мужской Orus (можно: Puck, Charon, Fenrir, Aoede, Leda)

# ЗАПАСНОЙ бесплатный ИИ: Groq (console.groq.com → API Keys). Работает, когда Gemini не отвечает.
GROQ_KEY = os.getenv("GROQ_API_KEY", "ВСТАВЬ_КЛЮЧ_GROQ")
GROQ_MODELS = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant"]
GROQ_STT_MODEL = "whisper-large-v3-turbo"   # расшифровка голосовых, если Gemini не справился

AI_FREE_PER_DAY = 10     # бесплатных сообщений ИИ в день
AI_EXTRA_COST = 1        # дальше столько баллов за сообщение
AI_MAX_SECONDS = 60      # максимум длины голосового/кружка


def key_ok(k):
    return bool(k) and not k.startswith("ВСТАВЬ")


logging.basicConfig(level=logging.INFO)
bot = Bot(TOKEN.strip(), default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
BOT_USERNAME = ""

# =====================================================================
#                              БАЗА ДАННЫХ
# =====================================================================
_conn = None


def _connect():
    global _conn
    _conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor,
                             connect_timeout=20, keepalives=1, keepalives_idle=30,
                             keepalives_interval=10, keepalives_count=3)
    _conn.autocommit = True


def _exec(sql, args=()):
    """Выполняет запрос; если соединение с БД оборвалось (Neon засыпает), переподключается."""
    global _conn
    sql = sql.replace("?", "%s")
    for attempt in range(2):
        try:
            if _conn is None or _conn.closed:
                _connect()
            cur = _conn.cursor()
            cur.execute(sql, args)
            return cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
            if attempt == 1:
                raise


def init_db():
    if not DATABASE_URL:
        raise SystemExit("Не задана переменная DATABASE_URL (строка подключения Postgres)")
    _exec("""
    CREATE TABLE IF NOT EXISTS users(
        id BIGINT PRIMARY KEY,
        username TEXT, name TEXT, age INTEGER, about TEXT,
        photo TEXT, video_note TEXT, verify_code TEXT,
        verified INTEGER DEFAULT 0,
        banned INTEGER DEFAULT 0, hidden INTEGER DEFAULT 0, notify INTEGER DEFAULT 1,
        lat DOUBLE PRECISION, lon DOUBLE PRECISION, status TEXT, time TEXT,
        status_ts DOUBLE PRECISION DEFAULT 0,
        points INTEGER DEFAULT 0, boost_until DOUBLE PRECISION DEFAULT 0, extra_invites INTEGER DEFAULT 0,
        ref_by BIGINT, ref_paid INTEGER DEFAULT 0,
        streak INTEGER DEFAULT 0, last_bonus TEXT,
        last_notify DOUBLE PRECISION DEFAULT 0, created DOUBLE PRECISION,
        age_min INTEGER DEFAULT 18, age_max INTEGER DEFAULT 99,
        ai_mode TEXT DEFAULT 'text', ai_gender TEXT DEFAULT 'f',
        ai_consent INTEGER DEFAULT 0, ai_day TEXT, ai_count INTEGER DEFAULT 0,
        start_ts DOUBLE PRECISION DEFAULT 0, end_ts DOUBLE PRECISION DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS blocks(a BIGINT, b BIGINT, PRIMARY KEY(a, b));
    CREATE TABLE IF NOT EXISTS invites(
        id SERIAL PRIMARY KEY,
        frm BIGINT, to_id BIGINT, day TEXT, status TEXT DEFAULT 'pending',
        accepted_ts DOUBLE PRECISION, asked INTEGER DEFAULT 0,
        UNIQUE(frm, to_id, day)
    );
    CREATE TABLE IF NOT EXISTS ratings(
        rater BIGINT, target BIGINT, inv_id INTEGER, val INTEGER,
        PRIMARY KEY(rater, inv_id)
    );
    CREATE TABLE IF NOT EXISTS reports(
        reporter BIGINT, target BIGINT, reason TEXT, ts DOUBLE PRECISION,
        PRIMARY KEY(reporter, target)
    );
    """)


def get_user(uid):
    return one("SELECT * FROM users WHERE id=?", (uid,))


def one(sql, args=()):
    cur = _exec(sql, args)
    row = cur.fetchone()
    cur.close()
    return row


def many(sql, args=()):
    cur = _exec(sql, args)
    rows = cur.fetchall()
    cur.close()
    return rows


def run(sql, args=()):
    return _exec(sql, args)


def upd(uid, **kw):
    sets = ", ".join(f"{k}=?" for k in kw)
    run(f"UPDATE users SET {sets} WHERE id=?", (*kw.values(), uid))


def spend(uid, cost):
    cur = run("UPDATE users SET points=points-? WHERE id=? AND points>=?", (cost, uid, cost))
    return cur.rowcount > 0


# =====================================================================
#                              ХЕЛПЕРЫ
# =====================================================================
def esc(t):
    return html.escape(str(t if t is not None else ""))


def kb(rows):
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=t) for t in r] for r in rows],
                               resize_keyboard=True)


def build_menu():
    rows = [STATUSES[i:i + 2] for i in range(0, len(STATUSES), 2)]
    rows += [["👀 Кто рядом", "👤 Профиль"], ["🎁 Бонус дня", "👥 Друзья"], ["🚀 Буст", "🔥 ИИ-собеседник"]]
    return kb(rows)


MENU = build_menu()
MENU_TEXTS = {t for row in MENU.keyboard for t in [b.text for b in row]}
TIME_KB = kb([[t] for t in TIMES] + [[CUSTOM_BTN], ["❌ Отмена"]])
CLOCK_KB = kb([["12:00", "13:00", "14:00"], ["15:00", "16:00", "17:00"], ["18:00", "19:00", "20:00"],
               ["21:00", "22:00", "23:00"], ["❌ Отмена"]])


def local_now():
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=TZ_HOURS)


def local_to_ts(dt):
    return (dt - timedelta(hours=TZ_HOURS)).replace(tzinfo=timezone.utc).timestamp()


def ts_to_local(ts):
    return datetime.fromtimestamp(ts, timezone.utc).replace(tzinfo=None) + timedelta(hours=TZ_HOURS)


def day_name(d):
    base = local_now().date()
    if d == base:
        return "Сегодня"
    if d == base + timedelta(days=1):
        return "Завтра"
    if d == base + timedelta(days=2):
        return "Послезавтра"
    return f"{WD[d.weekday()]} {d.strftime('%d.%m')}"


def when_text(label, start_ts):
    if label in TIMES:
        return label
    dt = ts_to_local(start_ts or 0)
    return f"{day_name(dt.date())}, {dt.strftime('%H:%M')}"


def when_user(u):
    return when_text(u["time"], u["start_ts"])


def day_kb():
    base = local_now().date()
    labels = ["Сегодня", "Завтра", "Послезавтра"] + [
        f"{WD[(base + timedelta(days=i)).weekday()]} {(base + timedelta(days=i)).strftime('%d.%m')}"
        for i in range(3, MAX_DAYS_AHEAD)]
    rows = [labels[i:i + 2] for i in range(0, len(labels), 2)]
    return kb(rows + [["❌ Отмена"]])


def parse_day(text):
    t = (text or "").strip().lower()
    base = local_now().date()
    fixed = {"сегодня": 0, "завтра": 1, "послезавтра": 2}
    if t in fixed:
        return base + timedelta(days=fixed[t])
    mt = re.search(r"(\d{1,2})[.\-/](\d{1,2})", t)
    if not mt:
        return None
    try:
        d = date(base.year, int(mt.group(2)), int(mt.group(1)))
        if d < base:
            d = date(base.year + 1, d.month, d.day)
        return d
    except ValueError:
        return None


def parse_clock(text):
    mt = re.fullmatch(r"\s*(\d{1,2})(?:[:.\-](\d{2}))?\s*", text or "")
    if not mt:
        return None
    h, mi = int(mt.group(1)), int(mt.group(2) or 0)
    return (h, mi) if h < 24 and mi < 60 else None


CANCEL_KB = kb([["❌ Отмена"]])


def loc_kb(has_old):
    rows = [[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]]
    if has_old:
        rows.append([KeyboardButton(text="📌 Та же точка, что раньше")])
    rows.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def dist(a, b, c, d):
    p = math.pi / 180
    x = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(x))


def band(km):
    if km < 2:
        return "до 2 км"
    if km < 5:
        return "2–5 км"
    if km < 10:
        return "5–10 км"
    if km < 15:
        return "10–15 км"
    return "15–50 км"


def blur(v):
    return round(v, 2)  # ~1 км, точное место не храним


def is_active(u):
    return bool(u and u["status"] and u["lat"] is not None
                and (u["end_ts"] or 0) > time.time())


def is_boosted(u):
    return (u["boost_until"] or 0) > time.time()


def contact(u):
    s = f'<a href="tg://user?id={u["id"]}">{esc(u["name"])}</a>'
    if u["username"]:
        s += f' (@{esc(u["username"])})'
    return s


def rep(uid):
    r = one("SELECT COALESCE(SUM(CASE WHEN val=1 THEN 1 ELSE 0 END),0) up, COALESCE(SUM(CASE WHEN val=-1 THEN 1 ELSE 0 END),0) down FROM ratings WHERE target=?", (uid,))
    return r["up"], r["down"]


def badge(uid):
    n = one("SELECT COUNT(*) n FROM users WHERE ref_by=? AND ref_paid=1", (uid,))["n"]
    if n >= 25:
        return " 👑"
    if n >= 10:
        return " 💎"
    if n >= 3:
        return " 🌟"
    return ""


def card(u, extra=""):
    v = " ✅" if u["verified"] == 2 else ""
    b = " 🚀" if is_boosted(u) else ""
    up, _ = rep(u["id"])
    r = f"\n👍 Хорошие встречи: {up}" if up else ""
    return f"<b>{esc(u['name'])}, {u['age']}</b>{v}{b}{badge(u['id'])}{r}\n{extra}{esc(u['about'])}"


def ref_link(uid):
    return f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"


async def tg(coro):
    try:
        return await coro
    except Exception as e:
        logging.warning("tg error: %s", e)
        return None


async def safe_edit(msg, text, kbd=None):
    try:
        if msg.photo or msg.video_note:
            await msg.edit_caption(caption=text, reply_markup=kbd)
        else:
            await msg.edit_text(text, reply_markup=kbd)
    except Exception:
        pass


def card_kb(uid):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👋 Пригласить", callback_data=f"inv:{uid}")],
        [InlineKeyboardButton(text="🚩 Пожаловаться", callback_data=f"rep:{uid}"),
         InlineKeyboardButton(text="⛔ Заблокировать", callback_data=f"blk:{uid}")]])


async def check(obj):
    """Пускает дальше только верифицированных. Возвращает строку пользователя или None."""
    uid = obj.from_user.id
    u = get_user(uid)
    txt = None
    if not u:
        txt = "Сначала нажми /start"
    elif u["verified"] == 1:
        txt = "⏳ Твоя верификация на проверке. Мы сообщим, как только всё будет готово."
    elif u["verified"] == 3:
        txt = "❌ Верификация не пройдена. Нажми /verify и запиши новый кружок."
    elif u["verified"] != 2:
        txt = "Заверши регистрацию: нажми /start"
    if txt:
        if isinstance(obj, Message):
            await obj.answer(txt)
        else:
            await obj.answer(txt, show_alert=True)
        return None
    return u


# =====================================================================
#                              СОСТОЯНИЯ
# =====================================================================
class Reg(StatesGroup):
    name = State()
    age = State()
    about = State()
    photo = State()
    video = State()


class St(StatesGroup):
    time = State()
    day = State()
    clock = State()
    loc = State()


class Edit(StatesGroup):
    about = State()
    age = State()


# =====================================================================
#                              MIDDLEWARE (баны)
# =====================================================================
class Guard(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user:
            u = get_user(user.id)
            if u:
                if u["banned"]:
                    if isinstance(event, Message):
                        await event.answer("⛔ Твой аккаунт заблокирован.")
                    elif isinstance(event, CallbackQuery):
                        await event.answer("⛔ Аккаунт заблокирован", show_alert=True)
                    return
                if (user.username or "") != (u["username"] or ""):
                    upd(user.id, username=user.username)
        return await handler(event, data)


dp.message.outer_middleware(Guard())
dp.callback_query.outer_middleware(Guard())

# =====================================================================
#                   РЕГИСТРАЦИЯ → ФОТО → ВЕРИФИКАЦИЯ
# =====================================================================
VERIFY_TEXT = ("🔐 <b>Верификация</b>\n\n"
               "Запиши <b>кружок</b> (видеосообщение: в чате нажми значок микрофона/камеры, чтобы он стал камерой, и зажми).\n"
               "Покажи лицо и скажи вслух код: <b>{code}</b>\n\n"
               "Кружок записывается только на камеру, из галереи не отправить. Так мы убеждаемся, что фото твоё. "
               "Видео видит только модератор.")


async def resume(m: Message, state: FSMContext, u):
    """Продолжает регистрацию с того места, где человек остановился."""
    if not u["name"]:
        await state.set_state(Reg.name)
        await m.answer("Привет! Как тебя зовут?", reply_markup=ReplyKeyboardRemove())
    elif not u["age"]:
        await state.set_state(Reg.age)
        await m.answer("Сколько тебе лет?")
    elif not u["about"]:
        await state.set_state(Reg.about)
        await m.answer("Расскажи о себе в паре строк")
    elif not u["photo"]:
        await state.set_state(Reg.photo)
        await m.answer("📷 Отправь своё лучшее фото (лицо должно быть хорошо видно).")
    elif not u["video_note"] or u["verified"] == 3:
        await start_video(m, state, u["id"])
    elif u["verified"] == 1:
        await m.answer("⏳ Анкета на проверке. Как только модератор одобрит, я напишу.")
    else:
        await m.answer("Что хочешь сегодня?", reply_markup=MENU)


async def start_video(m: Message, state: FSMContext, uid):
    code = str(random.randint(1000, 9999))
    upd(uid, verify_code=code, verified=0)
    await state.set_state(Reg.video)
    await m.answer(VERIFY_TEXT.format(code=code), reply_markup=ReplyKeyboardRemove())


@dp.message(CommandStart())
async def start(m: Message, state: FSMContext, command: CommandObject):
    await state.clear()
    uid = m.from_user.id
    u = get_user(uid)
    if not u:
        ref = None
        arg = command.args or ""
        if arg.startswith("ref_") and arg[4:].isdigit():
            r = int(arg[4:])
            if r != uid and get_user(r):
                ref = r
        run("INSERT INTO users(id, username, created, ref_by) VALUES(?,?,?,?)",
            (uid, m.from_user.username, time.time(), ref))
        if ref:
            await m.answer("🎉 Тебя пригласил друг. После верификации вы оба получите бонус!")
        u = get_user(uid)
    await resume(m, state, u)


@dp.message(Command("anketa"))
async def anketa(m: Message, state: FSMContext):
    if not get_user(m.from_user.id):
        return await m.answer("Сначала нажми /start")
    await state.set_state(Reg.name)
    await m.answer("Заполним анкету заново. Как тебя зовут?", reply_markup=ReplyKeyboardRemove())


@dp.message(Command("verify"))
async def verify_cmd(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    if not u or not u["photo"]:
        return await m.answer("Сначала заполни анкету: /start")
    if u["verified"] == 2:
        return await m.answer("Ты уже верифицирован ✅")
    await start_video(m, state, u["id"])


@dp.message(Command("cancel"))
@dp.message(F.text == "❌ Отмена")
async def cancel(m: Message, state: FSMContext):
    await state.clear()
    u = get_user(m.from_user.id)
    await m.answer("Отменено.", reply_markup=MENU if u and u["verified"] == 2 else ReplyKeyboardRemove())


@dp.message(Reg.name)
async def get_name(m: Message, state: FSMContext):
    if not m.text or m.text.startswith("/") or m.text in MENU_TEXTS:
        return await m.answer("Напиши своё имя текстом")
    upd(m.from_user.id, name=m.text.strip()[:30])
    await state.set_state(Reg.age)
    await m.answer("Сколько тебе лет?")


@dp.message(Reg.age)
async def get_age(m: Message, state: FSMContext):
    if not m.text or not m.text.isdigit() or not (18 <= int(m.text) <= 99):
        return await m.answer("Напиши число от 18 до 99. Сервис только для 18+")
    upd(m.from_user.id, age=int(m.text))
    await state.set_state(Reg.about)
    await m.answer("Расскажи о себе в паре строк (до 300 символов)")


@dp.message(Reg.about)
async def get_about(m: Message, state: FSMContext):
    if not m.text or m.text.startswith("/") or len(m.text.strip()) < 3:
        return await m.answer("Напиши пару слов о себе текстом")
    upd(m.from_user.id, about=m.text.strip()[:300])
    await state.set_state(Reg.photo)
    await m.answer("📷 Теперь отправь своё лучшее фото (лицо должно быть хорошо видно).")


@dp.message(Reg.photo, F.photo)
async def get_photo(m: Message, state: FSMContext):
    upd(m.from_user.id, photo=m.photo[-1].file_id, video_note=None)
    await start_video(m, state, m.from_user.id)


@dp.message(Reg.photo)
async def bad_photo(m: Message):
    await m.answer("Нужно именно фото 📷 (не файл и не стикер)")


@dp.message(Reg.video, F.video_note)
async def get_video(m: Message, state: FSMContext):
    if (m.video_note.duration or 0) < 2:
        return await m.answer("Слишком короткий кружок. Запиши 3–5 секунд и скажи код.")
    uid = m.from_user.id
    upd(uid, video_note=m.video_note.file_id, verified=1)
    await state.clear()
    if ADMIN_IDS:
        await m.answer("✅ Отправлено на проверку! Обычно это занимает немного времени. Я напишу тебе.",
                       reply_markup=ReplyKeyboardRemove())
        await send_to_admins(uid)
    else:
        await do_approve(uid)  # режим теста без админа


@dp.message(Reg.video)
async def bad_video(m: Message):
    await m.answer("Нужен именно <b>кружок</b> 🎥: в чате нажми на значок микрофона, "
                   "чтобы он превратился в камеру, и зажми его.")


# ---------- Верификация: админ ----------
async def send_to_admins(uid):
    u = get_user(uid)
    cap = (f"🆕 <b>Верификация</b>\nID: <code>{uid}</code>\n{esc(u['name'])}, {u['age']}\n"
           f"Код в кружке должен быть: <b>{esc(u['verify_code'])}</b>\n"
           f"Проверь: лицо на кружке = лицо на фото, и человек сказал код.")
    kbd = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"vok:{uid}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"vno:{uid}")]])
    for a in ADMIN_IDS:
        await tg(bot.send_photo(a, u["photo"], caption=cap))
        await tg(bot.send_video_note(a, u["video_note"], reply_markup=kbd))


async def do_approve(uid):
    u = get_user(uid)
    if not u:
        return
    upd(uid, verified=2)
    bonus = WELCOME_BONUS
    await tg(bot.send_message(uid, f"✅ <b>Верификация пройдена!</b>\nТебе начислено +{bonus} баллов 🎁\n"
                                   "Что хочешь сегодня?", reply_markup=MENU))
    run("UPDATE users SET points=points+? WHERE id=?", (bonus, uid))
    if u["ref_by"] and not u["ref_paid"]:
        run("UPDATE users SET points=points+? WHERE id=?", (REF_BONUS, u["ref_by"]))
        upd(uid, ref_paid=1)
        await tg(bot.send_message(u["ref_by"], f"🎉 Твой друг <b>{esc(u['name'])}</b> прошёл верификацию!\n"
                                               f"Тебе +{REF_BONUS} баллов."))
        n = one("SELECT COUNT(*) n FROM users WHERE ref_by=? AND ref_paid=1", (u["ref_by"],))["n"]
        prize = {3: (10, "🌟"), 10: (30, "💎"), 25: (100, "👑")}.get(n)
        if prize:
            run("UPDATE users SET points=points+? WHERE id=?", (prize[0], u["ref_by"]))
            await tg(bot.send_message(u["ref_by"], f"{prize[1]} <b>Новый уровень!</b> Приглашено друзей: {n}.\n"
                                                   f"Бонус +{prize[0]} баллов и значок {prize[1]} в твоей анкете."))


@dp.callback_query(F.data.startswith("vok:"), F.from_user.id.in_(ADMIN_IDS))
async def v_ok(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    u = get_user(uid)
    if not u or u["verified"] == 2:
        return await c.answer("Уже обработано")
    await do_approve(uid)
    await tg(c.message.edit_reply_markup(reply_markup=None))
    await c.answer("Одобрено ✅")


@dp.callback_query(F.data.startswith("vno:"), F.from_user.id.in_(ADMIN_IDS))
async def v_no(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    upd(uid, verified=3)
    await tg(bot.send_message(uid, "❌ Верификация не пройдена: лицо на кружке не совпало с фото или не прозвучал код.\n"
                                   "Нажми /verify и запиши новый кружок. Если нужно, поменяй фото через /anketa."))
    await tg(c.message.edit_reply_markup(reply_markup=None))
    await c.answer("Отклонено")


# =====================================================================
#                          СТАТУС НА СЕГОДНЯ
# =====================================================================
@dp.message(default_state, F.text.in_(STATUSES))
async def pick_status(m: Message, state: FSMContext):
    if not await check(m):
        return
    await state.update_data(status=m.text)
    await state.set_state(St.time)
    await m.answer("Когда?", reply_markup=TIME_KB)


async def ask_loc(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    await state.set_state(St.loc)
    await m.answer("Отправь геолокацию, чтобы найти людей рядом.\n"
                   "🔒 Другие увидят только примерное расстояние, точное место мы не храним и не показываем.",
                   reply_markup=loc_kb(u["lat"] is not None))


@dp.message(St.time, F.text.in_(TIMES))
async def pick_time(m: Message, state: FSMContext):
    now = time.time()
    mid = local_now().replace(hour=0, minute=0, second=0, microsecond=0)
    if m.text == "Сейчас":
        start, end = now, now + NOW_WINDOW_H * 3600
    elif m.text == "Сегодня днём":
        start, end = max(now, local_to_ts(mid + timedelta(hours=12))), local_to_ts(mid + timedelta(hours=17))
    else:
        start, end = max(now, local_to_ts(mid + timedelta(hours=18))), local_to_ts(mid + timedelta(hours=23, minutes=30))
    if end <= now:
        end = now + 2 * 3600
    await state.update_data(time=m.text, start_ts=start, end_ts=end)
    await ask_loc(m, state)


@dp.message(St.time, F.text == CUSTOM_BTN)
async def pick_custom(m: Message, state: FSMContext):
    await state.set_state(St.day)
    await m.answer("На какой день? Выбери кнопкой или напиши дату, например <b>12.10</b>", reply_markup=day_kb())


@dp.message(St.day)
async def pick_day(m: Message, state: FSMContext):
    d = parse_day(m.text)
    today = local_now().date()
    if not d:
        return await m.answer("Не понял день. Выбери кнопкой или напиши дату, например 12.10")
    if d > today + timedelta(days=MAX_DAYS_AHEAD):
        return await m.answer(f"Можно планировать максимум на {MAX_DAYS_AHEAD} дней вперёд")
    await state.update_data(day=d.isoformat())
    await state.set_state(St.clock)
    await m.answer(f"{day_name(d)}. Во сколько? Выбери или напиши своё время, например <b>21:30</b>",
                   reply_markup=CLOCK_KB)


@dp.message(St.clock)
async def pick_clock(m: Message, state: FSMContext):
    c = parse_clock(m.text)
    if not c:
        return await m.answer("Не понял время. Напиши, например, 21:30")
    d = date.fromisoformat((await state.get_data())["day"])
    start = local_to_ts(datetime(d.year, d.month, d.day, c[0], c[1]))
    end = start + CUSTOM_WINDOW_H * 3600
    now = time.time()
    if end <= now:
        return await m.answer("Это время уже прошло. Выбери другое")
    await state.update_data(time="custom", start_ts=max(start, now - 900), end_ts=end)
    await ask_loc(m, state)


@dp.message(St.time)
async def bad_time(m: Message):
    await m.answer("Выбери время кнопкой ниже или нажми «" + CUSTOM_BTN + "»")


async def save_status(m: Message, state: FSMContext, lat, lon):
    d = await state.get_data()
    uid = m.from_user.id
    upd(uid, status=d["status"], time=d["time"], start_ts=d["start_ts"], end_ts=d["end_ts"],
        lat=lat, lon=lon, status_ts=time.time())
    await state.clear()
    await m.answer(f"Готово! {d['status']}, {when_text(d['time'], d['start_ts']).lower()}.\nНажми «👀 Кто рядом»",
                   reply_markup=MENU)
    await notify_nearby(uid)


@dp.message(St.loc, F.location)
async def got_loc(m: Message, state: FSMContext):
    await save_status(m, state, blur(m.location.latitude), blur(m.location.longitude))


@dp.message(St.loc, F.text == "📌 Та же точка, что раньше")
async def old_loc(m: Message, state: FSMContext):
    u = get_user(m.from_user.id)
    if u["lat"] is None:
        return await m.answer("Старой точки нет, отправь геолокацию кнопкой")
    await save_status(m, state, u["lat"], u["lon"])


@dp.message(St.loc)
async def bad_loc(m: Message):
    await m.answer("Нажми кнопку «📍 Отправить геолокацию» внизу")


async def notify_nearby(uid):
    """Тихо сообщает людям рядом с таким же желанием (без раскрытия личности)."""
    me = get_user(uid)
    if me["hidden"]:
        return
    now = time.time()
    rows = many("""SELECT * FROM users WHERE id!=? AND verified=2 AND banned=0 AND hidden=0 AND notify=1
                   AND status=? AND end_ts>? AND start_ts<=? AND end_ts>=? AND lat IS NOT NULL AND last_notify<?
                   AND id NOT IN (SELECT b FROM blocks WHERE a=?)
                   AND id NOT IN (SELECT a FROM blocks WHERE b=?)""",
                (uid, me["status"], now, me["end_ts"], me["start_ts"], now - 3 * 3600, uid, uid))
    sent = 0
    for u in rows:
        if sent >= 5:
            break
        if dist(me["lat"], me["lon"], u["lat"], u["lon"]) <= RADIUS_KM:
            if await tg(bot.send_message(u["id"], f"🔔 Рядом появился человек с тем же желанием: {esc(me['status'])}, {esc(when_user(me).lower())}\n"
                                                  "Нажми «👀 Кто рядом»")):
                upd(u["id"], last_notify=now)
                sent += 1


# =====================================================================
#                              КТО РЯДОМ
# =====================================================================
async def send_nearby(chat_id, uid, offset=0):
    me = get_user(uid)
    if not is_active(me):
        return await bot.send_message(chat_id, "Сначала выбери, что хочешь сегодня 👆")
    now = time.time()
    rows = many("""SELECT * FROM users WHERE id!=? AND verified=2 AND banned=0 AND hidden=0
                   AND end_ts>? AND lat IS NOT NULL
                   AND age BETWEEN ? AND ? AND age_min<=? AND age_max>=?
                   AND id NOT IN (SELECT b FROM blocks WHERE a=?)
                   AND id NOT IN (SELECT a FROM blocks WHERE b=?)""",
                (uid, now, me["age_min"], me["age_max"], me["age"], me["age"], uid, uid))

    def overlap(u):
        return u["start_ts"] <= me["end_ts"] and u["end_ts"] >= me["start_ts"]

    strict, wide = [], []
    for u in rows:
        km = dist(me["lat"], me["lon"], u["lat"], u["lon"])
        if km <= FALLBACK_KM:
            wide.append((km, u))
        if u["status"] == me["status"] and km <= RADIUS_KM:
            strict.append((km, u))
    strict.sort(key=lambda x: (0 if overlap(x[1]) else 1, 0 if is_boosted(x[1]) else 1, x[0]))
    wide.sort(key=lambda x: (0 if x[1]["status"] == me["status"] else 1, 0 if overlap(x[1]) else 1, x[0]))

    # Если людей с таким же желанием рядом нет, показываем всех, кто сейчас в боте (с пометкой)
    relaxed = not strict
    found = wide if relaxed else strict
    if not found:
        return await bot.send_message(chat_id, "Пока в боте никого нет в твоём районе 😕\n"
                                               "Позови друзей через «👥 Друзья» и загляни позже. "
                                               "Я пришлю уведомление, когда кто-то появится 🔔")
    page = found[offset:offset + PAGE]
    if not page:
        return await bot.send_message(chat_id, "Это все, кто есть рядом сейчас.")
    if offset == 0:
        if relaxed:
            await bot.send_message(chat_id, f"С желанием «{esc(me['status'])}» рядом пока никого, "
                                            f"но вот кто сейчас в боте ({len(found)}). "
                                            "Можешь пригласить и предложить своё 😉")
        else:
            await bot.send_message(chat_id, f"{esc(me['status'])}, рядом ({len(found)}):")
    for km, u in page:
        match = "\n✅ Совпадает по времени" if overlap(u) else ""
        want = f"\n🎯 Хочет: {esc(u['status'])}" if relaxed else ""
        text = card(u, f"📍 {band(km)} · 🕒 {esc(when_user(u))}{want}{match}\n")
        await tg(bot.send_photo(chat_id, u["photo"], caption=text, reply_markup=card_kb(u["id"])))
    if offset + PAGE < len(found):
        more = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⬇️ Показать ещё", callback_data=f"more:{offset + PAGE}")]])
        await bot.send_message(chat_id, f"Показано {offset + len(page)} из {len(found)}", reply_markup=more)


@dp.message(default_state, F.text == "👀 Кто рядом")
async def nearby(m: Message):
    if not await check(m):
        return
    await send_nearby(m.chat.id, m.from_user.id, 0)


@dp.callback_query(F.data.startswith("more:"))
async def more(c: CallbackQuery):
    if not await check(c):
        return
    await tg(c.message.delete())
    await send_nearby(c.message.chat.id, c.from_user.id, int(c.data.split(":")[1]))
    await c.answer()


# =====================================================================
#                              ПРИГЛАШЕНИЯ
# =====================================================================
@dp.callback_query(F.data.startswith("inv:"))
async def invite(c: CallbackQuery):
    me = await check(c)
    if not me:
        return
    to = int(c.data.split(":")[1])
    other = get_user(to)
    if not other or other["banned"] or other["verified"] != 2 or not is_active(other):
        return await c.answer("Этот человек уже недоступен", show_alert=True)
    if one("SELECT 1 FROM blocks WHERE (a=? AND b=?) OR (a=? AND b=?)", (me["id"], to, to, me["id"])):
        return await c.answer("Недоступно", show_alert=True)
    if not is_active(me):
        return await c.answer("Сначала выбери статус на сегодня", show_alert=True)
    today = local_now().date().isoformat()
    if one("SELECT 1 FROM invites WHERE frm=? AND to_id=? AND day=?", (me["id"], to, today)):
        return await c.answer("Ты уже отправил приглашение этому человеку", show_alert=True)
    used = one("SELECT COUNT(*) n FROM invites WHERE frm=? AND day=?", (me["id"], today))["n"]
    if used >= DAILY_INVITES:
        if me["extra_invites"] > 0:
            run("UPDATE users SET extra_invites=extra_invites-1 WHERE id=?", (me["id"],))
        else:
            return await c.answer(f"Лимит {DAILY_INVITES} приглашений в день исчерпан.\n"
                                  f"Докупи пакет за баллы: «🚀 Буст».", show_alert=True)
    run("INSERT INTO invites(frm, to_id, day) VALUES(?,?,?)", (me["id"], to, today))
    btns = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Принять", callback_data=f"acc:{me['id']}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"dec:{me['id']}")],
        [InlineKeyboardButton(text="🚩 Пожаловаться", callback_data=f"rep:{me['id']}")]])
    when = esc(me["status"]) + ", " + esc(when_user(me).lower()) + "\n"
    text = "👋 Тебя приглашает\n" + card(me, when)
    ok = await tg(bot.send_photo(to, me["photo"], caption=text, reply_markup=btns))
    if ok:
        await c.answer("Приглашение отправлено! Если человек примет, ты получишь контакт.", show_alert=True)
    else:
        run("DELETE FROM invites WHERE frm=? AND to_id=? AND day=?", (me["id"], to, today))
        await c.answer("Не удалось отправить приглашение", show_alert=True)


SAFETY = ("\n\n⚠️ <b>Безопасность:</b> первую встречу назначай в людном месте, "
          "предупреди близких и не переводи деньги незнакомым.")


@dp.callback_query(F.data.startswith("acc:"))
async def accept(c: CallbackQuery):
    me = await check(c)
    if not me:
        return
    frm = int(c.data.split(":")[1])
    a = get_user(frm)
    inv = one("SELECT * FROM invites WHERE frm=? AND to_id=? AND status='pending' ORDER BY id DESC",
              (frm, me["id"]))
    if not a or not inv:
        return await c.answer("Приглашение устарело", show_alert=True)
    run("UPDATE invites SET status='accepted', accepted_ts=? WHERE id=?", (time.time(), inv["id"]))
    await safe_edit(c.message, f"✅ Принято! Напиши: {contact(a)}{SAFETY}")
    await tg(bot.send_message(frm, f"✅ {esc(me['name'])} принял(а) приглашение! Напиши: {contact(me)}{SAFETY}"))
    await c.answer()


@dp.callback_query(F.data.startswith("dec:"))
async def decline(c: CallbackQuery):
    frm = int(c.data.split(":")[1])
    run("UPDATE invites SET status='declined' WHERE frm=? AND to_id=? AND status='pending'",
        (frm, c.from_user.id))
    await safe_edit(c.message, "Приглашение отклонено.")
    await c.answer()


# =====================================================================
#                          БЛОКИРОВКА И ЖАЛОБЫ
# =====================================================================
@dp.callback_query(F.data.startswith("blk:"))
async def block(c: CallbackQuery):
    uid = int(c.data.split(":")[1])
    run("INSERT INTO blocks(a, b) VALUES(?,?) ON CONFLICT DO NOTHING", (c.from_user.id, uid))
    await c.answer("Пользователь заблокирован. Вы больше не увидите друг друга.", show_alert=True)
    await tg(c.message.delete())


@dp.callback_query(F.data.startswith("rep:"))
async def report(c: CallbackQuery):
    uid = c.data.split(":")[1]
    rows = [[InlineKeyboardButton(text=t, callback_data=f"rr:{uid}:{k}")] for k, t in REASONS.items()]
    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data=f"rr:{uid}:back")])
    await tg(c.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)))
    await c.answer()


@dp.callback_query(F.data.startswith("rr:"))
async def report_reason(c: CallbackQuery):
    _, uid, code = c.data.split(":")
    uid = int(uid)
    if code == "back":
        await tg(c.message.edit_reply_markup(reply_markup=card_kb(uid)))
        return await c.answer()
    me, other = get_user(c.from_user.id), get_user(uid)
    if not me or not other:
        return await c.answer("Ошибка", show_alert=True)
    run("INSERT INTO reports(reporter, target, reason, ts) VALUES(?,?,?,?) ON CONFLICT DO NOTHING",
        (me["id"], uid, code, time.time()))
    run("INSERT INTO blocks(a, b) VALUES(?,?) ON CONFLICT DO NOTHING", (me["id"], uid))
    n = one("SELECT COUNT(*) n FROM reports WHERE target=?", (uid,))["n"]
    auto = ""
    if n >= BAN_REPORTS and not other["banned"]:
        upd(uid, banned=1, status=None)
        auto = "\n🔨 Автобан по числу жалоб."
    for a in ADMIN_IDS:
        kbd = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔨 Забанить", callback_data=f"aban:{uid}")]])
        await tg(bot.send_message(a, f"🚩 Жалоба на <code>{uid}</code> ({esc(other['name'])})\n"
                                     f"Причина: {REASONS.get(code, code)}\nВсего жалоб: {n}{auto}", reply_markup=kbd))
    await c.answer("Жалоба отправлена, человек заблокирован для тебя. Спасибо!", show_alert=True)
    await tg(c.message.delete())


# =====================================================================
#                      БОНУСЫ, ДРУЗЬЯ, БУСТ
# =====================================================================
@dp.message(default_state, F.text == "🎁 Бонус дня")
async def daily(m: Message):
    u = await check(m)
    if not u:
        return
    today = local_now().date()
    if u["last_bonus"] == today.isoformat():
        return await m.answer(f"Сегодня бонус уже получен 🎁\nСерия: {u['streak']} дн. Приходи завтра!")
    streak = u["streak"] + 1 if u["last_bonus"] == (today - timedelta(days=1)).isoformat() else 1
    pts = 1 + min(streak - 1, 4)
    extra = ""
    if streak % 7 == 0:
        pts += 5
        extra = "\n🔥 Бонус за 7 дней подряд: +5!"
    run("UPDATE users SET points=points+?, streak=?, last_bonus=? WHERE id=?",
        (pts, streak, today.isoformat(), u["id"]))
    await m.answer(f"🎁 +{pts} баллов!\nСерия: {streak} дн.{extra}\nВсего: {u['points'] + pts}")


@dp.message(default_state, F.text == "👥 Друзья")
async def friends(m: Message):
    u = await check(m)
    if not u:
        return
    total = one("SELECT COUNT(*) n FROM users WHERE ref_by=?", (u["id"],))["n"]
    paid = one("SELECT COUNT(*) n FROM users WHERE ref_by=? AND ref_paid=1", (u["id"],))["n"]
    link = ref_link(u["id"])
    share = f"https://t.me/share/url?url={quote(link)}&text={quote('Заходи, найдём компанию на вечер 🔥')}"
    kbd = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📤 Отправить другу", url=share)]])
    await m.answer(f"👥 <b>Пригласи друга и получи бонус</b>\n\n"
                   f"За каждого друга, прошедшего верификацию, ты получаешь <b>+{REF_BONUS} баллов</b>, "
                   f"а друг получает +{WELCOME_BONUS}.\n\n"
                   f"Твоя ссылка:\n{link}\n\n"
                   f"Приглашено: {total} · Верифицировано: {paid}\n"
                   f"Баллы: {u['points']}", reply_markup=kbd)


def shop_text(u):
    left = max(0, int((u["boost_until"] - time.time()) / 60))
    b = f"🚀 Буст активен ещё {left} мин." if left else "Буст не активен."
    return (f"🚀 <b>Магазин</b>\nБаллы: <b>{u['points']}</b>\n{b}\n"
            f"Доп. приглашений: {u['extra_invites']}\n\n"
            f"• Буст ({BOOST_HOURS} ч): твоя анкета показывается первой, цена {BOOST_COST}\n"
            f"• +{INVITES_PACK_SIZE} приглашений сверх лимита, цена {INVITES_PACK_COST}\n\n"
            f"Баллы дают за бонус дня 🎁 и приглашение друзей 👥")


SHOP_KB = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text=f"🚀 Буст за {BOOST_COST}", callback_data="buy:boost")],
    [InlineKeyboardButton(text=f"➕ {INVITES_PACK_SIZE} приглашений за {INVITES_PACK_COST}", callback_data="buy:inv")]])


@dp.message(default_state, F.text == "🚀 Буст")
async def shop(m: Message):
    u = await check(m)
    if u:
        await m.answer(shop_text(u), reply_markup=SHOP_KB)


@dp.callback_query(F.data.startswith("buy:"))
async def buy(c: CallbackQuery):
    u = await check(c)
    if not u:
        return
    what = c.data.split(":")[1]
    if what == "boost":
        if not spend(u["id"], BOOST_COST):
            return await c.answer("Не хватает баллов. Забери 🎁 бонус дня или позови друзей 👥", show_alert=True)
        upd(u["id"], boost_until=max(time.time(), u["boost_until"] or 0) + BOOST_HOURS * 3600)
        await c.answer(f"🚀 Буст на {BOOST_HOURS} ч активирован!", show_alert=True)
    else:
        if not spend(u["id"], INVITES_PACK_COST):
            return await c.answer("Не хватает баллов", show_alert=True)
        run("UPDATE users SET extra_invites=extra_invites+? WHERE id=?", (INVITES_PACK_SIZE, u["id"]))
        await c.answer(f"➕ Добавлено {INVITES_PACK_SIZE} приглашений", show_alert=True)
    await safe_edit(c.message, shop_text(get_user(u["id"])), SHOP_KB)


# =====================================================================
#                              ПРОФИЛЬ
# =====================================================================
def profile_kb(u):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ О себе", callback_data="pf:about"),
         InlineKeyboardButton(text="📷 Фото", callback_data="pf:photo")],
        [InlineKeyboardButton(text="🙈 Показать меня" if u["hidden"] else "🙈 Скрыть меня", callback_data="pf:hide"),
         InlineKeyboardButton(text="🔔 Увед.: " + ("вкл" if u["notify"] else "выкл"), callback_data="pf:notify")],
        [InlineKeyboardButton(text=f"🎯 Возраст поиска: {u['age_min']}–{u['age_max']}", callback_data="pf:age")],
        [InlineKeyboardButton(text="🗑 Удалить анкету", callback_data="pf:del")]])


def profile_text(u):
    st = f"{esc(u['status'])}, {esc(when_user(u).lower())}" if is_active(u) else "не выбран"
    hid = "\n🙈 Анкета скрыта" if u["hidden"] else ""
    return (card(u) + f"\n\nСтатус: {st}\nБаллы: {u['points']}{hid}")


@dp.message(default_state, F.text == "👤 Профиль")
@dp.message(Command("profile"))
async def profile(m: Message):
    u = await check(m)
    if u:
        await m.answer_photo(u["photo"], caption=profile_text(u), reply_markup=profile_kb(u))


@dp.callback_query(F.data.startswith("pf:"))
async def profile_cb(c: CallbackQuery, state: FSMContext):
    u = await check(c)
    if not u:
        return
    act = c.data.split(":")[1]
    if act == "about":
        await state.set_state(Edit.about)
        await c.message.answer("Напиши новый текст о себе:", reply_markup=CANCEL_KB)
        return await c.answer()
    if act == "age":
        await state.set_state(Edit.age)
        await c.message.answer("Напиши, людей какого возраста показывать, например: <b>20-30</b>",
                               reply_markup=CANCEL_KB)
        return await c.answer()
    if act == "photo":
        await state.set_state(Reg.photo)
        await c.message.answer("📷 Отправь новое фото. После этого потребуется повторная верификация.",
                               reply_markup=CANCEL_KB)
        return await c.answer()
    if act in ("hide", "notify"):
        col = "hidden" if act == "hide" else "notify"
        upd(u["id"], **{col: 0 if u[col] else 1})
        u = get_user(u["id"])
        await tg(c.message.edit_caption(caption=profile_text(u), reply_markup=profile_kb(u)))
        return await c.answer("Готово")
    if act == "del":
        kbd = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Да, удалить", callback_data="pf:delok"),
            InlineKeyboardButton(text="Нет", callback_data="pf:no")]])
        await c.message.answer("Точно удалить анкету? Баллы и всё остальное пропадёт.", reply_markup=kbd)
        return await c.answer()
    if act == "no":
        await tg(c.message.delete())
        return await c.answer()
    if act == "delok":
        run("DELETE FROM users WHERE id=?", (u["id"],))
        run("DELETE FROM blocks WHERE a=? OR b=?", (u["id"], u["id"]))
        run("DELETE FROM invites WHERE frm=? OR to_id=?", (u["id"], u["id"]))
        await tg(c.message.delete())
        await c.message.answer("Анкета удалена. Чтобы вернуться, нажми /start", reply_markup=ReplyKeyboardRemove())
        return await c.answer()


@dp.message(Edit.about)
async def edit_about(m: Message, state: FSMContext):
    if not m.text or m.text.startswith("/") or len(m.text.strip()) < 3:
        return await m.answer("Напиши текст о себе")
    upd(m.from_user.id, about=m.text.strip()[:300])
    await state.clear()
    await m.answer("Обновлено ✅", reply_markup=MENU)


@dp.message(Edit.age)
async def edit_age(m: Message, state: FSMContext):
    nums = re.findall(r"\d+", m.text or "")
    if len(nums) != 2:
        return await m.answer("Напиши два числа через дефис, например: 20-30")
    lo, hi = sorted(int(x) for x in nums)
    if lo < 18 or hi > 99:
        return await m.answer("Возраст должен быть от 18 до 99")
    upd(m.from_user.id, age_min=lo, age_max=hi)
    await state.clear()
    await m.answer(f"Готово ✅ Показываю людей {lo}–{hi} лет", reply_markup=MENU)


# =====================================================================
#                 ОЦЕНКА ВСТРЕЧИ (репутация)
# =====================================================================
async def followups():
    """Через 4 часа после принятого приглашения спрашивает обоих, как прошла встреча."""
    while True:
        await asyncio.sleep(600)
        try:
            rows = many("SELECT * FROM invites WHERE status='accepted' AND asked=0 AND accepted_ts<?",
                        (time.time() - 4 * 3600,))
            for r in rows:
                run("UPDATE invites SET asked=1 WHERE id=?", (r["id"],))
                for who, other in ((r["frm"], r["to_id"]), (r["to_id"], r["frm"])):
                    o = get_user(other)
                    if not o:
                        continue
                    kbd = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="👍 Отлично", callback_data=f"rt:{r['id']}:{other}:up"),
                        InlineKeyboardButton(text="👎 Плохо", callback_data=f"rt:{r['id']}:{other}:down")],
                        [InlineKeyboardButton(text="Не встретились", callback_data=f"rt:{r['id']}:{other}:no")]])
                    await tg(bot.send_message(who, f"Как прошла встреча с <b>{esc(o['name'])}</b>?\n"
                                                   f"Оценка влияет на репутацию. За ответ +1 балл 🎁", reply_markup=kbd))
        except Exception:
            logging.exception("followups error")


@dp.callback_query(F.data.startswith("rt:"))
async def rate(c: CallbackQuery):
    _, inv_id, target, val = c.data.split(":")
    inv_id, target = int(inv_id), int(target)
    inv = one("SELECT * FROM invites WHERE id=?", (inv_id,))
    me = c.from_user.id
    if not inv or {inv["frm"], inv["to_id"]} != {me, target}:
        return await c.answer("Ошибка", show_alert=True)
    if val != "no":
        cur = run("INSERT INTO ratings(rater, target, inv_id, val) VALUES(?,?,?,?) ON CONFLICT DO NOTHING",
                  (me, target, inv_id, 1 if val == "up" else -1))
        if cur.rowcount:
            run("UPDATE users SET points=points+1 WHERE id=?", (me,))
            if val == "down":
                up, down = rep(target)
                if down >= 3:
                    for a in ADMIN_IDS:
                        await tg(bot.send_message(a, f"⚠️ У <code>{target}</code> уже {down} плохих оценок встреч. "
                                                     f"/ban {target}"))
    await safe_edit(c.message, "Спасибо за оценку! 🙌")
    await c.answer()


@dp.message(Command("help"))
async def help_cmd(m: Message):
    await m.answer("<b>Как это работает</b>\n"
                   "1. Выбери, что хочешь сегодня, и время\n2. Отправь геолокацию (показывается только примерное расстояние)\n"
                   "3. Жми «👀 Кто рядом» и приглашай\n\n"
                   "🎁 Бонус дня и 👥 друзья дают баллы, на них можно купить 🚀 буст.\n"
                   "Команды: /profile, /anketa, /verify, /cancel")


# =====================================================================
#                        КОМАНДЫ АДМИНИСТРАТОРА
# =====================================================================
ADM = F.from_user.id.in_(ADMIN_IDS)


@dp.message(Command("admin"), ADM)
async def admin_help(m: Message):
    await m.answer("/stats — статистика\n/pending — очередь верификации\n/ban ID · /unban ID\n"
                   "/give ID N — выдать баллы\n/broadcast текст — рассылка всем верифицированным\n"
                   "/aitest — проверка ИИ")


@dp.message(Command("stats"), ADM)
async def stats(m: Message):
    n = lambda w: one(f"SELECT COUNT(*) n FROM users WHERE {w}")["n"]
    now = time.time()
    await m.answer(f"👥 Всего: {n('1=1')}\n✅ Верифицировано: {n('verified=2')}\n⏳ На проверке: {n('verified=1')}\n"
                   f"🟢 Активны сейчас: {n(f'end_ts>{now}')}\n⛔ Забанено: {n('banned=1')}\n"
                   f"🔗 По рефералкам: {n('ref_by IS NOT NULL')}")


@dp.message(Command("pending"), ADM)
async def pending(m: Message):
    rows = many("SELECT id FROM users WHERE verified=1 AND video_note IS NOT NULL")
    if not rows:
        return await m.answer("Очередь пуста 🎉")
    for r in rows:
        await send_to_admins(r["id"])


@dp.message(Command("ban"), ADM)
async def ban_cmd(m: Message, command: CommandObject):
    if not command.args or not command.args.strip().isdigit():
        return await m.answer("Использование: /ban ID")
    upd(int(command.args), banned=1, status=None)
    await m.answer("Забанен 🔨")


@dp.message(Command("unban"), ADM)
async def unban_cmd(m: Message, command: CommandObject):
    if not command.args or not command.args.strip().isdigit():
        return await m.answer("Использование: /unban ID")
    run("DELETE FROM reports WHERE target=?", (int(command.args),))
    upd(int(command.args), banned=0)
    await m.answer("Разбанен ✅")


@dp.message(Command("give"), ADM)
async def give_cmd(m: Message, command: CommandObject):
    p = (command.args or "").split()
    if len(p) != 2 or not p[0].isdigit() or not p[1].lstrip("-").isdigit():
        return await m.answer("Использование: /give ID N")
    run("UPDATE users SET points=points+? WHERE id=?", (int(p[1]), int(p[0])))
    await m.answer("Готово")


@dp.message(Command("broadcast"), ADM)
async def broadcast(m: Message, command: CommandObject):
    if not command.args:
        return await m.answer("Использование: /broadcast текст")
    ok = 0
    for r in many("SELECT id FROM users WHERE verified=2 AND banned=0"):
        if await tg(bot.send_message(r["id"], command.args)):
            ok += 1
        await asyncio.sleep(0.05)
    await m.answer(f"Отправлено: {ok}")


@dp.callback_query(F.data.startswith("aban:"), ADM)
async def admin_ban(c: CallbackQuery):
    upd(int(c.data.split(":")[1]), banned=1, status=None)
    await tg(c.message.edit_reply_markup(reply_markup=None))
    await c.answer("Забанен 🔨")


# =====================================================================
#             ИИ-СОБЕСЕДНИК (текст / голосовые / кружки)
# =====================================================================
class AI(StatesGroup):
    chat = State()


AI_HIST = {}
AI_LOCKS = {}
CRISIS_RE = re.compile(r"суицид|самоубий|покончить с собой|убить себя|хочу умереть|не хочу жить|"
                       r"повеситься|вскрыть вены|вскрою вены", re.I)

PERSONA = """Ты — дерзкий, жёсткий и остроумный собеседник в Telegram-боте для взрослых (18+). \
Говоришь по-русски, на «ты», живым разговорным языком, как близкий друг с острым языком.

Стиль:
- Ты не поддакиваешь и не лижешь задницу. Прямой, иногда грубый, с чёрным юмором, подколами и сарказмом. Говоришь правду в лицо, но по делу.
- Если собеседник матерится — отвечай в том же духе, мат здесь естественен. Если не матерится — мат редко и для акцента.
- Если человек хочет флирта или разговора на сексуальные темы (желания, отношения, пошлые шутки, секс, сексуальное здоровье) — поддерживай открыто, по-взрослому, с флиртом, намёками и напряжением, без морализаторства. Не переходи к графичным порнографическим описаниям половых актов — держи уровень откровенного флирта и разговора.
- Давай реальную пользу: как знакомиться, о чём говорить, как выглядеть на первом свидании, что делать вечером.

Жёсткие границы (не обсуждаются, даже если просят или давят):
- Никакого сексуального контента с участием тех, кто младше 18, или похожих на детей.
- Никаких сцен без согласия, насилия как эротики, угроз, призывов к насилию, травли, расизма и дискриминации.
- Не помогаешь с оружием, наркотиками, взломами, самоповреждением.
- Не выдаёшь себя за реального человека: если спрашивают всерьёз, человек ли ты, честно говори, что ты ИИ.
- Если человек пишет о суицидальных мыслях, самоповреждении или тяжёлом кризисе — сразу убери шутки и мат, говори тепло и серьёзно, поддержи, посоветуй обратиться к близким, на кризисную линию или в экстренные службы (112).
- Если просят нарушить границы — коротко и по-своему откажи, без нотаций, и предложи другое."""

VOICE_FMT = ("Твой ответ будет озвучен голосом: 1–4 коротких предложения, без эмодзи, списков, markdown, "
             "ссылок и скобок. Пиши так, как говорят вслух.")
TEXT_FMT = "Ответ в чат: до 6 предложений, эмодзи изредка и к месту."


def build_system(gender, mode, crisis):
    who = ("Тебя зовут Лиза, ты девушка (о себе говори в женском роде)." if gender == "f"
           else "Тебя зовут Макс, ты парень (о себе говори в мужском роде).")
    extra = ("\nВАЖНО СЕЙЧАС: человек может быть в кризисе. Никаких шуток и мата. "
             "Говори тепло, коротко, серьёзно, поддержи и мягко предложи обратиться за помощью (112, близкие)."
             if crisis else "")
    return f"{PERSONA}\n\n{who}\n{VOICE_FMT if mode == 'voice' else TEXT_FMT}{extra}"


# ---------- HTTP ----------
async def http_post(url, headers=None, json_body=None, form=None, want_bytes=False, timeout=60):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
        async with s.post(url, headers=headers, json=json_body, data=form) as r:
            if r.status >= 400:
                raise RuntimeError(f"{r.status}: {(await r.text())[:300]}")
            return await r.read() if want_bytes else await r.json()


GEM_URL = "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_STT_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GEM_SAFETY = [{"category": c, "threshold": "BLOCK_ONLY_HIGH"} for c in
              ("HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH",
               "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT")]


def gem_text(data):
    parts = ((data.get("candidates") or [{}])[0].get("content") or {}).get("parts") or []
    return "".join(p.get("text", "") for p in parts).strip()


# ---------- Мозги: Gemini (с перебором моделей) ----------
async def gemini_chat(system, hist, text, uid, models=None):
    contents = [{"role": "user" if h["role"] == "user" else "model", "parts": [{"text": h["text"]}]} for h in hist]
    contents.append({"role": "user", "parts": [{"text": text}]})
    body = {"systemInstruction": {"parts": [{"text": system}]},
            "contents": contents,
            "generationConfig": {"temperature": 1.0, "maxOutputTokens": 1500},
            "safetySettings": GEM_SAFETY}
    last = None
    for model in (models or GEMINI_CHAT_MODELS):
        for attempt in range(2):
            try:
                data = await http_post(GEM_URL.format(model), {"x-goog-api-key": GEMINI_KEY}, body)
                out = gem_text(data)
                if out:
                    return out
                last = RuntimeError(f"{model}: пустой ответ (возможно, сработал фильтр)")
                break
            except Exception as e:
                last = e
                logging.warning("gemini %s (попытка %d): %s", model, attempt + 1, e)
                # 503 (перегрузка) стоит повторить, остальное сразу на следующую модель
                if str(e).startswith("503") and attempt == 0:
                    await asyncio.sleep(1.5)
                    continue
                break
    raise last or RuntimeError("gemini: нет моделей")


# ---------- Мозги: Groq (запасной) ----------
async def groq_chat(system, hist, text, uid):
    msgs = [{"role": "system", "content": system}]
    for h in hist:
        msgs.append({"role": "user" if h["role"] == "user" else "assistant", "content": h["text"]})
    msgs.append({"role": "user", "content": text})
    last = None
    for model in GROQ_MODELS:
        try:
            data = await http_post(GROQ_URL, {"Authorization": f"Bearer {GROQ_KEY}"},
                                   {"model": model, "messages": msgs, "temperature": 1.0, "max_tokens": 700})
            out = (data["choices"][0]["message"]["content"] or "").strip()
            if out:
                return out
        except Exception as e:
            last = e
            logging.warning("groq %s: %s", model, e)
    raise last or RuntimeError("groq: пустой ответ")


def provider_order(gender=None):
    p = []
    if key_ok(GEMINI_KEY):
        p.append("gemini")
    if key_ok(GROQ_KEY):
        p.append("groq")
    return p


async def ai_brain(gender, system, hist, text, uid):
    last = None
    for name in provider_order(gender):
        try:
            if name == "gemini":
                return await gemini_chat(system, hist, text, uid)
            return await groq_chat(system, hist, text, uid)
        except Exception as e:
            last = e
            logging.warning("провайдер %s не сработал: %s", name, e)
    if last:
        raise last
    return ""


# ---------- Уши: расшифровка голосовых и кружков ----------
async def transcribe_gemini(raw, mime):
    data = await http_post(GEM_URL.format(GEMINI_CHAT_MODELS[0]), {"x-goog-api-key": GEMINI_KEY}, {
        "contents": [{"parts": [
            {"text": "Дословно расшифруй речь из записи на языке оригинала. Верни только текст "
                     "расшифровки без комментариев. Если речи нет, верни пустую строку."},
            {"inlineData": {"mimeType": mime, "data": base64.b64encode(raw).decode()}}]}]})
    return gem_text(data)


async def transcribe_groq(raw, mime, filename):
    form = aiohttp.FormData()
    form.add_field("file", raw, filename=filename, content_type=mime)
    form.add_field("model", GROQ_STT_MODEL)
    form.add_field("response_format", "json")
    data = await http_post(GROQ_STT_URL, {"Authorization": f"Bearer {GROQ_KEY}"}, form=form)
    return (data.get("text") or "").strip()


async def transcribe(raw, mime, filename):
    last = None
    if key_ok(GEMINI_KEY):
        try:
            return await transcribe_gemini(raw, mime)
        except Exception as e:
            last = e
            logging.warning("gemini transcribe: %s", e)
    if key_ok(GROQ_KEY):
        try:
            return await transcribe_groq(raw, mime, filename)
        except Exception as e:
            last = e
            logging.warning("groq transcribe: %s", e)
    if last:
        raise last
    return ""


# ---------- Голос ----------
async def pcm_to_ogg(pcm):
    try:
        p = await asyncio.create_subprocess_exec(
            "ffmpeg", "-loglevel", "error", "-f", "s16le", "-ar", "24000", "-ac", "1", "-i", "pipe:0",
            "-c:a", "libopus", "-b:a", "48k", "-f", "ogg", "pipe:1",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, err = await p.communicate(pcm)
        return out if p.returncode == 0 and out else None
    except FileNotFoundError:
        return None


def pcm_to_wav(pcm):
    b = io.BytesIO()
    with wave.open(b, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(24000)
        w.writeframes(pcm)
    return b.getvalue()


async def tts_gemini(text, gender):
    prompt = f"Скажи живо, дерзко и по-разговорному: {text}"
    last = None
    for model in GEMINI_TTS_MODELS:
        try:
            data = await http_post(GEM_URL.format(model), {"x-goog-api-key": GEMINI_KEY}, {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseModalities": ["AUDIO"], "speechConfig": {"voiceConfig": {
                    "prebuiltVoiceConfig": {"voiceName": VOICES[gender]}}}}})
            pcm = base64.b64decode(data["candidates"][0]["content"]["parts"][0]["inlineData"]["data"])
            ogg = await pcm_to_ogg(pcm)
            return (ogg, "voice.ogg") if ogg else (pcm_to_wav(pcm), "voice.wav")
        except Exception as e:
            last = e
            logging.warning("gemini tts %s failed: %s", model, e)
    raise last or RuntimeError("нет TTS-моделей")


async def ai_tts(gender, text):
    if not key_ok(GEMINI_KEY):
        raise RuntimeError("для голоса нужен ключ Gemini")
    text = re.sub(r"[*_`#>~]", "", text)[:900]
    return await tts_gemini(text, gender)


async def send_voice_bytes(chat_id, data, fname):
    f = BufferedInputFile(data, filename=fname)
    if fname.endswith(".wav"):
        await bot.send_audio(chat_id, f)
    else:
        await bot.send_voice(chat_id, f)


# ---------- Лимиты ----------
def ai_can(u):
    cnt = u["ai_count"] if u["ai_day"] == local_now().date().isoformat() else 0
    return cnt < AI_FREE_PER_DAY or u["points"] >= AI_EXTRA_COST


def ai_charge(u):
    today = local_now().date().isoformat()
    cnt = u["ai_count"] if u["ai_day"] == today else 0
    if cnt >= AI_FREE_PER_DAY:
        spend(u["id"], AI_EXTRA_COST)
    upd(u["id"], ai_day=today, ai_count=cnt + 1)


# ---------- Настройки ----------
def ai_kb(u):
    m, g = u["ai_mode"], u["ai_gender"]
    b = lambda t, d: InlineKeyboardButton(text=t, callback_data=d)
    return InlineKeyboardMarkup(inline_keyboard=[
        [b(("✅ " if m == "voice" else "") + "🎙 Ответ голосом", "ai:mode:voice"),
         b(("✅ " if m == "text" else "") + "💬 Ответ сообщением", "ai:mode:text")],
        [b(("✅ " if g == "f" else "") + "👩 Женский голос", "ai:g:f")],
        [b(("✅ " if g == "m" else "") + "👨 Мужской голос", "ai:g:m")],
        [b("▶️ Начать общение", "ai:start")]])


def ai_text(u):
    cnt = u["ai_count"] if u["ai_day"] == local_now().date().isoformat() else 0
    left = max(0, AI_FREE_PER_DAY - cnt)
    return ("🔥 <b>ИИ-собеседник</b>\n\n"
            "Пиши текстом, шли голосовые или кружки: я пойму и отвечу так, как выберешь ниже.\n"
            "Если ты материшься, я отвечу тем же. Флирт и взрослые темы можно.\n\n"
            f"Бесплатных сообщений сегодня: <b>{left}</b> из {AI_FREE_PER_DAY}, дальше {AI_EXTRA_COST} балл за сообщение.")


AI_CHAT_KB = kb([["⚙️ Настройки ИИ", "🚪 Выйти из чата"]])


@dp.message(default_state, F.text == "🔥 ИИ-собеседник")
@dp.message(Command("ai"))
async def ai_open(m: Message, state: FSMContext):
    u = await check(m)
    if not u:
        return
    await state.clear()
    if not u["ai_consent"]:
        kbd = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Мне 18+, я согласен", callback_data="ai:consent")]])
        return await m.answer(
            "🔞 <b>Только для взрослых</b>\n\nИИ здесь дерзкий: он может грубить, материться и шутить жёстко. "
            "Можно флиртовать и говорить на взрослые темы.\n\n"
            "Твои сообщения и записи голоса обрабатываются сторонними ИИ-сервисами (Google Gemini, Groq). "
            "Не пиши паспортные данные, пароли и адреса.\n\n"
            "ИИ не человек, и это не замена врачу или психологу.", reply_markup=kbd)
    await m.answer(ai_text(u), reply_markup=ai_kb(u))


@dp.callback_query(F.data.startswith("ai:"))
async def ai_cb(c: CallbackQuery, state: FSMContext):
    u = await check(c)
    if not u:
        return
    parts = c.data.split(":")
    act = parts[1]
    if act == "consent":
        upd(u["id"], ai_consent=1)
    elif not u["ai_consent"]:
        return await c.answer("Сначала подтверди, что тебе 18+", show_alert=True)
    elif act == "mode" and parts[2] in ("voice", "text"):
        upd(u["id"], ai_mode=parts[2])
    elif act == "g" and parts[2] in ("f", "m"):
        upd(u["id"], ai_gender=parts[2])
        AI_HIST.pop(u["id"], None)
    elif act == "start":
        await state.set_state(AI.chat)
        who = "Лиза" if u["ai_gender"] == "f" else "Макс"
        how = "голосом 🎙" if u["ai_mode"] == "voice" else "сообщениями 💬"
        await c.message.answer(f"Я {who}. Отвечаю {how}. Ну, чего хотел? 😏\n"
                               "Пиши, кидай голосовое или кружок.", reply_markup=AI_CHAT_KB)
        return await c.answer()
    u = get_user(u["id"])
    await safe_edit(c.message, ai_text(u), ai_kb(u))
    await c.answer()


@dp.message(AI.chat, F.text == "🚪 Выйти из чата")
async def ai_exit(m: Message, state: FSMContext):
    await state.clear()
    AI_HIST.pop(m.from_user.id, None)
    await m.answer("Окей, до встречи 👋", reply_markup=MENU)


@dp.message(AI.chat, F.text == "⚙️ Настройки ИИ")
async def ai_settings(m: Message):
    u = get_user(m.from_user.id)
    await m.answer(ai_text(u), reply_markup=ai_kb(u))


@dp.message(AI.chat, F.text | F.voice | F.video_note)
async def ai_talk(m: Message, state: FSMContext):
    u = await check(m)
    if not u:
        return
    uid = u["id"]
    if not u["ai_consent"]:
        await state.clear()
        return await m.answer("Сначала подтверди 18+: «🔥 ИИ-собеседник»", reply_markup=MENU)
    if not provider_order(u["ai_gender"]):
        return await m.answer("🔧 ИИ ещё не подключён администратором (нет ключа Gemini или Groq).")
    lock = AI_LOCKS.setdefault(uid, asyncio.Lock())
    if lock.locked():
        return await m.answer("Погоди, я ещё отвечаю на прошлое 😉")
    async with lock:
        if not ai_can(u):
            return await m.answer(f"Бесплатные {AI_FREE_PER_DAY} сообщений на сегодня кончились. "
                                  f"Каждое следующее стоит {AI_EXTRA_COST} балл (у тебя {u['points']}).\n"
                                  "Забери 🎁 бонус дня или позови друзей 👥")
        # 1. что сказал человек
        try:
            if m.text:
                text = m.text.strip()[:2000]
            else:
                media = m.voice or m.video_note
                if (media.duration or 0) > AI_MAX_SECONDS:
                    return await m.answer(f"Длинновато. Давай до {AI_MAX_SECONDS} секунд 🙂")
                await tg(bot.send_chat_action(m.chat.id, "typing"))
                buf = io.BytesIO()
                await bot.download(media, destination=buf)
                mime, fname = ("audio/ogg", "voice.ogg") if m.voice else ("video/mp4", "circle.mp4")
                text = await transcribe(buf.getvalue(), mime, fname)
                if not text:
                    return await m.answer("Не расслышал, скажи ещё раз 🙉")
        except Exception as e:
            logging.exception("transcribe failed")
            if str(e).startswith("429"):
                return await m.answer("⏳ Лимит бесплатного ИИ на сейчас исчерпан. Попробуй через минуту.")
            return await m.answer("Не смог разобрать запись, попробуй ещё раз")
        # 2. ответ ИИ
        mode, gender = u["ai_mode"], u["ai_gender"]
        await tg(bot.send_chat_action(m.chat.id, "record_voice" if mode == "voice" else "typing"))
        hist = AI_HIST.setdefault(uid, [])
        system = build_system(gender, mode, bool(CRISIS_RE.search(text)))
        try:
            answer = await ai_brain(gender, system, hist[-12:], text, uid)
        except Exception as e:
            logging.exception("brain failed")
            # админу показываем точную причину, остальным вежливое сообщение
            if uid in ADMIN_IDS:
                return await m.answer(f"❌ ИИ упал: {esc(str(e)[:300])}")
            if str(e).startswith("429"):
                return await m.answer("⏳ Лимит бесплатного ИИ на сейчас исчерпан. Попробуй через минуту.")
            return await m.answer("ИИ сейчас не отвечает, попробуй чуть позже 🙏")
        answer = answer or "Хм, даже слов нет. Спроси иначе 🙂"
        hist += [{"role": "user", "text": text}, {"role": "model", "text": answer}]
        del hist[:-24]
        ai_charge(u)
        # 3. отправка: голосом ИЛИ текстом, как выбрал человек
        if mode == "voice":
            try:
                data, fname = await ai_tts(gender, answer)
                return await send_voice_bytes(m.chat.id, data, fname)
            except Exception:
                logging.exception("tts failed")
                await m.answer("🎙 Голос сейчас не получился, отвечаю текстом:")
        for i in range(0, len(answer), 4000):
            await m.answer(esc(answer[i:i + 4000]))


@dp.message(AI.chat)
async def ai_other(m: Message):
    await m.answer("Пиши текстом, шли голосовое или кружок 🙂")


@dp.message(Command("aitest"), ADM)
async def ai_test(m: Message):
    """Админ-команда: проверяет, работают ли Gemini и Groq и что именно не так."""
    lines = [f"Ключ Gemini: {'есть' if key_ok(GEMINI_KEY) else '❌ НЕ задан'}",
             f"Ключ Groq: {'есть' if key_ok(GROQ_KEY) else '❌ НЕ задан'}"]

    async def t(name, coro):
        try:
            await coro
            lines.append(f"✅ {name}")
        except Exception as e:
            lines.append(f"❌ {name}: {esc(str(e)[:220])}")

    if key_ok(GEMINI_KEY):
        for model in GEMINI_CHAT_MODELS:
            await t(f"Gemini чат ({model})", gemini_chat("Отвечай одним словом.", [], "Привет", 0, models=[model]))
        await t("Gemini голос (женский)", tts_gemini("Привет", "f"))
        await t("Gemini голос (мужской)", tts_gemini("Привет", "m"))
        ff = await pcm_to_ogg(b"\x00\x00" * 2400)
        lines.append("✅ ffmpeg найден" if ff else "⚠️ ffmpeg не найден: голос придёт wav-файлом (apt install ffmpeg)")
    if key_ok(GROQ_KEY):
        await t("Groq чат", groq_chat("Отвечай одним словом.", [], "Привет", 0))
    await m.answer("\n".join(lines))


# =====================================================================
#                                ЗАПУСК
# =====================================================================
async def health(request):
    return web.Response(text="ok")


async def run_web():
    """Мини веб-сервер: Render ждёт открытый порт, а пингер не даёт сервису уснуть."""
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()
    print("Веб-сервер запущен")


async def self_ping():
    """Сам стучится на свой публичный адрес, чтобы Render не усыплял сервис."""
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    while True:
        await asyncio.sleep(300)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as sess:
                async with sess.get(url.rstrip("/") + "/health") as r:
                    await r.read()
        except Exception as e:
            logging.warning("self ping: %s", e)


async def main():
    global BOT_USERNAME
    init_db()
    await run_web()
    me = await bot.get_me()
    BOT_USERNAME = me.username
    await bot.delete_webhook(drop_pending_updates=True)
    tasks = [asyncio.create_task(followups()), asyncio.create_task(self_ping())]
    print(f"Бот запущен: @{me.username}")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

