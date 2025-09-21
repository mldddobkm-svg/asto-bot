# -*- coding: utf-8 -*-
# bot.py — Telegram бот для предсказаний и Таро (полная версия, без заглушек)
# Фичи:
# - Адаптивный HTTPXRequest (PTB совместимость)
# - Чистый чат: удаляем старые UI-сообщения и временные фото
# - Таро-персоны: Учёный, Мистик, Популярная, Молодая (био + фото)
# - Фото сверху в «истории таролога», скрывается при возврате/выборе
# - Главное меню 2x2 (Предсказания, Таро / Настройки, Помощь)
# - Таро: 🎴 Вытянуть карту (со счётчиком), 👤 Профиль, 🧙 Таролог, ✨ Как это работает, 🛒 Купить, 👥 Рефералка
# - Предсказания из файлов predictions_db/<Знак>/<Категория>_{short|medium|long}.txt
# - Подсветка текущего таролога (✅) и блокировка кнопки «Выбрать», если он уже выбран
# - /tgtest для проверки Telegram API
#
# Требования: python-telegram-bot >= 20, aiosqlite, python3.10+
# В .env нужен BOT_TOKEN; опционально TELEGRAM_BASE_URL, HTTPS_PROXY/HTTP_PROXY

import os, asyncio, logging, datetime, inspect, random, json, re, unicodedata
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import aiosqlite
from dotenv import load_dotenv
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)
from telegram.error import BadRequest

# ----------------- БАЗОВАЯ НАСТРОЙКА -----------------

APP_DIR = Path(__file__).parent
load_dotenv(APP_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("❌ Нет BOT_TOKEN в .env")

TELEGRAM_BASE_URL = os.getenv("TELEGRAM_BASE_URL", "").strip()
HTTP_PROXY = os.getenv("HTTPS_PROXY", os.getenv("HTTP_PROXY", "")).strip()

DB_PATH = APP_DIR / "astro.db"
TZ = ZoneInfo("Europe/Moscow")


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ----------------- АДМИНЫ -----------------
MAIN_ADMIN_ID = int(os.getenv("MAIN_ADMIN_ID", "0") or 0)
_ADMIN_IDS_ENV = os.getenv("ADMIN_IDS", "").strip()
ADMINS = set()
if MAIN_ADMIN_ID:
    ADMINS.add(MAIN_ADMIN_ID)
for part in _ADMIN_IDS_ENV.replace(";", ",").split(","):
    part = part.strip()
    if part.isdigit():
        ADMINS.add(int(part))

def is_admin(user_id: int) -> bool:
    return user_id in ADMINS

# ----------------- КОНСТАНТЫ И ДАННЫЕ -----------------

BTN_CATPRED = "🗂 Предсказания по категориям"
BTN_TAROT   = "🔮 Таро"
BTN_PROFILE = "⚙️ Настройки"
BTN_HELP    = "❓ Помощь"

CATEGORY_LIST = [
    ("Любовь", "❤️"), ("Работа", "💼"), ("Финансы", "💰"),
    ("Здоровье", "🩺"), ("Настроение", "😊"), ("Совет", "🧭"), ("Учёба", "📚"),
]

TAROT_TAROLOGS = [
    ("scientist", "👨‍🔬 Учёный"),
    ("mystic",    "🌙 Мистик"),
    ("popular",   "🌟 Популярная"),
    ("young",     "✨ Молодая"),
]

TAROLOG_BIO = {
    "scientist": "Его зовут Виктор Лавринов 📚✨\n\nС юности он увлекался историей религий и символов 📖. Работая в университете 🏛️, однажды нашёл трактат XVII века 📜, где Таро описывалось как инструмент познания психики 🃏🧠.\n\nС тех пор Виктор собирал редкие колоды, искал закономерности и писал статьи ✍️. Одни называли его шарлатаном 🤷, другие — новатором 💡.\n\nТеперь он живёт между двумя мирами 🌌: строгой наукой 🔬 и мистикой 🔮, умея видеть скрытые связи 👁️.",
    "mystic":    "Её зовут Зора Велесса 🌙✨\n\n🌺 Родилась в семье с цыганскими корнями, где женщины передавали дар читать судьбу 🔮.\n🔥 В детстве слушала сказки у костра и видела, как бабушка раскладывает карты 🃏.\n\n🕯️ После её ухода Зора унаследовала старую колоду 🌌. В ту же ночь во сне предки велели продолжить путь 👁.\n\n✨ С тех пор Зора Велесса стала известна как женщина, для которой Таро — живая связь с духами рода 👻 и свет в сердцах.",
    "popular":   "Её зовут Алина Морэ ✨\n\nВыросла в обычной семье 👩‍👩‍👧, но в юности увлеклась эзотерикой 📚🔮. Первую колоду заказала в интернете, и вскоре заметила — карты будто «говорят» сами 🃏✨.\n\nРешила объединить мистику и соцсети 📱. Первый стрим в TikTok стал вирусным 🚀: тысячи зрителей, сотни вопросов в чате и море лайков ❤️.\n\nСегодня её знают как девушку, которая перенесла Таро в мир стримов 🌌. Для одних она гадалка, для других — подруга, а для самой Алины это искусство видеть глубже 👁️.",
    "young":     "Её зовут Селеста Дария 🔮\n\nВ архиве она нашла старую карту Таро 📜 — «Башня». Ночью приснился сон, где предки велели искать остальные.\n\nСобирая колоду по частям 🌌, Селеста открывала в себе дар 👁️. Когда карты объединились, они стали её голосом и зеркалом чужих судеб 🕯️.\n\nТеперь её знают как хранительницу «живой колоды» — учёную и пророчицу одновременно ⚡.",
}
TAROLOG_DESC = {
    "scientist": "👨‍🔬 <b>Учёный</b> — логичный, аккуратный, говорит по фактам.",
    "mystic":    "🌙 <b>Мистик</b> — интуитивный, поэтичный, мягкий.",
    "popular":   "🌟 <b>Популярная</b> — тренды, лайфстайл, уверенный тон.",
    "young":     "✨ <b>Молодая</b> — свежий взгляд, поддержка и мотивация.",
}
TAROT_DAILY_FREE = 1
TAROT_IMAGES_DIR = os.getenv("TAROT_IMAGES_DIR", "tarot_images")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp")

TAGLINES = [
    "Одна карта — и всё встаёт на свои места ✨",
    "Спроси путь у карт — интуиция подскажет 🌙",
    "Сегодняшний знак? Пусть Таро дадут ориентир 🧭",
    "Когда нужен намёк судьбы — карты рядом 🔮",
]

# ---- Tarot deck (file-driven) & spreads ----
TAROT_DECK_JSON = os.getenv("TAROT_DECK_JSON", "tarot_deck.json")  # ожидается список объектов: {code,upright,reversed,tags}
TAROT_OVERLAYS_DIR = os.getenv("TAROT_OVERLAYS_DIR", "tarot_overlays")  # файлы <Знак>.json: { "tag": "overlay text", ... }
TAROT_USE_REVERSED = True  # включаем перевёрнутые значения
TAROT_NO_REPEAT_DAYS = 30  # не повторять карты в течение N дней
TAROT_REVERSED_PROB = 0.5  # вероятность перевёрнутого положения

# Простейшие расклады: 1 и 3 карты
TAROT_SPREADS = {
    "one":   {"title": "Одна карта дня", "positions": ["Суть"]},
    "three": {"title": "Три карты — Прошлое/Настоящее/Будущее", "positions": ["Прошлое", "Настоящее", "Будущее"]},
}

# Фоллбек-колода (22 Старших Аркана) — с upright/reversed, если tarot_deck.json не найден
TAROT_DECK_FALLBACK = [
    {"code": "0 The Fool",         "upright": "Новые начала, доверие пути.",               "reversed": "Опасение шага, наивность без опоры.",           "tags": ["новое","шаг","доверие"]},
    {"code": "I The Magician",     "upright": "Ресурсы и фокус в твоих руках.",            "reversed": "Распыление сил, сомнение в себе.",              "tags": ["ресурсы","фокус","воля"]},
    {"code": "II The High Priestess","upright":"Тихая интуиция подсказывает ответ.",        "reversed": "Шум мешает услышать себя.",                     "tags": ["интуиция","тишина"]},
    {"code": "III The Empress",    "upright": "Рост, забота и изобилие.",                  "reversed": "Застаивание, переизбыток забот без границ.",    "tags": ["рост","забота","изобилие"]},
    {"code": "IV The Emperor",     "upright": "Структура и ответственность.",              "reversed": "Жёсткость мешает гибкости.",                    "tags": ["структура","контроль"]},
    {"code": "V The Hierophant",   "upright": "Традиции и обучение.",                      "reversed": "Слепое следование правилам, застой.",          "tags": ["традиции","обучение"]},
    {"code": "VI The Lovers",      "upright": "Выбор сердцем и согласие.",                 "reversed": "Колебания, расхождение ценностей.",             "tags": ["выбор","отношения"]},
    {"code": "VII The Chariot",    "upright": "Воля и движение вперёд.",                   "reversed": "Потеря курса, пробуксовка.",                    "tags": ["движение","цель"]},
    {"code": "VIII Strength",      "upright": "Тихая сила и выдержка.",                    "reversed": "Усталость, сомнение в стойкости.",              "tags": ["сила","терпение"]},
    {"code": "IX The Hermit",      "upright": "Внутренний поиск и понимание.",             "reversed": "Изоляция без смысла.",                          "tags": ["поиск","мудрость"]},
    {"code": "X Wheel of Fortune", "upright": "Поворот колеса, шанс.",                     "reversed": "Цикл задержек, упущенные возможности.",         "tags": ["шанс","цикл"]},
    {"code": "XI Justice",         "upright": "Равновесие и честность.",                   "reversed": "Неясность критериев, перекос.",                 "tags": ["справедливость","баланс"]},
    {"code": "XII The Hanged Man", "upright": "Пауза, другой взгляд.",                     "reversed": "Застревание, сопротивление переменам.",         "tags": ["пауза","перспектива"]},
    {"code": "XIII Death",         "upright": "Завершение и перерождение.",                "reversed": "Страх изменений, цепляние за прошлое.",         "tags": ["перемены","перерождение"]},
    {"code": "XIV Temperance",     "upright": "Баланс и гармония.",                        "reversed": "Перекосы, нужно выровнять темп.",               "tags": ["гармония","баланс"]},
    {"code": "XV The Devil",       "upright": "Привязки и искушения — осознай.",           "reversed": "Освобождение от излишних уз.",                  "tags": ["привязки","искушение"]},
    {"code": "XVI The Tower",      "upright": "Встряска к освобождению.",                  "reversed": "Отложенный кризис, затянутая перестройка.",     "tags": ["встряска","освобождение"]},
    {"code": "XVII The Star",      "upright": "Надежда и ясный ориентир.",                 "reversed": "Сомнение в свете, требуется вера.",             "tags": ["надежда","путь"]},
    {"code": "XVIII The Moon",     "upright": "Неясность — слушай ощущения.",              "reversed": "Прояснение страхов, выход из тумана.",          "tags": ["туман","ощущения"]},
    {"code": "XIX The Sun",        "upright": "Радость, ясность, успех.",                  "reversed": "Временная тень, дозируй силы.",                  "tags": ["радость","успех","ясность"]},
    {"code": "XX Judgement",       "upright": "Прозрение и решение.",                      "reversed": "Откладывание ответа, сомнения.",                "tags": ["решение","прозрение"]},
    {"code": "XXI The World",      "upright": "Завершение цикла, целостность.",            "reversed": "Незавершённость, нужен финальный шаг.",         "tags": ["целостность","завершение"]},
]

# ---- Predictions files config ----
PREDICTIONS_DB_DIR = os.getenv("PREDICTIONS_DB_DIR", "predictions_db")
ZODIACS = [
    "Овен","Телец","Близнецы","Рак","Лев","Дева",
    "Весы","Скорпион","Стрелец","Козерог","Водолей","Рыбы"
]
ZODIAC_SYMBOL = {"Овен":"♈","Телец":"♉","Близнецы":"♊","Рак":"♋","Лев":"♌","Дева":"♍","Весы":"♎","Скорпион":"♏","Стрелец":"♐","Козерог":"♑","Водолей":"♒","Рыбы":"♓"}

# ----------------- ВСПОМОГАТЕЛЬНОЕ -----------------

def _norm_tarolog(code: Optional[str]) -> Optional[str]:
    if not code: return None
    return code.strip().lower()

def _label_for_tarolog(code: Optional[str]) -> str:
    code = _norm_tarolog(code)
    for c, lbl in TAROT_TAROLOGS:
        if c == code:
            return lbl
    return "выбрать…"

def _tarot_img_path(code: str) -> Optional[Path]:
    base = Path(TAROT_IMAGES_DIR)
    if not base.exists():
        return None
    for ext in IMAGE_EXTS:
        p = base / f"{code}{ext}"
        if p.exists():
            return p
    for f in base.glob("*"):
        if f.is_file() and f.suffix.lower() in IMAGE_EXTS and f.stem.lower().startswith(code):
            return f
    return None

# --- Zodiac images (helper) ---
ZODIAC_IMAGES_DIR = os.getenv("ZODIAC_IMAGES_DIR", "zodiac_images")
def _zodiac_img_path(zodiac: str) -> Optional[Path]:
    base = Path(ZODIAC_IMAGES_DIR)
    if not base.is_absolute():
        base = APP_DIR / base
    if not base.exists():
        return None
    cand = []
    # русское имя
    if zodiac:
        cand.append(str(zodiac))
    # английский алиас (если есть)
    en = _EN_ALIAS.get(zodiac)
    if en:
        cand.append(en)
    # нормализованные варианты
    for c in list(cand):
        cand.append(c.lower())
        cand.append(c.capitalize())
    # прямые совпадения <name>.<ext> или файлы, начинающиеся с имени
    for name in cand:
        for ext in IMAGE_EXTS:
            p = base / f"{name}{ext}"
            if p.exists():
                return p
        for f in base.glob("*"):
            if f.is_file() and f.suffix.lower() in IMAGE_EXTS and f.stem.lower().startswith(str(name).lower()):
                return f
    return None

def today_str() -> str:
    return datetime.datetime.now(tz=TZ).strftime("%Y-%m-%d")

# ---------- Predictions filesystem helpers ----------
_EN_ALIAS = {
    "Овен":"aries","Телец":"taurus","Близнецы":"gemini","Рак":"cancer","Лев":"leo","Дева":"virgo",
    "Весы":"libra","Скорпион":"scorpio","Стрелец":"sagittarius","Козерог":"capricorn","Водолей":"aquarius","Рыбы":"pisces"
}

def _norm_name(s: str) -> str:
    return (s or "").strip().lower().replace("ё","е").replace(" ", "_")

def _pred_dirs() -> list[Path]:
    base = Path(PREDICTIONS_DB_DIR)
    return [base if base.is_absolute() else (APP_DIR / base)]

def _depth_suffix(depth: str) -> str:
    return "" if depth == "long" else ("_short" if depth == "short" else "_medium")

def _canon_category(name: str) -> str:
    n = (name or "").strip()
    n = n.replace("Ё","Е").replace("ё","е")
    if n.lower()=="учеба":
        return "Учёба"
    return n


# --- Robust filesystem resolvers for zodiac/category ---

def _norm_fs(s: str) -> str:
    # normalize for filesystem matching:
    # - Unicode normalize to NFC
    # - lower-case
    # - fold 'ё' -> 'е'
    # - remove spaces, underscores, hyphens (any type), dots and common punctuation
    # - drop combining marks
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s)
    # remove combining marks
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower().replace("ё", "е")
    # unify dashes
    s = s.replace("‐", "-").replace("‑", "-").replace("‒", "-").replace("–", "-").replace("—", "-").replace("―", "-")
    # strip separators and punctuation
    s = re.sub(r"[\\s\\-_.()\\[\\]{}]+", "", s)
    return s

def resolve_zodiac_dir(base: Path, zodiac: str) -> Path | None:
    """Try to find the directory for zodiac under base, tolerant to case/yo/underscores and RU/EN names."""
    zru = _norm_name(zodiac)                  # e.g., "овен"
    zen = _EN_ALIAS.get(zodiac, "")         # e.g., "aries"
    candidates = [zru, zru.replace("_", ""), zen, zen.replace("_", ""), zodiac, zodiac.title()]
    # 1) direct hits
    for cand in candidates:
        if not cand:
            continue
        p = base / cand
        if p.exists() and p.is_dir():
            return p
    # 2) fuzzy scan
    want = { _norm_fs(c) for c in candidates if c }
    try:
        for child in base.iterdir():
            if child.is_dir():
                if _norm_fs(child.name) in want:
                    return child
    except Exception:
        pass
    return None

def resolve_category_files(dir_path: Path, category: str, depth: str) -> list[Path]:
    """Return matching files inside dir_path for category and depth, tolerant to names/yo/case.
    Also supports long files saved as <Категория>_long.txt in addition to <Категория>.txt
    """
    suf = _depth_suffix(depth)               # "", "_short", "_medium"
    canon = _canon_category(category)

    stems: list[str] = [f"{canon}{suf}"]
    # Для long допускаем альтернативу <Категория>_long.txt
    if depth == "long":
        stems.append(f"{canon}_long")

    # ё/е варианты
    if "ё" in canon.lower():
        base_e = canon.replace('ё','е').replace('Ё','Е')
        stems.append(f"{base_e}{suf}")
        if depth == "long":
            stems.append(f"{base_e}_long")

    targets = { _norm_fs(st) for st in stems }
    out: list[Path] = []
    try:
        norm_targets = list(targets)
        for f in dir_path.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() != ".txt":
                continue
            name_wo_ext = f.stem
            n = _norm_fs(name_wo_ext)
            # allow exact match OR prefix match to tolerate suffixes like " (1)", "-v2", dates, etc.
            if any(n == t or n.startswith(t) for t in norm_targets):
                out.append(f)
        # deterministic order: exact canon without _long first, then others
        def _prio(p: Path) -> int:
            n = _norm_fs(p.stem)
            main = _norm_fs(f"{canon}{suf}")
            return 0 if (n == main or n.startswith(main)) else 1
        out.sort(key=_prio)
    except Exception:
        pass
    return out

def find_prediction_files(zodiac: str, category: str, depth: str) -> list[Path]:
    files: list[Path] = []
    for base in _pred_dirs():
        # 1) zodiac-specific dir
        zdir = resolve_zodiac_dir(base, zodiac)
        if zdir:
            files.extend(resolve_category_files(zdir, category, depth))
        # 2) _common fallback
        cdir = base / "_common"
        if cdir.exists():
            files.extend(resolve_category_files(cdir, category, depth))
    # de-duplicate while preserving order
    uniq, seen = [], set()
    for p in files:
        if p not in seen:
            seen.add(p); uniq.append(p)
    return uniq

def load_predictions(zodiac: str, category: str, depth: str) -> list[str]:
    for p in find_prediction_files(zodiac, category, depth):
        try:
            if p.exists():
                raw = p.read_text(encoding="utf-8")
                lines = [ln.strip() for ln in raw.replace("\r\n","\n").splitlines() if ln.strip()]
                if len(lines) <= 2 and "||" in raw:
                    lines = [x.strip() for x in raw.split("||") if x.strip()]
                if lines:
                    return lines
        except Exception:
            continue
    return []

def pick_prediction(zodiac: str, category: str, depth: str) -> str:
    pool = load_predictions(zodiac, category, depth)
    if not pool:
        return "Пока нет текста для этой категории. Попробуй другую или зайди позже."
    idx = abs(hash((today_str(), zodiac, category, depth))) % len(pool)
    return pool[idx]

# ---- Tarot loaders & helpers (78 карт, перевёрнутые, оверлеи) ----
def load_tarot_deck() -> list[dict]:
    """Загружает колоду из tarot_deck.json, иначе возвращает TAROT_DECK_FALLBACK."""
    try:
        deck_path = APP_DIR / TAROT_DECK_JSON
        if deck_path.exists():
            data = json.loads(deck_path.read_text(encoding="utf-8"))
            # ожидается список объектов с полями code,upright,reversed,tags
            norm = []
            for item in data:
                code = str(item.get("code","")).strip()
                up = str(item.get("upright","")).strip()
                rv = str(item.get("reversed","")).strip()
                tags = item.get("tags") or []
                if code and up:
                    norm.append({"code": code, "upright": up, "reversed": rv, "tags": list(tags)})
            if norm:
                return norm
    except Exception as e:
        logging.warning("Tarot deck load failed: %s", e)
    return TAROT_DECK_FALLBACK[:]

def load_zodiac_overlay(zodiac: str) -> dict:
    """Читает оверлей по знаку: словарь {tag: overlay_text}."""
    try:
        p = APP_DIR / TAROT_OVERLAYS_DIR / f"{zodiac}.json"
        if p.exists():
            raw = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return {str(k): str(v) for k,v in raw.items()}
    except Exception as e:
        logging.warning("Overlay load failed for %s: %s", zodiac, e)
    return {}

def apply_overlays(base_text: str, tags: list[str], overlay_map: dict) -> str:
    """Добавляет к значению карты уточнения по тегам для выбранного знака."""
    extra_lines = []
    for t in tags or []:
        add = overlay_map.get(t)
        if add:
            extra_lines.append(add.strip())
    if extra_lines:
        return base_text + "\n<i>" + " ".join(extra_lines) + "</i>"
    return base_text

def deck_rng_for(user_id: int, zodiac: str, spread_key: str) -> random.Random:
    seed = _seed_for_spread(user_id, zodiac, spread_key)
    return random.Random(seed)

def decide_orientation(rng: random.Random) -> bool:
    """True → перевёрнутая карта."""
    if not TAROT_USE_REVERSED:
        return False
    return rng.random() < TAROT_REVERSED_PROB

# ---- Tarot helpers ----
async def get_user_zodiac(user_id: int) -> Optional[str]:
    async with db_connect() as db:
        cur = await db.execute("SELECT zodiac FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row and row[0] else None

async def recent_cards_set(user_id: int, days: int) -> set[str]:
    # собрать уникальные коды карт за последние N дней (учитываем, что card_code может быть списком через запятую)
    since = datetime.datetime.now(tz=TZ) - datetime.timedelta(days=days)
    since_ts = int(since.timestamp())
    seen: set[str] = set()
    async with db_connect() as db:
        cur = await db.execute("SELECT card_code FROM tarot_draws WHERE user_id=? AND ts>=?", (user_id, since_ts))
        async for row in cur:
            val = row[0] or ""
            for part in str(val).split(","):
                p = part.strip()
                if p.endswith("(R)"):
                    p = p[:-3].strip()
                if p:
                    seen.add(p)
    return seen

def _seed_for_spread(user_id: int, zodiac: str, spread_key: str) -> int:
    # детерминированное семя для конкретного дня + пользователь + знак + тип расклада
    base = f"{today_str()}|{user_id}|{zodiac}|{spread_key}"
    return abs(hash(base)) % (2**31)

async def draw_unique_cards_for_spread(user_id: int, zodiac: str, spread_key: str) -> list[dict]:
    """Возвращает список карточек-объектов {code,upright,reversed,tags} без повторов за TAROT_NO_REPEAT_DAYS."""
    spread = TAROT_SPREADS[spread_key]
    need = len(spread["positions"])
    rng = deck_rng_for(user_id, zodiac, spread_key)
    deck = load_tarot_deck()
    rng.shuffle(deck)
    # фильтруем от недавних
    recent = await recent_cards_set(user_id, TAROT_NO_REPEAT_DAYS)
    fresh = [c for c in deck if c["code"] not in recent]
    chosen: list[dict] = []
    pool = fresh if len(fresh) >= need else deck
    for card in pool:
        if card in chosen:
            continue
        chosen.append(card)
        if len(chosen) == need:
            break
    return chosen


# --- Notifications (user-chosen time) ---
DEFAULT_NOTIFY_TIME = "09:00"  # Moscow time by default

async def get_notify_time(user_id: int) -> str:
    """Returns HH:MM (MSK). Enforces 07:00–12:00 with :00 minutes; else returns default 09:00."""
    async with db_connect() as db:
        cur = await db.execute("SELECT COALESCE(notify_time, '') FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        val = (row[0] or "").strip() if row else ""
    if not re.match(r"^\d{2}:\d{2}$", val or ""):
        return DEFAULT_NOTIFY_TIME
    hh, mm = val.split(":")
    try:
        h = int(hh); m = int(mm)
    except Exception:
        return DEFAULT_NOTIFY_TIME
    if m != 0 or h < 7 or h > 12:
        return DEFAULT_NOTIFY_TIME
    return f"{h:02d}:{m:02d}"

async def build_consent_text(user_id: int) -> str:
    nt = await get_notify_time(user_id)
    return (
        "Чтобы пользоваться ботом, подпишись на бесплатные ежедневные сообщения "
        f"(в {nt} МСК). Время можно изменить в ⚙️ Настройках."
    )


def load_daily_prediction(zodiac: Optional[str] = None) -> str:
    """Читает строку для дня из:
    1) файла APP_DIR / "predictions" (без расширения) ИЛИ APP_DIR / "predictions.txt"
    2) либо из каталога APP_DIR / "predictions/" с файлами:
       - <Знак>.txt или <Знак>
       - common.txt / all.txt / default.txt / common / all / default
    Возвращает одну строку на сегодня (циклически по количеству строк).
    Разделители строк: переносы или "||".
    """
    base_file = APP_DIR / "predictions"
    base_txt  = APP_DIR / "predictions.txt"
    base_dir  = APP_DIR / "predictions"

    def _read_lines(p: Path) -> list[str]:
        try:
            raw = p.read_text(encoding="utf-8").strip()
        except Exception:
            return []
        if not raw:
            return []
        # поддержка как переносов строк, так и "||"
        lines = [ln.strip() for ln in raw.replace("\r\n", "\n").splitlines() if ln.strip()]
        if len(lines) <= 2 and "||" in raw:
            lines = [x.strip() for x in raw.split("||") if x.strip()]
        return lines

    # 1) Обычный файл без расширения
    if base_file.exists() and base_file.is_file():
        lines = _read_lines(base_file)
        if lines:
            idx = abs(hash(today_str())) % len(lines)
            return lines[idx]

    # 2) Файл predictions.txt
    if base_txt.exists() and base_txt.is_file():
        lines = _read_lines(base_txt)
        if lines:
            idx = abs(hash(today_str())) % len(lines)
            return lines[idx]

    # 3) Директория predictions/
    if base_dir.exists() and base_dir.is_dir():
        cand: list[Path] = []
        if zodiac:
            # приоритет — точное имя знака, далее варианты без регистра/с расширением
            cand.extend([
                base_dir / f"{zodiac}.txt",
                base_dir / f"{zodiac}",
            ])
        # общие файлы по умолчанию
        for name in ("common", "all", "default"):
            cand.extend([base_dir / f"{name}.txt", base_dir / name])
        for p in cand:
            if p.exists() and p.is_file():
                lines = _read_lines(p)
                if lines:
                    idx = abs(hash(today_str() + (zodiac or ""))) % len(lines)
                    return lines[idx]

    return "✨ Сегодня нет текста предсказания. Проверьте: файл <code>predictions</code> или каталог <code>predictions/</code> рядом с bot.py."

async def _morning_digest_text(zodiac: str) -> str:
    today = today_str()
    zemo = ZODIAC_SYMBOL.get(zodiac, "✨")
    pred = load_daily_prediction(zodiac)
    return (
        f"🌅 <b>Доброе утро</b> · {zemo} <b>{zodiac}</b>\n"
        f"<i>{today}</i>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pred}"
    )

async def send_morning_digest(context: ContextTypes.DEFAULT_TYPE, user_id: int, chat_id: int, force: bool = False):
    async with db_connect() as db:
        cur = await db.execute("SELECT COALESCE(consent,0), COALESCE(zodiac,'') FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
    consent = int(row[0] or 0) if row else 0
    zodiac = (row[1] or '').strip() if row else ''
    if not force:
        if consent != 1 or not zodiac:
            return
    else:
        # Для теста админом — не требуем consent, подставим дефолтный знак
        if not zodiac:
            zodiac = 'Овен'

    # try to attach zodiac image on top if available
    zimg = None
    try:
        zimg = _zodiac_img_path(zodiac)
    except Exception:
        zimg = None

    if zimg:
        try:
            with open(zimg, "rb") as f:
                await context.bot.send_photo(chat_id=chat_id, photo=f)
        except Exception:
            pass

    text = await _morning_digest_text(zodiac)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Меню", callback_data="ui:menu")]])
    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML, reply_markup=kb)
# ----------------- DB -----------------

def db_connect():
    # Return coroutine from aiosqlite.connect; `async with db_connect() as db` will await it once.
    return aiosqlite.connect(DB_PATH.as_posix())

async def init_db():
    async with db_connect() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            chat_id INTEGER NOT NULL,
            zodiac TEXT,
            consent INTEGER DEFAULT 0,
            age INTEGER,
            gender TEXT
        )""")
        # Ensure notify_time column exists
        try:
            cur = await db.execute("PRAGMA table_info(users)")
            cols = [r[1] for r in await cur.fetchall()]
            if "notify_time" not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN notify_time TEXT")
        except Exception as e:
            logging.warning("Could not ensure notify_time column: %s", e)
        await db.execute("""CREATE TABLE IF NOT EXISTS tarot_users(
            user_id INTEGER PRIMARY KEY,
            cards_balance INTEGER DEFAULT 0,
            tarolog TEXT,
            free_last_date TEXT,
            free_used INTEGER DEFAULT 0
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS tarot_referrals(
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL UNIQUE,
            ts INTEGER NOT NULL
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS tarot_draws(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            date TEXT NOT NULL,
            card_code TEXT,
            tarolog TEXT,
            is_free INTEGER NOT NULL DEFAULT 0
        )""")
        await db.commit()

async def ensure_user_row(user_id: int, chat_id: int):
    async with db_connect() as db:
        await db.execute("INSERT OR IGNORE INTO users(user_id, chat_id) VALUES(?,?)", (user_id, chat_id))
        await db.commit()

async def tarot_get_user(user_id: int):
    async with db_connect() as db:
        cur = await db.execute("SELECT COALESCE(cards_balance,0), COALESCE(tarolog,''), COALESCE(free_last_date,''), COALESCE(free_used,0) FROM tarot_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if row:
            bal, tar, last, used = row
            return bal, (_norm_tarolog(tar) or ''), last, used
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id) VALUES(?)", (user_id,))
        await db.commit()
        return 0, '', '', 0

async def tarot_set_tarolog(user_id: int, code: str):
    async with db_connect() as db:
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id) VALUES(?)", (user_id,))
        await db.execute("UPDATE tarot_users SET tarolog=? WHERE user_id=?", (_norm_tarolog(code), user_id))
        await db.commit()

async def tarot_try_use_free(user_id: int) -> bool:
    today = today_str()
    async with db_connect() as db:
        cur = await db.execute("SELECT COALESCE(free_last_date,''), COALESCE(free_used,0) FROM tarot_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            await db.execute("INSERT OR IGNORE INTO tarot_users(user_id, free_last_date, free_used) VALUES(?,?,0)", (user_id, today))
            await db.commit()
            return True
        last, used = row
        if last != today:
            await db.execute("UPDATE tarot_users SET free_last_date=?, free_used=0 WHERE user_id=?", (today, user_id))
            await db.commit()
            used = 0
        if used < TAROT_DAILY_FREE:
            await db.execute("UPDATE tarot_users SET free_used = free_used + 1 WHERE user_id=?", (user_id,))
            await db.commit()
            return True
        return False

async def tarot_log_draw(user_id: int, card_code: str, tarolog: Optional[str], is_free: int):
    async with db_connect() as db:
        ts = int(datetime.datetime.now(tz=TZ).timestamp())
        await db.execute(
            "INSERT INTO tarot_draws(user_id, ts, date, card_code, tarolog, is_free) VALUES(?,?,?,?,?,?)",
            (user_id, ts, today_str(), card_code, tarolog or None, int(is_free))
        )
        await db.commit()

async def tarot_add_cards(user_id: int, n: int):
    async with db_connect() as db:
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id, cards_balance) VALUES(?, 0)", (user_id,))
        await db.execute("UPDATE tarot_users SET cards_balance = COALESCE(cards_balance,0) + ? WHERE user_id=?", (n, user_id))
        await db.commit()


# --- Списание одной платной карты (возвращает True, если успешно) ---
async def tarot_consume_paid_card(user_id: int) -> bool:
    """Пытается списать 1 карту с баланса. Возвращает True, если получилось."""
    async with db_connect() as db:
        # Убедимся, что запись существует
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id, cards_balance) VALUES(?, 0)", (user_id,))
        cur = await db.execute("SELECT COALESCE(cards_balance,0) FROM tarot_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        bal = int(row[0]) if row and row[0] is not None else 0
        if bal <= 0:
            return False
        await db.execute("UPDATE tarot_users SET cards_balance = cards_balance - 1 WHERE user_id=?", (user_id,))
        await db.commit()
        return True

# --- Списание N платных карт атомарно (возвращает True, если успешно) ---
async def tarot_consume_paid_cards(user_id: int, n: int) -> bool:
    """Списывает n платных карт атомарно. Возвращает True, если удалось."""
    if n <= 0:
        return True
    async with db_connect() as db:
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id, cards_balance) VALUES(?, 0)", (user_id,))
        cur = await db.execute("SELECT COALESCE(cards_balance,0) FROM tarot_users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        bal = int(row[0]) if row and row[0] is not None else 0
        if bal < n:
            return False
        await db.execute("UPDATE tarot_users SET cards_balance = cards_balance - ? WHERE user_id=?", (n, user_id))
        await db.commit()
        return True

async def tarot_add_referral(referrer_id: int, referred_id: int) -> bool:
    if referrer_id == referred_id:
        return False
    async with db_connect() as db:
        cur = await db.execute("SELECT 1 FROM tarot_referrals WHERE referred_id=?", (referred_id,))
        if await cur.fetchone():
            return False
        ts = int(datetime.datetime.now(tz=TZ).timestamp())
        await db.execute("INSERT OR IGNORE INTO tarot_referrals(referrer_id, referred_id, ts) VALUES(?,?,?)", (referrer_id, referred_id, ts))
        await db.execute("INSERT OR IGNORE INTO tarot_users(user_id, cards_balance) VALUES(?, 0)", (referrer_id,))
        await db.execute("UPDATE tarot_users SET cards_balance = COALESCE(cards_balance,0) + 1 WHERE user_id=?", (referrer_id,))
        await db.commit()
        return True

# ----------------- UI УТИЛИТЫ -----------------

async def safe_answer(query, text=None, show_alert=False):
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception:
        pass

async def safe_edit(query, text: str, reply_markup=None, parse_mode=None):
    try:
        msg = query.message
        if getattr(msg, "photo", None) or getattr(msg, "caption", None):
            try:
                await query.edit_message_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
                return
            except BadRequest as e:
                if "message to edit not found" in str(e).lower() or "message is not modified" in str(e).lower() or "can't parse entities" in str(e).lower():
                    chat_id = msg.chat.id
                    new_msg = await query.get_bot().send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
                    try:
                        await msg.delete()
                    except Exception:
                        pass
                    return
                raise
        await query.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as e:
        msg = str(e).lower()
        if ("query is too old" in msg) or ("query id is invalid" in msg) or ("message to edit not found" in msg) or ("message is not modified" in msg):
            chat_id = query.message.chat.id
            new_msg = await query.get_bot().send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
            try:
                await query.message.delete()
            except Exception:
                pass
        else:
            raise

async def ui_show(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, reply_markup=None, parse_mode=None):
    """Показывает/обновляет одно «окно»; удаляет предыдущее при необходимости."""
    mid = context.chat_data.get("ui_mid")

    from telegram import ReplyKeyboardMarkup as _RKM
    if isinstance(reply_markup, _RKM):
        if mid:
            try: await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception: pass
        m = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        context.chat_data["ui_mid"] = m.message_id
        return

    if mid:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
        except Exception:
            pass

    old_mid = mid
    m = await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    context.chat_data["ui_mid"] = m.message_id
    if old_mid and old_mid != m.message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=old_mid)
        except Exception:
            pass

async def try_delete_last_prediction(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    info = context.user_data.get("last_pred_msg")
    if not info:
        return
    chat_id = info.get("chat_id"); message_id = info.get("message_id")
    if chat_id and message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
    context.user_data.pop("last_pred_msg", None)
    # Удалим сохранённое фото предсказания (если было)
    info_photo = context.user_data.get("last_pred_photo")
    if info_photo:
        p_chat = info_photo.get("chat_id"); p_mid = info_photo.get("message_id")
        if p_chat and p_mid:
            try:
                await context.bot.delete_message(chat_id=p_chat, message_id=p_mid)
            except Exception:
                pass
        context.user_data.pop("last_pred_photo", None)


async def tarot_cleanup_about_photo(context: ContextTypes.DEFAULT_TYPE):
    info = context.user_data.pop("tarot_about_photo", None)
    if not info:
        return
    chat_id = info.get("chat_id"); message_id = info.get("message_id")
    if chat_id and message_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass

# --- Новый, более универсальный очиститель фото таролога ---
async def tarot_cleanup_all_photos(context: ContextTypes.DEFAULT_TYPE):
    """Удаляет любые сохранённые фото таролога (и из user_data, и из chat_data)."""
    # 1) Старый ключ в user_data
    info = context.user_data.pop("tarot_about_photo", None)
    if info:
        chat_id = info.get("chat_id"); message_id = info.get("message_id")
        if chat_id and message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except Exception:
                pass
    # 2) Универсальный ключ в chat_data
    info2 = context.chat_data.pop("tarot_photo", None)
    if info2:
        chat_id = info2.get("chat_id"); message_id = info2.get("message_id")
        if chat_id and message_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except Exception:
                pass


# ----------------- КЛАВИАТУРЫ -----------------

from telegram import InlineKeyboardMarkup as _IKM2, InlineKeyboardButton as _IKB2

def admin_main_kb() -> _IKM2:
    return _IKM2([
        [InlineKeyboardButton("🌅 Тест утренней рассылки", callback_data="admin:test_morning")],
        # Баланс
        [_IKB2("🃏 +5 карт мне", callback_data="admin:give5"), _IKB2("📊 Статистика карт", callback_data="admin:cards_stats")],
        [_IKB2("➕ Выдать карты пользователю", callback_data="admin:grant_cards")],
        # Статистика
        [_IKB2("👥 По знакам", callback_data="admin:stats_zodiac"), _IKB2("🚻 По полу", callback_data="admin:stats_gender")],
        [_IKB2("🎂 По возрастам", callback_data="admin:stats_age"), _IKB2("🔔 Подписки", callback_data="admin:stats_subs")],
        # Контент
        [_IKB2("📂 Проверить предсказания", callback_data="admin:pred_overview")],
        [_IKB2("✏️ Редактировать предсказание", callback_data="admin:pred_edit")],
        [_IKB2("🔮 Тестовый расклад", callback_data="admin:test_spread")],
        # Коммуникации
        [_IKB2("📬 Рассылка", callback_data="admin:broadcast")],
        # Управление
        [_IKB2("👑 Админы", callback_data="admin:admins"), _IKB2("🗑 Очистить кеш", callback_data="admin:cleanup")],
        [_IKB2("🔄 Перезапуск (подсказка)", callback_data="admin:restart")],
        [_IKB2("⬅️ Меню", callback_data="ui:menu")],
    ])

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_CATPRED), KeyboardButton(BTN_TAROT)],
            [KeyboardButton(BTN_PROFILE), KeyboardButton(BTN_HELP)],
        ],
        resize_keyboard=True
    )

def categories_inline_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"{emo} {name}", callback_data=f"catpred:{name}")] for name, emo in CATEGORY_LIST]
    rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="ui:menu")])
    return InlineKeyboardMarkup(rows)

def consent_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔔 Начать получать гороскопы", callback_data="consent:yes")],
    ])

def depth_inline_kb(category: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⚡ Короткий", callback_data=f"catdepth:{category}:short")],
        [InlineKeyboardButton("🔎 Средний", callback_data=f"catdepth:{category}:medium")],
        [InlineKeyboardButton("📖 Полный",  callback_data=f"catdepth:{category}:long")],
        [InlineKeyboardButton("⬅️ Категории", callback_data="catpred:open")],
    ]
    return InlineKeyboardMarkup(rows)

def tarot_profile_kb(balance: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 Купить карты", callback_data="tarot:buy")],
        [InlineKeyboardButton("👥 Рефералка", callback_data="tarot:ref")],
        [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
    ])

def tarot_pick_kb(current: Optional[str]) -> InlineKeyboardMarkup:
    rows = []
    for code, label in TAROT_TAROLOGS:
        rows.append([InlineKeyboardButton(label, callback_data=f"tarot:about:{code}")])
    rows.append([InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")])
    return InlineKeyboardMarkup(rows)

async def tarot_main_kb(user_id: int, balance: int, tarolog: Optional[str]) -> InlineKeyboardMarkup:
    label = _label_for_tarolog(tarolog)
    # draw_label always the same, no free_left shown
    draw_label = "🎴 Вытянуть карту"
    rows = [
        [InlineKeyboardButton(draw_label, callback_data="tarot:draw_entry")],
        [InlineKeyboardButton("👤 Профиль", callback_data="tarot:profile")],
        [InlineKeyboardButton(f"🧙 Таролог: {label}", callback_data="tarot:choose")],
        [InlineKeyboardButton("✨ Как это работает • FAQ", callback_data="tarot:howto")],
        [InlineKeyboardButton("⬅️ Меню", callback_data="ui:menu")],
    ]
    return InlineKeyboardMarkup(rows)

def tarot_spread_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🃏 Одна карта", callback_data="tarot:draw:one")],
        [InlineKeyboardButton("🔮 Три карты", callback_data="tarot:draw:three")],
        [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
    ])

def tarot_howto_text() -> str:
    return (
        "<b>✨ Как это работает</b>\n\n"
        "🎴 <b>Карты</b>\n"
        f"• Бесплатно: {TAROT_DAILY_FREE} карта в день (счётчик на кнопке).\n"
        "• Остальные — списываются с баланса.\n\n"
        "🧙 <b>Таролог</b>\n"
        "• Выбери стиль ответа: Учёный, Мистик, Популярная или Молодая.\n"
        "• В любой момент можно сменить — это повлияет на тон и подачу.\n\n"
        "💳 <b>Покупка карт</b>\n"
        "• Пакеты пополнения: +5 / +15 / +50 карт (пока через поддержку).\n\n"
        "👥 <b>Рефералы</b>\n"
        "• За каждого друга по твоей ссылке — +1 карта на баланс.\n\n"
        "💡 <i>Совет:</i> начни с бесплатной карты сегодня, а за деталями возвращайся к платным."
    )

def tarot_intro_text(user_id: int, balance: int, tarolog: Optional[str]) -> str:
    tl = random.choice([
        "Одна карта — и всё встаёт на свои места ✨",
        "Спроси путь у карт — интуиция подскажет 🌙",
        "Сегодняшний знак? Пусть Таро дадут ориентир 🧭",
        "Когда нужен намёк судьбы — карты рядом 🔮",
    ])
    who = TAROLOG_DESC.get(_norm_tarolog(tarolog) or "mystic", TAROLOG_DESC["mystic"])
    tarolog_label = _label_for_tarolog(tarolog)
    return (
        "<b>🔮 Таро</b>\n" + tl + "\n\n"
        f"👤 Таролог: <b>{tarolog_label}</b>\n"
        f"\n{who}\n"
        f"\n💼 Баланс: <b>{balance}</b> карт\n"
        "\nНажми «🎴 Вытянуть карту». Если не выбран таролог — я предложу выбрать."
    )


# ----------------- НАСТРОЙКИ (Settings) -----------------

def settings_main_text(zodiac: Optional[str], age: Optional[int], gender: Optional[str], notify_time: Optional[str]) -> str:
    z_emoji = ZODIAC_SYMBOL.get(zodiac or '', '✨')
    z_line = f"{z_emoji} {zodiac}" if zodiac else "не выбран"
    a_line = str(age) if (isinstance(age, int) and age > 0) else "не указан"
    g_line = (gender or "").strip() or "не указан"
    t_line = (notify_time or DEFAULT_NOTIFY_TIME)
    return (
        "<b>⚙️ Настройки</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"♈ <b>Знак:</b> {z_line}\n"
        f"🎂 <b>Возраст:</b> {a_line}\n"
        f"🚻 <b>Пол:</b> {g_line}\n"
        f"⏰ <b>Время рассылки:</b> {t_line} МСК\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🛠 Выберите, что изменить:"
    )


def settings_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("♈ Сменить знак зодиака", callback_data="settings:zodiac")],
        [InlineKeyboardButton("🎂 Указать возраст", callback_data="settings:age")],
        [InlineKeyboardButton("🚻 Указать пол", callback_data="settings:gender")],
        [InlineKeyboardButton("⏰ Сменить время рассылки", callback_data="settings:notify")],
        [InlineKeyboardButton("⬅️ Меню", callback_data="ui:menu")],
    ])


def gender_pick_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👨 Мужской", callback_data="settings:gender:set:Мужской")],
        [InlineKeyboardButton("👩 Женский", callback_data="settings:gender:set:Женский")],
        [InlineKeyboardButton("🙈 Не указывать", callback_data="settings:gender:set:Не указан")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="settings:open")],
    ])


def zodiac_pick_kb_settings() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"{ZODIAC_SYMBOL[z]} {z}", callback_data=f"settings:setz:{z}")] for z in ZODIACS]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="settings:open")])
    return InlineKeyboardMarkup(rows)

def notify_time_kb() -> InlineKeyboardMarkup:
    # Разрешаем только окно 07:00–12:00 (МСК)
    opts = ["07:00","08:00","09:00","10:00","11:00","12:00"]
    rows = []
    for i in range(0, len(opts), 2):
        pair = opts[i:i+2]
        rows.append([InlineKeyboardButton(f"⏰ {t}", callback_data=f"settings:notify:set:{t}") for t in pair])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="settings:open")])
    return InlineKeyboardMarkup(rows)
# ----------------- ОБРАБОТЧИКИ -----------------

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id

    # Gate: требуем подписку и выбранный знак
    async with db_connect() as db:
        cur = await db.execute(
            "SELECT COALESCE(consent,0), COALESCE(zodiac,'') FROM users WHERE user_id=?",
            (uid,)
        )
        row = await cur.fetchone()
        consent = int(row[0]) if row else 0
        zodiac  = str(row[1]) if row and row[1] else ''

    if not consent:
        await ui_show(context, chat_id, await build_consent_text(uid), reply_markup=consent_inline_kb())
        return
    if not zodiac:
        await ui_show(context, chat_id, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
        return

    if not is_admin(uid):
        await update.effective_message.reply_text("⛔ Доступ только для администраторов.")
        return
    await ui_show(context, chat_id, "<b>Админ-панель</b>", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    await ensure_user_row(uid, chat_id)

    # /start ref12345
        # Gate: сначала подписка, затем знак зодиака
    async with db_connect() as db:
        cur = await db.execute(
            "SELECT COALESCE(consent,0), COALESCE(zodiac,'') FROM users WHERE user_id=?",
            (uid,)
        )
        row = await cur.fetchone()
        consent = int(row[0]) if row else 0
        zodiac  = str(row[1]) if row and row[1] else ''

    if not consent:
        await ui_show(
            context,
            chat_id,
            await build_consent_text(uid),
            reply_markup=consent_inline_kb()
        )
        return

    if not zodiac:
        await ui_show(context, chat_id, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
        return

    await ui_show(context, chat_id, "🏠 Главное меню", reply_markup=main_menu_kb())

async def tgtest_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        me = await context.bot.get_me()
        await update.effective_message.reply_text(f"✅ API OK: @{me.username}")
    except Exception as e:
        await update.effective_message.reply_text(f"❌ API error: {e}")

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.effective_message.text or "").strip()
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    await ensure_user_row(uid, chat_id)

    # Gate: без согласия и знака не пускаем никуда
    async with db_connect() as db:
        cur = await db.execute("SELECT COALESCE(consent,0), COALESCE(zodiac,'') FROM users WHERE user_id=?", (uid,))
        row = await cur.fetchone()
        consent = int(row[0]) if row else 0
        zodiac  = str(row[1]) if row and row[1] else ''
    if not consent:
        await ui_show(context, chat_id,
            await build_consent_text(uid),
            reply_markup=consent_inline_kb())
        return
    if not zodiac and text != BTN_PROFILE:
        await ui_show(context, chat_id, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
        return

    # Сброс режимов ожидания админки при переходе по обычным кнопкам меню
    if text in (BTN_CATPRED, BTN_TAROT, BTN_PROFILE, BTN_HELP, "Отмена", "Назад"):
        context.user_data.pop("await_grant_cards", None)
        context.user_data.pop("await_broadcast", None)

    # Ожидание данных для выдачи карт (admin:grant_cards)
    if context.user_data.get("await_grant_cards"):
        try:
            parts = (text or "").replace("\n"," ").split()
            uid_target = int(parts[0]); amount = int(parts[1])
            if amount <= 0: raise ValueError
        except Exception:
            await ui_show(context, chat_id, "Формат: <code>user_id количество</code>\nПример: <code>123456789 5</code>", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)
            return
        await tarot_add_cards(uid_target, amount)
        context.user_data.pop("await_grant_cards", None)
        await ui_show(context, chat_id, f"✅ Начислено <b>+{amount}</b> карт пользователю <code>{uid_target}</code>.", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)
        return

    # Ожидание текста для рассылки (тест: отправляем только себе)
    if context.user_data.get("await_broadcast"):
        context.user_data.pop("await_broadcast", None)
        try:
            async with db_connect() as db:
                cur = await db.execute("SELECT chat_id FROM users WHERE COALESCE(consent,0)=1")
                rows = await cur.fetchall()
            sent = 0
            for (cid,) in rows:
                try:
                    await context.bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML)
                    sent += 1
                except Exception:
                    continue
            await ui_show(context, chat_id, f"✅ Рассылка отправлена {sent} пользователям.", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML)
        except Exception as e:
            await ui_show(context, chat_id, f"❌ Ошибка отправки: {e}", reply_markup=admin_main_kb())
        return

    if text == BTN_CATPRED:
        await tarot_cleanup_about_photo(context)
        await try_delete_last_prediction(context, uid)
        # если знак не выбран — сначала его
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac FROM users WHERE user_id=?", (uid,))
            row = await cur.fetchone()
            zodiac = row[0] if row else None
        if not zodiac:
            await ui_show(context, chat_id, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
            return
        await ui_show(context, chat_id, "Выберите категорию:", reply_markup=categories_inline_kb())
        return

    if text == BTN_TAROT:
        await try_delete_last_prediction(context, uid)
        await tarot_cleanup_about_photo(context)
        bal, tar, _, _ = await tarot_get_user(uid)
        kb = await tarot_main_kb(uid, bal, tar)
        await ui_show(context, chat_id, tarot_intro_text(uid, bal, tar), reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if text == BTN_PROFILE:
        await try_delete_last_prediction(context, uid)
        await tarot_cleanup_about_photo(context)
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (uid,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        await ui_show(context, chat_id, settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    # Ввод возраста (ожидание текстом)
    if context.user_data.get("await_set_age"):
        txt = (text or "").strip()
        try:
            val = int(txt)
            if val < 10 or val > 120:
                raise ValueError
        except Exception:
            await ui_show(context, chat_id, "Введите возраст числом от 10 до 120:", reply_markup=settings_main_kb())
            return
        async with db_connect() as db:
            await db.execute("UPDATE users SET age=? WHERE user_id=?", (val, uid))
            await db.commit()
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (uid,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        context.user_data.pop("await_set_age", None)
        await ui_show(context, chat_id, "✅ Возраст обновлён.\n\n" + settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    if text == BTN_HELP:
        await try_delete_last_prediction(context, uid)
        await tarot_cleanup_about_photo(context)
        await ui_show(context, chat_id, "❓ Помощь: напишите администратору @username", reply_markup=main_menu_kb())
        return

    await ui_show(context, chat_id, "🏠 Главное меню", reply_markup=main_menu_kb())

# Zodiac inline keyboard (placed here to avoid forward-ref issue)
from telegram import InlineKeyboardMarkup as _IKM, InlineKeyboardButton as _IKB
def zodiac_pick_kb() -> _IKM:
    rows = [[_IKB(f"{ZODIAC_SYMBOL[z]} {z}", callback_data=f"setz:{z}")] for z in ZODIACS]
    rows.append([_IKB("⬅️ Меню", callback_data="ui:menu")])
    return _IKM(rows)

# ---- Progress style builder for tarot drawing ----
def build_progress_stages(spread_key: str, style: str) -> tuple[list[str], tuple[int, int]]:
    """Returns (stages, (min_seconds, max_seconds)) for given spread and style."""
    one = (10, 20)
    three = (20, 30)
    is_one = (spread_key == "one")

    if style == "ritual":  # 🔥🕯️ атмосферный ритуал
        stages = ([
            "🔄 Перемешиваю колоду…",
            "🕯️ Зажигаю свечу намерения…",
            "✨ Шёпот карт… прислушиваюсь…",
            "👁️ Выбираю карту…",
        ] if is_one else [
            "🔄 Перемешиваю колоду…",
            "🕯️ Зажигаю свечу намерения…",
            "🌬 Снимаю колоду — вдох/выдох…",
            "🃏 Открываю 1-ю карту…",
            "🃏 Открываю 2-ю карту…",
            "🃏 Открываю 3-ю карту…",
        ])
        return stages, (one if is_one else three)

    # дефолт — базовые этапы
    stages = ([
        "🔄 Перемешиваю колоду…",
        "✋ Снимаю колоду…",
        "👁️ Выбираю карту…",
    ] if is_one else [
        "🔄 Перемешиваю колоду…",
        "✋ Снимаю колоду…",
        "🃏 Открываю 1-ю карту…",
        "🃏 Открываю 2-ю карту…",
        "🃏 Открываю 3-ю карту…",
    ])
    return stages, (one if is_one else three)

# --- Progress stages for predictions by category ---
def build_pred_progress(zodiac: Optional[str] = None) -> tuple[list[str], int]:
    """Возвращает (этапы, total_delay_seconds 5–7). Без упоминания знака в тексте."""
    stages = [
        "🔭 Смотрю на звёзды…",
        "✨ Согласую категорию…",
        "🧭 Подбираю формулировку…",
    ]
    total = random.randint(5, 7)
    return stages, total

# --- Category card (header with date + 3 meters) ---
_CAT_EMO = {name: emo for name, emo in CATEGORY_LIST}

def _daily_score_rng(zodiac: str, category: str) -> random.Random:
    seed = f"{today_str()}|{zodiac}|{category}|v1"
    return random.Random(abs(hash(seed)) % (2**31))

def _build_meter(value: int, total: int = 5) -> str:
    value = max(0, min(total, int(value)))
    full = "▮" * value
    empty = "▯" * (total - value)
    return f"{full}{empty} ({value}/{total})"

def build_category_card(zodiac: str, category: str) -> str:
    """Beautiful header with zodiac + category + date and three meters (🍀 luck, ⚡ energy, 🎯 focus)."""
    zemo = ZODIAC_SYMBOL.get(zodiac, "✨")
    cemo = _CAT_EMO.get(category, "🧭")
    today = today_str()
    rng = _daily_score_rng(zodiac, category)
    luck = rng.randint(3, 5)
    energy = rng.randint(2, 5)
    focus = rng.randint(3, 5)
    lines = [
        f"{zemo} <b>{zodiac}</b> · {cemo} <b>{category}</b>\n<i>{today}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🍀 <b>Удача:</b>  {_build_meter(luck)}",
        f"⚡ <b>Энергия:</b> {_build_meter(energy)}",
        f"🎯 <b>Фокус:</b>  {_build_meter(focus)}",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    return "\n".join(lines)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await safe_answer(query)
    data = query.data
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    # --- НАСТРОЙКИ (Settings) ---
    if data == "settings:open":
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        await safe_edit(query, settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    if data == "settings:zodiac":
        await safe_edit(query, "Выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb_settings())
        return

    if data.startswith("settings:setz:"):
        new_z = data.split(":", 2)[-1]
        async with db_connect() as db:
            await db.execute("UPDATE users SET zodiac=? WHERE user_id=?", (new_z, user_id))
            await db.commit()
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        await safe_edit(query, f"✅ Знак обновлён: <b>{ZODIAC_SYMBOL.get(new_z,'✨')} {new_z}</b>\n\n" + settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    if data == "settings:age":
        context.user_data["await_set_age"] = True
        await safe_edit(
            query,
            "🎂 <b>Укажите возраст</b>\n\nВведите возраст числом (например, 25):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="settings:open")]]),
            parse_mode=ParseMode.HTML
        )
        return

    if data == "settings:gender":
        await safe_edit(query, "Выберите пол:", reply_markup=gender_pick_kb())
        return

    if data.startswith("settings:gender:set:"):
        val = data.split(":", 3)[-1]
        async with db_connect() as db:
            await db.execute("UPDATE users SET gender=? WHERE user_id=?", (val, user_id))
            await db.commit()
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        await safe_edit(query, f"✅ Пол обновлён: <b>{val}</b>\n\n" + settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    if data == "settings:notify":
        await safe_edit(query, "Выберите время для ежедневной рассылки (МСК, доступно 07:00–12:00):", reply_markup=notify_time_kb())
        return

    if data.startswith("settings:notify:set:"):
        val = data.split(":", 3)[-1]
        # Проверяем формат и допустимый диапазон 07:00–12:00 МСК
        if not re.match(r"^\d{2}:\d{2}$", val):
            await safe_answer(query, "Неверный формат времени.", show_alert=True)
            return
        hh, mm = val.split(":")
        try:
            h = int(hh); m = int(mm)
        except Exception:
            await safe_answer(query, "Неверный формат времени.", show_alert=True)
            return
        # Только целые часы, минутная точность 00, диапазон 07:00–12:00
        if m != 0 or h < 7 or h > 12:
            await safe_answer(query, "Можно выбрать время с 07:00 до 12:00 (МСК).", show_alert=True)
            return
        async with db_connect() as db:
            await db.execute("UPDATE users SET notify_time=? WHERE user_id=?", (val, user_id))
            await db.commit()
            cur = await db.execute("SELECT zodiac, age, gender, COALESCE(notify_time,'') FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            if row:
                zodiac, age, gender, notify_time = row
            else:
                zodiac, age, gender, notify_time = (None, None, None, DEFAULT_NOTIFY_TIME)
        await safe_edit(query, f"✅ Время обновлено: <b>{val}</b> (МСК)\n\n" + settings_main_text(zodiac, age, gender, notify_time), reply_markup=settings_main_kb(), parse_mode=ParseMode.HTML)
        return

    # --- АДМИНКА ---
    if data == "admin:test_morning":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        try:
            await send_morning_digest(context, user_id, chat_id, force=True)
            await safe_edit(query, "✅ Тестовое утреннее сообщение отправлено.", reply_markup=admin_main_kb()); return
        except Exception as e:
            logging.exception("admin:test_morning failed: %s", e)
            await safe_answer(query, f"Ошибка: {e}", show_alert=True); return
    if data == "admin:give5":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        await tarot_add_cards(user_id, 5)
        bal, _, _, _ = await tarot_get_user(user_id)
        await safe_edit(query, f"✅ Начислено +5 карт. Баланс: <b>{bal}</b>", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:cards_stats":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        async with db_connect() as db:
            cur = await db.execute("SELECT COUNT(1), COALESCE(SUM(cards_balance),0) FROM tarot_users")
            row = await cur.fetchone()
            users_with_tarot = int(row[0] or 0)
            total_balance = int(row[1] or 0)
            cur = await db.execute("SELECT COUNT(1) FROM tarot_draws")
            total_draws = int((await cur.fetchone())[0] or 0)
            cur = await db.execute("SELECT COUNT(1) FROM tarot_draws WHERE is_free=1")
            free_draws = int((await cur.fetchone())[0] or 0)
        paid_draws = max(0, total_draws - free_draws)
        txt = ("<b>📊 Статистика карт</b>\n"
               f"Пользователей с профилем Таро: <b>{users_with_tarot}</b>\n"
               f"Суммарный баланс карт у всех: <b>{total_balance}</b>\n"
               f"Всего раскладов: <b>{total_draws}</b> (бесплатных: {free_draws}, платных: {paid_draws})")
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:grant_cards":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        context.user_data["await_grant_cards"] = True
        await safe_edit(query, "Введите: <code>user_id количество</code> (пример: <code>123456789 5</code>)", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:stats_zodiac":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac, COUNT(1) FROM users GROUP BY zodiac ORDER BY COUNT(1) DESC")
            rows = await cur.fetchall()
        lines = [f"{ZODIAC_SYMBOL.get(z or '', '✨')} {z or '—'}: <b>{n}</b>" for z, n in rows]
        txt = "<b>👥 Пользователи по знакам</b>\n" + ("\n".join(lines) if lines else "— нет данных")
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:stats_gender":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        async with db_connect() as db:
            cur = await db.execute("SELECT COALESCE(gender,'Не указан'), COUNT(1) FROM users GROUP BY COALESCE(gender,'Не указан')")
            rows = await cur.fetchall()
        lines = [f"{g}: <b>{n}</b>" for g, n in rows]
        txt = "<b>🚻 Пользователи по полу</b>\n" + ("\n".join(lines) if lines else "— нет данных")
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:stats_age":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        buckets = [(0,17,'<18'),(18,24,'18–24'),(25,34,'25–34'),(35,44,'35–44'),(45,54,'45–54'),(55,200,'55+')]
        counts = {label:0 for _,_,label in buckets}; unknown = 0
        async with db_connect() as db:
            cur = await db.execute("SELECT age FROM users")
            async for (age,) in cur:
                try: a = int(age)
                except Exception: a = None
                if not a or a <= 0: unknown += 1; continue
                for lo,hi,label in buckets:
                    if lo <= a <= hi: counts[label] += 1; break
        lines = [f"{k}: <b>{v}</b>" for k,v in counts.items()] + [f"Не указан: <b>{unknown}</b>"]
        txt = "<b>🎂 Пользователи по возрастам</b>\n" + "\n".join(lines)
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:stats_subs":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        async with db_connect() as db:
            cur = await db.execute("SELECT COALESCE(consent,0), COUNT(1) FROM users GROUP BY COALESCE(consent,0)")
            rows = await cur.fetchall()
        on = sum(n for c,n in rows if int(c)==1)
        off = sum(n for c,n in rows if int(c)!=1)
        txt = f"<b>🔔 Подписки</b>\nВключена: <b>{on}</b>\nВыключена: <b>{off}</b>"
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:pred_overview":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
        zodiac = (row[0] if row else None) or "Овен"
        lines = []
        for name, _emo in CATEGORY_LIST:
            s = []
            for depth in ("short","medium","long"):
                files = find_prediction_files(zodiac, name, depth)
                s.append(f"{depth}:{len([p for p in files if p.exists()])}")
            lines.append(f"{name}: " + ", ".join(s))
        txt = "<b>📂 Проверка предсказаний</b>\n" + "\n".join(lines)
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:pred_edit":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        await safe_edit(query, "Редактирование из админки в разработке. Изменяйте файлы в <code>predictions_db</code>.", reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:test_spread":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        await safe_edit(query, "<b>Выберите расклад (тест)</b>", reply_markup=tarot_spread_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:broadcast":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        context.user_data["await_broadcast"] = True
        await safe_edit(query, "Отправь одним сообщением текст рассылки. (Тест: отправится только тебе.)", reply_markup=admin_main_kb()); return

    if data == "admin:admins":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        lst = sorted(ADMINS)
        txt = "<b>👑 Администраторы</b>\n" + "\n".join([f"• <code>{aid}</code>{' (главный)' if aid==MAIN_ADMIN_ID else ''}" for aid in lst])
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    if data == "admin:cleanup":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        await tarot_cleanup_all_photos(context)
        prev_ids = context.user_data.pop("tarot_album_ids", None)
        if prev_ids:
            for mid in prev_ids:
                try: await context.bot.delete_message(chat_id=chat_id, message_id=mid)
                except Exception: pass
        await safe_edit(query, "✅ Временные сообщения/фото очищены.", reply_markup=admin_main_kb()); return

    if data == "admin:restart":
        if not is_admin(user_id):
            await safe_answer(query, "Только для админов", show_alert=True); return
        txt = ("<b>🔄 Перезапуск</b>\n"
               "Локально: <code>Ctrl+C</code> и снова <code>python3 bot.py</code>.\n"
               "Если сервис: перезапусти unit в supervisor/systemd.")
        await safe_edit(query, txt, reply_markup=admin_main_kb(), parse_mode=ParseMode.HTML); return

    # Главное меню
    if data == "ui:menu":
        # выходим из режимов ожидания админки
        context.user_data.pop("await_grant_cards", None)
        context.user_data.pop("await_broadcast", None)
        await try_delete_last_prediction(context, user_id)
        await tarot_cleanup_about_photo(context)
        await ui_show(context, chat_id, "🏠 Главное меню", reply_markup=main_menu_kb())
        return

    # Категории
    if data == "catpred:open":
        context.user_data.pop("await_grant_cards", None)
        context.user_data.pop("await_broadcast", None)
        await try_delete_last_prediction(context, user_id)
        await safe_edit(query, "Выберите категорию:", reply_markup=categories_inline_kb())
        return

    if data.startswith("catpred:"):
        _, cat = data.split(":", 1)
        await safe_edit(query, f"Категория: <b>{cat}</b>\nВыберите формат:", reply_markup=depth_inline_kb(cat), parse_mode=ParseMode.HTML)
        return

    if data.startswith("catdepth:"):
        _, cat, depth = data.split(":", 2)
        # Проверим знак зодиака
        async with db_connect() as db:
            cur = await db.execute("SELECT zodiac FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            zodiac = row[0] if row else None
        if not zodiac:
            await safe_edit(query, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
            return
        # Подготовим анимацию 5–7 сек с картинкой знака
        await try_delete_last_prediction(context, user_id)
        # Подготовим путь к картинке знака (покажем её после «анимации»)
        zimg = _zodiac_img_path(zodiac)
        # Анимация прогресса
        stages, total = build_pred_progress()
        per_step = max(2, total // max(1, len(stages)))
        try:
            prog_msg = await context.bot.send_message(chat_id=chat_id, text=stages[0])
        except Exception:
            prog_msg = None
        remaining = total
        await asyncio.sleep(per_step); remaining -= per_step
        for s in stages[1:]:
            try:
                if prog_msg:
                    await context.bot.edit_message_text(chat_id=chat_id, message_id=prog_msg.message_id, text=s)
                else:
                    prog_msg = await context.bot.send_message(chat_id=chat_id, text=s)
            except Exception:
                pass
            await asyncio.sleep(per_step); remaining -= per_step
        if remaining > 0:
            await asyncio.sleep(remaining)
        # Удалим прогресс
        if prog_msg:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prog_msg.message_id)
            except Exception:
                pass
        # Отправим картинку знака сверху (если есть)
        if zimg and zimg.exists():
            try:
                with zimg.open("rb") as f:
                    ph2 = await context.bot.send_photo(chat_id=chat_id, photo=f)
                context.user_data["last_pred_photo"] = {"chat_id": chat_id, "message_id": ph2.message_id}
            except Exception:
                pass
        # Теперь текст предсказания с нижней кнопкой "Меню"
        text = pick_prediction(zodiac, cat, depth)
        card = build_category_card(zodiac, cat)
        pred_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Меню", callback_data="ui:menu")]])
        sent = await context.bot.send_message(chat_id=chat_id, text=card + "\n" + text, parse_mode=ParseMode.HTML, reply_markup=pred_kb)
        context.user_data["last_pred_msg"] = {"chat_id": chat_id, "message_id": sent.message_id}
        # Обновим панель выбора формата ниже
        await safe_edit(query, f"Категория: <b>{cat}</b>\nФормат: <b>{'Короткий' if depth=='short' else ('Средний' if depth=='medium' else 'Полный')}</b>", reply_markup=depth_inline_kb(cat), parse_mode=ParseMode.HTML)
        return

    if data.startswith("setz:"):
        zodiac = data.split(":",1)[1]
        async with db_connect() as db:
            await db.execute("UPDATE users SET zodiac=? WHERE user_id=?", (zodiac, user_id))
            await db.commit()
            cur = await db.execute("SELECT COALESCE(consent,0) FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            consent = int(row[0]) if row else 0
        if consent:
            await ui_show(context, chat_id, "🏠 Главное меню", reply_markup=main_menu_kb())
        else:
            await safe_edit(query, f"Ваш знак сохранён: <b>{ZODIAC_SYMBOL.get(zodiac,'✨')} {zodiac}</b>\n" + await build_consent_text(user_id), reply_markup=consent_inline_kb(), parse_mode=ParseMode.HTML)
        return

    if data == "consent:yes":
        async with db_connect() as db:
            # Set consent on and initialize notify_time to default if it's empty
            await db.execute("UPDATE users SET consent=1 WHERE user_id=?", (user_id,))
            cur = await db.execute("SELECT COALESCE(notify_time,''), COALESCE(zodiac,'') FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()
            current_time = (row[0] or '').strip() if row else ''
            zodiac = str(row[1]) if row and row[1] else ''
            if not current_time:
                await db.execute("UPDATE users SET notify_time=? WHERE user_id=?", (DEFAULT_NOTIFY_TIME, user_id))
            await db.commit()
        if zodiac:
            await ui_show(context, chat_id, "🏠 Главное меню", reply_markup=main_menu_kb())
        else:
            await safe_edit(query, "Отлично! Теперь выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
        return

    # --- ТАРО ---

    if data == "noop":
        # ничего не делаем, просто закрываем спиннер
        return

    if data == "tarot:open":
        context.user_data.pop("await_grant_cards", None)
        context.user_data.pop("await_broadcast", None)
        await tarot_cleanup_about_photo(context)
        bal, tar, _, _ = await tarot_get_user(user_id)
        kb = await tarot_main_kb(user_id, bal, tar)
        await safe_edit(query, tarot_intro_text(user_id, bal, tar), reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:howto":
        await tarot_cleanup_about_photo(context)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            [InlineKeyboardButton("👤 Профиль", callback_data="tarot:profile")],
        ])
        await safe_edit(query, tarot_howto_text(), reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:buy":
        await tarot_cleanup_about_photo(context)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("+5 карт — бесплатно (тест)", callback_data="tarot:buy:p5")],
            [InlineKeyboardButton("+15 карт — бесплатно (тест)", callback_data="tarot:buy:p15")],
            [InlineKeyboardButton("+50 карт — бесплатно (тест)", callback_data="tarot:buy:p50")],
            [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
        ])
        await safe_edit(
            query,
            "<b>🛒 Покупка карт</b>\n\n<b>Тестовый режим:</b> пока все пакеты бесплатные. Выберите нужный — и мы мгновенно начислим карты на баланс.",
            reply_markup=kb,
            parse_mode=ParseMode.HTML
        )
        return

    if data == "tarot:buy:p5":
        await tarot_add_cards(user_id, 5)
        bal, tar, last, used = await tarot_get_user(user_id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            [InlineKeyboardButton("🛒 Ещё пакеты", callback_data="tarot:buy")],
        ])
        await safe_edit(query, f"✅ Начислено: <b>+5</b> карт.\nТекущий баланс: <b>{bal}</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:buy:p15":
        await tarot_add_cards(user_id, 15)
        bal, tar, last, used = await tarot_get_user(user_id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            [InlineKeyboardButton("🛒 Ещё пакеты", callback_data="tarot:buy")],
        ])
        await safe_edit(query, f"✅ Начислено: <b>+15</b> карт.\nТекущий баланс: <b>{bal}</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:buy:p50":
        await tarot_add_cards(user_id, 50)
        bal, tar, last, used = await tarot_get_user(user_id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            [InlineKeyboardButton("🛒 Ещё пакеты", callback_data="tarot:buy")],
        ])
        await safe_edit(query, f"✅ Начислено: <b>+50</b> карт.\nТекущий баланс: <b>{bal}</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:ref":
        await tarot_cleanup_about_photo(context)
        me = await context.bot.get_me()
        ref_link = f"https://t.me/{me.username}?start=ref{user_id}"
        txt = (
            "<b>👥 Рефералка</b>\n"
            "Приглашай друзей — за каждого активного друга +1 карта на баланс.\n\n"
            f"Твоя ссылка: {ref_link}\n\n"
            "Как это работает: друг заходит по ссылке и запускает бота. После первого действия мы начислим 1 карту на твой баланс."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")]])
        await safe_edit(query, txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:profile":
        await tarot_cleanup_about_photo(context)
        bal, tar, last, used = await tarot_get_user(user_id)
        txt = (
            "<b>👤 Профиль Таро</b>\n"
            f"Баланс: <b>{bal}</b>\n"
            f"Таролог: <b>{_label_for_tarolog(tar)}</b>\n"
            f"Бесплатных сегодня: {max(0, TAROT_DAILY_FREE - (used if last == today_str() else 0))}/{TAROT_DAILY_FREE}"
        )
        await safe_edit(query, txt, reply_markup=tarot_profile_kb(bal), parse_mode=ParseMode.HTML)
        return

    if data == "tarot:choose":
        await tarot_cleanup_about_photo(context)
        bal, tar, _, _ = await tarot_get_user(user_id)
        await safe_edit(query, "<b>Выбери таролога</b>", reply_markup=tarot_pick_kb(tar), parse_mode=ParseMode.HTML)
        return

    if data.startswith("tarot:about:"):
        code = data.split(":", 2)[-1]
        label = _label_for_tarolog(code)
        bio = TAROLOG_BIO.get(code, "Описание скоро появится.")
        desc = TAROLOG_DESC.get(code, "")
        caption = f"<b>{label}</b>\n{bio}\n\n<i>Стиль:</i> {desc}"

        # Определим кнопки «выбрать/текущий»
        _, current, _, _ = await tarot_get_user(user_id)
        if _norm_tarolog(current) == _norm_tarolog(code):
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Текущий таролог", callback_data="noop")],
                [InlineKeyboardButton("⬅️ К выбору таролога", callback_data="tarot:choose")],
            ])
        else:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Выбрать этого таролога", callback_data=f"tarot:set_tarolog:{code}")],
                [InlineKeyboardButton("⬅️ К выбору таролога", callback_data="tarot:choose")],
            ])

        # Удаляем любые предыдущие фото (если были)
        await tarot_cleanup_all_photos(context)

        # Удаляем старое UI-сообщение, чтобы порядок был: СНАЧАЛА фото, НИЖЕ текст
        try:
            await query.message.delete()
        except Exception:
            pass

        # Сначала отправляем фото (если есть) — оно окажется ВЫШЕ (старше) текста
        img = _tarot_img_path(code)
        if img and img.exists():
            try:
                with img.open("rb") as f:
                    photo_msg = await context.bot.send_photo(chat_id=chat_id, photo=f)
                # Сохраняем в обоих пространствах (на случай смены экрана)
                context.user_data["tarot_about_photo"] = {"chat_id": chat_id, "message_id": photo_msg.message_id}
                context.chat_data["tarot_photo"] = {"chat_id": chat_id, "message_id": photo_msg.message_id}
            except Exception:
                pass

        # Затем отправляем текст с кнопками — он будет НИЖЕ фото
        text_msg = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
        # фиксируем как текущее UI-сообщение, чтобы другие экраны могли его корректно обновлять/удалять
        context.chat_data["ui_mid"] = text_msg.message_id
        return

    if data.startswith("tarot:set_tarolog:"):
        code = data.split(":", 2)[-1]
        await tarot_cleanup_about_photo(context)
        await tarot_set_tarolog(user_id, code)
        bal, tar, _, _ = await tarot_get_user(user_id)
        kb = await tarot_main_kb(user_id, bal, tar)
        await safe_edit(query, f"✅ Таролог выбран: <b>{_label_for_tarolog(tar)}</b>", reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if data == "tarot:draw_entry":
        # Перед карточками попросим выбрать расклад, и убедимся, что у пользователя задан знак
        zodiac = await get_user_zodiac(user_id)
        if not zodiac:
            await safe_edit(query, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
            return
        # Проверим доступность попыток: бесплатная или платная
        bal, tar, last, used = await tarot_get_user(user_id)
        free_left = max(0, TAROT_DAILY_FREE - (used if last == today_str() else 0))
        if free_left <= 0 and bal <= 0:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 Купить карты", callback_data="tarot:buy")],
                [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            ])
            await safe_edit(query, "<b>Нет доступных попыток</b>\n\nСегодня бесплатные попытки израсходованы, а баланс равен нулю. В тестовом режиме пополнить можно бесплатно — выбери пакет в магазине.", reply_markup=kb, parse_mode=ParseMode.HTML)
            return
        await safe_edit(query, "<b>Выберите расклад</b>", reply_markup=tarot_spread_kb(), parse_mode=ParseMode.HTML)
        return

    if data.startswith("tarot:draw:"):
        _, _, spread_key = data.split(":", 2)

        # Проверим знак зодиака заранее
        zodiac = await get_user_zodiac(user_id)
        if not zodiac:
            await safe_edit(query, "Сначала выберите ваш знак зодиака:", reply_markup=zodiac_pick_kb())
            return

        # Сколько карт нужно для выбранного расклада
        positions = TAROT_SPREADS.get(spread_key, {}).get("positions", [])
        cards_needed = len(positions) if positions else 1

        # Текущий баланс и доступные бесплатные на сегодня
        bal, tar_tmp, last, used = await tarot_get_user(user_id)
        free_left = TAROT_DAILY_FREE - (used if last == today_str() else 0)
        free_left = max(0, free_left)

        # Проверка достаточности ресурсов
        total_avail = free_left + bal
        if total_avail < cards_needed:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🛒 Купить карты", callback_data="tarot:buy")],
                [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
            ])
            await safe_edit(
                query,
                f"<b>Недостаточно карт</b>\n\nДля этого расклада нужно: <b>{cards_needed}</b>. Доступно: <b>{total_avail}</b> (бесплатные сегодня: {free_left}, на балансе: {bal}).",
                reply_markup=kb,
                parse_mode=ParseMode.HTML,
            )
            return

        # Резервируем: сначала бесплатную (если есть), затем платные на остаток
        use_free = 1 if (free_left > 0 and cards_needed > 0) else 0
        paid_need = cards_needed - use_free

        if paid_need > 0:
            ok_paid = await tarot_consume_paid_cards(user_id, paid_need)
            if not ok_paid:
                # гонка: кто-то потратил карты; просим пополнить
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛒 Купить карты", callback_data="tarot:buy")],
                    [InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")],
                ])
                await safe_edit(query, "<b>Недостаточно карт</b>\n\nПохоже, баланс изменился. Пополни карты и попробуй снова.", reply_markup=kb, parse_mode=ParseMode.HTML)
                return

        is_free_used = False
        if use_free:
            # Списываем бесплатную попытку (их максимум 1 в день)
            is_free_used = await tarot_try_use_free(user_id)
            # Если по какой-то причине не удалось — это не критично, бесплатная просто не зачлась.

        # Защита от двойного нажатия: если уже идёт расклад — игнорируем
        if context.user_data.get("tarot_busy"):
            try:
                await safe_answer(query, "Идёт расклад…")
            except Exception:
                pass
            return
        context.user_data["tarot_busy"] = True

        # Прогресс-этапы перед показом расклада
        await tarot_cleanup_all_photos(context)
        try:
            await query.message.delete()
        except Exception:
            pass

        # Покажем фото текущего таролога СРАЗУ (оно будет выше всех дальнейших сообщений прогресса и результата)
        _, tar, _, _ = await tarot_get_user(user_id)
        img = _tarot_img_path(_norm_tarolog(tar) or "")
        if img and img.exists():
            try:
                with img.open("rb") as f:
                    photo_msg = await context.bot.send_photo(chat_id=chat_id, photo=f)
                context.user_data["tarot_about_photo"] = {"chat_id": chat_id, "message_id": photo_msg.message_id}
                context.chat_data["tarot_photo"] = {"chat_id": chat_id, "message_id": photo_msg.message_id}
            except Exception:
                pass

        # Выберем стиль прогресса и длительность
        style = "ritual"  # варианты на будущее: "ritual","cards","stars","tech","calm"
        stages, (min_s, max_s) = build_progress_stages(spread_key, style)
        total_delay = random.randint(min_s, max_s)
        # Отправляем первое сообщение прогресса и постепенно обновляем
        try:
            prog_msg = await context.bot.send_message(chat_id=chat_id, text=stages[0])
        except Exception:
            prog_msg = None

        # Распределим время равномерно по этапам (не менее 2 секунд на шаг)
        per_step = max(2, total_delay // max(1, len(stages)))
        # Небольшая коррекция, чтобы суммарно не меньше total_delay
        remaining = total_delay
        for i, txt in enumerate(stages):
            if i == 0:
                # уже показали первый шаг
                await asyncio.sleep(per_step)
                remaining -= per_step
                continue
            try:
                if prog_msg:
                    await context.bot.edit_message_text(chat_id=chat_id, message_id=prog_msg.message_id, text=txt)
            except Exception:
                # если не удалось отредактировать — пробуем отправить новое
                try:
                    prog_msg = await context.bot.send_message(chat_id=chat_id, text=txt)
                except Exception:
                    pass
            await asyncio.sleep(per_step)
            remaining -= per_step
        # Если осталось время из-за округления — досыпаем
        if remaining > 0:
            await asyncio.sleep(remaining)

        # Готовим расклад
        cards = await draw_unique_cards_for_spread(user_id, zodiac, spread_key)
        positions = TAROT_SPREADS[spread_key]["positions"]
        title = TAROT_SPREADS[spread_key]["title"]

        style_pre = {"scientist": "👨‍🔬 Учёный","mystic": "🌙 Мистик","popular": "🌟 Популярная","young": "✨ Молодая"}
        # tar уже получен ранее перед прогрессом
        persona = style_pre.get((_norm_tarolog(tar) or "mystic"), "🌙 Мистик")

        # Загрузим оверлей под знак
        overlay_map = load_zodiac_overlay(zodiac)
        rng = deck_rng_for(user_id, zodiac, spread_key)

        lines = [f"<b>🔮 {title}</b> • {ZODIAC_SYMBOL.get(zodiac,'✨')} {zodiac}", ""]
        logged_codes = []
        for idx, (pos) in enumerate(positions):
            card = cards[idx]
            is_rev = decide_orientation(rng)
            code = card["code"] + (" (R)" if is_rev else "")
            base_text = (card["reversed"] if is_rev and card.get("reversed") else card["upright"])
            with_overlay = apply_overlays(base_text, card.get("tags") or [], overlay_map)
            lines.append(f"<b>{pos}:</b> {code}\n<i>{with_overlay}</i>")
            logged_codes.append(card["code"])

        # Финальный текст без подсказок от персоны
        text_out = "\n".join(lines)

        await tarot_log_draw(user_id, ",".join(logged_codes), (_norm_tarolog(tar) or None), 1 if is_free_used else 0)

        # Удаляем прогресс
        if prog_msg:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=prog_msg.message_id)
            except Exception:
                pass

        # Затем отправляем текст расклада с кнопкой «Назад»
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Таро", callback_data="tarot:open")]])
        text_msg = await context.bot.send_message(chat_id=chat_id, text=text_out, reply_markup=kb, parse_mode=ParseMode.HTML)
        context.chat_data["ui_mid"] = text_msg.message_id
        # Снимаем флаг «занято»
        context.user_data.pop("tarot_busy", None)
        return

# ----------------- APP INIT -----------------

def build_application():
    builder = ApplicationBuilder().token(BOT_TOKEN)
    if TELEGRAM_BASE_URL:
        try:
            builder = builder.base_url(TELEGRAM_BASE_URL)
        except Exception:
            logging.warning("base_url() not supported; ignoring TELEGRAM_BASE_URL")
    try:
        from telegram.request import HTTPXRequest as _HTTPXRequest
        sig = inspect.signature(_HTTPXRequest)
        kwargs = {}
        if 'connect_timeout' in sig.parameters: kwargs['connect_timeout'] = 20.0
        if 'read_timeout'    in sig.parameters: kwargs['read_timeout']    = 40.0
        if 'write_timeout'   in sig.parameters: kwargs['write_timeout']   = 20.0
        if 'pool_timeout'    in sig.parameters: kwargs['pool_timeout']    = 10.0
        if 'proxies' in sig.parameters and HTTP_PROXY:
            kwargs['proxies'] = HTTP_PROXY
        request = _HTTPXRequest(**kwargs)
        builder = builder.request(request)
    except Exception as e:
        logging.warning("Using default Telegram request (no custom timeouts): %s", e)

    builder = builder.concurrent_updates(True)
    app = builder.build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("tgtest", tgtest_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))
    return app

def main():
    # Backward-compatible sync entrypoint for environments expecting a sync function.
    # We delegate to the async main to ensure an event loop exists on Python 3.13+.
    asyncio.run(main_async())

async def main_async():
    await init_db()
    app = build_application()
    logging.info("Starting bot…")
    # Explicit async lifecycle to avoid "no current event loop" on Python 3.13
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    # Block forever until the process receives SIGINT/SIGTERM (Ctrl+C)
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        # Best-effort graceful shutdown
        try:
            await app.updater.stop()
        except Exception:
            pass
        try:
            await app.stop()
        except Exception:
            pass

def build_application():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    # handlers
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("tgtest", tgtest_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))
    return app

def main():
    # Инициализация БД до старта приложения
    asyncio.run(init_db())

    app = build_application()
    # run_polling — синхронный метод PTB; он сам запустит async-хендлеры корректно
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()


async def _schedule_mornings(app):
    """Каждую минуту проверяет время (МСК) и шлёт дайджест 1 раз в день."""
    await init_db()
    while True:
        try:
            now = datetime.datetime.now(tz=TZ)
            hhmm = now.strftime("%H:%M")
            today = today_str()
            async with db_connect() as db:
                cur = await db.execute(
                    "SELECT user_id, chat_id, COALESCE(notify_time,'') FROM users WHERE COALESCE(consent,0)=1"
                )
                rows = await cur.fetchall()
            for uid, cid, nt in rows:
                try:
                    target = nt.strip() if re.match(r"^\d{2}:\d{2}$", nt or "") else DEFAULT_NOTIFY_TIME
                    if target != hhmm:
                        continue
                    # skip if already sent today
                    async with db_connect() as db:
                        cur = await db.execute("SELECT COALESCE(last_morning_date,'') FROM users WHERE user_id=?", (uid,))
                        row = await cur.fetchone()
                        last = (row[0] or '').strip() if row else ''
                    if last == today:
                        continue
                    await send_morning_digest(app, uid, cid)
                    async with db_connect() as db:
                        await db.execute("UPDATE users SET last_morning_date=? WHERE user_id=?", (today, uid))
                        await db.commit()
                except Exception as e:
                    logging.warning("morning send failed for %s: %s", uid, e)
        except Exception as e:
            logging.warning("scheduler loop error: %s", e)
        await asyncio.sleep(60)
