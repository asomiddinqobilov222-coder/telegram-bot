import asyncio
import base64
import hashlib
import hmac
import html
import logging
import os
import re
import sqlite3
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import aiohttp
import yt_dlp
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.request import HTTPXRequest


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "bot.env"
DB_PATH = BASE_DIR / "bot.db"
LOG_PATH = BASE_DIR / "bot.log"
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(exist_ok=True)

load_dotenv(ENV_PATH)


def _read_env_value(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value:
        return value.strip().strip('"').strip("'")

    if not ENV_PATH.exists():
        return default

    pattern = re.compile(rf"^\s*{re.escape(name)}\s*=\s*(.+?)\s*$")
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        match = pattern.match(raw_line)
        if match:
            return match.group(1).strip().strip('"').strip("'")
    return default


BOT_TOKEN = _read_env_value("BOT_TOKEN")
ACR_HOST = _read_env_value("ACR_HOST")
ACR_KEY = _read_env_value("ACR_KEY")
ACR_SECRET = _read_env_value("ACR_SECRET")

SUPPORTED_SITES = (
    "youtube.com",
    "youtu.be",
    "instagram.com",
    "tiktok.com",
    "vm.tiktok.com",
    "facebook.com",
    "fb.watch",
)

BASE_YTDLP_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "noplaylist": True,
    "windowsfilenames": True,
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def db_connect() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def db_init() -> None:
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                url TEXT,
                format TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS searches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                query TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS recognitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        conn.commit()


def db_add_user(user_id: int, username: str | None, first_name: str | None) -> None:
    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, username or "", first_name or ""),
            )
            conn.commit()
    except Exception:
        logger.exception("db_add_user failed")


def db_log_download(user_id: int, url: str, file_format: str) -> None:
    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO downloads (user_id, url, format) VALUES (?, ?, ?)",
                (user_id, url, file_format),
            )
            conn.commit()
    except Exception:
        logger.exception("db_log_download failed")


def db_log_search(user_id: int, query: str) -> None:
    try:
        with db_connect() as conn:
            conn.execute(
                "INSERT INTO searches (user_id, query) VALUES (?, ?)",
                (user_id, query),
            )
            conn.commit()
    except Exception:
        logger.exception("db_log_search failed")


def db_log_recognition(user_id: int) -> None:
    try:
        with db_connect() as conn:
            conn.execute("INSERT INTO recognitions (user_id) VALUES (?)", (user_id,))
            conn.commit()
    except Exception:
        logger.exception("db_log_recognition failed")


def db_get_stats() -> dict[str, int]:
    try:
        with db_connect() as conn:
            return {
                "users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
                "downloads": conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0],
                "searches": conn.execute("SELECT COUNT(*) FROM searches").fetchone()[0],
                "recognitions": conn.execute("SELECT COUNT(*) FROM recognitions").fetchone()[0],
            }
    except Exception:
        logger.exception("db_get_stats failed")
        return {"users": 0, "downloads": 0, "searches": 0, "recognitions": 0}


def is_url(text: str) -> bool:
    lowered = text.lower()
    return lowered.startswith(("http://", "https://")) and any(site in lowered for site in SUPPORTED_SITES)


def fmt_duration(seconds: Any) -> str:
    try:
        total = int(seconds or 0)
    except (TypeError, ValueError):
        total = 0
    minutes, seconds = divmod(total, 60)
    return f"{minutes}:{seconds:02d}"


def escape_html(value: str) -> str:
    return html.escape(value or "")


def cleanup_file(path: str | Path | None) -> None:
    try:
        if path:
            Path(path).unlink(missing_ok=True)
    except Exception:
        logger.exception("cleanup_file failed")


def get_upload_chat_action(mode: str) -> str:
    if mode == "audio":
        return ChatAction.UPLOAD_DOCUMENT
    return ChatAction.UPLOAD_VIDEO


def get_user_cache(context: ContextTypes.DEFAULT_TYPE, name: str) -> dict[str, Any]:
    cache = context.user_data.setdefault(name, {})
    if not isinstance(cache, dict):
        cache = {}
        context.user_data[name] = cache
    return cache


def remember_payload(context: ContextTypes.DEFAULT_TYPE, bucket: str, payload: dict[str, Any]) -> str:
    storage = get_user_cache(context, bucket)
    key = uuid.uuid4().hex[:12]
    storage[key] = payload
    return key


def get_payload(context: ContextTypes.DEFAULT_TYPE, bucket: str, key: str) -> dict[str, Any] | None:
    storage = get_user_cache(context, bucket)
    value = storage.get(key)
    return value if isinstance(value, dict) else None


def get_source_name(url: str) -> str:
    lowered = url.lower()
    if "youtube" in lowered or "youtu.be" in lowered:
        return "YouTube"
    if "instagram" in lowered:
        return "Instagram"
    if "tiktok" in lowered:
        return "TikTok"
    if "facebook" in lowered or "fb.watch" in lowered:
        return "Facebook"
    return "Link"


async def search_music(query: str, max_results: int = 40) -> list[dict[str, Any]]:
    def _run() -> list[dict[str, Any]]:
        opts = {**BASE_YTDLP_OPTS, "extract_flat": True}
        with yt_dlp.YoutubeDL(opts) as ydl:
            result = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
        entries = result.get("entries") or []
        found: list[dict[str, Any]] = []
        for entry in entries:
            if not entry:
                continue
            video_id = entry.get("id")
            if not video_id:
                continue
            found.append(
                {
                    "title": entry.get("title") or "Noma'lum",
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "duration": entry.get("duration") or 0,
                }
            )
        return found

    return await asyncio.to_thread(_run)


async def download_media(url: str, mode: str) -> tuple[str, dict[str, Any]]:
    def _run() -> tuple[str, dict[str, Any]]:
        template = str(DOWNLOAD_DIR / "%(title).80s.%(ext)s")
        opts = {**BASE_YTDLP_OPTS, "outtmpl": template}

        if mode == "audio":
            opts.update(
                {
                    "format": "bestaudio/best",
                    "postprocessors": [
                        {
                            "key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3",
                            "preferredquality": "192",
                        }
                    ],
                }
            )
        else:
            opts.update(
                {
                    "format": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
                    "merge_output_format": "mp4",
                }
            )

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            path = Path(ydl.prepare_filename(info))

        if mode == "audio":
            path = path.with_suffix(".mp3")
            if not path.exists():
                requested = info.get("requested_downloads") or []
                for item in requested:
                    filepath = item.get("filepath")
                    if filepath:
                        candidate = Path(filepath).with_suffix(".mp3")
                        if candidate.exists():
                            path = candidate
                            break

            if not path.exists():
                stem = path.stem
                matches = sorted(
                    DOWNLOAD_DIR.glob(f"{stem}*.mp3"),
                    key=lambda item: item.stat().st_mtime,
                    reverse=True,
                )
                if matches:
                    path = matches[0]
        elif path.suffix.lower() != ".mp4":
            path = path.with_suffix(".mp4")

        return str(path), info

    return await asyncio.to_thread(_run)


async def trim_audio_for_recognition(source_path: Path) -> Path:
    trimmed_path = source_path.with_name(f"{source_path.stem}_trimmed.mp3")

    def _run() -> Path:
        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(source_path),
                "-t",
                "15",
                "-acodec",
                "libmp3lame",
                "-ar",
                "44100",
                "-ab",
                "128k",
                str(trimmed_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 or not trimmed_path.exists() or trimmed_path.stat().st_size == 0:
            raise RuntimeError(result.stderr.strip() or "ffmpeg trim failed")
        return trimmed_path

    return await asyncio.to_thread(_run)


async def recognize_audio(file_path: str) -> dict[str, str] | None:
    if not (ACR_HOST and ACR_KEY and ACR_SECRET):
        logger.warning("ACRCloud credentials not configured")
        return None

    source_path = Path(file_path)
    trimmed_path: Path | None = None

    try:
        try:
            trimmed_path = await trim_audio_for_recognition(source_path)
            use_path = trimmed_path
        except Exception:
            logger.warning("Audio trim failed, using original file", exc_info=True)
            use_path = source_path

        sample_bytes = use_path.read_bytes()
        timestamp = str(int(time.time()))
        string_to_sign = "\n".join(
            ["POST", "/v1/identify", ACR_KEY, "audio", "1", timestamp]
        )
        signature = base64.b64encode(
            hmac.new(
                ACR_SECRET.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                hashlib.sha1,
            ).digest()
        ).decode("utf-8")

        form = aiohttp.FormData()
        form.add_field("sample", sample_bytes, filename=use_path.name, content_type="audio/mpeg")
        form.add_field("sample_bytes", str(len(sample_bytes)))
        form.add_field("access_key", ACR_KEY)
        form.add_field("data_type", "audio")
        form.add_field("signature_version", "1")
        form.add_field("signature", signature)
        form.add_field("timestamp", timestamp)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=45)) as session:
            async with session.post(f"https://{ACR_HOST}/v1/identify", data=form) as response:
                data = await response.json(content_type=None)

        status = (data.get("status") or {}).get("code")
        if status != 0:
            logger.info("ACRCloud did not identify audio: %s", data.get("status"))
            return None

        metadata = data.get("metadata") or {}
        musics = metadata.get("music") or []
        if not musics:
            return None

        track = musics[0]
        artists = ", ".join(artist.get("name", "") for artist in track.get("artists", []) if artist.get("name"))
        album_data = track.get("album") or {}
        release_date = track.get("release_date") or "-"

        return {
            "title": track.get("title") or "Noma'lum",
            "artist": artists or "Noma'lum",
            "album": album_data.get("name") or "-",
            "release_date": release_date,
        }
    except Exception:
        logger.exception("recognize_audio failed")
        return None
    finally:
        cleanup_file(trimmed_path)


async def show_search_page(message, context: ContextTypes.DEFAULT_TYPE, page: int) -> None:
    results = context.user_data.get("search_results", [])
    if not results:
        await message.edit_text("Natijalar topilmadi.")
        return

    per_page = 10
    total = len(results)
    total_pages = max((total + per_page - 1) // per_page, 1)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    end = min(start + per_page, total)
    page_results = results[start:end]

    lines = [f"Natijalar {start + 1}-{end} / {total}\n"]
    buttons: list[list[InlineKeyboardButton]] = []

    for index, item in enumerate(page_results, start=start + 1):
        title = item.get("title") or "Noma'lum"
        lines.append(f"{index}. {escape_html(title[:70])} - {fmt_duration(item.get('duration'))}")
        key = remember_payload(context, "download_requests", {"url": item["url"], "source": "search"})
        buttons.append(
            [
                InlineKeyboardButton(
                    f"{index}. {title[:35]}",
                    callback_data=f"pick_audio:{key}",
                )
            ]
        )

    navigation: list[InlineKeyboardButton] = []
    if page > 0:
        navigation.append(InlineKeyboardButton("◀️ Oldingi", callback_data=f"page:{page - 1}"))
    navigation.append(InlineKeyboardButton("✕ Yopish", callback_data="cancel"))
    if end < total:
        navigation.append(InlineKeyboardButton("Keyingi ▶️", callback_data=f"page:{page + 1}"))
    buttons.append(navigation)

    await message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode=ParseMode.HTML,
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    db_add_user(user.id, user.username, user.first_name)
    first_name = escape_html(user.first_name or "do'st")

    keyboard = [
        [
            InlineKeyboardButton("Qo'llanma", callback_data="help"),
            InlineKeyboardButton("Statistika", callback_data="stats"),
        ]
    ]
    text = (
        f"Salom, <b>{first_name}</b>!\n\n"
        "Bu bot quyidagilarni bajaradi:\n"
        "1. Linkdan MP3 yoki MP4 yuklab beradi.\n"
        "2. Qo'shiq nomi bo'yicha YouTube qidiradi.\n"
        "3. Audio yoki voice xabardan qo'shiqni aniqlaydi."
    )
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "<b>Qo'llanma</b>\n\n"
        "Link yuborsangiz, bot MP3 yoki MP4 taklif qiladi.\n"
        "Qo'shiq nomini yozsangiz, YouTube dan natijalar chiqaradi.\n"
        "Audio yoki voice yuborsangiz, bot qo'shiqni aniqlashga harakat qiladi.\n\n"
        "Buyruqlar:\n"
        "/start\n"
        "/help\n"
        "/stats"
    )
    if update.callback_query:
        await update.callback_query.message.edit_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stats = db_get_stats()
    text = (
        "<b>Statistika</b>\n\n"
        f"Foydalanuvchilar: <b>{stats['users']}</b>\n"
        f"Yuklamalar: <b>{stats['downloads']}</b>\n"
        f"Qidiruvlar: <b>{stats['searches']}</b>\n"
        f"Audio aniqlashlar: <b>{stats['recognitions']}</b>"
    )
    if update.callback_query:
        await update.callback_query.message.edit_text(text, parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()
    if not text:
        return

    if is_url(text):
        request_key = remember_payload(
            context,
            "download_requests",
            {"url": text, "source": get_source_name(text)},
        )
        keyboard = [
            [
                InlineKeyboardButton("MP3 yuklab olish", callback_data=f"audio:{request_key}"),
                InlineKeyboardButton("MP4 yuklab olish", callback_data=f"video:{request_key}"),
            ],
            [InlineKeyboardButton("Bekor qilish", callback_data="cancel")],
        ]
        await update.message.reply_text(
            f"<b>{escape_html(get_source_name(text))}</b> link aniqlandi.\nQaysi format kerak?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML,
        )
        return

    db_log_search(update.effective_user.id, text)
    waiting = await update.message.reply_text(
        f"Qidirilmoqda: <i>{escape_html(text)}</i>",
        parse_mode=ParseMode.HTML,
    )
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

    try:
        results = await search_music(text, max_results=40)
    except Exception:
        logger.exception("search_music failed")
        await waiting.edit_text("Qidiruvda xatolik yuz berdi. Keyinroq qayta urinib ko'ring.")
        return

    if not results:
        await waiting.edit_text(
            "Hech narsa topilmadi.\nArtist va qo'shiq nomini to'liqroq yozib ko'ring."
        )
        return

    context.user_data["search_results"] = results
    await show_search_page(waiting, context, page=0)


async def handle_audio(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    media = update.message.audio or update.message.voice
    if not media:
        return

    waiting = await update.message.reply_text(
        "Qo'shiq aniqlanmoqda. Biroz kuting...",
        parse_mode=ParseMode.HTML,
    )
    await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)

    suffix = ".mp3" if update.message.audio else ".ogg"
    file_path = DOWNLOAD_DIR / f"recognize_{update.effective_user.id}_{uuid.uuid4().hex[:8]}{suffix}"

    try:
        telegram_file = await context.bot.get_file(media.file_id)
        await telegram_file.download_to_drive(str(file_path))

        result = await recognize_audio(str(file_path))
        db_log_recognition(update.effective_user.id)

        if not result:
            await waiting.edit_text(
                "Qo'shiq aniqlanmadi.\nAudio tozaroq yoki uzunroq bo'lsa, qayta urinib ko'ring."
            )
            return

        search_query = f"{result['artist']} {result['title']}"
        search_results = await search_music(search_query, max_results=1)
        buttons: list[list[InlineKeyboardButton]] = []
        if search_results:
            key = remember_payload(
                context,
                "download_requests",
                {"url": search_results[0]["url"], "source": "recognition"},
            )
            buttons.append(
                [InlineKeyboardButton("MP3 yuklab olish", callback_data=f"audio:{key}")]
            )

        await waiting.edit_text(
            "<b>Qo'shiq aniqlandi</b>\n\n"
            f"Nomi: <b>{escape_html(result['title'])}</b>\n"
            f"Artist: <b>{escape_html(result['artist'])}</b>\n"
            f"Album: <b>{escape_html(result['album'])}</b>\n"
            f"Sana: <b>{escape_html(result['release_date'])}</b>",
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None,
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        logger.exception("handle_audio failed")
        await waiting.edit_text("Audio tahlilida xatolik yuz berdi.")
    finally:
        cleanup_file(file_path)


async def send_download(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    mode: str,
    url: str,
    keep_search: bool = False,
) -> None:
    """
    keep_search=True  → qidiruv jadvali xabari o'zgarmaydi,
                         holat va fayl alohida yangi xabar sifatida yuboriladi.
    keep_search=False → eski xabar o'rnida holat ko'rsatiladi (link orqali yuklab olish).
    """
    title_label = "MP3" if mode == "audio" else "MP4"
    chat_id = query.message.chat_id

    # Holat xabarini qayerga chiqarish
    if keep_search:
        # Jadval xabarini teginmasdan, yangi holat xabari yuborish
        status_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ <b>{title_label}</b> tayyorlanmoqda...",
            parse_mode=ParseMode.HTML,
        )
    else:
        await query.message.edit_text(
            f"⏳ {title_label} tayyorlanmoqda...\nYuklab olinmoqda, biroz kuting.",
            parse_mode=ParseMode.HTML,
        )
        status_msg = query.message

    action = get_upload_chat_action(mode)
    await context.bot.send_chat_action(chat_id, action)

    file_path: str | None = None
    try:
        file_path, info = await download_media(url, mode)
        if not file_path or not Path(file_path).exists():
            raise FileNotFoundError("Downloaded file not found")

        title = str(info.get("title") or "Noma'lum")
        duration = int(info.get("duration") or 0)

        file_size = Path(file_path).stat().st_size
        if file_size > 50 * 1024 * 1024:
            await status_msg.edit_text("❌ Fayl hajmi 50 MB dan katta. Yuborib bo'lmadi.")
            return

        await status_msg.edit_text("📤 Fayl tayyor. Yuborilmoqda...", parse_mode=ParseMode.HTML)

        with open(file_path, "rb") as media_file:
            if mode == "audio":
                await context.bot.send_audio(
                    chat_id=chat_id,
                    audio=media_file,
                    title=title[:64],
                    duration=duration,
                    caption=f"<b>{escape_html(title[:100])}</b>",
                    parse_mode=ParseMode.HTML,
                    write_timeout=300,
                    read_timeout=300,
                    connect_timeout=60,
                )
            else:
                await context.bot.send_video(
                    chat_id=chat_id,
                    video=media_file,
                    duration=duration,
                    caption=f"<b>{escape_html(title[:100])}</b>",
                    parse_mode=ParseMode.HTML,
                    write_timeout=300,
                    read_timeout=300,
                    connect_timeout=60,
                )

        db_log_download(query.from_user.id, url, mode)

        # Holat xabarini o'chir (fayl yuborilgandan so'ng keraksiz)
        try:
            await status_msg.delete()
        except Exception:
            pass

    except Exception:
        logger.exception("send_download failed")
        try:
            await status_msg.edit_text(
                "❌ Yuklab olishda xatolik yuz berdi.\nLinkni tekshirib, qayta urinib ko'ring."
            )
        except Exception:
            pass
    finally:
        cleanup_file(file_path)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    # ── "Yopish" tugmasi: xabarni o'chirmasdan tugmalarni olib tashlaydi ──
    if data == "cancel":
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if data == "help":
        await help_command(update, context)
        return

    if data == "stats":
        await stats_command(update, context)
        return

    if data.startswith("page:"):
        await show_search_page(query.message, context, int(data.split(":", 1)[1]))
        return

    # ── Qo'shiq tanlandi (qidiruv natijasidan) ──
    if data.startswith("pick_audio:"):
        key = data.split(":", 1)[1]
        payload = get_payload(context, "download_requests", key)
        if not payload:
            await query.answer("So'rov eskirgan! Qayta qidiring.", show_alert=True)
            return
        # keep_search=True: jadval xabari o'chmaydi, ko'proq qo'shiq tanlanishi mumkin
        await send_download(query, context, "audio", payload["url"], keep_search=True)
        return

    # ── Saqlangan faylni yuborish (shorts/reels) ──
    if data.startswith("send_file:"):
        key = data.split(":", 1)[1]
        payload = get_payload(context, "download_requests", key)
        if not payload:
            await query.message.edit_text("So'rov eskirgan. Qayta urinib ko'ring.")
            return

        stored_path = payload.get("file_path")
        if not stored_path or not Path(stored_path).exists():
            await send_download(query, context, "audio", payload["url"])
            return

        await query.message.edit_text("Yuborilmoqda...", parse_mode=ParseMode.HTML)
        try:
            with open(stored_path, "rb") as f:
                await context.bot.send_audio(
                    chat_id=query.message.chat_id,
                    audio=f,
                    caption="<b>Shorts audio</b>",
                    parse_mode=ParseMode.HTML,
                    write_timeout=300,
                    read_timeout=300,
                    connect_timeout=60,
                )
            db_log_download(query.from_user.id, payload["url"], "audio")
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            logger.exception("send_file callback failed")
            await query.message.edit_text("Yuborishda xatolik yuz berdi.")
        finally:
            cleanup_file(stored_path)
        return

    # ── MP3 / MP4 yuklash (link orqali) ──
    if data.startswith("audio:") or data.startswith("video:"):
        mode, key = data.split(":", 1)
        payload = get_payload(context, "download_requests", key)
        if not payload:
            await query.message.edit_text("So'rov eskirgan yoki topilmadi. Qayta urinib ko'ring.")
            return
        await send_download(query, context, mode, payload["url"])


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled bot error", exc_info=context.error)


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN topilmadi. bot.env faylini tekshiring.")

    db_init()
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=500,
        write_timeout=500,
        connect_timeout=50,
    )
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.AUDIO | filters.VOICE, handle_audio))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(error_handler)

    webhook_url = _read_env_value("WEBHOOK_URL")

    async with app:
        await app.initialize()
        if webhook_url:
            port = int(os.getenv("PORT", "8080"))
            logger.info("Webhook rejimida ishga tushdi: %s", webhook_url)
            await app.bot.set_webhook(
                url=f"{webhook_url.rstrip('/')}/{BOT_TOKEN}",
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
            await app.start()
            await app.updater.start_webhook(
                listen="0.0.0.0",
                port=port,
                url_path=BOT_TOKEN,
            )
            await asyncio.Event().wait()
        else:
            logger.info("Polling rejimida ishga tushdi")
            await app.updater.start_polling(
                allowed_updates=Update.ALL_TYPES,
                drop_pending_updates=True,
            )
            await app.start()
            await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
