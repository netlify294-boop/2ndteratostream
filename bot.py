"""
TeraBot — Telegram bot with integrated streaming server.

Architecture:
  - FastAPI web server  →  /stream/{file_id}  (HTTP range-request proxy)
  - Telegram bot (polling) runs in a background thread
  - Both share the same process on Render (Worker dyno)

Stream link format:
  https://<your-render-service>.onrender.com/stream/<telegram_file_id>

This bypasses Telegram's 20 MB getFile limit by:
  1. Using the Bot API's /bot{token}/file download endpoint directly
  2. Forwarding Range headers so Chrome's <video> player can seek
"""

import os
import re
import asyncio
import threading
import aiohttp
import aiofiles
import logging
import tempfile
from itertools import cycle
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse

from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder, MessageHandler, filters,
    ContextTypes, CommandHandler
)
from telegram.constants import ParseMode
from telegram.error import TelegramError

from telethon import TelegramClient
from telethon.sessions import StringSession

# ═══════════════════════ Logging ════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("TeraBot")

# ═══════════════════════ Config ═════════════════════════
BOT_TOKEN          = os.environ["BOT_TOKEN"]
ADMIN_IDS_RAW      = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS          = set(int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip())
PRIVATE_CHANNEL_ID = int(os.environ["PRIVATE_CHANNEL_ID"])

# Telethon (MTProto) — needed to stream files > 20MB
# Get API_ID and API_HASH from https://my.telegram.org
TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")  # StringSession string

# Telethon connection pool — multiple clients for concurrent streams
POOL_SIZE = 5
_telethon_pool: list[TelegramClient] = []
_pool_lock = asyncio.Lock()
_pool_index = 0

async def _make_client() -> TelegramClient:
    client = TelegramClient(
        StringSession(TELETHON_SESSION),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        connection_retries=5,
        retry_delay=2,
    )
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError("Telethon session is not authorized. Regenerate TELETHON_SESSION.")
    return client

async def init_telethon_pool():
    """Call once at startup to pre-create all connections."""
    global _telethon_pool
    logger.info("Initializing Telethon pool (size=%d)...", POOL_SIZE)
    _telethon_pool = []
    for i in range(POOL_SIZE):
        client = await _make_client()
        _telethon_pool.append(client)
        logger.info("Telethon client %d connected", i + 1)
    logger.info("Telethon pool ready.")

async def get_telethon_client() -> TelegramClient:
    """Round-robin pick from pool, reconnect if needed."""
    global _pool_index
    async with _pool_lock:
        client = _telethon_pool[_pool_index % POOL_SIZE]
        _pool_index += 1

    if not client.is_connected():
        logger.warning("Telethon client disconnected, reconnecting...")
        await client.connect()
    return client

# Public URL of this Render service (e.g. https://terabot.onrender.com)
# Render sets RENDER_EXTERNAL_URL automatically — fallback to manual setting
PUBLIC_URL = (
    os.environ.get("RENDER_EXTERNAL_URL")
    or os.environ.get("PUBLIC_URL", "http://localhost:8000")
).rstrip("/")

PORT = int(os.environ.get("PORT", 8000))

# 8 xapiverse API keys
XAPI_KEYS_RAW = os.environ.get("XAPI_KEYS", "")
XAPI_KEYS     = [k.strip() for k in XAPI_KEYS_RAW.split(",") if k.strip()]
if not XAPI_KEYS:
    raise ValueError("XAPI_KEYS env variable is empty — add your API keys.")

_api_key_cycle = cycle(XAPI_KEYS)
_api_key_lock  = asyncio.Lock()

async def next_api_key() -> str:
    async with _api_key_lock:
        return next(_api_key_cycle)

# ═══════════════════ Regex Patterns ═════════════════════
TERABOX_PATTERN = re.compile(
    r"https?://(?:www\.)?"
    r"(?:"
    r"terabox\.com|"
    r"1024terabox\.com|1024tera\.com|"
    r"teraboxapp\.com|"
    r"terabox\.app|"
    r"nephobox\.com|"
    r"mirrorbox\.com|mirrobox\.com|"
    r"momerybox\.com|"
    r"freeterabox\.com|"
    r"teraboxlink\.com|"
    r"4funbox\.com|"
    r"terafileshare\.com|"
    r"teraboxshare\.com|"
    r"terasharelink\.com|"
    r"tibibox\.com|"
    r"terabox\.fun"
    r")"
    r"/[^\s\"\'><]+",
    re.IGNORECASE,
)

# ══════════════════ FastAPI App ══════════════════════════
web_app = FastAPI(title="TeraBot Stream Server")

@web_app.get("/", response_class=HTMLResponse)
async def index():
    return "<h2>TeraBot is running ✅</h2>"

@web_app.get("/health")
async def health():
    return {"status": "ok"}

@web_app.on_event("startup")
async def startup_event():
    """Init Telethon pool in FastAPI's event loop — must be done here."""
    await init_telethon_pool()


# Semaphore — max concurrent streams (1 per pool client)
_stream_semaphore = asyncio.Semaphore(POOL_SIZE * 3)

@web_app.get("/stream/{msg_id:int}")
async def stream_video(msg_id: int, request: Request):
    """
    Stream a Telegram channel message's video via Telethon (MTProto).
    Works for ANY file size — no 20MB Bot API limit.
    URL format: /stream/<message_id>
    """
    async with _stream_semaphore:
        try:
            client = await get_telethon_client()
            messages = await client.get_messages(PRIVATE_CHANNEL_ID, ids=msg_id)
            if not messages or not messages.media:
                raise HTTPException(404, "Message not found or has no media")

            media = messages.media
            doc   = getattr(media, "document", None) or getattr(media, "video", None)
            if not doc:
                raise HTTPException(404, "No video in this message")

            file_size = doc.size

            range_header   = request.headers.get("range", "")
            start = 0
            end   = file_size - 1

            if range_header.startswith("bytes="):
                parts = range_header[6:].split("-")
                start = int(parts[0]) if parts[0] else 0
                end   = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1

            content_length = end - start + 1

            async def generator():
                offset    = start
                remaining = content_length
                async for chunk in client.iter_download(doc, offset=offset, request_size=1024*1024):
                    if remaining <= 0:
                        break
                    data = chunk[:remaining]
                    remaining -= len(data)
                    yield data

            status_code = 206 if range_header else 200
            resp_headers = {
                "Content-Type":   "video/mp4",
                "Accept-Ranges":  "bytes",
                "Content-Length": str(content_length),
                "Content-Range":  f"bytes {start}-{end}/{file_size}",
                "Cache-Control":  "no-cache",
            }
            return StreamingResponse(generator(), status_code=status_code, headers=resp_headers)

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Stream error for msg_id=%s: %s", msg_id, e)
            raise HTTPException(500, f"Stream error: {e}")


# ═══════════════════ Catbox Upload ══════════════════════
async def upload_to_catbox(image_bytes: bytes, filename: str = "image.jpg") -> str | None:
    url  = "https://catbox.moe/user/api.php"
    form = aiohttp.FormData()
    form.add_field("reqtype", "fileupload")
    form.add_field("userhash", "")
    form.add_field("fileToUpload", image_bytes, filename=filename, content_type="image/jpeg")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=60)) as r:
                text = await r.text()
                if r.status == 200 and text.startswith("https://"):
                    return text.strip()
                logger.warning("Catbox failed: %s %s", r.status, text)
    except Exception as e:
        logger.error("Catbox exception: %s", e)
    return None


# ══════════════════ URL Resolver ════════════════════════
async def resolve_terabox_url(url: str) -> str:
    """
    Follow redirects to get the final URL.
    Many short/mirror domains (terasharelink, nephobox, etc.)
    redirect to the real terabox.com URL — xapiverse needs that.
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                final = str(resp.url)
                if final != url:
                    logger.info("Resolved %s → %s", url, final)
                return final
    except Exception as e:
        logger.warning("URL resolve failed for %s: %s — using original", url, e)
        return url

# ══════════════════ xapiverse Helper ════════════════════
# API: POST https://xapiverse.com/api/terabox
# Headers: Content-Type: application/json, xAPIverse-Key: <key>
# Body:    {"url": "<terabox_url>"}
# Response: {"status":"success","list":[{"normal_dlink":..,"name":..,"size":..}]}

XAPI_BASE = "https://xapiverse.com/api/terabox"

async def xapi_get_download_link(terabox_url: str) -> dict | None:
    tried_keys = set()

    for _ in range(len(XAPI_KEYS)):
        key = await next_api_key()
        if key in tried_keys:
            continue
        tried_keys.add(key)

        headers = {
            "Content-Type": "application/json",
            "xAPIverse-Key": key,
        }
        payload = {"url": terabox_url}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    XAPI_BASE,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    raw = await resp.text()
                    logger.info("xapi [key=...%s] status=%s body=%s",
                                key[-6:], resp.status, raw[:500])

                    if resp.status != 200:
                        continue

                    data = await resp.json(content_type=None)

                    if data.get("status") != "success":
                        continue

                    file_list = data.get("list") or []
                    if not file_list:
                        continue

                    item  = file_list[0]
                    title = item.get("name") or "video"
                    size  = item.get("size") or 0

                    # normal_dlink = permanent download link, use this for downloading
                    dl_url = (
                        item.get("normal_dlink")
                        or item.get("download_url")
                        or item.get("url")
                    )

                    if dl_url:
                        return {
                            "download_url": dl_url,
                            "title":        title,
                            "size":         size,
                        }

        except Exception as e:
            logger.warning("xapi key=...%s exception: %s", key[-6:], e)

    return None


# ═══════════════════ Video Download ═════════════════════
async def download_video(url: str) -> str | None:
    ext = ".mp4"
    path_part = urlparse(url).path
    if "." in path_part.split("/")[-1]:
        candidate = "." + path_part.split("/")[-1].split(".")[-1].split("?")[0]
        if len(candidate) <= 5:
            ext = candidate

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="terabot_")
    tmp.close()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=900),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status != 200:
                    logger.error("Video DL HTTP %s", resp.status)
                    return None
                async with aiofiles.open(tmp.name, "wb") as f:
                    async for chunk in resp.content.iter_chunked(512 * 1024):
                        await f.write(chunk)
        return tmp.name
    except Exception as e:
        logger.error("Video DL exception: %s", e)
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
        return None


# ═══════════════════ Helpers ════════════════════════════
def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS

async def safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except TelegramError:
        pass

def build_stream_url(message_id: int) -> str:
    return f"{PUBLIC_URL}/stream/{message_id}"


# ═══════════════════ Bot Handlers ═══════════════════════
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    user    = update.effective_user
    if not message or not user:
        return
    if not is_admin(user.id):
        return

    text     = message.caption or message.text or ""
    has_photo = bool(
        message.photo
        or (message.document and (message.document.mime_type or "").startswith("image/"))
    )
    terabox_urls = TERABOX_PATTERN.findall(text)

    if not has_photo and not terabox_urls:
        return

    status_msg = await message.reply_text("⏳ <b>Processing…</b>", parse_mode=ParseMode.HTML)
    results    = []

    # ── Image handling ───────────────────────────────────
    if has_photo:
        await safe_edit(status_msg, "🖼️ <b>Uploading image to Catbox…</b>")
        try:
            tg_file = (
                await message.photo[-1].get_file()
                if message.photo
                else await message.document.get_file()
            )
            img_bytes  = await tg_file.download_as_bytearray()
            catbox_url = await upload_to_catbox(bytes(img_bytes))
            if catbox_url:
                results.append(
                    f"🖼️ <b>Image Preview:</b>\n"
                    f"<a href='{catbox_url}'>{catbox_url}</a>"
                )
            else:
                results.append("❌ <b>Catbox upload failed.</b>")
        except Exception as e:
            logger.error("Image error: %s", e)
            results.append(f"❌ <b>Image error:</b> <code>{e}</code>")

    # ── Terabox handling (first link only) ───────────────
    if terabox_urls:
        first_url = terabox_urls[0]
        ignored   = len(terabox_urls) - 1

        await safe_edit(status_msg, f"🔗 <b>Resolving Terabox link…</b>\n<code>{first_url}</code>")

        # Follow redirects — mirror domains redirect to actual terabox.com URL
        resolved_url = await resolve_terabox_url(first_url)
        xapi_data = await xapi_get_download_link(resolved_url)
        if not xapi_data:
            results.append(
                f"❌ <b>xapiverse could not resolve this link.</b>\n"
                f"<code>{first_url}</code>"
            )
        else:
            title        = xapi_data["title"] or "video"
            download_url = xapi_data["download_url"]
            safe_title   = re.sub(r"[^\w\s\-\.]", "", title)[:60] or "video"

            await safe_edit(
                status_msg,
                f"⬇️ <b>Downloading:</b> <i>{safe_title}</i>\n"
                f"⏳ Large files may take several minutes…"
            )

            video_path = await download_video(download_url)
            if not video_path:
                results.append("❌ <b>Video download failed.</b>")
            else:
                await safe_edit(status_msg, f"📤 <b>Uploading to private channel…</b>")
                try:
                    # Open as file object — no RAM spike, 10min timeout for large files
                    with open(video_path, "rb") as vf:
                        sent = await context.bot.send_video(
                            chat_id=PRIVATE_CHANNEL_ID,
                            video=vf,
                            caption=f"🎬 {safe_title}\n\nSource: {first_url}",
                            supports_streaming=True,
                            filename=f"{safe_title}.mp4",
                            write_timeout=600,
                            read_timeout=600,
                            connect_timeout=30,
                        )

                    # Use message_id for Telethon streaming (no size limit)
                    stream_url = build_stream_url(sent.message_id)

                    results.append(
                        f"🎬 <b>{safe_title}</b>\n\n"
                        f"🌐 <b>Stream Link (Chrome mein kholo):</b>\n"
                        f"<code>{stream_url}</code>\n\n"
                        f"<a href='{stream_url}'>▶️ Stream Now</a>"
                    )
                except TelegramError as te:
                    logger.error("Channel upload error: %s", te)
                    results.append(f"❌ <b>Channel upload failed:</b> <code>{te}</code>")
                finally:
                    try:
                        os.unlink(video_path)
                    except Exception:
                        pass

        if ignored > 0:
            results.append(f"ℹ️ <i>{ignored} extra Terabox link(s) ignored.</i>")

    final = "\n\n".join(results) or "✅ Done."
    try:
        await status_msg.edit_text(final, parse_mode=ParseMode.HTML)
    except TelegramError:
        await message.reply_text(final, parse_mode=ParseMode.HTML)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Access denied.")
        return
    await update.message.reply_text(
        "👋 <b>TeraBot ready!</b>\n\n"
        "Koi bhi post forward karo jisme ho:\n"
        "• 🖼️ <b>Image</b> → Catbox.moe pe upload\n"
        "• 🔗 <b>Terabox link</b> → Private channel + stream link\n\n"
        f"🌐 Stream server: <code>{PUBLIC_URL}</code>",
        parse_mode=ParseMode.HTML,
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        f"✅ <b>Bot Status</b>\n\n"
        f"🔑 API keys: <b>{len(XAPI_KEYS)}</b>\n"
        f"📢 Channel: <code>{PRIVATE_CHANNEL_ID}</code>\n"
        f"🌐 Stream URL: <code>{PUBLIC_URL}</code>\n"
        f"👤 Admins: {len(ADMIN_IDS)}",
        parse_mode=ParseMode.HTML,
    )


# ═══════════════════ Entrypoint ═════════════════════════
def run_bot():
    """Run the Telegram bot in a separate thread (blocking)."""
    import asyncio as _asyncio

    async def _bot_main():
        app = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .write_timeout(600)
            .read_timeout(600)
            .connect_timeout(30)
            .build()
        )
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(
            MessageHandler(
                (filters.TEXT | filters.PHOTO | filters.Document.IMAGE) & ~filters.COMMAND,
                handle_message,
            )
        )
        logger.info("Telegram bot polling started.")
        async with app:
            await app.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            # Run forever
            while True:
                await _asyncio.sleep(3600)

    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)
    loop.run_until_complete(_bot_main())


if __name__ == "__main__":
    # Start bot in a background daemon thread
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info(f"Stream server starting on port {PORT} — public URL: {PUBLIC_URL}")
    # Start FastAPI server (blocks main thread)
    uvicorn.run(web_app, host="0.0.0.0", port=PORT, log_level="info")
