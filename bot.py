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


@web_app.get("/stream/{file_id:path}")
async def stream_video(file_id: str, request: Request):
    """
    Proxy-stream a Telegram file to the browser with Range support.
    file_id can be the Telegram file_id (unique_id won't work — use file_id).
    """
    # Step 1: resolve file path via getFile
    get_file_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(get_file_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json()
    except Exception as e:
        logger.error("getFile error: %s", e)
        raise HTTPException(502, "Could not contact Telegram API")

    if not data.get("ok"):
        logger.warning("getFile not ok: %s", data)
        raise HTTPException(404, "File not found on Telegram")

    file_path = data["result"]["file_path"]
    tg_url    = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    file_size = data["result"].get("file_size", 0)

    # Step 2: forward Range header (enables seeking in Chrome)
    range_header = request.headers.get("range")
    req_headers  = {"User-Agent": "TeraBot/1.0"}
    if range_header:
        req_headers["Range"] = range_header

    # Step 3: stream from Telegram → browser
    async def generator():
        async with aiohttp.ClientSession() as session:
            async with session.get(
                tg_url,
                headers=req_headers,
                timeout=aiohttp.ClientTimeout(total=0),   # unlimited
            ) as tg_resp:
                async for chunk in tg_resp.content.iter_chunked(256 * 1024):
                    yield chunk

    # Detect content type
    ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else "mp4"
    mime_map = {
        "mp4": "video/mp4", "mkv": "video/x-matroska",
        "avi": "video/x-msvideo", "mov": "video/quicktime",
        "webm": "video/webm", "ts": "video/mp2t",
    }
    content_type = mime_map.get(ext, "video/mp4")

    # Build response headers
    resp_headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
        "Cache-Control": "no-cache",
    }
    if file_size:
        resp_headers["Content-Length"] = str(file_size)

    status_code = 206 if range_header else 200
    return StreamingResponse(generator(), status_code=status_code, headers=resp_headers)


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
XAPI_BASE = "https://xapiverse.com/api"

# xapiverse API key HEADER mein jaati hai: "xAPIverse-Key: <key>"
# Endpoint formats (url param naam alag ho sakta hai)
XAPI_ENDPOINTS = [
    # Format 1: GET /api/terabox?url=...   (key header mein)
    lambda base, url, key: (
        "GET", f"{base}/terabox", {"url": url}, None, {"xAPIverse-Key": key}
    ),
    # Format 2: GET /api/terabox?link=...  (key header mein)
    lambda base, url, key: (
        "GET", f"{base}/terabox", {"link": url}, None, {"xAPIverse-Key": key}
    ),
    # Format 3: POST /api/terabox JSON     (key header mein)
    lambda base, url, key: (
        "POST", f"{base}/terabox", {}, {"url": url}, {"xAPIverse-Key": key}
    ),
    # Format 4: GET /api/download?url=...  (key header mein)
    lambda base, url, key: (
        "GET", f"{base}/download", {"url": url}, None, {"xAPIverse-Key": key}
    ),
]

def _extract_dl(data: dict) -> str | None:
    """Extract download URL from any known xapiverse response shape."""
    # Direct fields
    for field in ("download_url", "url", "link", "direct_link", "dlink", "download"):
        val = data.get(field)
        if val and val.startswith("http"):
            return val
    # Nested under "data"
    nested = data.get("data") or data.get("result") or {}
    if isinstance(nested, dict):
        for field in ("download_url", "url", "link", "direct_link", "dlink"):
            val = nested.get(field)
            if val and val.startswith("http"):
                return val
    # List of results → take first
    if isinstance(nested, list) and nested:
        item = nested[0]
        for field in ("download_url", "url", "link", "dlink"):
            val = item.get(field, "") if isinstance(item, dict) else ""
            if val and val.startswith("http"):
                return val
    return None

def _extract_title(data: dict) -> str:
    for field in ("title", "name", "filename", "file_name"):
        v = data.get(field)
        if v:
            return v
    nested = data.get("data") or data.get("result") or {}
    if isinstance(nested, dict):
        for field in ("title", "name", "filename"):
            v = nested.get(field)
            if v:
                return v
    return "video"

async def xapi_get_download_link(terabox_url: str) -> dict | None:
    """
    Try all 8 API keys × all endpoint formats until one succeeds.
    Logs full response so we can debug new API shapes easily.
    """
    tried_keys = set()
    for _ in range(len(XAPI_KEYS)):
        key = await next_api_key()
        if key in tried_keys:
            continue
        tried_keys.add(key)

        for endpoint_fn in XAPI_ENDPOINTS:
            method, url, params, json_body, req_headers = endpoint_fn(XAPI_BASE, terabox_url, key)
            try:
                async with aiohttp.ClientSession() as session:
                    if method == "GET":
                        req = session.get(url, params=params, headers=req_headers, timeout=aiohttp.ClientTimeout(total=30))
                    else:
                        req = session.post(url, params=params, json=json_body, headers=req_headers, timeout=aiohttp.ClientTimeout(total=30))

                    async with req as resp:
                        raw = await resp.text()
                        logger.info("xapi [%s %s key=…%s] status=%s body=%s",
                                    method, url, key[-6:], resp.status, raw[:300])
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            continue

                        if resp.status == 200:
                            dl = _extract_dl(data)
                            if dl:
                                return {
                                    "download_url": dl,
                                    "title": _extract_title(data),
                                    "size": (
                                        data.get("size")
                                        or (data.get("data") or {}).get("size")
                                        or 0
                                    ),
                                }
            except Exception as e:
                logger.warning("xapi [%s key=…%s] exception: %s", url, key[-6:], e)

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

def build_stream_url(file_id: str) -> str:
    return f"{PUBLIC_URL}/stream/{file_id}"


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
                    async with aiofiles.open(video_path, "rb") as vf:
                        video_bytes = await vf.read()

                    sent = await context.bot.send_video(
                        chat_id=PRIVATE_CHANNEL_ID,
                        video=video_bytes,
                        caption=f"🎬 {safe_title}\n\nSource: {first_url}",
                        supports_streaming=True,
                        filename=f"{safe_title}.mp4",
                    )

                    # Use file_id for streaming (works for any size)
                    vid_file_id  = sent.video.file_id
                    stream_url   = build_stream_url(vid_file_id)

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
