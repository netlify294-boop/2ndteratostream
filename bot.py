"""
TeraBot — Smart hybrid streaming:
  - Files <= 20MB  → Telegram CDN (very fast)
  - Files >  20MB  → Telethon MTProto stream (no size limit)
  - Token-based secure URLs (unguessable)
  - No caption on channel upload
  - Public channel used
"""

import os, re, asyncio, threading, secrets, aiohttp, aiofiles, logging, tempfile
from itertools import cycle
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse

from telegram import Update, Message
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler
from telegram.constants import ParseMode
from telegram.error import TelegramError

from telethon import TelegramClient
from telethon.sessions import StringSession

# ═══════════════════════ Logging ════════════════════════
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("TeraBot")

# ═══════════════════════ Config ═════════════════════════
BOT_TOKEN         = os.environ["BOT_TOKEN"]
ADMIN_IDS         = set(int(x.strip()) for x in os.environ.get("ADMIN_IDS","").split(",") if x.strip())
PUBLIC_CHANNEL_ID = int(os.environ["PUBLIC_CHANNEL_ID"])

TELEGRAM_API_ID   = int(os.environ["TELEGRAM_API_ID"])
TELEGRAM_API_HASH = os.environ["TELEGRAM_API_HASH"]
TELETHON_SESSION  = os.environ.get("TELETHON_SESSION", "")

PUBLIC_URL = (os.environ.get("RENDER_EXTERNAL_URL") or os.environ.get("PUBLIC_URL","http://localhost:8000")).rstrip("/")
PORT       = int(os.environ.get("PORT", 8000))

XAPI_KEYS = [k.strip() for k in os.environ.get("XAPI_KEYS","").split(",") if k.strip()]
if not XAPI_KEYS:
    raise ValueError("XAPI_KEYS is empty")

_api_key_cycle = cycle(XAPI_KEYS)
_api_key_lock  = asyncio.Lock()
async def next_api_key():
    async with _api_key_lock:
        return next(_api_key_cycle)

CDN_LIMIT = 20 * 1024 * 1024  # 20MB — Telegram Bot API getFile limit

# ═══════════════ Telethon Pool ══════════════════════════
POOL_SIZE = 5
_telethon_pool = []
_pool_lock  = asyncio.Lock()
_pool_index = 0
_channel_entity = None

async def _make_client():
    c = TelegramClient(StringSession(TELETHON_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH,
                       connection_retries=5, retry_delay=1, flood_sleep_threshold=0, request_retries=3)
    await c.connect()
    if not await c.is_user_authorized():
        raise RuntimeError("Telethon session not authorized. Regenerate TELETHON_SESSION.")
    return c

async def init_telethon_pool():
    global _telethon_pool, _channel_entity
    logger.info("Initializing Telethon pool (size=%d)...", POOL_SIZE)
    _telethon_pool = []
    for i in range(POOL_SIZE):
        c = await _make_client()
        _telethon_pool.append(c)
        logger.info("Telethon client %d ready", i+1)
    _channel_entity = await _telethon_pool[0].get_entity(PUBLIC_CHANNEL_ID)
    logger.info("Channel entity resolved: %s", _channel_entity)

async def get_telethon_client():
    global _pool_index
    async with _pool_lock:
        c = _telethon_pool[_pool_index % POOL_SIZE]
        _pool_index += 1
    if not c.is_connected():
        await c.connect()
    return c

# ═══════════════ Token Store ════════════════════════════
# token → {"file_id": str, "size": int, "msg_id": int}
_token_store: dict[str, dict] = {}
_store_lock = asyncio.Lock()

async def create_token(file_id: str, size: int, msg_id: int) -> str:
    token = secrets.token_hex(32)
    async with _store_lock:
        _token_store[token] = {"file_id": file_id, "size": size, "msg_id": msg_id}
    return token

async def get_token_data(token: str) -> dict | None:
    async with _store_lock:
        return _token_store.get(token)

# ═══════════════ Regex ══════════════════════════════════
TERABOX_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:terabox\.com|1024terabox\.com|1024tera\.com|teraboxapp\.com|"
    r"terabox\.app|nephobox\.com|mirrorbox\.com|mirrobox\.com|momerybox\.com|freeterabox\.com|"
    r"teraboxlink\.com|4funbox\.com|terafileshare\.com|teraboxshare\.com|terasharelink\.com|"
    r"tibibox\.com|terabox\.fun)/[^\s\"\'><]+",
    re.IGNORECASE,
)

# ══════════════════ FastAPI ══════════════════════════════
web_app = FastAPI(title="TeraBot Stream Server")

@web_app.get("/", response_class=HTMLResponse)
async def index():
    return "<h2>TeraBot is running ✅</h2>"

@web_app.get("/health")
async def health():
    return {"status": "ok"}

@web_app.on_event("startup")
async def startup_event():
    await init_telethon_pool()


@web_app.get("/stream/{token}")
async def stream_video(token: str, request: Request):
    data = await get_token_data(token)
    if not data:
        raise HTTPException(404, "Stream link not found or expired.")

    file_size = data["size"]
    use_cdn   = file_size <= CDN_LIMIT

    range_header = request.headers.get("range", "")

    if use_cdn:
        # ── Fast path: Telegram CDN ──────────────────────
        get_file_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={data['file_id']}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(get_file_url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                    gf = await r.json()
        except Exception as e:
            raise HTTPException(502, f"Telegram API error: {e}")

        if not gf.get("ok"):
            raise HTTPException(404, "File not found on Telegram")

        file_path = gf["result"]["file_path"]
        cdn_url   = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

        req_hdrs = {"User-Agent": "TeraBot/1.0"}
        if range_header:
            req_hdrs["Range"] = range_header

        async def cdn_generator():
            async with aiohttp.ClientSession() as session:
                async with session.get(cdn_url, headers=req_hdrs, timeout=aiohttp.ClientTimeout(total=0)) as r:
                    async for chunk in r.content.iter_chunked(512*1024):
                        yield chunk

        resp_headers = {"Accept-Ranges": "bytes", "Content-Type": "video/mp4",
                        "Cache-Control": "no-cache", "Content-Length": str(file_size)}
        return StreamingResponse(cdn_generator(), status_code=206 if range_header else 200, headers=resp_headers)

    else:
        # ── Large file path: Telethon MTProto ────────────
        try:
            client   = await get_telethon_client()
            messages = await client.get_messages(_channel_entity, ids=data["msg_id"])
            if not messages or not messages.media:
                raise HTTPException(404, "Message not found")

            doc = getattr(messages.media, "document", None) or getattr(messages.media, "video", None)
            if not doc:
                raise HTTPException(404, "No video in message")

            start, end = 0, file_size - 1
            if range_header.startswith("bytes="):
                parts = range_header[6:].split("-")
                start = int(parts[0]) if parts[0] else 0
                end   = int(parts[1]) if len(parts) > 1 and parts[1] else file_size - 1

            content_length = end - start + 1
            PART           = 512 * 1024
            aligned_start  = (start // PART) * PART
            skip_bytes     = start - aligned_start

            async def telethon_generator():
                remaining = content_length
                first     = True
                async for chunk in client.iter_download(doc, offset=aligned_start, request_size=PART, stride=PART, limit=None):
                    if first and skip_bytes:
                        chunk = chunk[skip_bytes:]
                        first = False
                    if remaining <= 0:
                        break
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                    remaining -= len(chunk)
                    yield chunk

            resp_headers = {
                "Content-Type":   "video/mp4",
                "Accept-Ranges":  "bytes",
                "Content-Length": str(content_length),
                "Content-Range":  f"bytes {start}-{end}/{file_size}",
                "Cache-Control":  "no-cache",
            }
            return StreamingResponse(telethon_generator(), status_code=206 if range_header else 200, headers=resp_headers)

        except HTTPException:
            raise
        except Exception as e:
            logger.error("Telethon stream error: %s", e)
            raise HTTPException(500, f"Stream error: {e}")


# ═══════════════════ Catbox ═════════════════════════════
async def upload_to_catbox(image_bytes: bytes, filename="image.jpg") -> str | None:
    form = aiohttp.FormData()
    form.add_field("reqtype", "fileupload")
    form.add_field("userhash", "")
    form.add_field("fileToUpload", image_bytes, filename=filename, content_type="image/jpeg")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://catbox.moe/user/api.php", data=form, timeout=aiohttp.ClientTimeout(total=60)) as r:
                text = await r.text()
                if r.status == 200 and text.startswith("https://"):
                    return text.strip()
    except Exception as e:
        logger.error("Catbox: %s", e)
    return None

# ═══════════════════ URL Resolver ═══════════════════════
async def resolve_terabox_url(url: str) -> str:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=15),
                             headers={"User-Agent": "Mozilla/5.0"}) as r:
                return str(r.url)
    except:
        return url

# ═══════════════════ xapiverse ══════════════════════════
async def xapi_get_download_link(terabox_url: str) -> dict | None:
    tried = set()
    for _ in range(len(XAPI_KEYS)):
        key = await next_api_key()
        if key in tried:
            continue
        tried.add(key)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://xapiverse.com/api/terabox",
                                  json={"url": terabox_url},
                                  headers={"Content-Type":"application/json","xAPIverse-Key":key},
                                  timeout=aiohttp.ClientTimeout(total=30)) as r:
                    raw = await r.text()
                    logger.info("xapi [key=...%s] %s %s", key[-6:], r.status, raw[:300])
                    if r.status != 200: continue
                    data = await r.json(content_type=None)
                    if data.get("status") != "success": continue
                    item = (data.get("list") or [{}])[0]
                    dl   = item.get("normal_dlink") or item.get("download_url") or item.get("url")
                    if dl:
                        return {"download_url": dl, "title": item.get("name","video"), "size": item.get("size",0)}
        except Exception as e:
            logger.warning("xapi: %s", e)
    return None

# ═══════════════════ Video Download ═════════════════════
async def download_video(url: str) -> str | None:
    ext = ".mp4"
    p   = urlparse(url).path
    if "." in p.split("/")[-1]:
        c = "." + p.split("/")[-1].split(".")[-1].split("?")[0]
        if len(c) <= 5: ext = c
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext, prefix="terabot_")
    tmp.close()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=900), headers={"User-Agent":"Mozilla/5.0"}) as r:
                if r.status != 200: return None
                async with aiofiles.open(tmp.name, "wb") as f:
                    async for chunk in r.content.iter_chunked(512*1024):
                        await f.write(chunk)
        return tmp.name
    except Exception as e:
        logger.error("Download: %s", e)
        try: os.unlink(tmp.name)
        except: pass
        return None

# ═══════════════════ Helpers ════════════════════════════
def is_admin(uid): return uid in ADMIN_IDS
async def safe_edit(msg, text):
    try: await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except: pass
def build_stream_url(token): return f"{PUBLIC_URL}/stream/{token}"

# ═══════════════════ Bot Handler ════════════════════════
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message, user = update.effective_message, update.effective_user
    if not message or not user or not is_admin(user.id): return

    text      = message.caption or message.text or ""
    has_photo = bool(message.photo or (message.document and (message.document.mime_type or "").startswith("image/")))
    tb_urls   = TERABOX_PATTERN.findall(text)

    if not has_photo and not tb_urls: return

    status_msg = await message.reply_text("⏳ <b>Processing…</b>", parse_mode=ParseMode.HTML)
    results    = []

    # Image
    if has_photo:
        await safe_edit(status_msg, "🖼️ <b>Uploading image to Catbox…</b>")
        try:
            tg_file   = await message.photo[-1].get_file() if message.photo else await message.document.get_file()
            catbox_url = await upload_to_catbox(bytes(await tg_file.download_as_bytearray()))
            results.append(f"🖼️ <b>Image Preview:</b>\n<a href='{catbox_url}'>{catbox_url}</a>" if catbox_url else "❌ <b>Catbox upload failed.</b>")
        except Exception as e:
            results.append(f"❌ <b>Image error:</b> <code>{e}</code>")

    # Terabox
    if tb_urls:
        first_url = tb_urls[0]
        ignored   = len(tb_urls) - 1

        await safe_edit(status_msg, "🔗 <b>Resolving Terabox link…</b>")
        resolved  = await resolve_terabox_url(first_url)
        xapi_data = await xapi_get_download_link(resolved)

        if not xapi_data:
            results.append(f"❌ <b>xapiverse could not resolve this link.</b>\n<code>{first_url}</code>")
        else:
            safe_title = re.sub(r"[^\w\s\-\.]", "", xapi_data["title"])[:60] or "video"
            await safe_edit(status_msg, f"⬇️ <b>Downloading:</b> <i>{safe_title}</i>\n⏳ Please wait…")

            video_path = await download_video(xapi_data["download_url"])
            if not video_path:
                results.append("❌ <b>Video download failed.</b>")
            else:
                await safe_edit(status_msg, "📤 <b>Uploading to channel…</b>")
                try:
                    with open(video_path, "rb") as vf:
                        sent = await context.bot.send_video(
                            chat_id=PUBLIC_CHANNEL_ID, video=vf, caption=None,
                            supports_streaming=True, filename=f"{safe_title}.mp4",
                            write_timeout=600, read_timeout=600, connect_timeout=30,
                        )
                    file_size  = sent.video.file_size or xapi_data.get("size", 0)
                    token      = await create_token(sent.video.file_id, file_size, sent.message_id)
                    stream_url = build_stream_url(token)
                    method     = "⚡ CDN" if file_size <= CDN_LIMIT else "🔗 Telethon"
                    results.append(
                        f"🎬 <b>{safe_title}</b>\n"
                        f"<i>Stream method: {method}</i>\n\n"
                        f"🌐 <b>Stream Link:</b>\n"
                        f"<code>{stream_url}</code>\n\n"
                        f"<a href='{stream_url}'>▶️ Stream Now</a>"
                    )
                except TelegramError as te:
                    results.append(f"❌ <b>Channel upload failed:</b> <code>{te}</code>")
                finally:
                    try: os.unlink(video_path)
                    except: pass

        if ignored > 0:
            results.append(f"ℹ️ <i>{ignored} extra Terabox link(s) ignored.</i>")

    final = "\n\n".join(results) or "✅ Done."
    try: await status_msg.edit_text(final, parse_mode=ParseMode.HTML)
    except: await message.reply_text(final, parse_mode=ParseMode.HTML)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ Access denied."); return
    await update.message.reply_text(
        "👋 <b>TeraBot ready!</b>\n\n"
        "Forward any post with image or Terabox link.\n"
        "• ≤20MB → ⚡ Telegram CDN (fast)\n"
        "• >20MB → 🔗 Telethon stream (no limit)\n"
        "• 🔐 Secure random URLs",
        parse_mode=ParseMode.HTML)

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    await update.message.reply_text(
        f"✅ <b>Status</b>\n\n"
        f"🔑 API keys: {len(XAPI_KEYS)}\n"
        f"📢 Channel: <code>{PUBLIC_CHANNEL_ID}</code>\n"
        f"🔐 Active tokens: {len(_token_store)}\n"
        f"🌐 URL: <code>{PUBLIC_URL}</code>",
        parse_mode=ParseMode.HTML)

# ═══════════════════ Bot Thread ═════════════════════════
def run_bot():
    import asyncio as _asyncio
    async def _main():
        app = (ApplicationBuilder().token(BOT_TOKEN)
               .write_timeout(600).read_timeout(600).connect_timeout(30).build())
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("status", cmd_status))
        app.add_handler(MessageHandler(
            (filters.TEXT | filters.PHOTO | filters.Document.IMAGE) & ~filters.COMMAND,
            handle_message))
        async with app:
            await app.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
            while True: await _asyncio.sleep(3600)
    loop = _asyncio.new_event_loop()
    _asyncio.set_event_loop(loop)
    loop.run_until_complete(_main())

if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info("Stream server on port %d — %s", PORT, PUBLIC_URL)
    uvicorn.run(web_app, host="0.0.0.0", port=PORT, log_level="info")
