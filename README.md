# 🤖 TeraBot — Telegram Bot + Streaming Server

Forwarded posts mein image aur Terabox links automatically process karta hai.
Har size ki video ke liye permanent browser stream link deta hai.

## Features
- 🖼️ Image → Catbox.moe pe upload → preview link
- 🔗 Terabox link → xapiverse API (8 keys rotate) → download → private channel → **stream link**
- 🌐 Apna stream server — kisi bhi size ki video Chrome mein seedha stream ho
- Sirf **pehli** Terabox link process hoti hai
- Sirf designated **admins** use kar sakte hain

## Stream Link kaise kaam karta hai

```
Video upload hoti hai private channel mein
         ↓
Bot file_id save karta hai
         ↓
Stream URL: https://terabot.onrender.com/stream/<file_id>
         ↓
Render server Telegram se chunks fetch karke browser ko serve karta hai
Range headers forward hote hain → Chrome mein seeking/scrubbing kaam karta hai
```

---

## Setup

### Step 1 — Telegram Bot
1. [@BotFather](https://t.me/BotFather) → `/newbot`
2. Token copy karo → `BOT_TOKEN`

### Step 2 — Apna User ID
1. [@userinfobot](https://t.me/userinfobot) → `/start`
2. ID copy karo → `ADMIN_IDS`

### Step 3 — Private Channel
1. Telegram mein ek **Private Channel** banao
2. Bot ko channel ka **Admin** banao (Post Messages permission)
3. Channel ID pane ke liye — channel ka koi message [@JsonDumpBot](https://t.me/JsonDumpBot) ko forward karo
4. `"chat": {"id": -100XXXXXXXXXX}` → yeh ID copy karo → `PRIVATE_CHANNEL_ID`

### Step 4 — xapiverse Keys
- [xapiverse.com](https://xapiverse.com) se 8 API keys lo
- `XAPI_KEYS=key1,key2,...,key8`

---

## Render Deploy

### Method: Blueprint (Recommended)

1. Is folder ko GitHub repo mein push karo:
```bash
git init && git add . && git commit -m "init" && git push
```

2. [render.com](https://render.com) → **New** → **Blueprint**

3. GitHub repo connect karo (`render.yaml` auto-detect hoga)

4. **Environment Variables** fill karo:

| Variable | Value |
|---|---|
| `BOT_TOKEN` | BotFather se token |
| `ADMIN_IDS` | Aapka Telegram user ID |
| `PRIVATE_CHANNEL_ID` | `-100...` format mein |
| `XAPI_KEYS` | 8 keys comma separated |

> `RENDER_EXTERNAL_URL` Render khud set karta hai — manually mat daalna

5. Deploy karo ✅

Deploy hone ke baad aapki stream URL hogi:
`https://terabot.onrender.com/stream/<file_id>`

---

## Bot Commands

| Command | Description |
|---|---|
| `/start` | Bot status aur stream server URL |
| `/status` | API keys, channel, URL check |

---

## Local Testing

```bash
pip install -r requirements.txt

export BOT_TOKEN="..."
export ADMIN_IDS="123456"
export PRIVATE_CHANNEL_ID="-100..."
export XAPI_KEYS="key1,key2,...,key8"
export PUBLIC_URL="http://localhost:8000"
export PORT=8000

python bot.py
```
