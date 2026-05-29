# 🎙️ Variety Vibes Radio Hub

Full-stack radio platform with live streaming, AI DJ agent, song request queue, MP3 library (Cloudflare R2), news reader, and voice.

## Deploy to Railway in 5 steps

### Step 1 — Push to GitHub
1. Go to github.com → New repository → name it `vv-radio-hub`
2. Run these commands in this folder:
```
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/vv-radio-hub.git
git push -u origin main
```

### Step 2 — Deploy on Railway
1. Go to railway.app → Sign in with GitHub
2. Click **New Project** → **Deploy from GitHub repo**
3. Select `vv-radio-hub`
4. Railway will auto-detect Node.js and deploy

### Step 3 — Add environment variables on Railway
In your Railway project → **Variables** tab → add each one:
```
ANTHROPIC_API_KEY    = sk-ant-YOUR_KEY
STREAM_URL           = https://hello.citrus3.com:2020/stream/varietyvibesradio
R2_ENDPOINT          = https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID     = your_r2_access_key
R2_SECRET_KEY        = your_r2_secret_key
R2_BUCKET            = vv-radio-mp3s
R2_PUBLIC_URL        = https://pub-YOURHASH.r2.dev
```

### Step 4 — Create Cloudflare R2 bucket
1. Go to dash.cloudflare.com → R2
2. Create bucket named `vv-radio-mp3s`
3. Create an API token with R2 read/write permissions
4. Copy the Account ID, Access Key, Secret Key into Railway variables
5. Optional: Enable "Public access" on the bucket and copy the public URL

### Step 5 — Add custom domain (optional)
In Railway → Settings → Custom Domain → add `radio.varietyvibesradio.com`
Then add a CNAME record in your Cloudflare DNS pointing to the Railway URL.

## Local development
```bash
cp .env.example .env
# Fill in your values in .env
npm install
npm start
# Open http://localhost:3000
```

## Features
- 📡 Live stream from Citrus3 (CORS-proxied through server)
- 🤖 AI DJ agent — song requests, playlists, news (Claude API)
- 🔊 Voice DJ — Text-to-Speech with DJ and anchor modes
- 🎵 MP3 Library — upload to Cloudflare R2, play and queue tracks
- 📋 Song request queue — real-time via WebSocket
- 📰 News reader — AI-generated headlines with voice
- 💰 Sponsor pricing + station submission form
- 📱 Responsive — works on mobile and desktop
