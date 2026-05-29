require('dotenv').config();
const express      = require('express');
const cors         = require('cors');
const http         = require('http');
const https        = require('https');
const WebSocket    = require('ws');
const fetch        = require('node-fetch');
const multer       = require('multer');
const path         = require('path');
const { v4: uuidv4 } = require('uuid');
const {
  S3Client, PutObjectCommand, DeleteObjectCommand,
  ListObjectsV2Command, GetObjectCommand
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// ── ENV ──────────────────────────────────────────────────────────────────────
const PORT          = process.env.PORT          || 3000;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';
const STREAM_URL    = process.env.STREAM_URL    || 'https://hello.citrus3.com:2020/stream/varietyvibesradio';

// Cloudflare R2 (S3-compatible)
const R2_ENDPOINT        = process.env.R2_ENDPOINT        || ''; // https://<account>.r2.cloudflarestorage.com
const R2_ACCESS_KEY_ID   = process.env.R2_ACCESS_KEY_ID   || '';
const R2_SECRET_KEY      = process.env.R2_SECRET_KEY      || '';
const R2_BUCKET          = process.env.R2_BUCKET          || 'vv-radio-mp3s';
const R2_PUBLIC_URL      = process.env.R2_PUBLIC_URL      || ''; // optional public bucket URL

// ── R2 CLIENT ────────────────────────────────────────────────────────────────
const r2 = new S3Client({
  region: 'auto',
  endpoint: R2_ENDPOINT,
  credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_KEY },
});

// ── APP SETUP ─────────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// multer: hold upload in memory (max 50 MB per file)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = /audio\/(mpeg|mp3|mp4|wav|ogg|flac|aac)|video\/mp4/.test(file.mimetype)
      || /\.(mp3|wav|ogg|flac|aac|m4a)$/i.test(file.originalname);
    cb(null, ok);
  },
});

// ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
let songQueue   = [];   // [{id, song, artist, requestedBy, status, djMessage, ts}]
let nowPlaying  = null;
let newsCache   = [];
let newsCacheAt = 0;
let trackLib    = [];   // [{id, key, title, artist, genre, size, url, uploadedAt}]
let allClients  = new Set();

// ── WEBSOCKET ─────────────────────────────────────────────────────────────────
function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload, ts: Date.now() });
  allClients.forEach(ws => { if (ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', ws => {
  allClients.add(ws);
  ws.send(JSON.stringify({ type: 'init', payload: { queue: songQueue, nowPlaying, trackCount: trackLib.length, streamUrl: STREAM_URL } }));
  ws.on('close',  () => allClients.delete(ws));
  ws.on('error',  () => allClients.delete(ws));
});

// ── STREAM PROXY ──────────────────────────────────────────────────────────────
app.get('/api/stream', (req, res) => {
  const target = req.query.url || STREAM_URL;
  const parsed = new URL(target);
  const mod    = parsed.protocol === 'https:' ? https : http;
  res.setHeader('Access-Control-Allow-Origin', '*');
  const pr = mod.get(target, { headers: { 'User-Agent': 'Mozilla/5.0', 'Icy-MetaData': '1' } }, upstream => {
    res.setHeader('Content-Type', upstream.headers['content-type'] || 'audio/mpeg');
    upstream.pipe(res);
    upstream.on('error', () => res.end());
  });
  pr.on('error', err => { if (!res.headersSent) res.status(502).json({ error: err.message }); });
  req.on('close', () => pr.destroy());
});

// ── AI HELPER ─────────────────────────────────────────────────────────────────
async function claude(system, user, maxTokens = 600) {
  if (!ANTHROPIC_KEY) throw new Error('ANTHROPIC_API_KEY not set');
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: 'claude-sonnet-4-20250514', max_tokens: maxTokens, system, messages: [{ role: 'user', content: user }] }),
  });
  const d = await r.json();
  if (d.error) throw new Error(d.error.message);
  return d.content?.[0]?.text || '';
}

// ── SONG REQUEST ──────────────────────────────────────────────────────────────
app.post('/api/request', async (req, res) => {
  const { song, artist, requestedBy } = req.body;
  if (!song) return res.status(400).json({ error: 'Song name required' });

  const sys = `You are VV DJ Agent for Variety Vibes Radio Hub. A listener requested a song.
Respond with a 1-sentence energetic DJ confirmation starting with 🎙️.
Then on a new line output: REQ:{"song":"...","artist":"...","genre":"...","vibeNote":"short dj note"}`;

  let djMessage = `🎙️ "${song}" is locked in and coming your way!`;
  let meta = { song, artist: artist || 'Unknown', genre: 'Various', vibeNote: `${song} is coming up!` };

  try {
    const reply = await claude(sys, `Song: "${song}" by "${artist || 'unknown'}" from: ${requestedBy || 'Anonymous'}`);
    if (reply.includes('REQ:')) {
      const [pre, json] = reply.split('REQ:');
      djMessage = pre.trim() || djMessage;
      try { meta = { ...meta, ...JSON.parse(json.trim()) }; } catch {}
    } else { djMessage = reply.trim() || djMessage; }
  } catch {}

  const entry = { id: uuidv4(), ...meta, requestedBy: requestedBy || 'Anonymous', djMessage, status: 'queued', ts: new Date().toISOString() };
  songQueue.push(entry);
  broadcast('queue_update', { queue: songQueue, newRequest: entry });
  res.json({ success: true, entry, djMessage });
});

// ── PLAYLIST BUILD ────────────────────────────────────────────────────────────
app.post('/api/playlist', async (req, res) => {
  const { vibe, genre, mood, count = 6 } = req.body;
  const sys = `You are VV DJ for Variety Vibes Radio. Build a playlist.
Respond with a 1-sentence DJ intro, then:
PLAYLIST:{"title":"...","tracks":[{"song":"...","artist":"...","genre":"...","duration":"3:30"}],"djIntro":"..."}
Include ${count} tracks.`;
  try {
    const reply = await claude(sys, `vibe="${vibe||'variety'}" genre="${genre||'mixed'}" mood="${mood||'upbeat'}"`, 900);
    let intro = '', playlist = null;
    if (reply.includes('PLAYLIST:')) {
      const [pre, json] = reply.split('PLAYLIST:');
      intro = pre.trim();
      try { playlist = JSON.parse(json.trim()); } catch {}
    }
    res.json({ success: true, intro, playlist });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── NEWS ──────────────────────────────────────────────────────────────────────
app.get('/api/news', async (req, res) => {
  if (newsCache.length && Date.now() - newsCacheAt < 5 * 60 * 1000)
    return res.json({ success: true, news: newsCache, cached: true });
  const sys = `You are VV News Anchor for Variety Vibes Radio.
Generate 5 realistic music/entertainment headlines.
Output ONLY valid JSON — no markdown:
{"items":[{"headline":"...","source":"Billboard","time":"3 min ago","summary":"One sentence.","category":"Music"}]}`;
  try {
    const reply = await claude(sys, 'Top 5 music and entertainment news headlines now.', 700);
    const clean = reply.replace(/```json|```/g, '').trim();
    const parsed = JSON.parse(clean);
    newsCache   = parsed.items || [];
    newsCacheAt = Date.now();
    res.json({ success: true, news: newsCache });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── CHAT ──────────────────────────────────────────────────────────────────────
app.post('/api/chat', async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'Message required' });
  const sys = `You are VV Agent, the AI DJ for Variety Vibes Radio Hub — live 24/7 online radio broadcasting worldwide from Portland, OR.
Be warm, energetic, knowledgeable about music. 2-3 sentences max.
Queue: ${songQueue.length} requests. ${nowPlaying ? `Now playing: "${nowPlaying.song}" by ${nowPlaying.artist}.` : ''}
Library: ${trackLib.length} uploaded tracks.`;
  try {
    const reply = await claude(sys, message, 350);
    res.json({ success: true, reply });
  } catch { res.json({ success: true, reply: "Hey! VV Agent here — having a quick technical moment, back in a sec!" }); }
});

// ── QUEUE MANAGEMENT ─────────────────────────────────────────────────────────
app.get('/api/queue',                (req, res) => res.json({ queue: songQueue, nowPlaying }));
app.post('/api/queue/clear',         (req, res) => { songQueue = []; nowPlaying = null; broadcast('queue_update', { queue: [] }); res.json({ success: true }); });
app.delete('/api/queue/:id',         (req, res) => { songQueue = songQueue.filter(t => t.id !== req.params.id); broadcast('queue_update', { queue: songQueue }); res.json({ success: true }); });
app.post('/api/queue/play/:id',      (req, res) => {
  const t = songQueue.find(x => x.id === req.params.id);
  if (!t) return res.status(404).json({ error: 'Not found' });
  t.status  = 'playing';
  nowPlaying = t;
  broadcast('now_playing', { track: t });
  res.json({ success: true, track: t });
});

// ── MP3 UPLOAD TO R2 ──────────────────────────────────────────────────────────
app.post('/api/tracks/upload', upload.array('files', 20), async (req, res) => {
  if (!req.files?.length) return res.status(400).json({ error: 'No files received' });
  if (!R2_ENDPOINT || !R2_ACCESS_KEY_ID) return res.status(503).json({ error: 'R2 storage not configured — add R2 env vars' });

  const results = [];
  for (const file of req.files) {
    const ext  = path.extname(file.originalname) || '.mp3';
    const key  = `tracks/${uuidv4()}${ext}`;
    const title  = req.body.title  || path.basename(file.originalname, ext);
    const artist = req.body.artist || 'Unknown Artist';
    const genre  = req.body.genre  || 'Various';

    try {
      await r2.send(new PutObjectCommand({
        Bucket: R2_BUCKET, Key: key,
        Body: file.buffer, ContentType: file.mimetype,
        Metadata: { title, artist, genre, originalName: file.originalname },
      }));

      // Build URL (use public URL if configured, else signed URL)
      let url;
      if (R2_PUBLIC_URL) {
        url = `${R2_PUBLIC_URL.replace(/\/$/, '')}/${key}`;
      } else {
        url = await getSignedUrl(r2, new GetObjectCommand({ Bucket: R2_BUCKET, Key: key }), { expiresIn: 86400 * 7 });
      }

      const track = { id: uuidv4(), key, title, artist, genre, size: file.size, url, uploadedAt: new Date().toISOString() };
      trackLib.push(track);
      results.push(track);
    } catch (err) {
      results.push({ error: err.message, file: file.originalname });
    }
  }

  broadcast('library_update', { tracks: trackLib });
  res.json({ success: true, uploaded: results, total: trackLib.length });
});

// ── GET TRACK LIBRARY ─────────────────────────────────────────────────────────
app.get('/api/tracks', async (req, res) => {
  // If R2 configured and library empty, try to list from bucket
  if (trackLib.length === 0 && R2_ENDPOINT && R2_ACCESS_KEY_ID) {
    try {
      const data = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: 'tracks/' }));
      trackLib = (data.Contents || []).map(obj => ({
        id: uuidv4(), key: obj.Key,
        title: path.basename(obj.Key, path.extname(obj.Key)),
        artist: 'Unknown', genre: 'Various',
        size: obj.Size, url: R2_PUBLIC_URL ? `${R2_PUBLIC_URL.replace(/\/$/, '')}/${obj.Key}` : '',
        uploadedAt: obj.LastModified,
      }));
    } catch {}
  }
  res.json({ success: true, tracks: trackLib });
});

// ── DELETE TRACK ──────────────────────────────────────────────────────────────
app.delete('/api/tracks/:id', async (req, res) => {
  const track = trackLib.find(t => t.id === req.params.id);
  if (!track) return res.status(404).json({ error: 'Track not found' });
  try {
    if (R2_ENDPOINT && R2_ACCESS_KEY_ID)
      await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: track.key }));
    trackLib = trackLib.filter(t => t.id !== req.params.id);
    broadcast('library_update', { tracks: trackLib });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── REQUEST TRACK FROM LIBRARY ────────────────────────────────────────────────
app.post('/api/tracks/:id/request', async (req, res) => {
  const track = trackLib.find(t => t.id === req.params.id);
  if (!track) return res.status(404).json({ error: 'Track not found' });
  const djMessage = `🎙️ "${track.title}" by ${track.artist} is next up in the queue!`;
  const entry = { id: uuidv4(), song: track.title, artist: track.artist, genre: track.genre, requestedBy: req.body.requestedBy || 'Library', djMessage, status: 'queued', trackUrl: track.url, ts: new Date().toISOString() };
  songQueue.push(entry);
  broadcast('queue_update', { queue: songQueue, newRequest: entry });
  res.json({ success: true, entry, djMessage });
});

// ── STATION SUBMISSION (email placeholder) ────────────────────────────────────
app.post('/api/submit-station', async (req, res) => {
  const { stationName, contact, email, plan } = req.body;
  if (!stationName || !email) return res.status(400).json({ error: 'Station name and email required' });
  // In production: send to email via SendGrid/Mailgun here
  console.log(`📻 New station submission: ${stationName} | ${plan} | ${email}`);
  broadcast('new_submission', { stationName, plan });
  res.json({ success: true, message: 'Application received! We\'ll be in touch within 48 hours.' });
});

// ── HEALTH CHECK ──────────────────────────────────────────────────────────────
app.get('/api/health', (req, res) => res.json({
  status: 'ok', queue: songQueue.length, library: trackLib.length,
  r2: !!R2_ENDPOINT, ai: !!ANTHROPIC_KEY, stream: STREAM_URL,
}));

server.listen(PORT, () => {
  console.log(`\n🎙️  Variety Vibes Radio Hub`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   AI:     ${ANTHROPIC_KEY ? '✅ configured' : '❌ missing ANTHROPIC_API_KEY'}`);
  console.log(`   R2:     ${R2_ENDPOINT   ? '✅ configured' : '⚠️  not configured (MP3 upload disabled)'}`);
  console.log(`   Stream: ${STREAM_URL}\n`);
});
