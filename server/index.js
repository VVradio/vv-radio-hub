require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const https      = require('https');
const WebSocket  = require('ws');
const fetch      = require('node-fetch');
const multer     = require('multer');
const path       = require('path');
const bcrypt     = require('bcryptjs');
const jwt        = require('jsonwebtoken');
const { v4: uuid } = require('uuid');
const {
  S3Client, PutObjectCommand, DeleteObjectCommand,
  ListObjectsV2Command, GetObjectCommand, HeadObjectCommand
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// ── ENV ───────────────────────────────────────────────────────────────────────
const PORT          = process.env.PORT           || 3000;
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';
const STREAM_URL    = process.env.STREAM_URL     || 'https://hello.citrus3.com:2020/stream/varietyvibesradio';
const JWT_SECRET    = process.env.JWT_SECRET     || 'vv-radio-secret-change-me-in-production';
const ADMIN_PASS    = process.env.ADMIN_PASSWORD || 'varietyvibes2024';
const R2_ENDPOINT   = process.env.R2_ENDPOINT    || '';
const R2_KEY_ID     = process.env.R2_ACCESS_KEY_ID || '';
const R2_SECRET     = process.env.R2_SECRET_KEY  || '';
const R2_BUCKET     = process.env.R2_BUCKET      || 'vv-radio-mp3s';
const R2_PUBLIC_URL = (process.env.R2_PUBLIC_URL  || '').trim();

// ── R2 CLIENT ─────────────────────────────────────────────────────────────────
const r2 = new S3Client({
  region: 'auto', endpoint: R2_ENDPOINT,
  credentials: { accessKeyId: R2_KEY_ID, secretAccessKey: R2_SECRET },
});

// ── STATIONS (source of truth) ────────────────────────────────────────────────
const STATIONS = [
  { id: 'vvr',     abbr: 'VV', name: 'Variety Vibes Radio',  genre: 'Indie / Multi-genre',   streamUrl: STREAM_URL },
  { id: 'hiphop',  abbr: 'HH', name: 'Hip Hop Heat',          genre: 'Hip Hop / Rap',          streamUrl: '' },
  { id: 'deepho',  abbr: 'DH', name: 'Deep House Nation',     genre: 'Electronic / Dance',     streamUrl: '' },
  { id: 'gospel',  abbr: 'GG', name: 'Gospel Glory',          genre: 'Gospel / Christian',     streamUrl: '' },
  { id: 'oldsch',  abbr: 'OS', name: 'Old School Jams',       genre: 'R&B / Soul',             streamUrl: '' },
  { id: 'soullvb', abbr: 'SV', name: 'Soul Vibes R&B',        genre: 'R&B / Soul',             streamUrl: '' },
  { id: 'popnat',  abbr: 'PN', name: 'Pop Nation',            genre: 'Pop / Top 40',           streamUrl: '' },
  { id: 'country', abbr: 'CR', name: 'Country Roads FM',      genre: 'Country',                streamUrl: '' },
  { id: 'rockfq',  abbr: 'RF', name: 'Rock Frequency',        genre: 'Rock',                   streamUrl: '' },
];

// ── IN-MEMORY STATE ───────────────────────────────────────────────────────────
// stationOwners: { email -> { id, email, passwordHash, stationId, name, createdAt } }
let stationOwners = {};
// trackLibrary: { stationId -> [{ id, key, title, artist, genre, size, url, uploadedAt }] }
let trackLibrary  = {};
// songQueue per station
let songQueues    = {};
let newsCache = [];
let newsCacheAt = 0;
let allClients    = new Set();

STATIONS.forEach(s => { trackLibrary[s.id] = []; songQueues[s.id] = []; });

// ── EXPRESS + WS ──────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());

// Serve admin panel
app.use('/admin', express.static(path.join(__dirname, '../public/admin')));
// Serve main frontend
app.use(express.static(path.join(__dirname, '../public')));

// multer — memory, 100MB max
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 100 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ok = /audio\//i.test(file.mimetype) || /\.(mp3|wav|ogg|flac|aac|m4a)$/i.test(file.originalname);
    cb(null, ok);
  },
});

// ── WEBSOCKET ─────────────────────────────────────────────────────────────────
function broadcast(type, payload) {
  const msg = JSON.stringify({ type, payload, ts: Date.now() });
  allClients.forEach(ws => ws.readyState === WebSocket.OPEN && ws.send(msg));
}

wss.on('connection', ws => {
  allClients.add(ws);
  ws.send(JSON.stringify({ type: 'init', payload: { stations: STATIONS, queues: songQueues, streamUrl: STREAM_URL } }));
  ws.on('close',  () => allClients.delete(ws));
  ws.on('error',  () => allClients.delete(ws));
});

// ── AUTH MIDDLEWARE ───────────────────────────────────────────────────────────
function authAdmin(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded.role !== 'admin') return res.status(403).json({ error: 'Forbidden' });
    req.user = decoded;
    next();
  } catch { res.status(401).json({ error: 'Invalid token' }); }
}

function authOwner(req, res, next) {
  const token = req.headers.authorization?.split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Unauthorized' });
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (decoded.role !== 'admin' && decoded.role !== 'owner') return res.status(403).json({ error: 'Forbidden' });
    req.user = decoded;
    next();
  } catch { res.status(401).json({ error: 'Invalid token' }); }
}

// Station access guard — admin sees all, owners see only their station
function canAccessStation(req, res, next) {
  const { stationId } = req.params;
  if (!STATIONS.find(s => s.id === stationId)) return res.status(404).json({ error: 'Station not found' });
  if (req.user.role === 'admin') return next();
  if (req.user.stationId !== stationId) return res.status(403).json({ error: 'Access denied — wrong station' });
  next();
}

// ── AUTH ROUTES ───────────────────────────────────────────────────────────────
// Admin login
app.post('/api/auth/admin-login', async (req, res) => {
  const { password } = req.body;
  if (!password || password !== ADMIN_PASS) return res.status(401).json({ error: 'Invalid password' });
  const token = jwt.sign({ role: 'admin', email: 'admin' }, JWT_SECRET, { expiresIn: '24h' });
  res.json({ success: true, token, role: 'admin' });
});

// Station owner login
app.post('/api/auth/owner-login', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  const owner = stationOwners[email.toLowerCase()];
  if (!owner) return res.status(401).json({ error: 'Invalid email or password' });
  const ok = await bcrypt.compare(password, owner.passwordHash);
  if (!ok) return res.status(401).json({ error: 'Invalid email or password' });
  const station = STATIONS.find(s => s.id === owner.stationId);
  const token = jwt.sign({ role: 'owner', email: owner.email, stationId: owner.stationId, name: owner.name }, JWT_SECRET, { expiresIn: '24h' });
  res.json({ success: true, token, role: 'owner', stationId: owner.stationId, stationName: station?.name, ownerName: owner.name });
});

// Admin: create station owner account
app.post('/api/admin/owners', authAdmin, async (req, res) => {
  const { email, password, stationId, name } = req.body;
  if (!email || !password || !stationId || !name) return res.status(400).json({ error: 'All fields required' });
  if (!STATIONS.find(s => s.id === stationId)) return res.status(400).json({ error: 'Invalid station ID' });
  if (stationOwners[email.toLowerCase()]) return res.status(409).json({ error: 'Email already registered' });
  const passwordHash = await bcrypt.hash(password, 10);
  stationOwners[email.toLowerCase()] = { id: uuid(), email: email.toLowerCase(), passwordHash, stationId, name, createdAt: new Date().toISOString() };
  res.json({ success: true, message: `Owner account created for ${name} → ${STATIONS.find(s=>s.id===stationId)?.name}` });
});

// Admin: list all owners
app.get('/api/admin/owners', authAdmin, (req, res) => {
  const list = Object.values(stationOwners).map(o => ({
    id: o.id, email: o.email, name: o.name, stationId: o.stationId,
    stationName: STATIONS.find(s=>s.id===o.stationId)?.name, createdAt: o.createdAt
  }));
  res.json({ success: true, owners: list });
});

// Admin: delete owner
app.delete('/api/admin/owners/:email', authAdmin, (req, res) => {
  const email = req.params.email.toLowerCase();
  if (!stationOwners[email]) return res.status(404).json({ error: 'Owner not found' });
  delete stationOwners[email];
  res.json({ success: true });
});

// ── R2 HELPERS ────────────────────────────────────────────────────────────────
async function uploadToR2(buffer, key, contentType, metadata = {}) {
  await r2.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: key, Body: buffer,
    ContentType: contentType, Metadata: metadata,
  }));
  if (R2_PUBLIC_URL) return `${R2_PUBLIC_URL.replace(/\/$/, '')}/${key}`;
  return await getSignedUrl(r2, new GetObjectCommand({ Bucket: R2_BUCKET, Key: key }), { expiresIn: 86400 * 7 });
}

async function deleteFromR2(key) {
  await r2.send(new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: key }));
}

// ── MP3 UPLOAD (per station) ──────────────────────────────────────────────────
app.post('/api/stations/:stationId/tracks', authOwner, canAccessStation, upload.array('files', 30), async (req, res) => {
  const { stationId } = req.params;
  if (!req.files?.length) return res.status(400).json({ error: 'No files received' });
  if (!R2_ENDPOINT || !R2_KEY_ID) return res.status(503).json({ error: 'R2 storage not configured' });

  const station = STATIONS.find(s => s.id === stationId);
  const results = [];

  for (const file of req.files) {
    const ext   = path.extname(file.originalname) || '.mp3';
    const key   = `stations/${stationId}/${uuid()}${ext}`;
    const title  = req.body.title  || path.basename(file.originalname, ext);
    const artist = req.body.artist || station.name;
    const genre  = req.body.genre  || station.genre;

    try {
      const url = await uploadToR2(file.buffer, key, file.mimetype, { title, artist, genre, stationId, originalName: file.originalname });
      const track = { id: uuid(), key, title, artist, genre, stationId, size: file.size, url, uploadedAt: new Date().toISOString() };
      if (!trackLibrary[stationId]) trackLibrary[stationId] = [];
      trackLibrary[stationId].push(track);
      results.push({ success: true, track });
    } catch (err) {
      console.error('Upload error:', err);
results.push({ success: false, file: file.originalname, error: err.message });
    }
  }

  broadcast('library_update', { stationId, tracks: trackLibrary[stationId] });
  res.json({ success: true, uploaded: results, total: trackLibrary[stationId].length });
});

// Get tracks for a station
app.get('/api/stations/:stationId/tracks', authOwner, canAccessStation, async (req, res) => {
  const { stationId } = req.params;
  // Refresh from R2 if empty
  if (!trackLibrary[stationId]?.length && R2_ENDPOINT && R2_KEY_ID) {
    try {
      const data = await r2.send(new ListObjectsV2Command({ Bucket: R2_BUCKET, Prefix: `stations/${stationId}/` }));
      trackLibrary[stationId] = await Promise.all((data.Contents || []).map(async obj => {
        let meta = {};
        try {
          const head = await r2.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: obj.Key }));
          meta = head.Metadata || {};
        } catch {}
        const url = R2_PUBLIC_URL ? `${R2_PUBLIC_URL.replace(/\/$/, '')}/${obj.Key}` :
          await getSignedUrl(r2, new GetObjectCommand({ Bucket: R2_BUCKET, Key: obj.Key }), { expiresIn: 86400 * 7 });
        return {
          id: uuid(), key: obj.Key,
          title: meta.title || path.basename(obj.Key, path.extname(obj.Key)),
          artist: meta.artist || 'Unknown', genre: meta.genre || 'Various',
          stationId, size: obj.Size, url, uploadedAt: obj.LastModified,
        };
      }));
    } catch {}
  }
  res.json({ success: true, tracks: trackLibrary[stationId] || [], stationId });
});

// Public track list (for listeners)
app.get('/api/public/stations/:stationId/tracks', (req, res) => {
  const { stationId } = req.params;
  const tracks = (trackLibrary[stationId] || []).map(t => ({
    id: t.id, title: t.title, artist: t.artist, genre: t.genre, url: t.url, stationId: t.stationId
  }));
  res.json({ success: true, tracks });
});

// Delete a track
app.delete('/api/stations/:stationId/tracks/:trackId', authOwner, canAccessStation, async (req, res) => {
  const { stationId, trackId } = req.params;
  const idx = (trackLibrary[stationId] || []).findIndex(t => t.id === trackId);
  if (idx === -1) return res.status(404).json({ error: 'Track not found' });
  const track = trackLibrary[stationId][idx];
  try {
    if (R2_ENDPOINT && R2_KEY_ID) await deleteFromR2(track.key);
    trackLibrary[stationId].splice(idx, 1);
    broadcast('library_update', { stationId, tracks: trackLibrary[stationId] });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin: get all tracks across all stations
app.get('/api/admin/tracks', authAdmin, (req, res) => {
  const all = {};
  STATIONS.forEach(s => { all[s.id] = { station: s, tracks: trackLibrary[s.id] || [] }; });
  res.json({ success: true, library: all });
});

// Admin: move track to different station
app.post('/api/admin/tracks/move', authAdmin, async (req, res) => {
  const { trackId, fromStation, toStation } = req.body;
  if (!STATIONS.find(s=>s.id===toStation)) return res.status(400).json({ error: 'Invalid target station' });
  const idx = (trackLibrary[fromStation] || []).findIndex(t => t.id === trackId);
  if (idx === -1) return res.status(404).json({ error: 'Track not found' });
  const track = trackLibrary[fromStation][idx];
  // Move key in R2
  const newKey = track.key.replace(`stations/${fromStation}/`, `stations/${toStation}/`);
  try {
    // Copy to new location
    await r2.send(new PutObjectCommand({
      Bucket: R2_BUCKET, Key: newKey, Body: Buffer.alloc(0),
      CopySource: `${R2_BUCKET}/${track.key}`
    }));
    await deleteFromR2(track.key);
    const url = R2_PUBLIC_URL ? `${R2_PUBLIC_URL.replace(/\/$/, '')}/${newKey}` :
      await getSignedUrl(r2, new GetObjectCommand({ Bucket: R2_BUCKET, Key: newKey }), { expiresIn: 86400*7 });
    const updated = { ...track, key: newKey, stationId: toStation, url };
    trackLibrary[fromStation].splice(idx, 1);
    if (!trackLibrary[toStation]) trackLibrary[toStation] = [];
    trackLibrary[toStation].push(updated);
    broadcast('library_update', { stationId: fromStation, tracks: trackLibrary[fromStation] });
    broadcast('library_update', { stationId: toStation,   tracks: trackLibrary[toStation] });
    res.json({ success: true, track: updated });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── SONG QUEUE (per station) ──────────────────────────────────────────────────
app.post('/api/stations/:stationId/queue', async (req, res) => {
  const { stationId } = req.params;
  const { song, artist, requestedBy, trackId } = req.body;
  if (!song) return res.status(400).json({ error: 'Song name required' });

  // If trackId provided, look up URL from library
  let trackUrl = null;
  if (trackId) {
    const t = (trackLibrary[stationId] || []).find(t => t.id === trackId);
    if (t) trackUrl = t.url;
  }

  const sys = `You are VV DJ Agent for Variety Vibes Radio Hub. A listener requested a song for ${STATIONS.find(s=>s.id===stationId)?.name || stationId}.
Reply with 1 energetic sentence starting with 🎙️.`;
  let djMessage = `🎙️ "${song}" is locked in for ${STATIONS.find(s=>s.id===stationId)?.name}!`;
  try {
    djMessage = await claude(sys, `Song: "${song}" by "${artist||'unknown'}" from: ${requestedBy||'Anonymous'}`, 150);
  } catch {}

  const entry = { id: uuid(), song, artist: artist||'Unknown', requestedBy: requestedBy||'Anonymous', djMessage, status: 'queued', trackUrl, stationId, ts: new Date().toISOString() };
  if (!songQueues[stationId]) songQueues[stationId] = [];
  songQueues[stationId].push(entry);
  broadcast('queue_update', { stationId, queue: songQueues[stationId], newRequest: entry });
  res.json({ success: true, entry, djMessage });
});

app.get('/api/stations/:stationId/queue', (req, res) => res.json({ queue: songQueues[req.params.stationId] || [] }));
app.post('/api/stations/:stationId/queue/clear', authOwner, canAccessStation, (req, res) => {
  songQueues[req.params.stationId] = [];
  broadcast('queue_update', { stationId: req.params.stationId, queue: [] });
  res.json({ success: true });
});
app.delete('/api/stations/:stationId/queue/:id', authOwner, canAccessStation, (req, res) => {
  const { stationId, id } = req.params;
  songQueues[stationId] = (songQueues[stationId]||[]).filter(t=>t.id!==id);
  broadcast('queue_update', { stationId, queue: songQueues[stationId] });
  res.json({ success: true });
});

// ── AI HELPERS ────────────────────────────────────────────────────────────────
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

app.post('/api/playlist', async (req, res) => {
  const { vibe, genre, count = 6 } = req.body;
  const sys = `You are VV DJ. Build a playlist. One DJ intro sentence, then: PLAYLIST:{"title":"...","tracks":[{"song":"...","artist":"...","duration":"3:30"}],"djIntro":"..."} Include ${count} tracks.`;
  try {
    const reply = await claude(sys, `vibe="${vibe}" genre="${genre||'mixed'}"`, 900);
    let intro = '', playlist = null;
    if (reply.includes('PLAYLIST:')) { const [p,j]=reply.split('PLAYLIST:'); intro=p.trim(); try{playlist=JSON.parse(j.trim());}catch{} }
    res.json({ success: true, intro, playlist });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/news', async (req, res) => {
  if (newsCache.length && Date.now() - newsCacheAt < 5 * 60 * 1000)
    return res.json({ success: true, news: newsCache, cached: true });
  try {
    const r = await fetch(`https://newsapi.org/v2/everything?q=music+entertainment&sortBy=publishedAt&pageSize=5&apiKey=${process.env.NEWS_API_KEY}`);
    const d = await r.json();
    const news = (d.articles || []).map(a => ({
      headline: a.title,
      source: a.source?.name || 'News',
      time: new Date(a.publishedAt).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}),
      summary: a.description || '',
      category: 'Music',
      url: a.url
    }));
    newsCache = news;
    newsCacheAt = Date.now();
    res.json({ success: true, news });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/chat', async (req, res) => {
  const { message, stationId } = req.body;
  const station = STATIONS.find(s=>s.id===stationId) || STATIONS[0];
  const sys = `You are VV Agent, AI DJ for ${station.name} on Variety Vibes Radio Hub. Be warm and energetic. 2-3 sentences max.`;
  try { res.json({ success: true, reply: await claude(sys, message, 300) }); }
  catch { res.json({ success: true, reply: "VV Agent is having a quick moment — back shortly!" }); }
});

// ── STREAM PROXY ──────────────────────────────────────────────────────────────
app.get('/api/stream', (req, res) => {
  const target = req.query.url || STREAM_URL;
  const parsed = new URL(target);
  const mod = parsed.protocol === 'https:' ? https : http;
  res.setHeader('Access-Control-Allow-Origin', '*');
  const pr = mod.get(target, { headers: { 'User-Agent': 'Mozilla/5.0', 'Icy-MetaData': '1' } }, upstream => {
    res.setHeader('Content-Type', upstream.headers['content-type'] || 'audio/mpeg');
    upstream.pipe(res);
  });
  pr.on('error', err => { if (!res.headersSent) res.status(502).json({ error: err.message }); });
  req.on('close', () => pr.destroy());
});

// ── STATION SUBMISSION ────────────────────────────────────────────────────────
app.post('/api/submit-station', async (req, res) => {
  console.log(`📻 Station submission: ${req.body.stationName} | ${req.body.plan} | ${req.body.email}`);
  res.json({ success: true });
});

// ── PUBLIC INFO ───────────────────────────────────────────────────────────────
app.get('/api/stations', (req, res) => res.json({ stations: STATIONS }));
app.get('/api/admin/fix-urls', authAdmin, (req, res) => {
  let fixed = 0;
  Object.keys(trackLibrary).forEach(stationId => {
    trackLibrary[stationId] = trackLibrary[stationId].map(t => {
      if (t.url && t.url.includes('\n')) { t.url = t.url.replace(/\n/g, ''); fixed++; }
      return t;
    });
  });
  res.json({ success: true, fixed });
});
app.get('/api/health', (req, res) => res.json({ status: 'ok', r2: !!R2_ENDPOINT, ai: !!ANTHROPIC_KEY }));

server.listen(PORT, () => {
  console.log(`\n🎙️  Variety Vibes Radio Hub v2.0`);
  console.log(`   Main:  http://localhost:${PORT}`);
  console.log(`   Admin: http://localhost:${PORT}/admin`);
  console.log(`   AI:    ${ANTHROPIC_KEY ? '✅' : '❌ missing ANTHROPIC_API_KEY'}`);
  console.log(`   R2:    ${R2_ENDPOINT   ? '✅' : '⚠️  not configured'}\n`);
  console.log(`   Admin password: ${ADMIN_PASS}`);
});
