import express from 'express';
import Database from 'better-sqlite3';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import he from 'he';
import { XMLParser } from 'fast-xml-parser';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');
const PUBLIC_DIR = path.join(ROOT_DIR, 'public');
const DATA_DIR = path.join(ROOT_DIR, 'data');
const DB_PATH = path.join(DATA_DIR, 'alerts.db');

const PORT = Number(process.env.PORT || 3000);
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS || 15000);
const HISTORY_WINDOW_HOURS = 24;
const INTEL_REFRESH_MS = Number(process.env.INTEL_REFRESH_MS || 180000);

const MASTODON_ACCOUNT_ID = '111439235689517975';
const MASTODON_API_BASE = 'https://mastodon.social/api/v1';
const LOOKUP_API_BASE = 'https://agg.rocketalert.live/api/v1';
const ALERT_TYPES = {
  ROCKET: 1,
  UAV: 2
};

const RSS_NEWS_SOURCES = [
  {
    id: 'google-news-alerts',
    name: 'Google News - Alerts Query',
    url: 'https://news.google.com/rss/search?q=(Israel+OR+Gaza+OR+Lebanon)+AND+(rocket+OR+missile+OR+siren+OR+UAV)&hl=en-US&gl=US&ceid=US:en'
  },
  {
    id: 'bbc-middle-east',
    name: 'BBC Middle East',
    url: 'https://feeds.bbci.co.uk/news/world/middle_east/rss.xml'
  },
  {
    id: 'times-of-israel',
    name: 'The Times of Israel',
    url: 'https://www.timesofisrael.com/feed/'
  },
  {
    id: 'rocketalert-mastodon',
    name: 'RocketAlert Mastodon Feed',
    url: 'https://mastodon.social/@rocketalert.rss'
  }
];

const WEBCAM_SOURCES = [];

const COMMAND_LINKS = [];

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '',
  trimValues: true,
  parseTagValue: false
});

await ensureDataDir();
const db = initDatabase();
const app = express();

app.use(express.json());
app.use(express.static(PUBLIC_DIR));

const clients = new Set();
let sinceId = getMaxAlertId();
let isPolling = false;
let intelRefreshPromise = null;
let intelCache = {
  updatedAt: null,
  news: [],
  images: [],
  webcams: WEBCAM_SOURCES,
  dashboards: COMMAND_LINKS,
  status: 'warming',
  errors: []
};

app.get('/api/bootstrap', async (_req, res) => {
  await ensureIntelFresh();
  res.json({
    now: new Date().toISOString(),
    alerts: getAlertsWithinHours(HISTORY_WINDOW_HOURS),
    stats: getStatsWithinHours(HISTORY_WINDOW_HOURS),
    predictions: getPredictions(),
    intel: getIntelSnapshot()
  });
});

app.get('/api/alerts', (req, res) => {
  const hours = clampHours(req.query.hours);
  res.json({
    now: new Date().toISOString(),
    windowHours: hours,
    alerts: getAlertsWithinHours(hours)
  });
});

app.get('/api/stats', (req, res) => {
  const hours = clampHours(req.query.hours);
  res.json({
    now: new Date().toISOString(),
    windowHours: hours,
    stats: getStatsWithinHours(hours)
  });
});

app.get('/api/predictions', (_req, res) => {
  res.json({
    now: new Date().toISOString(),
    predictions: getPredictions()
  });
});

app.get('/api/intel', async (_req, res) => {
  await ensureIntelFresh();
  res.json({
    now: new Date().toISOString(),
    intel: getIntelSnapshot()
  });
});

app.get('/api/stream', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive'
  });

  const client = { id: Math.random().toString(36).slice(2), res };
  clients.add(client);

  sendSse(client, 'hello', {
    message: 'stream-connected',
    now: new Date().toISOString()
  });

  const keepAlive = setInterval(() => {
    sendSse(client, 'keepalive', { now: new Date().toISOString() });
  }, 15000);

  req.on('close', () => {
    clearInterval(keepAlive);
    clients.delete(client);
  });
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true,
    now: new Date().toISOString(),
    clients: clients.size,
    sinceId,
    intel: {
      status: intelCache.status,
      updatedAt: intelCache.updatedAt
    }
  });
});

app.listen(PORT, () => {
  console.log(`Rocket monitor listening on http://localhost:${PORT}`);
});

try {
  await seedLocationDirectory();
} catch (error) {
  console.error('Location directory seed failed:', error.message);
}

try {
  await backfillLast24Hours();
} catch (error) {
  console.error('Initial backfill failed:', error.message);
}

try {
  await refreshIntelCache({ force: true });
} catch (error) {
  console.error('Initial intel refresh failed:', error.message);
}
void pollLoop();
setInterval(pruneOldData, 5 * 60 * 1000).unref();
setInterval(() => {
  void seedLocationDirectory();
}, 30 * 60 * 1000).unref();
setInterval(() => {
  void refreshIntelCache({ force: false });
}, INTEL_REFRESH_MS).unref();

async function ensureDataDir() {
  await import('node:fs/promises').then((fs) => fs.mkdir(DATA_DIR, { recursive: true }));
}

function initDatabase() {
  const sqlite = new Database(DB_PATH);
  sqlite.pragma('journal_mode = WAL');
  sqlite.pragma('foreign_keys = ON');

  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS alerts (
      alert_id TEXT PRIMARY KEY,
      source_url TEXT NOT NULL,
      type TEXT NOT NULL,
      summary TEXT NOT NULL,
      raw_text TEXT NOT NULL,
      published_at TEXT NOT NULL,
      ingested_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS alert_places (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      alert_id TEXT NOT NULL,
      place_raw TEXT NOT NULL,
      place_normalized TEXT NOT NULL,
      lat REAL,
      lon REAL,
      resolution_source TEXT,
      FOREIGN KEY(alert_id) REFERENCES alerts(alert_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_alerts_published_at ON alerts(published_at DESC);
    CREATE INDEX IF NOT EXISTS idx_alert_places_alert_id ON alert_places(alert_id);
    CREATE INDEX IF NOT EXISTS idx_alert_places_name ON alert_places(place_normalized);

    CREATE TABLE IF NOT EXISTS location_directory (
      place_normalized TEXT PRIMARY KEY,
      place_label TEXT NOT NULL,
      lat REAL NOT NULL,
      lon REAL NOT NULL,
      source TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS geocode_cache (
      place_normalized TEXT PRIMARY KEY,
      place_label TEXT NOT NULL,
      lat REAL,
      lon REAL,
      success INTEGER NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);

  return sqlite;
}

function clampHours(rawHours) {
  const parsed = Number(rawHours || HISTORY_WINDOW_HOURS);
  if (!Number.isFinite(parsed)) return HISTORY_WINDOW_HOURS;
  return Math.max(1, Math.min(72, Math.floor(parsed)));
}

function getMaxAlertId() {
  const row = db
    .prepare('SELECT alert_id FROM alerts ORDER BY CAST(alert_id AS INTEGER) DESC LIMIT 1')
    .get();
  return row?.alert_id ?? null;
}

function toSqlIso(date) {
  return new Date(date).toISOString();
}

function cutoffIso(hours) {
  const cutoff = Date.now() - hours * 60 * 60 * 1000;
  return new Date(cutoff).toISOString();
}

function normalizePlaceName(name) {
  return String(name || '')
    .replace(/\u00A0/g, ' ')
    .replace(/\s*\([^)]*\)\s*$/u, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

function cleanPlaceLabel(name) {
  return String(name || '').replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();
}

function htmlToLines(html) {
  const decoded = he.decode(String(html || ''));
  return decoded
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/\u00A0/g, ' ')
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function parseStatusToAlert(status) {
  const lines = htmlToLines(status.content);
  if (lines.length === 0) return null;

  const summaryLine = lines[0];
  const summaryMatch = summaryLine.match(/^(.+?)\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}:?$/);
  const type = summaryMatch ? summaryMatch[1].trim() : summaryLine;

  const places = [...new Set(lines.slice(1).map(cleanPlaceLabel).filter(Boolean))];

  return {
    alertId: String(status.id),
    sourceUrl: status.url || `https://mastodon.social/@rocketalert/${status.id}`,
    publishedAt: toSqlIso(status.created_at),
    type,
    summary: summaryLine,
    rawText: lines.join('\n'),
    places
  };
}

async function fetchMastodonStatuses({ limit = 40, maxId = null, since = null } = {}) {
  const url = new URL(`${MASTODON_API_BASE}/accounts/${MASTODON_ACCOUNT_ID}/statuses`);
  url.searchParams.set('exclude_reblogs', 'true');
  url.searchParams.set('exclude_replies', 'true');
  url.searchParams.set('limit', String(limit));
  if (maxId) url.searchParams.set('max_id', String(maxId));
  if (since) url.searchParams.set('since_id', String(since));

  const response = await fetch(url, {
    headers: { 'User-Agent': 'rocket-alert-monitor/0.1 (+https://localhost)' }
  });

  if (!response.ok) {
    throw new Error(`Mastodon API failed (${response.status})`);
  }

  return response.json();
}

function getIntelSnapshot() {
  return {
    updatedAt: intelCache.updatedAt,
    status: intelCache.status,
    errors: intelCache.errors,
    news: intelCache.news,
    images: intelCache.images,
    webcams: intelCache.webcams,
    dashboards: intelCache.dashboards
  };
}

async function ensureIntelFresh() {
  const last = intelCache.updatedAt ? Date.parse(intelCache.updatedAt) : 0;
  const ageMs = Date.now() - (Number.isFinite(last) ? last : 0);
  if (!intelCache.updatedAt || ageMs > INTEL_REFRESH_MS) {
    await refreshIntelCache({ force: false });
  }
}

async function refreshIntelCache({ force }) {
  if (intelRefreshPromise && !force) {
    return intelRefreshPromise;
  }

  intelRefreshPromise = (async () => {
    const errors = [];
    const nowIso = new Date().toISOString();
    const [newsSettled, imagesSettled] = await Promise.allSettled([
      fetchRssNews(),
      fetchVisualImageFeed()
    ]);

    const nextNews = newsSettled.status === 'fulfilled' ? newsSettled.value : intelCache.news;
    const nextImages = imagesSettled.status === 'fulfilled' ? imagesSettled.value : intelCache.images;
    const effectiveImages =
      nextImages.length > 0 ? nextImages : buildFallbackImagesFromNews(nextNews);

    if (newsSettled.status === 'rejected') {
      errors.push(`news: ${newsSettled.reason?.message || 'unknown error'}`);
    }
    if (imagesSettled.status === 'rejected') {
      errors.push(`images: ${imagesSettled.reason?.message || 'unknown error'}`);
    }

    intelCache = {
      updatedAt: nowIso,
      status: errors.length > 0 ? 'degraded' : 'ok',
      errors,
      news: nextNews,
      images: effectiveImages,
      webcams: WEBCAM_SOURCES,
      dashboards: COMMAND_LINKS
    };
    console.log(
      `Intel refresh ${intelCache.status}: ${intelCache.news.length} news, ${intelCache.images.length} images.`
    );
  })()
    .catch((error) => {
      intelCache = {
        ...intelCache,
        status: 'degraded',
        errors: [`refresh: ${error.message}`]
      };
    })
    .finally(() => {
      intelRefreshPromise = null;
    });

  return intelRefreshPromise;
}

async function fetchRssNews() {
  const perSource = await Promise.allSettled(RSS_NEWS_SOURCES.map((source) => fetchNewsSource(source)));
  const allItems = [];

  for (const result of perSource) {
    if (result.status === 'fulfilled') {
      allItems.push(...result.value);
    }
  }

  const deduped = dedupeNews(allItems)
    .sort((a, b) => {
      const aTime = a.publishedAt ? Date.parse(a.publishedAt) : 0;
      const bTime = b.publishedAt ? Date.parse(b.publishedAt) : 0;
      return bTime - aTime;
    })
    .slice(0, 48);

  return deduped;
}

async function fetchNewsSource(source) {
  const response = await fetchWithTimeout(source.url, 9000, {
    headers: {
      'User-Agent': 'rocket-alert-monitor/0.1 (+https://localhost)',
      Accept: 'application/rss+xml, application/atom+xml, text/xml, application/xml'
    }
  });

  if (!response.ok) {
    throw new Error(`${source.id} (${response.status})`);
  }

  const xmlText = await response.text();
  const parsed = xmlParser.parse(xmlText);
  const entries = extractFeedItems(parsed).slice(0, 16);
  const normalized = [];

  for (const entry of entries) {
    const title = textFromNode(entry.title);
    const link = urlFromNode(entry.link) || textFromNode(entry.guid);
    if (!title || !link || !/^https?:\/\//i.test(link)) continue;

    const summary = textFromNode(entry.description || entry.summary || entry.content);
    const publishedAt = toOptionalIso(
      entry.pubDate || entry.published || entry.updated || entry['dc:date'] || null
    );
    const thumb = mediaThumbnailFromEntry(entry);
    normalized.push({
      id: `${source.id}-${hashKey(link)}`,
      sourceId: source.id,
      sourceName: source.name,
      title,
      url: link,
      summary,
      publishedAt,
      image: thumb
    });
  }

  return normalized;
}

function extractFeedItems(parsed) {
  if (parsed?.rss?.channel) {
    return asArray(parsed.rss.channel.item);
  }

  if (parsed?.feed?.entry) {
    return asArray(parsed.feed.entry);
  }

  return [];
}

function asArray(value) {
  if (Array.isArray(value)) return value;
  if (value === null || value === undefined) return [];
  return [value];
}

function stripHtml(value) {
  return String(value || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function textFromNode(node) {
  if (!node) return '';
  if (typeof node === 'string') return he.decode(stripHtml(node));
  if (Array.isArray(node)) return textFromNode(node[0]);
  if (typeof node === 'object') {
    if (typeof node['#text'] === 'string') return he.decode(stripHtml(node['#text']));
    if (typeof node.__cdata === 'string') return he.decode(stripHtml(node.__cdata));
    if (typeof node.text === 'string') return he.decode(stripHtml(node.text));
  }
  return '';
}

function urlFromNode(linkNode) {
  if (!linkNode) return '';
  if (typeof linkNode === 'string') return linkNode.trim();
  if (Array.isArray(linkNode)) {
    for (const item of linkNode) {
      const picked = urlFromNode(item);
      if (picked) return picked;
    }
    return '';
  }
  if (typeof linkNode === 'object') {
    if (typeof linkNode.href === 'string') return linkNode.href.trim();
    if (typeof linkNode['@_href'] === 'string') return linkNode['@_href'].trim();
  }
  return '';
}

function mediaThumbnailFromEntry(entry) {
  const thumb = entry?.['media:thumbnail'];
  if (thumb) {
    const first = Array.isArray(thumb) ? thumb[0] : thumb;
    if (typeof first?.url === 'string') return first.url;
  }

  const content = entry?.['media:content'];
  if (content) {
    const first = Array.isArray(content) ? content[0] : content;
    if (typeof first?.url === 'string' && String(first.type || '').startsWith('image')) {
      return first.url;
    }
  }

  const enclosure = entry?.enclosure;
  if (enclosure) {
    const first = Array.isArray(enclosure) ? enclosure[0] : enclosure;
    if (typeof first?.url === 'string' && String(first.type || '').startsWith('image')) {
      return first.url;
    }
  }

  return null;
}

function toOptionalIso(value) {
  if (!value) return null;
  const time = Date.parse(String(value));
  if (!Number.isFinite(time)) return null;
  return new Date(time).toISOString();
}

function hashKey(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash * 31 + value.charCodeAt(i)) | 0;
  }
  return Math.abs(hash).toString(36);
}

function dedupeNews(items) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = `${item.url}|${item.title}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function buildFallbackImagesFromNews(newsItems) {
  return (newsItems || [])
    .filter((item) => /^https?:\/\//i.test(item.image || ''))
    .slice(0, 12)
    .map((item) => ({
      id: hashKey(`${item.url}|${item.image || ''}`),
      title: item.title || 'News image',
      url: item.url,
      image: item.image,
      seenAt: item.publishedAt || null,
      sourceCountry: null,
      domain: item.sourceName || null
    }));
}

async function fetchVisualImageFeed() {
  const query =
    'sourcelang:english AND (Israel OR Gaza OR Lebanon) AND (rocket OR missile OR siren OR drone)';
  const url =
    `https://api.gdeltproject.org/api/v2/doc/doc?query=${encodeURIComponent(query)}` +
    '&mode=ArtList&maxrecords=28&format=json&sort=DateDesc';
  const response = await fetchWithTimeout(url, 9000, {
    headers: { 'User-Agent': 'rocket-alert-monitor/0.1 (+https://localhost)' }
  });
  if (!response.ok) {
    throw new Error(`gdelt (${response.status})`);
  }

  const raw = await response.text();
  if (/^Please limit requests/i.test(raw.trim())) {
    throw new Error('gdelt rate-limited');
  }

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error('gdelt malformed payload');
  }

  const rows = Array.isArray(parsed.articles) ? parsed.articles : [];
  return rows
    .filter((article) => /^https?:\/\//i.test(article.socialimage || ''))
    .slice(0, 24)
    .map((article) => ({
      id: hashKey(`${article.url}|${article.socialimage}`),
      title: String(article.title || '').trim() || 'Untitled',
      url: String(article.url || '').trim(),
      image: String(article.socialimage || '').trim(),
      seenAt: toOptionalIso(article.seendate),
      sourceCountry: article.sourcecountry || null,
      domain: article.domain || null
    }))
    .filter((entry) => /^https?:\/\//i.test(entry.url));
}

async function fetchWithTimeout(url, timeoutMs, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function seedLocationDirectory() {
  const now = new Date();
  const from = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);
  const params = new URLSearchParams({
    from: toApiDateTime(from),
    to: toApiDateTime(now)
  });

  const [rocketRows, uavRows] = await Promise.all([
    fetchLocationRows(ALERT_TYPES.ROCKET, params),
    fetchLocationRows(ALERT_TYPES.UAV, params)
  ]);

  const stamp = new Date().toISOString();
  const upsert = db.prepare(`
    INSERT INTO location_directory (place_normalized, place_label, lat, lon, source, updated_at)
    VALUES (@place_normalized, @place_label, @lat, @lon, @source, @updated_at)
    ON CONFLICT(place_normalized) DO UPDATE SET
      place_label = excluded.place_label,
      lat = excluded.lat,
      lon = excluded.lon,
      source = excluded.source,
      updated_at = excluded.updated_at
  `);

  const txn = db.transaction((rows) => {
    for (const row of rows) {
      upsert.run({
        place_normalized: row.key,
        place_label: row.label,
        lat: row.lat,
        lon: row.lon,
        source: row.source,
        updated_at: stamp
      });
    }
  });

  txn([...rocketRows, ...uavRows]);
  pruneDirectoryStaleness();
  console.log(`Location directory refreshed (${rocketRows.length + uavRows.length} entries).`);
}

function toApiDateTime(date) {
  return new Date(date).toISOString().slice(0, 19);
}

async function fetchLocationRows(alertTypeId, sharedParams) {
  const url = new URL(`${LOOKUP_API_BASE}/alerts/details`);
  url.search = sharedParams.toString();
  url.searchParams.set('alertTypeId', String(alertTypeId));

  const response = await fetch(url, {
    headers: { 'User-Agent': 'rocket-alert-monitor/0.1 (+https://localhost)' }
  });

  if (!response.ok) return [];

  const payload = await response.json();
  if (!payload?.success || !Array.isArray(payload.payload)) return [];

  const byKey = new Map();

  for (const dayBucket of payload.payload) {
    for (const alert of dayBucket.alerts || []) {
      if (!Number.isFinite(alert.lat) || !Number.isFinite(alert.lon)) continue;

      const candidates = [alert.name, alert.englishName].map(cleanPlaceLabel).filter(Boolean);

      for (const candidate of candidates) {
        const key = normalizePlaceName(candidate);
        if (!key) continue;
        if (!byKey.has(key)) {
          byKey.set(key, {
            key,
            label: candidate,
            lat: alert.lat,
            lon: alert.lon,
            source: 'rocketalert.lookup'
          });
        }
      }
    }
  }

  return [...byKey.values()];
}

function pruneDirectoryStaleness() {
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
  db.prepare('DELETE FROM location_directory WHERE updated_at < ?').run(cutoff);
}

async function backfillLast24Hours() {
  const cutoff = Date.now() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  let maxId = null;

  for (let page = 0; page < 12; page += 1) {
    const statuses = await fetchMastodonStatuses({ limit: 40, maxId });
    if (!Array.isArray(statuses) || statuses.length === 0) break;

    let stop = false;
    for (const status of statuses) {
      const createdAtMs = Date.parse(status.created_at);
      if (!Number.isFinite(createdAtMs)) continue;
      if (createdAtMs < cutoff) {
        stop = true;
        continue;
      }
      const stored = await ingestStatus(status, { emitEvent: false, allowGeocode: false });
      if (stored && (!sinceId || compareIds(stored.id, sinceId) > 0)) {
        sinceId = stored.id;
      }
    }

    const oldest = statuses[statuses.length - 1];
    maxId = oldest?.id;
    if (stop || !maxId) break;
  }

  pruneOldData();
  sinceId = getMaxAlertId();
  console.log('Initial backfill complete.');
}

async function pollLoop() {
  if (isPolling) return;
  isPolling = true;

  while (true) {
    try {
      const statuses = await fetchMastodonStatuses({ limit: 20, since: sinceId });
      if (Array.isArray(statuses) && statuses.length > 0) {
        const sorted = [...statuses].sort((a, b) => compareIds(a.id, b.id));
        const newlyStored = [];

        for (const status of sorted) {
          const stored = await ingestStatus(status, { emitEvent: true, allowGeocode: true });
          if (stored) newlyStored.push(stored);
          if (!sinceId || compareIds(status.id, sinceId) > 0) {
            sinceId = String(status.id);
          }
        }

        if (newlyStored.length > 0) {
          await ensureIntelFresh();
          broadcast('batch', {
            now: new Date().toISOString(),
            alerts: newlyStored,
            stats: getStatsWithinHours(HISTORY_WINDOW_HOURS),
            predictions: getPredictions(),
            intel: getIntelSnapshot()
          });
        }
      }

      pruneOldData();
    } catch (error) {
      console.error('Polling error:', error.message);
    }

    await delay(POLL_INTERVAL_MS);
  }
}

function compareIds(a, b) {
  const left = BigInt(String(a));
  const right = BigInt(String(b));
  if (left === right) return 0;
  return left > right ? 1 : -1;
}

async function ingestStatus(status, { emitEvent, allowGeocode }) {
  const alert = parseStatusToAlert(status);
  if (!alert) return null;

  const insertAlert = db.prepare(`
    INSERT OR IGNORE INTO alerts (alert_id, source_url, type, summary, raw_text, published_at, ingested_at)
    VALUES (@alert_id, @source_url, @type, @summary, @raw_text, @published_at, @ingested_at)
  `);

  const result = insertAlert.run({
    alert_id: alert.alertId,
    source_url: alert.sourceUrl,
    type: alert.type,
    summary: alert.summary,
    raw_text: alert.rawText,
    published_at: alert.publishedAt,
    ingested_at: new Date().toISOString()
  });

  if (result.changes === 0) return null;

  const placesOut = [];
  const placeInsert = db.prepare(`
    INSERT INTO alert_places (alert_id, place_raw, place_normalized, lat, lon, resolution_source)
    VALUES (@alert_id, @place_raw, @place_normalized, @lat, @lon, @resolution_source)
  `);

  for (const place of alert.places) {
    const resolved = await resolvePlace(place, { allowGeocode });

    placeInsert.run({
      alert_id: alert.alertId,
      place_raw: place,
      place_normalized: resolved.normalized,
      lat: resolved.lat,
      lon: resolved.lon,
      resolution_source: resolved.source
    });

    placesOut.push({
      name: place,
      lat: resolved.lat,
      lon: resolved.lon,
      source: resolved.source
    });
  }

  const payload = {
    id: alert.alertId,
    sourceUrl: alert.sourceUrl,
    type: alert.type,
    summary: alert.summary,
    publishedAt: alert.publishedAt,
    places: placesOut
  };

  if (emitEvent) {
    broadcast('alert', payload);
  }

  return payload;
}

async function resolvePlace(placeRaw, { allowGeocode }) {
  const normalized = normalizePlaceName(placeRaw);
  if (!normalized) {
    return { normalized: '', lat: null, lon: null, source: 'none' };
  }

  const inDirectory = db
    .prepare(
      'SELECT lat, lon, source FROM location_directory WHERE place_normalized = ? LIMIT 1'
    )
    .get(normalized);

  if (inDirectory) {
    return {
      normalized,
      lat: inDirectory.lat,
      lon: inDirectory.lon,
      source: inDirectory.source
    };
  }

  const cached = db
    .prepare('SELECT lat, lon, success FROM geocode_cache WHERE place_normalized = ? LIMIT 1')
    .get(normalized);

  if (cached) {
    return {
      normalized,
      lat: cached.success ? cached.lat : null,
      lon: cached.success ? cached.lon : null,
      source: cached.success ? 'nominatim.cache' : 'unknown'
    };
  }

  if (!allowGeocode) {
    return { normalized, lat: null, lon: null, source: 'unknown' };
  }

  const geocoded = await geocodePlace(placeRaw);
  db.prepare(`
    INSERT INTO geocode_cache (place_normalized, place_label, lat, lon, success, updated_at)
    VALUES (@place_normalized, @place_label, @lat, @lon, @success, @updated_at)
    ON CONFLICT(place_normalized) DO UPDATE SET
      place_label = excluded.place_label,
      lat = excluded.lat,
      lon = excluded.lon,
      success = excluded.success,
      updated_at = excluded.updated_at
  `).run({
    place_normalized: normalized,
    place_label: cleanPlaceLabel(placeRaw),
    lat: geocoded.lat,
    lon: geocoded.lon,
    success: geocoded.success ? 1 : 0,
    updated_at: new Date().toISOString()
  });

  return {
    normalized,
    lat: geocoded.success ? geocoded.lat : null,
    lon: geocoded.success ? geocoded.lon : null,
    source: geocoded.success ? 'nominatim.live' : 'unknown'
  };
}

async function geocodePlace(placeRaw) {
  const queries = [`${cleanPlaceLabel(placeRaw)}, Israel`, cleanPlaceLabel(placeRaw)];

  for (const query of queries) {
    const url = new URL('https://nominatim.openstreetmap.org/search');
    url.searchParams.set('format', 'jsonv2');
    url.searchParams.set('limit', '1');
    url.searchParams.set('q', query);

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 6000);
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'User-Agent': 'rocket-alert-monitor/0.1 (+https://localhost)'
        }
      });
      clearTimeout(timeout);

      if (!response.ok) continue;
      const rows = await response.json();
      if (Array.isArray(rows) && rows.length > 0) {
        const first = rows[0];
        const lat = Number(first.lat);
        const lon = Number(first.lon);

        if (Number.isFinite(lat) && Number.isFinite(lon)) {
          return { success: true, lat, lon };
        }
      }
    } catch {
      // Ignore and continue to fallback query.
    }
  }

  return { success: false, lat: null, lon: null };
}

function pruneOldData() {
  const cutoff = cutoffIso(HISTORY_WINDOW_HOURS);
  db.prepare('DELETE FROM alerts WHERE published_at < ?').run(cutoff);
}

function getAlertsWithinHours(hours) {
  const cutoff = cutoffIso(hours);
  const alerts = db
    .prepare(
      `
      SELECT alert_id, source_url, type, summary, raw_text, published_at
      FROM alerts
      WHERE published_at >= ?
      ORDER BY published_at DESC
      LIMIT 2000
    `
    )
    .all(cutoff);

  const placeRows = db
    .prepare(
      `
      SELECT alert_id, place_raw, place_normalized, lat, lon, resolution_source
      FROM alert_places
      WHERE alert_id IN (
        SELECT alert_id FROM alerts WHERE published_at >= ?
      )
    `
    )
    .all(cutoff);

  const byAlert = new Map();
  for (const row of placeRows) {
    if (!byAlert.has(row.alert_id)) byAlert.set(row.alert_id, []);
    byAlert.get(row.alert_id).push({
      name: row.place_raw,
      normalized: row.place_normalized,
      lat: row.lat,
      lon: row.lon,
      source: row.resolution_source
    });
  }

  return alerts.map((alert) => ({
    id: alert.alert_id,
    sourceUrl: alert.source_url,
    type: alert.type,
    summary: alert.summary,
    rawText: alert.raw_text,
    publishedAt: alert.published_at,
    places: byAlert.get(alert.alert_id) || []
  }));
}

function getStatsWithinHours(hours) {
  const cutoff = cutoffIso(hours);
  const totalAlerts = db
    .prepare('SELECT COUNT(*) AS count FROM alerts WHERE published_at >= ?')
    .get(cutoff).count;

  const latest = db
    .prepare(
      'SELECT alert_id, type, summary, published_at, source_url FROM alerts WHERE published_at >= ? ORDER BY published_at DESC LIMIT 1'
    )
    .get(cutoff);

  const hotspots = db
    .prepare(
      `
      SELECT place_raw AS name, COUNT(*) AS count
      FROM alert_places ap
      JOIN alerts a ON a.alert_id = ap.alert_id
      WHERE a.published_at >= ?
      GROUP BY place_raw
      ORDER BY count DESC
      LIMIT 8
    `
    )
    .all(cutoff);

  const byType = db
    .prepare(
      `
      SELECT type, COUNT(*) AS count
      FROM alerts
      WHERE published_at >= ?
      GROUP BY type
      ORDER BY count DESC
    `
    )
    .all(cutoff);

  return {
    totalAlerts,
    latest,
    hotspots,
    byType
  };
}

function getPredictions() {
  const alerts = db
    .prepare(
      `
      SELECT alert_id, published_at
      FROM alerts
      WHERE published_at >= ?
      ORDER BY published_at ASC
    `
    )
    .all(cutoffIso(HISTORY_WINDOW_HOURS));

  const times = alerts
    .map((row) => Date.parse(row.published_at))
    .filter((value) => Number.isFinite(value));
  const intervals = [];
  for (let i = 1; i < alerts.length; i += 1) {
    const prev = Date.parse(alerts[i - 1].published_at);
    const current = Date.parse(alerts[i].published_at);
    if (Number.isFinite(prev) && Number.isFinite(current) && current > prev) {
      intervals.push((current - prev) / (60 * 1000));
    }
  }

  const avgIntervalMin =
    intervals.length > 0
      ? Number((intervals.reduce((sum, v) => sum + v, 0) / intervals.length).toFixed(1))
      : null;

  const latestAt = alerts.length ? Date.parse(alerts[alerts.length - 1].published_at) : null;
  const nextLikelyAt =
    avgIntervalMin && latestAt
      ? new Date(latestAt + avgIntervalMin * 60 * 1000).toISOString()
      : null;

  const rollingErrors = [];
  // Self-scoring: predict each new alert from prior cadence only, then score error.
  for (let idx = 6; idx < times.length; idx += 1) {
    const historyIntervals = [];
    for (let j = 1; j < idx; j += 1) {
      const minutes = (times[j] - times[j - 1]) / (60 * 1000);
      if (minutes > 0) historyIntervals.push(minutes);
    }

    if (historyIntervals.length === 0) continue;
    const average =
      historyIntervals.reduce((sum, value) => sum + value, 0) / historyIntervals.length;
    const predictedAt = times[idx - 1] + average * 60 * 1000;
    const errorMinutes = Math.abs(times[idx] - predictedAt) / (60 * 1000);
    rollingErrors.push(errorMinutes);
  }

  const sortedErrors = [...rollingErrors].sort((a, b) => a - b);
  const mae =
    rollingErrors.length > 0
      ? Number((rollingErrors.reduce((sum, value) => sum + value, 0) / rollingErrors.length).toFixed(2))
      : null;
  const rmse =
    rollingErrors.length > 0
      ? Number(
          Math.sqrt(
            rollingErrors.reduce((sum, value) => sum + value * value, 0) / rollingErrors.length
          ).toFixed(2)
        )
      : null;
  const median =
    sortedErrors.length > 0
      ? Number(sortedErrors[Math.floor(sortedErrors.length / 2)].toFixed(2))
      : null;
  const within2 =
    rollingErrors.length > 0
      ? Number(
          (
            (rollingErrors.filter((value) => value <= 2).length / rollingErrors.length) *
            100
          ).toFixed(1)
        )
      : null;
  const within5 =
    rollingErrors.length > 0
      ? Number(
          (
            (rollingErrors.filter((value) => value <= 5).length / rollingErrors.length) *
            100
          ).toFixed(1)
        )
      : null;

  const recentHotspots = db
    .prepare(
      `
      SELECT place_raw AS name, COUNT(*) AS count
      FROM alert_places ap
      JOIN alerts a ON ap.alert_id = a.alert_id
      WHERE a.published_at >= ?
      GROUP BY place_raw
      ORDER BY count DESC
      LIMIT 5
    `
    )
    .all(cutoffIso(6));

  const hourly = db
    .prepare(
      `
      SELECT strftime('%H', published_at) AS hour, COUNT(*) AS count
      FROM alerts
      WHERE published_at >= ?
      GROUP BY hour
      ORDER BY hour ASC
    `
    )
    .all(cutoffIso(HISTORY_WINDOW_HOURS));

  let confidence = alerts.length >= 30 ? 'medium' : alerts.length >= 10 ? 'low-medium' : 'low';
  if (within5 !== null && within5 >= 70) confidence = 'high';
  else if (within5 !== null && within5 >= 45) confidence = 'medium';
  else if (within5 !== null && within5 >= 25) confidence = 'low-medium';
  else if (within5 !== null) confidence = 'low';

  return {
    model: 'rolling-interval-heuristic-v1',
    note: 'Heuristic only. Not safety-grade forecasting.',
    samples: alerts.length,
    averageIntervalMinutes: avgIntervalMin,
    nextLikelyAlertAt: nextLikelyAt,
    likelyHotspots: recentHotspots,
    hourlyTrend: hourly,
    score: {
      evaluations: rollingErrors.length,
      meanAbsoluteErrorMinutes: mae,
      rootMeanSquaredErrorMinutes: rmse,
      medianAbsoluteErrorMinutes: median,
      within2MinutesRate: within2,
      within5MinutesRate: within5,
      latestErrorMinutes:
        rollingErrors.length > 0
          ? Number(rollingErrors[rollingErrors.length - 1].toFixed(2))
          : null
    },
    confidence
  };
}

function sendSse(client, event, payload) {
  client.res.write(`event: ${event}\n`);
  client.res.write(`data: ${JSON.stringify(payload)}\n\n`);
}

function broadcast(event, payload) {
  for (const client of clients) {
    sendSse(client, event, payload);
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
