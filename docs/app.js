const MAX_TOASTS = 10;
const TOAST_TTL_MS = 60_000;
const HISTORY_WINDOW_HOURS = 24;
const STREET_DETAIL_ZOOM = 10.25;
const RECENT_ALERTS_VISIBLE = 14;
const MAX_ALERTS_STORED = 2000;
const POLL_INTERVAL_MS = 15_000;
const INTEL_REFRESH_MS = 120_000;
const LOCATION_DIRECTORY_REFRESH_MS = 30 * 60 * 1000;
const GEOCODE_TIMEOUT_MS = 6_000;

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

const PROXY_FETCH_CHAIN = [
  {
    name: 'allorigins-raw',
    buildUrl: (target) => `https://api.allorigins.win/raw?url=${encodeURIComponent(target)}`,
    parse: async (response) => response.text()
  },
  {
    name: 'allorigins-get',
    buildUrl: (target) => `https://api.allorigins.win/get?url=${encodeURIComponent(target)}`,
    parse: async (response) => {
      const payload = await response.json();
      return typeof payload?.contents === 'string' ? payload.contents : '';
    }
  },
  {
    name: 'corsproxy-io',
    buildUrl: (target) => `https://corsproxy.io/?${encodeURIComponent(target)}`,
    parse: async (response) => response.text()
  },
  {
    name: 'codetabs',
    buildUrl: (target) => `https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(target)}`,
    parse: async (response) => response.text()
  }
];

const STORAGE_KEYS = {
  alerts: 'raem.alerts.v2',
  runtime: 'raem.runtime.v2',
  intel: 'raem.intel.v2',
  placeDirectory: 'raem.placeDirectory.v2',
  geocodeCache: 'raem.geocodeCache.v2'
};

const state = {
  alerts: [],
  stats: null,
  predictions: null,
  intel: null,
  activeDrawer: null,
  newsUnread: false,
  latestNewsKey: null,
  connected: false,
  audioEnabled: false,
  audioContext: null,
  toasts: [],
  sinceId: null,
  placeDirectory: new Map(),
  geocodeCache: new Map(),
  locationDirectoryUpdatedAt: null,
  intelRefreshInFlight: false,
  alertsRefreshInFlight: false
};

const listEl = document.getElementById('alert-list');
const kpisEl = document.getElementById('kpis');
const hotspotEl = document.getElementById('hotspot-list');
const predictionEl = document.getElementById('prediction');
const livePillEl = document.getElementById('live-pill');
const audioToggleEl = document.getElementById('audio-toggle');
const toastStackEl = document.getElementById('live-alert-stack');
const resetViewEl = document.getElementById('reset-view');
const openInsightsTabEl = document.getElementById('open-insights-tab');
const openNewsTabEl = document.getElementById('open-news-tab');
const closeInsightsEl = document.getElementById('close-insights');
const closeNewsEl = document.getElementById('close-news');
const drawerBackdropEl = document.getElementById('drawer-backdrop');
const insightsDrawerEl = document.getElementById('insights-drawer');
const newsDrawerEl = document.getElementById('news-drawer');
const newsFeedEl = document.getElementById('news-feed');
const imageFeedEl = document.getElementById('image-feed');
const intelRibbonEl = document.getElementById('intel-ribbon');
const MAP_DEFAULT_CENTER = [17, 24];
const MAP_DEFAULT_ZOOM = 2.25;
const ALERT_FOCUS_PITCH = 62;
const ALERT_FOCUS_BEARING = -22;
const HISTORY_SOURCE_ID = 'alerts-history-source';
const HISTORY_LAYER_ID = 'alerts-history-layer';
const LIVE_SOURCE_ID = 'alerts-live-source';
const LIVE_RING_LAYER_ID = 'alerts-live-ring';
const LIVE_CORE_LAYER_ID = 'alerts-live-core';
const DARK_BASE_LAYER_ID = 'dark-base';
const DARK_LABEL_LAYER_ID = 'dark-labels';
const DETAIL_BASE_LAYER_ID = 'detail-base';
const DETAIL_LABEL_LAYER_ID = 'detail-labels';
const TERRAIN_SOURCE_ID = 'terrain-dem';

const map = new maplibregl.Map({
  container: 'map',
  style: buildMapStyle(),
  center: MAP_DEFAULT_CENTER,
  zoom: MAP_DEFAULT_ZOOM,
  pitch: 0,
  bearing: 0,
  minZoom: 2,
  maxZoom: 18,
  antialias: true
});
map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-left');

let hasInitialMapFit = false;
let mapReady = false;
let mapVisualMode = null;
let livePulseFeatures = [];

map.on('load', () => {
  initMapDataLayers();
  mapReady = true;
  setMapVisualMode('dark', { force: true });
  renderMap({ fitToData: true });
});

map.on('zoomend', () => {
  if (!mapReady) return;
  const shouldUseDetail = map.getZoom() >= STREET_DETAIL_ZOOM;
  setMapVisualMode(shouldUseDetail ? 'detail' : 'dark');
});

setInterval(() => {
  pruneLivePulseFeatures();
}, 1500);

setInterval(() => {
  pruneToasts();
}, 3000);

setDrawerMode(null);

function buildMapStyle() {
  return {
    version: 8,
    sources: {
      darkBase: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png',
          'https://d.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}.png'
        ],
        tileSize: 256,
        maxzoom: 19,
        attribution: '&copy; OpenStreetMap &copy; CARTO'
      },
      darkLabels: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png',
          'https://d.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}.png'
        ],
        tileSize: 256,
        maxzoom: 19
      },
      detailBase: {
        type: 'raster',
        tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
        tileSize: 256,
        maxzoom: 19,
        attribution: 'Tiles &copy; Esri'
      },
      detailLabels: {
        type: 'raster',
        tiles: [
          'https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}'
        ],
        tileSize: 256,
        maxzoom: 19
      },
      [TERRAIN_SOURCE_ID]: {
        type: 'raster-dem',
        url: 'https://demotiles.maplibre.org/terrain-tiles/tiles.json',
        tileSize: 256
      }
    },
    layers: [
      {
        id: DARK_BASE_LAYER_ID,
        type: 'raster',
        source: 'darkBase',
        layout: { visibility: 'visible' }
      },
      {
        id: DARK_LABEL_LAYER_ID,
        type: 'raster',
        source: 'darkLabels',
        layout: { visibility: 'visible' },
        paint: { 'raster-opacity': 0.9 }
      },
      {
        id: DETAIL_BASE_LAYER_ID,
        type: 'raster',
        source: 'detailBase',
        layout: { visibility: 'none' }
      },
      {
        id: DETAIL_LABEL_LAYER_ID,
        type: 'raster',
        source: 'detailLabels',
        layout: { visibility: 'none' },
        paint: { 'raster-opacity': 0.93 }
      }
    ]
  };
}

function initMapDataLayers() {
  map.addSource(HISTORY_SOURCE_ID, {
    type: 'geojson',
    data: emptyFeatureCollection()
  });

  map.addLayer({
    id: HISTORY_LAYER_ID,
    type: 'circle',
    source: HISTORY_SOURCE_ID,
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['get', 'ageRatio'], 0.05, 3, 1, 10],
      'circle-color': ['interpolate', ['linear'], ['get', 'ageRatio'], 0.05, '#ff9463', 1, '#ffd46f'],
      'circle-opacity': ['interpolate', ['linear'], ['get', 'ageRatio'], 0.05, 0.22, 1, 0.85],
      'circle-stroke-width': 1.2,
      'circle-stroke-color': ['interpolate', ['linear'], ['get', 'ageRatio'], 0.05, '#ffd9c8', 1, '#fff2da']
    }
  });

  map.addSource(LIVE_SOURCE_ID, {
    type: 'geojson',
    data: emptyFeatureCollection()
  });

  map.addLayer({
    id: LIVE_RING_LAYER_ID,
    type: 'circle',
    source: LIVE_SOURCE_ID,
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 10, 10, 18, 15, 26],
      'circle-color': 'rgba(255, 146, 110, 0.06)',
      'circle-opacity': 1,
      'circle-stroke-width': 2.2,
      'circle-stroke-color': 'rgba(255, 146, 110, 0.88)'
    }
  });

  map.addLayer({
    id: LIVE_CORE_LAYER_ID,
    type: 'circle',
    source: LIVE_SOURCE_ID,
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 4, 10, 6, 15, 8],
      'circle-color': '#ff6f6f',
      'circle-opacity': 0.95,
      'circle-stroke-width': 2,
      'circle-stroke-color': '#ffe4dc'
    }
  });

  bindMapTooltip(HISTORY_LAYER_ID);
  bindMapTooltip(LIVE_CORE_LAYER_ID);
}

function bindMapTooltip(layerId) {
  const popup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    className: 'map-tooltip',
    offset: 14
  });

  map.on('mousemove', layerId, (event) => {
    const feature = event.features?.[0];
    if (!feature) return;

    const coordinates = feature.geometry?.coordinates;
    if (!Array.isArray(coordinates) || coordinates.length !== 2) return;

    const type = feature.properties?.type || 'Alert';
    const place = feature.properties?.place || 'Unknown';
    const published = feature.properties?.published || '';
    popup
      .setLngLat([coordinates[0], coordinates[1]])
      .setHTML(
        `<strong>${escapeHtml(type)}</strong><br>${escapeHtml(place)}<br>${escapeHtml(formatTime(published))}`
      )
      .addTo(map);
    map.getCanvas().style.cursor = 'pointer';
  });

  map.on('mouseleave', layerId, () => {
    popup.remove();
    map.getCanvas().style.cursor = '';
  });
}

function setSourceData(sourceId, features) {
  const source = map.getSource(sourceId);
  if (!source) return;
  source.setData(featureCollection(features));
}

function featureCollection(features) {
  return {
    type: 'FeatureCollection',
    features
  };
}

function emptyFeatureCollection() {
  return featureCollection([]);
}

bootstrap().catch((error) => {
  console.error(error);
  livePillEl.textContent = 'Failed to load';
});

audioToggleEl.addEventListener('click', () => {
  if (!state.audioContext) {
    state.audioContext = new (window.AudioContext || window.webkitAudioContext)();
  }

  state.audioEnabled = !state.audioEnabled;
  audioToggleEl.textContent = state.audioEnabled ? 'Audio Enabled' : 'Enable Audio Alerts';
  audioToggleEl.classList.toggle('active', state.audioEnabled);
  persistRuntime();
});

toastStackEl.addEventListener('click', (event) => {
  const closeButton = event.target.closest('[data-toast-close]');
  if (!closeButton) return;

  const toastId = closeButton.getAttribute('data-toast-close');
  state.toasts = state.toasts.filter((toast) => toast.id !== toastId);
  renderToasts();
});

listEl.addEventListener('click', (event) => {
  if (event.target.closest('a')) return;

  const row = event.target.closest('[data-alert-id]');
  if (!row) return;

  const alertId = row.getAttribute('data-alert-id');
  if (!alertId) return;
  focusAlertFromList(alertId);
});

listEl.addEventListener('keydown', (event) => {
  if (event.key !== 'Enter' && event.key !== ' ') return;

  const row = event.target.closest('[data-alert-id]');
  if (!row) return;

  event.preventDefault();
  const alertId = row.getAttribute('data-alert-id');
  if (!alertId) return;
  focusAlertFromList(alertId);
});

resetViewEl.addEventListener('click', () => {
  fitMapToHistory({ animate: true });
});

openInsightsTabEl.addEventListener('click', () => {
  const next = state.activeDrawer === 'insights' ? null : 'insights';
  setDrawerMode(next);
});

openNewsTabEl.addEventListener('click', () => {
  const next = state.activeDrawer === 'news' ? null : 'news';
  setDrawerMode(next);
});

closeInsightsEl.addEventListener('click', () => {
  setDrawerMode(null);
});

closeNewsEl.addEventListener('click', () => {
  setDrawerMode(null);
});

drawerBackdropEl.addEventListener('click', () => {
  setDrawerMode(null);
});

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    setDrawerMode(null);
  }
});

async function bootstrap() {
  loadPersistedState();
  recalculateDerivedData();
  renderAll({ fitToData: true });

  await seedLocationDirectory({ force: true });
  await refreshAlerts({ backfill: state.alerts.length === 0, fitToData: true, announce: false });
  await refreshIntel({ force: true });

  setInterval(() => {
    void refreshAlerts({ backfill: false, fitToData: false, announce: true });
  }, POLL_INTERVAL_MS);

  setInterval(() => {
    void refreshIntel({ force: false });
  }, INTEL_REFRESH_MS);

  setInterval(() => {
    void seedLocationDirectory({ force: false });
  }, LOCATION_DIRECTORY_REFRESH_MS);

  setInterval(() => {
    pruneOldAlerts();
    persistAlerts();
  }, 60_000);
}

function loadPersistedState() {
  state.alerts = normalizeAlerts(readStorage(STORAGE_KEYS.alerts, [])).slice(0, MAX_ALERTS_STORED);
  pruneOldAlerts();

  const runtime = readStorage(STORAGE_KEYS.runtime, {});
  state.sinceId = runtime.sinceId || getMaxAlertId(state.alerts);
  state.latestNewsKey = runtime.latestNewsKey || null;
  state.audioEnabled = Boolean(runtime.audioEnabled);
  state.locationDirectoryUpdatedAt = runtime.locationDirectoryUpdatedAt || null;

  const directoryRows = readStorage(STORAGE_KEYS.placeDirectory, []);
  state.placeDirectory = new Map(
    directoryRows
      .filter((row) => row && typeof row.key === 'string')
      .map((row) => [row.key, row])
  );

  const geocodeRows = readStorage(STORAGE_KEYS.geocodeCache, []);
  state.geocodeCache = new Map(
    geocodeRows
      .filter((row) => row && typeof row.key === 'string')
      .map((row) => [row.key, row])
  );

  state.intel = readStorage(STORAGE_KEYS.intel, emptyIntel());

  audioToggleEl.textContent = state.audioEnabled ? 'Audio Enabled' : 'Enable Audio Alerts';
  audioToggleEl.classList.toggle('active', state.audioEnabled);
}

function persistAlerts() {
  writeStorage(STORAGE_KEYS.alerts, state.alerts.slice(0, MAX_ALERTS_STORED));
}

function persistRuntime() {
  writeStorage(STORAGE_KEYS.runtime, {
    sinceId: state.sinceId,
    latestNewsKey: state.latestNewsKey,
    audioEnabled: state.audioEnabled,
    locationDirectoryUpdatedAt: state.locationDirectoryUpdatedAt
  });
}

function persistIntel() {
  writeStorage(STORAGE_KEYS.intel, state.intel || emptyIntel());
}

function persistLocationDirectory() {
  writeStorage(STORAGE_KEYS.placeDirectory, [...state.placeDirectory.values()].slice(0, 5000));
}

function persistGeocodeCache() {
  writeStorage(STORAGE_KEYS.geocodeCache, [...state.geocodeCache.values()].slice(0, 5000));
}

function readStorage(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function writeStorage(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    console.warn('localStorage write failed for', key, error);
  }
}

async function refreshAlerts({ backfill = false, fitToData = false, announce = true } = {}) {
  if (state.alertsRefreshInFlight) return;
  state.alertsRefreshInFlight = true;

  try {
    const fetchedStatuses = backfill
      ? await backfillLast24Hours()
      : await fetchMastodonStatuses({ limit: 20, since: state.sinceId });

    if (!Array.isArray(fetchedStatuses) || fetchedStatuses.length === 0) {
      setLive(true);
      return;
    }

    const sortedStatuses = [...fetchedStatuses].sort((a, b) => compareIds(a.id, b.id));
    const incomingAlerts = await statusesToAlerts(sortedStatuses, {
      allowGeocode: !backfill,
      allowLiveLookup: !backfill
    });

    const newlyAdded = ingestAlerts(incomingAlerts, { fitToData });
    if (newlyAdded.length > 0 && announce) {
      for (const alert of newlyAdded.slice(-MAX_TOASTS)) {
        showAlertToast(alert);
        pulseLivePlaces(alert);
      }
      const newest = newlyAdded[newlyAdded.length - 1];
      if (newest) {
        playTone();
        zoomToAlert(newest);
      }
      renderKpis();
      renderHotspots();
      renderPrediction();
    }

    const latestId = sortedStatuses[sortedStatuses.length - 1]?.id;
    if (latestId && (!state.sinceId || compareIds(latestId, state.sinceId) > 0)) {
      state.sinceId = String(latestId);
      persistRuntime();
    }

    setLive(true);
  } catch (error) {
    console.error('alert refresh failed', error);
    setLive(false);
  } finally {
    state.alertsRefreshInFlight = false;
  }
}

async function backfillLast24Hours() {
  const cutoff = Date.now() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  const rows = [];
  let maxId = null;

  for (let page = 0; page < 12; page += 1) {
    const statuses = await fetchMastodonStatuses({ limit: 40, maxId });
    if (!Array.isArray(statuses) || statuses.length === 0) break;

    let reachedCutoff = false;
    for (const status of statuses) {
      const created = Date.parse(status.created_at);
      if (!Number.isFinite(created) || created < cutoff) {
        reachedCutoff = true;
        continue;
      }
      rows.push(status);
    }

    const oldest = statuses[statuses.length - 1];
    maxId = oldest?.id || null;
    if (reachedCutoff || !maxId) break;
  }

  return rows;
}

async function fetchMastodonStatuses({ limit = 40, maxId = null, since = null } = {}) {
  const url = new URL(`${MASTODON_API_BASE}/accounts/${MASTODON_ACCOUNT_ID}/statuses`);
  url.searchParams.set('exclude_reblogs', 'true');
  url.searchParams.set('exclude_replies', 'true');
  url.searchParams.set('limit', String(limit));
  if (maxId) url.searchParams.set('max_id', String(maxId));
  if (since) url.searchParams.set('since_id', String(since));

  const response = await fetchWithTimeout(url, 12_000, {
    cache: 'no-store'
  });
  if (!response.ok) {
    throw new Error(`Mastodon API failed (${response.status})`);
  }

  return response.json();
}

async function statusesToAlerts(statuses, { allowGeocode, allowLiveLookup }) {
  const out = [];
  for (const status of statuses) {
    const parsed = parseStatusToAlert(status);
    if (!parsed) continue;

    const places = [];
    for (const place of parsed.places) {
      const resolved = await resolvePlace(place, { allowGeocode, allowLiveLookup });
      places.push({
        name: place,
        lat: resolved.lat,
        lon: resolved.lon,
        source: resolved.source
      });
    }

    out.push({
      id: parsed.alertId,
      sourceUrl: parsed.sourceUrl,
      type: parsed.type,
      summary: parsed.summary,
      rawText: parsed.rawText,
      publishedAt: parsed.publishedAt,
      places
    });
  }
  return out;
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
    publishedAt: new Date(status.created_at).toISOString(),
    type,
    summary: summaryLine,
    rawText: lines.join('\n'),
    places
  };
}

function htmlToLines(html) {
  const parser = new DOMParser();
  const documentNode = parser.parseFromString(`<div>${String(html || '')}</div>`, 'text/html');
  const root = documentNode.body.firstElementChild;
  if (!root) return [];

  root.querySelectorAll('br').forEach((br) => br.replaceWith('\n'));
  root.querySelectorAll('p').forEach((p) => p.append('\n'));

  return root.textContent
    .replace(/\u00A0/g, ' ')
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);
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

async function seedLocationDirectory({ force = false } = {}) {
  const last = state.locationDirectoryUpdatedAt ? Date.parse(state.locationDirectoryUpdatedAt) : 0;
  const isFresh = Number.isFinite(last) && Date.now() - last < LOCATION_DIRECTORY_REFRESH_MS;
  if (!force && isFresh) return;

  try {
    const now = new Date();
    const from = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000);

    const [rocketRows, uavRows] = await Promise.all([
      fetchLocationRows(ALERT_TYPES.ROCKET, from, now),
      fetchLocationRows(ALERT_TYPES.UAV, from, now)
    ]);

    let changed = false;
    for (const row of [...rocketRows, ...uavRows]) {
      if (!row || !row.key) continue;
      if (!state.placeDirectory.has(row.key)) {
        changed = true;
      }
      state.placeDirectory.set(row.key, row);
    }

    state.locationDirectoryUpdatedAt = new Date().toISOString();
    persistRuntime();
    if (changed) persistLocationDirectory();
  } catch (error) {
    console.warn('location directory refresh failed', error);
  }
}

async function fetchLocationRows(alertTypeId, fromDate, toDate) {
  const url = new URL(`${LOOKUP_API_BASE}/alerts/details`);
  url.searchParams.set('from', toApiDateTime(fromDate));
  url.searchParams.set('to', toApiDateTime(toDate));
  url.searchParams.set('alertTypeId', String(alertTypeId));

  const payload = await fetchJsonWithProxyFallback(url, 12_000);
  if (!payload?.success || !Array.isArray(payload.payload)) return [];

  const byKey = new Map();
  for (const dayBucket of payload.payload) {
    for (const alert of dayBucket.alerts || []) {
      if (!Number.isFinite(alert.lat) || !Number.isFinite(alert.lon)) continue;

      const candidates = [alert.name, alert.englishName].map(cleanPlaceLabel).filter(Boolean);
      for (const candidate of candidates) {
        const key = normalizePlaceName(candidate);
        if (!key || byKey.has(key)) continue;

        byKey.set(key, {
          key,
          label: candidate,
          lat: alert.lat,
          lon: alert.lon,
          source: 'rocketalert.lookup',
          updatedAt: new Date().toISOString()
        });
      }
    }
  }

  return [...byKey.values()];
}

async function resolvePlace(placeRaw, { allowGeocode, allowLiveLookup }) {
  const normalized = normalizePlaceName(placeRaw);
  if (!normalized) {
    return { normalized: '', lat: null, lon: null, source: 'none' };
  }

  const fromDirectory = state.placeDirectory.get(normalized);
  if (fromDirectory) {
    return {
      normalized,
      lat: fromDirectory.lat,
      lon: fromDirectory.lon,
      source: fromDirectory.source || 'rocketalert.lookup'
    };
  }

  const fromCache = state.geocodeCache.get(normalized);
  if (fromCache) {
    return {
      normalized,
      lat: fromCache.success ? fromCache.lat : null,
      lon: fromCache.success ? fromCache.lon : null,
      source: fromCache.success ? 'nominatim.cache' : 'unknown'
    };
  }

  if (allowLiveLookup) {
    const live = await tryResolveViaLookupApi(normalized, placeRaw);
    if (live) {
      state.placeDirectory.set(normalized, live);
      persistLocationDirectory();
      return {
        normalized,
        lat: live.lat,
        lon: live.lon,
        source: live.source
      };
    }
  }

  if (!allowGeocode) {
    return { normalized, lat: null, lon: null, source: 'unknown' };
  }

  const geocoded = await geocodePlace(placeRaw);
  state.geocodeCache.set(normalized, {
    key: normalized,
    label: cleanPlaceLabel(placeRaw),
    lat: geocoded.lat,
    lon: geocoded.lon,
    success: geocoded.success,
    updatedAt: new Date().toISOString()
  });
  persistGeocodeCache();

  return {
    normalized,
    lat: geocoded.success ? geocoded.lat : null,
    lon: geocoded.success ? geocoded.lon : null,
    source: geocoded.success ? 'nominatim.live' : 'unknown'
  };
}

async function tryResolveViaLookupApi(normalized, placeRaw) {
  try {
    const now = new Date();
    const from = new Date(Date.now() - 6 * 60 * 60 * 1000);
    const [rocketRows, uavRows] = await Promise.all([
      fetchLocationRows(ALERT_TYPES.ROCKET, from, now),
      fetchLocationRows(ALERT_TYPES.UAV, from, now)
    ]);

    for (const row of [...rocketRows, ...uavRows]) {
      if (!row?.key) continue;
      state.placeDirectory.set(row.key, row);
    }
    persistLocationDirectory();

    const found = state.placeDirectory.get(normalized);
    if (found) return found;

    const fallbackKey = normalizePlaceName(cleanPlaceLabel(placeRaw));
    return state.placeDirectory.get(fallbackKey) || null;
  } catch {
    return null;
  }
}

async function geocodePlace(placeRaw) {
  const queries = [`${cleanPlaceLabel(placeRaw)}, Israel`, cleanPlaceLabel(placeRaw)];

  for (const query of queries) {
    try {
      const url = new URL('https://nominatim.openstreetmap.org/search');
      url.searchParams.set('format', 'jsonv2');
      url.searchParams.set('limit', '1');
      url.searchParams.set('q', query);

      const rows = await fetchJsonWithProxyFallback(url, GEOCODE_TIMEOUT_MS);
      if (!Array.isArray(rows) || rows.length === 0) continue;

      const lat = Number(rows[0]?.lat);
      const lon = Number(rows[0]?.lon);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        return { success: true, lat, lon };
      }
    } catch {
      // Continue to fallback query.
    }
  }

  return { success: false, lat: null, lon: null };
}

async function refreshIntel({ force = false } = {}) {
  const last = state.intel?.updatedAt ? Date.parse(state.intel.updatedAt) : 0;
  const freshEnough = Number.isFinite(last) && Date.now() - last < INTEL_REFRESH_MS;
  if (!force && freshEnough) return;
  if (state.intelRefreshInFlight) return;

  state.intelRefreshInFlight = true;
  try {
    const errors = [];
    const [newsSettled, imagesSettled] = await Promise.allSettled([
      fetchRssNews(),
      fetchVisualImageFeed()
    ]);

    const nextNews = newsSettled.status === 'fulfilled' ? newsSettled.value : state.intel?.news || [];
    const nextImages =
      imagesSettled.status === 'fulfilled'
        ? imagesSettled.value
        : state.intel?.images || buildFallbackImagesFromNews(nextNews);

    if (newsSettled.status === 'rejected') {
      errors.push(`news: ${newsSettled.reason?.message || 'unknown error'}`);
    }
    if (imagesSettled.status === 'rejected') {
      errors.push(`images: ${imagesSettled.reason?.message || 'unknown error'}`);
    }

    state.intel = {
      updatedAt: new Date().toISOString(),
      status: errors.length ? 'degraded' : 'ok',
      errors,
      news: nextNews,
      images: nextImages,
      webcams: [],
      dashboards: []
    };

    persistIntel();
    renderIntel();
  } catch (error) {
    console.error('intel refresh failed', error);
  } finally {
    state.intelRefreshInFlight = false;
  }
}

async function fetchRssNews() {
  const perSource = await Promise.allSettled(RSS_NEWS_SOURCES.map((source) => fetchNewsSource(source)));
  const all = [];

  for (const settled of perSource) {
    if (settled.status === 'fulfilled') {
      all.push(...settled.value);
    }
  }

  return dedupeNews(all)
    .sort((a, b) => {
      const left = a.publishedAt ? Date.parse(a.publishedAt) : 0;
      const right = b.publishedAt ? Date.parse(b.publishedAt) : 0;
      return right - left;
    })
    .slice(0, 48);
}

async function fetchNewsSource(source) {
  const xmlText = await fetchTextWithProxyFallback(source.url, 10_000);
  const parser = new DOMParser();
  const xml = parser.parseFromString(xmlText, 'text/xml');
  if (xml.querySelector('parsererror')) {
    throw new Error(`${source.id}: malformed XML`);
  }

  const rssItems = [...xml.getElementsByTagName('item')];
  const atomEntries = [...xml.getElementsByTagName('entry')];
  const entries = (rssItems.length > 0 ? rssItems : atomEntries).slice(0, 16);

  return entries
    .map((entry) => normalizeFeedEntry(entry, source))
    .filter((item) => item && /^https?:\/\//i.test(item.url));
}

function normalizeFeedEntry(entry, source) {
  const title = textFromXmlElement(firstXmlElement(entry, ['title']));
  const link = extractEntryLink(entry);
  if (!title || !link) return null;

  const summaryNode = firstXmlElement(entry, ['description', 'summary', 'content', 'content:encoded']);
  const summaryRaw = summaryNode?.textContent || '';
  const summary = stripHtml(summaryRaw);
  const publishedRaw = textFromXmlElement(
    firstXmlElement(entry, ['pubDate', 'published', 'updated', 'dc:date'])
  );
  const image = extractEntryImage(entry, summaryRaw);

  return {
    id: `${source.id}-${hashKey(link)}`,
    sourceId: source.id,
    sourceName: source.name,
    title,
    url: link,
    summary,
    publishedAt: toOptionalIso(publishedRaw),
    image
  };
}

function firstXmlElement(root, names) {
  for (const name of names) {
    const node = root.getElementsByTagName(name)[0];
    if (node) return node;
  }
  return null;
}

function textFromXmlElement(node) {
  if (!node) return '';
  return String(node.textContent || '').trim();
}

function extractEntryLink(entry) {
  const linkNodes = [...entry.getElementsByTagName('link')];
  for (const link of linkNodes) {
    const href = link.getAttribute('href');
    if (href && /^https?:\/\//i.test(href)) return href.trim();

    const value = String(link.textContent || '').trim();
    if (value && /^https?:\/\//i.test(value)) return value;
  }

  const guid = textFromXmlElement(firstXmlElement(entry, ['guid', 'id']));
  return /^https?:\/\//i.test(guid) ? guid : '';
}

function extractEntryImage(entry, summaryRaw) {
  const mediaThumb = firstXmlElement(entry, ['media:thumbnail', 'thumbnail']);
  if (mediaThumb) {
    const url = mediaThumb.getAttribute('url');
    if (url) return url;
  }

  const mediaContent = firstXmlElement(entry, ['media:content']);
  if (mediaContent) {
    const url = mediaContent.getAttribute('url');
    const type = String(mediaContent.getAttribute('type') || '');
    if (url && (!type || type.startsWith('image'))) return url;
  }

  const enclosure = firstXmlElement(entry, ['enclosure']);
  if (enclosure) {
    const url = enclosure.getAttribute('url');
    const type = String(enclosure.getAttribute('type') || '');
    if (url && (!type || type.startsWith('image'))) return url;
  }

  const match = String(summaryRaw || '').match(/<img[^>]+src=["']([^"']+)["']/i);
  return match ? match[1] : null;
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

async function fetchVisualImageFeed() {
  const query =
    'sourcelang:english AND (Israel OR Gaza OR Lebanon) AND (rocket OR missile OR siren OR drone)';
  const url =
    `https://api.gdeltproject.org/api/v2/doc/doc?query=${encodeURIComponent(query)}` +
    '&mode=ArtList&maxrecords=40&format=json&sort=DateDesc';

  const raw = await fetchTextWithProxyFallback(url, 10_000);
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
    .slice(0, 36)
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

async function fetchTextWithProxyFallback(url, timeoutMs = 10_000) {
  const target = String(url);
  const attempts = [];

  if (shouldAttemptDirectFetch(target)) {
    attempts.push({
      name: 'direct',
      url: target,
      parse: async (response) => response.text()
    });
  }

  for (const proxy of PROXY_FETCH_CHAIN) {
    attempts.push({
      name: proxy.name,
      url: proxy.buildUrl(target),
      parse: proxy.parse
    });
  }

  const errors = [];
  for (const attempt of attempts) {
    try {
      const response = await fetchWithTimeout(attempt.url, timeoutMs, { cache: 'no-store' });
      if (!response.ok) {
        errors.push(`${attempt.name}: status ${response.status}`);
        continue;
      }

      const body = await attempt.parse(response);
      if (typeof body === 'string' && body.trim()) {
        return body;
      }
      errors.push(`${attempt.name}: empty body`);
    } catch (error) {
      errors.push(`${attempt.name}: ${error?.message || 'request failed'}`);
    }
  }

  throw new Error(`All fetch attempts failed for ${target} (${errors.join(' | ')})`);
}

async function fetchJsonWithProxyFallback(url, timeoutMs = 10_000) {
  const raw = await fetchTextWithProxyFallback(url, timeoutMs);
  try {
    return JSON.parse(raw);
  } catch (error) {
    throw new Error(`json parse failed: ${error?.message || 'invalid JSON'}`);
  }
}

function shouldAttemptDirectFetch(url) {
  try {
    const parsed = new URL(String(url), window.location.href);
    if (parsed.origin === window.location.origin) return true;
    return !/\.github\.io$/i.test(window.location.hostname);
  } catch {
    return false;
  }
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

function toApiDateTime(date) {
  return new Date(date).toISOString().slice(0, 19);
}

function toOptionalIso(value) {
  if (!value) return null;
  const time = Date.parse(String(value));
  if (!Number.isFinite(time)) return null;
  return new Date(time).toISOString();
}

function stripHtml(value) {
  return String(value || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function hashKey(value) {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash * 31 + value.charCodeAt(i)) | 0;
  }
  return Math.abs(hash).toString(36);
}

function normalizeAlerts(alerts) {
  return [...alerts].sort((a, b) => Date.parse(b.publishedAt) - Date.parse(a.publishedAt));
}

function ingestAlerts(incoming, { fitToData = false } = {}) {
  const byId = new Map(state.alerts.map((item) => [item.id, item]));
  const newlyAdded = [];

  for (const alert of incoming) {
    if (!alert?.id) continue;
    if (!byId.has(alert.id)) {
      newlyAdded.push(alert);
    }
    byId.set(alert.id, alert);
  }

  state.alerts = [...byId.values()]
    .sort((a, b) => Date.parse(b.publishedAt) - Date.parse(a.publishedAt))
    .slice(0, MAX_ALERTS_STORED);

  pruneOldAlerts();
  recalculateDerivedData();
  persistAlerts();

  renderAlerts();
  renderMap({ fitToData });

  return newlyAdded;
}

function pruneOldAlerts() {
  const cutoff = Date.now() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  state.alerts = state.alerts.filter((alert) => {
    const ts = Date.parse(alert.publishedAt);
    return Number.isFinite(ts) && ts >= cutoff;
  });
}

function recalculateDerivedData() {
  state.stats = computeStats(state.alerts);
  state.predictions = computePredictions(state.alerts);
}

function computeStats(alerts) {
  const cutoff = Date.now() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  const windowAlerts = alerts.filter((alert) => {
    const ts = Date.parse(alert.publishedAt);
    return Number.isFinite(ts) && ts >= cutoff;
  });

  const byPlace = new Map();
  const byType = new Map();

  for (const alert of windowAlerts) {
    const type = String(alert.type || 'Alert').trim() || 'Alert';
    byType.set(type, (byType.get(type) || 0) + 1);

    for (const place of alert.places || []) {
      const name = cleanPlaceLabel(place.name);
      if (!name) continue;
      byPlace.set(name, (byPlace.get(name) || 0) + 1);
    }
  }

  const hotspots = [...byPlace.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  const byTypeRows = [...byType.entries()]
    .map(([type, count]) => ({ type, count }))
    .sort((a, b) => b.count - a.count);

  return {
    totalAlerts: windowAlerts.length,
    latest:
      windowAlerts[0] && windowAlerts[0].id
        ? {
            alert_id: windowAlerts[0].id,
            type: windowAlerts[0].type,
            summary: windowAlerts[0].summary,
            published_at: windowAlerts[0].publishedAt,
            source_url: windowAlerts[0].sourceUrl
          }
        : null,
    hotspots,
    byType: byTypeRows
  };
}

function computePredictions(alerts) {
  const cutoff = Date.now() - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  const timeRows = alerts
    .filter((alert) => {
      const ts = Date.parse(alert.publishedAt);
      return Number.isFinite(ts) && ts >= cutoff;
    })
    .map((alert) => ({ id: alert.id, publishedAt: alert.publishedAt }))
    .sort((a, b) => Date.parse(a.publishedAt) - Date.parse(b.publishedAt));

  const times = timeRows.map((row) => Date.parse(row.publishedAt)).filter((value) => Number.isFinite(value));

  const intervals = [];
  for (let i = 1; i < times.length; i += 1) {
    const minutes = (times[i] - times[i - 1]) / (60 * 1000);
    if (minutes > 0) intervals.push(minutes);
  }

  const avgIntervalMin =
    intervals.length > 0
      ? Number((intervals.reduce((sum, value) => sum + value, 0) / intervals.length).toFixed(1))
      : null;

  const latestAt = times.length ? times[times.length - 1] : null;
  const nextLikelyAt =
    avgIntervalMin && latestAt ? new Date(latestAt + avgIntervalMin * 60 * 1000).toISOString() : null;

  const rollingErrors = [];
  for (let idx = 6; idx < times.length; idx += 1) {
    const historyIntervals = [];
    for (let j = 1; j < idx; j += 1) {
      const minutes = (times[j] - times[j - 1]) / (60 * 1000);
      if (minutes > 0) historyIntervals.push(minutes);
    }

    if (historyIntervals.length === 0) continue;
    const average = historyIntervals.reduce((sum, value) => sum + value, 0) / historyIntervals.length;
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
    sortedErrors.length > 0 ? Number(sortedErrors[Math.floor(sortedErrors.length / 2)].toFixed(2)) : null;
  const within2 =
    rollingErrors.length > 0
      ? Number(((rollingErrors.filter((value) => value <= 2).length / rollingErrors.length) * 100).toFixed(1))
      : null;
  const within5 =
    rollingErrors.length > 0
      ? Number(((rollingErrors.filter((value) => value <= 5).length / rollingErrors.length) * 100).toFixed(1))
      : null;

  const recentCutoff = Date.now() - 6 * 60 * 60 * 1000;
  const hotspotMap = new Map();
  for (const alert of alerts) {
    const ts = Date.parse(alert.publishedAt);
    if (!Number.isFinite(ts) || ts < recentCutoff) continue;
    for (const place of alert.places || []) {
      const name = cleanPlaceLabel(place.name);
      if (!name) continue;
      hotspotMap.set(name, (hotspotMap.get(name) || 0) + 1);
    }
  }

  const recentHotspots = [...hotspotMap.entries()]
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 5);

  const hourlyCounts = new Map();
  for (const row of timeRows) {
    const date = new Date(row.publishedAt);
    const hour = String(date.getHours()).padStart(2, '0');
    hourlyCounts.set(hour, (hourlyCounts.get(hour) || 0) + 1);
  }

  const hourlyTrend = [...hourlyCounts.entries()]
    .map(([hour, count]) => ({ hour, count }))
    .sort((a, b) => a.hour.localeCompare(b.hour));

  let confidence = timeRows.length >= 30 ? 'medium' : timeRows.length >= 10 ? 'low-medium' : 'low';
  if (within5 !== null && within5 >= 70) confidence = 'high';
  else if (within5 !== null && within5 >= 45) confidence = 'medium';
  else if (within5 !== null && within5 >= 25) confidence = 'low-medium';
  else if (within5 !== null) confidence = 'low';

  return {
    model: 'rolling-interval-heuristic-v1',
    note: 'Heuristic only. Not safety-grade forecasting.',
    samples: timeRows.length,
    averageIntervalMinutes: avgIntervalMin,
    nextLikelyAlertAt: nextLikelyAt,
    likelyHotspots: recentHotspots,
    hourlyTrend,
    score: {
      evaluations: rollingErrors.length,
      meanAbsoluteErrorMinutes: mae,
      rootMeanSquaredErrorMinutes: rmse,
      medianAbsoluteErrorMinutes: median,
      within2MinutesRate: within2,
      within5MinutesRate: within5,
      latestErrorMinutes:
        rollingErrors.length > 0 ? Number(rollingErrors[rollingErrors.length - 1].toFixed(2)) : null
    },
    confidence
  };
}

function renderAll({ fitToData = false } = {}) {
  renderKpis();
  renderAlerts();
  renderHotspots();
  renderPrediction();
  renderIntel();
  renderToasts();
  renderMap({ fitToData });
}

function setDrawerMode(mode) {
  state.activeDrawer = mode;
  const isOpen = Boolean(mode);
  document.body.classList.toggle('drawer-open', isOpen);
  document.body.classList.toggle('drawer-insights-open', mode === 'insights');
  document.body.classList.toggle('drawer-news-open', mode === 'news');
  insightsDrawerEl.setAttribute('aria-hidden', mode === 'insights' ? 'false' : 'true');
  newsDrawerEl.setAttribute('aria-hidden', mode === 'news' ? 'false' : 'true');
  drawerBackdropEl.setAttribute('aria-hidden', isOpen ? 'false' : 'true');

  if (mode === 'news') {
    state.newsUnread = false;
    renderNewsBadge();
  }
}

function renderNewsBadge() {
  document.body.classList.toggle('news-unread', state.newsUnread);
}

function renderKpis() {
  const total = state.stats?.totalAlerts ?? state.alerts.length;
  const latest = state.stats?.latest?.published_at || state.alerts[0]?.publishedAt;
  const hotspot = state.stats?.hotspots?.[0];
  const within5 = state.predictions?.score?.within5MinutesRate;

  const cards = [
    {
      label: '⚡ Alerts (24h)',
      value: String(total)
    },
    {
      label: '🕒 Latest',
      value: latest ? formatAgo(latest) : 'No data'
    },
    {
      label: '📍 Hotspot',
      value: hotspot ? `${hotspot.name} (${hotspot.count})` : 'n/a'
    },
    {
      label: '🎯 Hit <=5m',
      value: within5 !== null && within5 !== undefined ? `${within5}%` : 'n/a'
    }
  ];

  kpisEl.innerHTML = cards
    .map(
      (card) => `
      <article class="kpi">
        <div class="label">${escapeHtml(card.label)}</div>
        <div class="value">${escapeHtml(card.value)}</div>
      </article>
    `
    )
    .join('');
}

function renderAlerts() {
  listEl.innerHTML = state.alerts.slice(0, RECENT_ALERTS_VISIBLE).map(renderAlertItem).join('');
}

function renderAlertItem(alert) {
  const places = (alert.places || []).map((p) => p.name).filter(Boolean);
  const published = formatTime(alert.publishedAt);

  return `
    <li class="alert-item interactive" data-alert-id="${escapeAttr(alert.id)}" role="button" tabindex="0">
      <div class="meta">
        <span class="type">${escapeHtml(alert.type || 'Alert')}</span>
        <span>${escapeHtml(published)}</span>
      </div>
      <div class="places">${escapeHtml(places.slice(0, 4).join(' • ') || 'Location unavailable')}</div>
      <div class="meta">
        <a href="${escapeAttr(alert.sourceUrl)}" target="_blank" rel="noreferrer">Source</a>
        <span>${escapeHtml(formatAgo(alert.publishedAt))}</span>
      </div>
    </li>
  `;
}

function focusAlertFromList(alertId) {
  const alert = state.alerts.find((entry) => String(entry.id) === String(alertId));
  if (!alert) return;
  zoomToAlert(alert);
  pulseLivePlaces(alert);
}

function renderHotspots() {
  const hotspots = state.stats?.hotspots || [];
  hotspotEl.innerHTML = hotspots.length
    ? hotspots
        .slice(0, 5)
        .map(
          (item, index) =>
            `<li><span>#${index + 1} ${escapeHtml(item.name)}</span><strong>${escapeHtml(
              String(item.count)
            )}</strong></li>`
        )
        .join('')
    : '<li><span>No hotspot data yet</span><strong>--</strong></li>';
}

function renderPrediction() {
  const p = state.predictions || {};
  const score = p.score || {};

  predictionEl.innerHTML = `
    <div class="pred-line"><span class="pred-label">Next likely</span><span class="pred-value">${
      p.nextLikelyAlertAt ? escapeHtml(formatTime(p.nextLikelyAlertAt)) : 'n/a'
    }</span></div>
    <div class="pred-line"><span class="pred-label">Confidence</span><span class="pred-value">${escapeHtml(
      p.confidence || 'low'
    )}</span></div>
    <div class="pred-line"><span class="pred-label">MAE</span><span class="pred-value">${
      score.meanAbsoluteErrorMinutes ?? 'n/a'
    } min</span></div>
    <div class="pred-line"><span class="pred-label">RMSE</span><span class="pred-value">${
      score.rootMeanSquaredErrorMinutes ?? 'n/a'
    } min</span></div>
    <div class="pred-line"><span class="pred-label">Hit <=2m</span><span class="pred-value">${
      score.within2MinutesRate ?? 'n/a'
    }%</span></div>
    <div class="pred-line"><span class="pred-label">Hit <=5m</span><span class="pred-value">${
      score.within5MinutesRate ?? 'n/a'
    }%</span></div>
  `;
}

function emptyIntel() {
  return {
    updatedAt: null,
    status: 'warming',
    errors: [],
    news: [],
    images: [],
    webcams: [],
    dashboards: []
  };
}

function renderIntel() {
  const intel = state.intel || emptyIntel();
  renderNews(intel.news || []);
  renderImages(intel.images || []);
}

function renderNews(newsItems) {
  if (!newsItems.length) {
    newsFeedEl.innerHTML = '<li class="empty-feed">News feed warming up…</li>';
    renderNewsBadge();
    return;
  }

  const head = newsItems[0];
  const nextHeadKey = head?.id || head?.url || null;
  if (state.latestNewsKey && nextHeadKey && nextHeadKey !== state.latestNewsKey) {
    if (state.activeDrawer !== 'news') {
      state.newsUnread = true;
    }
  }
  if (nextHeadKey) {
    state.latestNewsKey = nextHeadKey;
    persistRuntime();
  }
  renderNewsBadge();

  newsFeedEl.innerHTML = newsItems
    .slice(0, 12)
    .map((item) => {
      const source = item.sourceName || 'Source';
      const published = item.publishedAt ? formatAgo(item.publishedAt) : 'new';
      return `
        <li class="news-item">
          <a href="${escapeAttr(item.url)}" target="_blank" rel="noreferrer">${escapeHtml(item.title)}</a>
          <div class="news-meta">
            <span>${escapeHtml(source)}</span>
            <span>${escapeHtml(published)}</span>
          </div>
        </li>
      `;
    })
    .join('');
}

function renderImages(images) {
  if (!images.length) {
    imageFeedEl.innerHTML = '<div class="empty-feed">Visual feed warming…</div>';
    intelRibbonEl.innerHTML = '';
    return;
  }

  imageFeedEl.innerHTML = images
    .slice(0, 12)
    .map((item) => {
      const label = [item.domain, item.sourceCountry].filter(Boolean).join(' • ');
      const imageUrl = String(item.image || '').trim();
      const hasImage = /^https?:\/\//i.test(imageUrl);
      return `
        <a class="image-card${hasImage ? '' : ' image-broken'}" href="${escapeAttr(item.url)}" target="_blank" rel="noreferrer">
          ${
            hasImage
              ? `<img data-intel-image="1" src="${escapeAttr(imageUrl)}" alt="${escapeAttr(
                  item.title
                )}" loading="lazy" referrerpolicy="no-referrer" />`
              : ''
          }
          <div class="image-overlay">
            <strong>${escapeHtml(item.title)}</strong>
            <span>${escapeHtml(label || 'visual')}</span>
          </div>
        </a>
      `;
    })
    .join('');

  bindIntelImageFallbacks();
}

function bindIntelImageFallbacks() {
  const imgs = imageFeedEl.querySelectorAll('img[data-intel-image]');
  for (const img of imgs) {
    if (img.dataset.bound === '1') continue;
    img.dataset.bound = '1';
    img.addEventListener(
      'error',
      () => {
        img.classList.add('is-hidden');
        img.closest('.image-card')?.classList.add('image-broken');
      },
      { once: true }
    );
  }
}

function renderMap({ fitToData = false } = {}) {
  if (!mapReady) return;
  const now = Date.now();
  const cutoff = now - HISTORY_WINDOW_HOURS * 60 * 60 * 1000;
  const features = [];
  const points = [];

  for (const alert of state.alerts) {
    const ts = Date.parse(alert.publishedAt);
    if (!Number.isFinite(ts) || ts < cutoff) continue;

    const ageRatio = Math.min(
      1,
      Math.max(0.05, 1 - (now - ts) / (HISTORY_WINDOW_HOURS * 60 * 60 * 1000))
    );
    for (const place of alert.places || []) {
      if (!isFiniteLatLon(place.lat, place.lon)) continue;
      const point = [place.lon, place.lat];
      points.push(point);
      features.push({
        type: 'Feature',
        geometry: {
          type: 'Point',
          coordinates: point
        },
        properties: {
          type: alert.type || 'Alert',
          place: place.name || 'Unknown',
          published: alert.publishedAt || '',
          ageRatio
        }
      });
    }
  }

  setSourceData(HISTORY_SOURCE_ID, features);

  if (fitToData && points.length > 0) {
    const bounds = boundsFromPoints(points);
    hasInitialMapFit = true;
    const camera = map.cameraForBounds(bounds, {
      padding: { top: 30, right: 30, bottom: 30, left: 30 },
      maxZoom: 8,
      pitch: 0,
      bearing: 0
    });
    if (camera) {
      map.easeTo({
        ...camera,
        duration: 900
      });
    }
  } else if (fitToData && !hasInitialMapFit) {
    map.easeTo({
      center: MAP_DEFAULT_CENTER,
      zoom: MAP_DEFAULT_ZOOM,
      pitch: 0,
      bearing: 0,
      duration: 600
    });
  }
}

function pulseLivePlaces(alert) {
  const expiresAt = Date.now() + 32_000;
  for (const place of alert.places || []) {
    if (!isFiniteLatLon(place.lat, place.lon)) continue;
    livePulseFeatures.push({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [place.lon, place.lat]
      },
      properties: {
        type: alert.type || 'Alert',
        place: place.name || 'Unknown',
        published: alert.publishedAt || '',
        expiresAt
      }
    });
  }
  pruneLivePulseFeatures();
}

function pruneLivePulseFeatures() {
  if (!mapReady) return;
  const now = Date.now();
  livePulseFeatures = livePulseFeatures.filter((feature) => Number(feature.properties?.expiresAt || 0) > now);
  setSourceData(LIVE_SOURCE_ID, livePulseFeatures);
}

function zoomToAlert(alert) {
  if (!mapReady) return;
  const coords = (alert.places || [])
    .filter((place) => isFiniteLatLon(place.lat, place.lon))
    .map((place) => [place.lon, place.lat]);

  if (coords.length === 0) return;

  setMapVisualMode('detail', { force: true });
  map.once('moveend', () => {
    setMapVisualMode('detail', { force: true });
  });

  if (coords.length === 1) {
    const targetZoom = Math.min(16.9, Math.max(map.getZoom() + 1.8, 13.6));
    map.easeTo({
      center: coords[0],
      zoom: targetZoom,
      pitch: ALERT_FOCUS_PITCH,
      bearing: ALERT_FOCUS_BEARING,
      duration: 1450,
      easing: easeOutExpo
    });
    return;
  }

  const bounds = boundsFromPoints(coords);
  const camera = map.cameraForBounds(bounds, {
    padding: { top: 90, right: 90, bottom: 90, left: 90 },
    maxZoom: 14.5
  });
  if (!camera) return;

  const averageCenter = [
    coords.reduce((sum, point) => sum + point[0], 0) / coords.length,
    coords.reduce((sum, point) => sum + point[1], 0) / coords.length
  ];
  const targetCenter = camera.center || averageCenter;
  const targetZoom = Math.min(16.2, Math.max(12.4, Number(camera.zoom || 0)));

  map.easeTo({
    center: targetCenter,
    zoom: targetZoom,
    pitch: ALERT_FOCUS_PITCH - 6,
    bearing: ALERT_FOCUS_BEARING,
    duration: 1500,
    easing: easeOutExpo
  });
}

function fitMapToHistory({ animate = true } = {}) {
  if (!mapReady) return;
  const points = [];
  for (const alert of state.alerts) {
    for (const place of alert.places || []) {
      if (isFiniteLatLon(place.lat, place.lon)) {
        points.push([place.lon, place.lat]);
      }
    }
  }

  if (points.length === 0) {
    setMapVisualMode('dark');
    map.easeTo({
      center: MAP_DEFAULT_CENTER,
      zoom: MAP_DEFAULT_ZOOM,
      pitch: 0,
      bearing: 0,
      duration: animate ? 900 : 0
    });
    return;
  }

  const bounds = boundsFromPoints(points);
  const camera = map.cameraForBounds(bounds, {
    padding: { top: 30, right: 30, bottom: 30, left: 30 },
    maxZoom: 8
  });
  if (!camera) return;

  setMapVisualMode('dark');
  map.easeTo({
    ...camera,
    pitch: 0,
    bearing: 0,
    duration: animate ? 900 : 0
  });
}

function setMapVisualMode(mode, { force = false } = {}) {
  if (!mapReady) return;
  if (!force && mode === mapVisualMode) return;

  if (mode === 'detail') {
    map.setLayoutProperty(DARK_BASE_LAYER_ID, 'visibility', 'none');
    map.setLayoutProperty(DARK_LABEL_LAYER_ID, 'visibility', 'none');
    map.setLayoutProperty(DETAIL_BASE_LAYER_ID, 'visibility', 'visible');
    map.setLayoutProperty(DETAIL_LABEL_LAYER_ID, 'visibility', 'visible');
    map.setTerrain({ source: TERRAIN_SOURCE_ID, exaggeration: 1.35 });
    map.getContainer().classList.add('street-3d');
  } else {
    map.setLayoutProperty(DETAIL_BASE_LAYER_ID, 'visibility', 'none');
    map.setLayoutProperty(DETAIL_LABEL_LAYER_ID, 'visibility', 'none');
    map.setLayoutProperty(DARK_BASE_LAYER_ID, 'visibility', 'visible');
    map.setLayoutProperty(DARK_LABEL_LAYER_ID, 'visibility', 'visible');
    map.setTerrain(null);
    map.getContainer().classList.remove('street-3d');
  }

  mapVisualMode = mode;
}

function boundsFromPoints(points) {
  const [first, ...rest] = points;
  const bounds = new maplibregl.LngLatBounds(first, first);
  for (const point of rest) bounds.extend(point);
  return bounds;
}

function easeOutExpo(value) {
  return value === 1 ? 1 : 1 - 2 ** (-10 * value);
}

function showAlertToast(alert) {
  const places = (alert.places || []).map((place) => place.name).filter(Boolean);
  const toast = {
    id: `${alert.id}-${Date.now()}`,
    alertId: alert.id,
    type: alert.type || 'Alert',
    publishedAt: alert.publishedAt,
    sourceUrl: alert.sourceUrl,
    places: places.slice(0, 6),
    expiresAt: Date.now() + TOAST_TTL_MS
  };

  state.toasts = [toast, ...state.toasts.filter((item) => item.alertId !== alert.id)].slice(
    0,
    MAX_TOASTS
  );
  renderToasts();
}

function pruneToasts() {
  const now = Date.now();
  const next = state.toasts.filter((toast) => Number(toast.expiresAt || 0) > now);
  if (next.length !== state.toasts.length) {
    state.toasts = next;
    renderToasts();
  }
}

function renderToasts() {
  toastStackEl.innerHTML = state.toasts
    .map((toast) => {
      const levelClass = toast.type.toLowerCase().includes('uav') ? 'uav' : 'rocket';
      return `
        <article class="live-toast ${levelClass}">
          <header>
            <strong>${escapeHtml(toast.type)}</strong>
            <button data-toast-close="${escapeAttr(toast.id)}" type="button" aria-label="Dismiss alert">✕</button>
          </header>
          <p>${escapeHtml(toast.places.join(' • ') || 'Location unavailable')}</p>
          <footer>
            <span>${escapeHtml(formatTime(toast.publishedAt))}</span>
            <a href="${escapeAttr(toast.sourceUrl)}" target="_blank" rel="noreferrer">Source</a>
          </footer>
        </article>
      `;
    })
    .join('');
}

function playTone() {
  if (!state.audioEnabled || !state.audioContext) return;

  const ctx = state.audioContext;
  const now = ctx.currentTime;
  beep(ctx, now, 780, 0.12, 0.07, 'triangle');
  beep(ctx, now + 0.14, 980, 0.1, 0.08, 'sine');
  beep(ctx, now + 0.29, 1160, 0.08, 0.06, 'sine');
}

function beep(ctx, startAt, freq, duration, volume, wave) {
  const oscillator = ctx.createOscillator();
  const gain = ctx.createGain();

  oscillator.type = wave;
  oscillator.frequency.value = freq;
  gain.gain.setValueAtTime(0.0001, startAt);
  gain.gain.exponentialRampToValueAtTime(volume, startAt + 0.015);
  gain.gain.exponentialRampToValueAtTime(0.0001, startAt + duration);

  oscillator.connect(gain);
  gain.connect(ctx.destination);

  oscillator.start(startAt);
  oscillator.stop(startAt + duration + 0.03);
}

function setLive(isLive) {
  state.connected = isLive;
  livePillEl.textContent = isLive ? 'Live stream active' : 'Disconnected';
  livePillEl.classList.toggle('live', isLive);
}

function getMaxAlertId(alerts) {
  let max = null;
  for (const alert of alerts) {
    if (!alert?.id) continue;
    if (!max || compareIds(alert.id, max) > 0) {
      max = String(alert.id);
    }
  }
  return max;
}

function compareIds(a, b) {
  const left = BigInt(String(a));
  const right = BigInt(String(b));
  if (left === right) return 0;
  return left > right ? 1 : -1;
}

function formatTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'n/a';
  return date.toLocaleString();
}

function formatAgo(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'n/a';

  const diffMs = Date.now() - date.getTime();
  const abs = Math.abs(diffMs);

  if (abs < 60_000) return 'just now';
  if (abs < 3_600_000) {
    const minutes = Math.round(abs / 60_000);
    return diffMs >= 0 ? `${minutes}m ago` : `in ${minutes}m`;
  }

  const hours = Math.round(abs / 3_600_000);
  return diffMs >= 0 ? `${hours}h ago` : `in ${hours}h`;
}

function isFiniteLatLon(lat, lon) {
  return Number.isFinite(lat) && Number.isFinite(lon);
}

function escapeHtml(input) {
  return String(input)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function escapeAttr(input) {
  return escapeHtml(input).replaceAll('`', '');
}
