# RocketAlert Earth Monitor (Static)

This version runs fully in the browser so it can be hosted on GitHub Pages.

## What changed

- No SQLite, no Express server, no SSE endpoint.
- 24-hour alert history is stored in browser `localStorage`.
- Live updates come from direct browser polling of the RocketAlert Mastodon account.
- Prediction scoring (MAE/RMSE/hit rates) is computed client-side from local 24h history.
- Intel feeds are fetched client-side (with proxy fallback when feeds block CORS).

## Features

- Real-time RocketAlert ingestion from Mastodon.
- 24-hour map history + live pulse markers.
- 3D-perspective zoom focus on new alerts.
- Audio notifications (after user enables audio).
- Stacked live alert toasts (max 10, auto-expire).
- Intel links/news visual feed.
- Insights drawer with prediction and hotspot metrics.

## Run locally

```bash
npm run dev
```

Then open `http://localhost:3000`.

## GitHub Pages hosting

1. Push this repo to GitHub.
2. In GitHub Pages settings, publish a static site from this repository.
3. Serve the `public/` files (for example via Pages workflow artifact or by moving/copying `public/*` into your Pages publish folder).

## Data sources

- Alerts: Mastodon API (`@rocketalert` account statuses)
- Location enrichment: RocketAlert lookup API + Nominatim fallback
- News: RSS sources (Google News, BBC Middle East, Times of Israel, RocketAlert RSS)
- Visual intel: GDELT ArtList

## Notes

- `localStorage` is per-browser/per-device.
- Prediction output is heuristic and not safety-grade forecasting.
- If a source blocks direct CORS, the app falls back to a public proxy endpoint.
