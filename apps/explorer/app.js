// Sentinel-2 explorer. Everything below talks to static files:
//   stats/mgrs-monthly.parquet  – choropleth + timeline
//   stats/mgrs.pmtiles          – MGRS tile footprints
//   sentinel-2-l2a/year=*/…     – item queries (Task 12; one part per year
//                                 by the tile's UTM zone from 2019, Task 18;
//                                 eight parts from 2021, Task 19)
//   …/preview.jpg (thumbnail_url) – a scene's thumbnail: the card image, and
//                                 the instant preview under "Show on map"
//   …/TCI.tif (next to it)      – a scene's visual COG, drawn on the map
//                                 straight from its overviews (Task 20),
//                                 replacing the preview as its tiles load
//                                 (Task 27)
//   …/B01.tif … B12.tif, B8A, SCL, AOT, WVP (same directory) – any band or
//                                 composite, NDVI/NDWI or the SCL classes,
//                                 read band by band on demand and stretched
//                                 in the browser (Task 28, bands.js)
// There is no API, no server and no database behind this page: DuckDB-WASM
// issues HTTP range reads straight at the object store, and so do the COG
// reads. The map is MapLibre for the camera; the tiles are drawn by deck.gl
// interleaved into the same canvas (Task 20), because MapLibre's per-feature
// state and filter changes re-parse the 33k-polygon tile on every update.
import maplibregl from "https://esm.sh/maplibre-gl@4.7.1";
import { PMTiles, Protocol } from "https://esm.sh/pmtiles@3.2.0";
// 1.32.0 (DuckDB v1.4.3) is a floor, not a preference: the item parts are
// GeoParquet 2.0.0, and 1.29.0 (DuckDB v1.1.1) refuses them outright with
// "Geoparquet version 2.0.0 is not supported".
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm";
import { parse } from "https://esm.sh/@loaders.gl/core@4.5.1";
import { MVTLoader } from "https://esm.sh/@loaders.gl/mvt@4.5.1";
import { cogTileLayer, previewImage, previewLayer, openScene, sceneCog, loadOverviews,
  sceneIndexStats, bandPreviewImage, bandTileLayer, bandHref } from "./cog.js";
import { BANDS, bandTitle, INDICES, SCL_CLASSES, PRESETS, bandsOf, HIST_BINS } from "./bands.js";
import { dayRange, valueRange } from "./rangeslider.js";
// deck.gl comes from its pinned dist bundle (index.html), not an ESM CDN
// transpile: the esm.sh build draws but cannot pick. One bundle, one luma.gl.
// A classic script that failed to load is a missing global, not an import
// error, so it is checked here and said out loud rather than thrown.
if (!window.deck?.MapboxOverlay) {
  const el = document.getElementById("status");
  el.textContent = "deck.gl did not load (cdn.jsdelivr.net/npm/deck.gl@9.4.0/dist.min.js is "
    + "blocked or unreachable) — the map cannot be drawn. Reload once the CDN is reachable.";
  el.classList.add("error");
  throw new Error("deck.gl bundle missing");
}
const { MapboxOverlay, GeoJsonLayer, MVTLayer } = window.deck;

// ?base=http://localhost:8081 points the whole app at a local publish tree,
// which is how it is developed before the bucket is populated.
export const BASE = new URLSearchParams(location.search).get("base")
  ?? "https://data.source.coop/portolan-mirrors/sentinel-2-catalog";

// The stats collection is three parquet products cut from one table
// (tools/s2_stats.py): the app never reads the full table whole.
//  - timeline.parquet: one row per month over all tiles, a few KB. Fetched
//    whole on load; it gives the month span and the global timeline.
//  - months/YYYY-MM.parquet: one month's rows with the paint columns, sorted
//    by tile, ~100-150 KB. Fetched whole when that month is shown, then
//    kept registered so a revisit is free.
//  - mgrs-monthly.parquet: the full table, ~21 MB, sorted by tile in 50k-row
//    groups. Only ever range-read over httpfs with WHERE mgrs_tile = ..., the
//    way the scene search reads the year parts: one tile's history is a
//    ~55 KB footer plus one row group's column chunks, not the file.
const STATS = `${BASE}/stats/mgrs-monthly.parquet`;
const TIMELINE = `${BASE}/stats/timeline.parquet`;
const TIMELINE_FILE = "timeline.parquet";
const monthUrl = (ym) => `${BASE}/stats/months/${ym}.parquet`;
// Only these may reach the SQL string; the <select> is not trusted input.
const METRICS = new Set(["min_cloud_cover", "scene_count", "median_cloud_cover", "max_cover"]);

const $ = (id) => document.getElementById(id);
// ?debug logs the COG timeline (preview shown, viewport loaded) to the console.
const DEBUG = new URLSearchParams(location.search).has("debug");
const debug = (...args) => { if (DEBUG) console.info(...args); };
const say = (msg, isError = false) => {
  const el = $("status");
  el.textContent = msg;
  el.classList.toggle("error", isError);
};

async function initDb() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker = new Worker(URL.createObjectURL(new Blob(
    [`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })));
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  // Measured, not assumed: duckdb-wasm 1.32.0 defaults forceFullHTTPReads to
  // true, so a query against a 573 MB year part downloaded the whole file
  // (162 s) instead of range-reading the ~5 MB it needed. With the flag off
  // the same query is ~30 range GETs. Nothing else about the FS is changed.
  await db.open({ filesystem: { forceFullHTTPReads: false } });
  return db;
}

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
const map = new maplibregl.Map({
  container: "map",
  style: { version: 8, sources: {}, layers: [
    { id: "bg", type: "background", paint: { "background-color": "#0b1020" } }] },
  center: [10, 30], zoom: 2,
  attributionControl: { compact: true },
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
// Registered before any await, so a fast style load cannot be missed.
const mapReady = new Promise((resolve) => map.on("load", resolve));

const db = await initDb();
const conn = await db.connect();
// A console handle saves reaching into the module; the deck.gl overlay and
// the per-month lookup are added below once they exist.
window.S2 = { BASE, db, conn, map };

// The choropleth ramp, shared by the map fill, the legend and the timeline
// bars so one colour always means one thing.
const RAMP = [[0, "#1a9850"], [25, "#fee08b"], [60, "#d73027"], [100, "#4d0013"]];
const rampRGB = (v) => {
  const x = Math.min(100, Math.max(0, Number(v)));
  for (let i = 1; i < RAMP.length; i++) {
    const [a, ca] = RAMP[i - 1], [b, cb] = RAMP[i];
    if (x > b) continue;
    const t = (x - a) / (b - a);
    const mix = (j) => Math.round(parseInt(ca.slice(1 + j, 3 + j), 16) * (1 - t)
      + parseInt(cb.slice(1 + j, 3 + j), 16) * t);
    return [mix(0), mix(2), mix(4)];
  }
  const c = RAMP[RAMP.length - 1][1];
  return [parseInt(c.slice(1, 3), 16), parseInt(c.slice(3, 5), 16), parseInt(c.slice(5, 7), 16)];
};
const rampColor = (v) => `rgb(${rampRGB(v).join(",")})`;
// 33k fills per repaint: a 101-entry lookup instead of 33k interpolations.
const FILL_ALPHA = 140;                          // fill-opacity 0.55, as before
const RAMP_LUT = Array.from({ length: 101 }, (_, i) => [...rampRGB(i), FILL_ALPHA]);
const UNPAINTED = [34, 34, 51, FILL_ALPHA];       // #223: no stats for the month
const DIMMED = [70, 78, 96, 70];                  // clearest scene over the slider
const HOVER_LINE = [232, 240, 255, 255];          // #e8f0ff

await mapReady;

// ---------------------------------------------------------------------------
// The MGRS choropleth. The fills are drawn by deck.gl: the footprints come
// out of the same PMTiles archive as before, and the colour of each tile is
// looked up in a Map rebuilt per month from the stats query. A month change,
// a metric change or a slider drag bumps `paintKey`, and deck.gl recomputes
// one colour attribute for the 33k polygons and uploads it — no per-feature
// state, no filter change, no tile re-parse. The outlines stay a MapLibre
// line layer: nothing ever changes on it, so its tile is parsed once, and a
// deck.gl PathLayer of the same 33k outlines was measured at 0.5-0.9 s a
// frame under software GL where MapLibre's lines take a few ms. The fills
// are slotted beneath it with beforeId.
// ---------------------------------------------------------------------------
map.addSource("mgrs", { type: "vector", url: `pmtiles://${BASE}/stats/mgrs.pmtiles` });
map.addLayer({ id: "mgrs-line", type: "line", source: "mgrs",
  "source-layer": "mgrs",
  paint: { "line-color": "#8899bb", "line-width": 0.4 } });
const archive = new PMTiles(`${BASE}/stats/mgrs.pmtiles`);
let tileZoom = { minZoom: 0, maxZoom: 0 };
try {
  const h = await archive.getHeader();
  tileZoom = { minZoom: h.minZoom, maxZoom: h.maxZoom };
} catch (err) {
  say(`Could not open ${BASE}/stats/mgrs.pmtiles — ${err.message}`, true);
}
// MVTLayer asks for "{z}/{x}/{y}" of its data template; the bytes come from
// the PMTiles archive (one range read per tile, cached by the library) and
// are parsed on this thread with loaders.gl's MVTLoader, using the options
// the layer hands over (binary output, local tile coordinates). The layer's
// own default loader is worker-only, which is why it is not used here. The
// parsed polygons also feed the hit-test index below.
async function fetchMvt(url, { loadOptions, signal }) {
  const [z, x, y] = url.split("/").slice(-3).map(Number);
  if (![z, x, y].every(Number.isInteger)) throw new Error(`bad tile key ${url}`);
  const t = await archive.getZxy(z, x, y, signal);
  if (!t?.data) return null;
  const data = await parse(t.data, MVTLoader, { ...loadOptions, worker: false });
  if (data?.polygons) hitIndex.add(data.polygons, z, x, y);
  return data;
}

// Which tile is under the pointer, answered on the CPU. deck.gl's own picking
// re-renders every pickable polygon into a picking buffer and reads it back
// each frame the pointer moves; on a real GPU that is a few ms, under a
// software GL it was measured at ~0.8 s per frame. A point-in-polygon test
// against the same decoded geometry, through a 2-degree grid of polygon
// bounding boxes, is microseconds anywhere — so the choropleth is not
// pickable at all, and MapLibre's mousemove/click events drive hover and
// selection.
// The archive is one z0 tile today, so the index is built exactly once; a
// deeper archive would hand every zoom's copy of the same polygons through
// fetchMvt, so tiles are indexed by (z, x, y) once and the index keeps only
// the first zoom it saw — the coarsest, which is enough for a hit test.
const hitIndex = {
  cell: 2,                       // degrees
  grid: new Map(),               // "cx,cy" -> [polygon ids]
  polys: [],                     // {tile, rings: [[lon, lat, ...]], bbox}
  seen: new Set(),               // "z/x/y" already indexed
  zoom: null,                    // the one zoom level indexed
  add(polygons, z, tx, ty) {
    const key = `${z}/${tx}/${ty}`;
    if (this.seen.has(key) || (this.zoom !== null && z !== this.zoom)) return;
    this.seen.add(key);
    this.zoom = z;
    const { positions, polygonIndices, primitivePolygonIndices, featureIds, properties } = polygons;
    const P = positions.value, size = positions.size;
    const n = 2 ** z;
    // Local tile coordinates (0..1, y down) to lon/lat.
    const lon = (u) => ((tx + u) / n) * 360 - 180;
    const lat = (v) => (Math.atan(Math.sinh(Math.PI * (1 - (2 * (ty + v)) / n))) * 180) / Math.PI;
    const ringStarts = primitivePolygonIndices.value;
    let r = 0;
    for (let p = 0; p < polygonIndices.value.length - 1; p++) {
      const start = polygonIndices.value[p], end = polygonIndices.value[p + 1];
      const tile = properties[featureIds.value[start]]?.mgrs_tile;
      const rings = [];
      let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
      while (r < ringStarts.length - 1 && ringStarts[r] < end) {
        const a = ringStarts[r], b = Math.min(ringStarts[r + 1], end);
        const ring = new Float64Array((b - a) * 2);
        for (let i = a, k = 0; i < b; i++, k += 2) {
          const X = lon(P[i * size]), Y = lat(P[i * size + 1]);
          ring[k] = X; ring[k + 1] = Y;
          if (X < x0) x0 = X; if (X > x1) x1 = X; if (Y < y0) y0 = Y; if (Y > y1) y1 = Y;
        }
        rings.push(ring);
        r++;
      }
      if (!tile || !rings.length) continue;
      const id = this.polys.push({ tile, rings, bbox: [x0, y0, x1, y1] }) - 1;
      for (let cx = Math.floor(x0 / this.cell); cx <= Math.floor(x1 / this.cell); cx++) {
        for (let cy = Math.floor(y0 / this.cell); cy <= Math.floor(y1 / this.cell); cy++) {
          const key = `${cx},${cy}`;
          const list = this.grid.get(key);
          if (list) list.push(id); else this.grid.set(key, [id]);
        }
      }
    }
  },
  // The topmost (last drawn) polygon containing the point, or null.
  at(lng, lat) {
    const list = this.grid.get(`${Math.floor(lng / this.cell)},${Math.floor(lat / this.cell)}`);
    if (!list) return null;
    for (let i = list.length - 1; i >= 0; i--) {
      const poly = this.polys[list[i]];
      const [x0, y0, x1, y1] = poly.bbox;
      if (lng < x0 || lng > x1 || lat < y0 || lat > y1) continue;
      // Even-odd over every ring: holes cancel, multipolygon parts add.
      let inside = false;
      for (const ring of poly.rings) {
        for (let a = 0, b = ring.length - 2; a < ring.length; b = a, a += 2) {
          const ax = ring[a], ay = ring[a + 1], bx = ring[b], by = ring[b + 1];
          if ((ay > lat) !== (by > lat) && lng < ((bx - ax) * (lat - ay)) / (by - ay) + ax) inside = !inside;
        }
      }
      if (inside) return poly;
    }
    return null;
  },
  // The polygon as a GeoJSON feature, for the hover outline.
  feature(poly) {
    return { type: "Feature", properties: { mgrs_tile: poly.tile },
      geometry: { type: "Polygon", coordinates: poly.rings.map((ring) => {
        const out = [];
        for (let i = 0; i < ring.length; i += 2) out.push([ring[i], ring[i + 1]]);
        return out;
      }) } };
  },
};

// Per-tile stats for the shown month: mgrs_tile -> {v: 0..100 on the ramp,
// cc: clearest scene's cloud %}. `v` is already rescaled per metric.
let lookup = new Map();
let paintKey = 0;
let maxCloud = Number($("maxcloud").value);
let minCoverage = Number($("mincoverage").value);
let minScenes = Number($("minscenes").value);
let hovered = null;          // the hovered feature (GeoJSON, WGS84) or null
let cogLayer = null;         // the shown scene's TileLayer, or null
let cogPreview = null;       // its thumbnail warp, beneath the tiles until they load
let selectedTile = null;

// All three sliders AND together in one accessor; a NULL metric is "unknown"
// rather than "over/under the slider" and is never dimmed for that reason
// (the Task 20 rule, extended to coverage and scene count).
function fillColor(f) {
  const s = lookup.get(f.properties.mgrs_tile);
  if (!s) return UNPAINTED;
  if (s.cc !== null && s.cc > maxCloud) return DIMMED;
  if (s.cover !== null && s.cover < minCoverage) return DIMMED;
  if (s.sc !== null && s.sc < minScenes) return DIMMED;
  return RAMP_LUT[Math.round(s.v)];
}

const overlay = new MapboxOverlay({
  interleaved: true,
  layers: [],
  // deck.gl resets the canvas cursor after every pointer frame; the hover
  // state below is the one source of truth for it.
  getCursor: () => (hovered ? "pointer" : ""),
});
map.addControl(overlay);
Object.defineProperties(window.S2, {
  overlay: { value: overlay },
  hitIndex: { value: hitIndex },
  lookup: { get: () => lookup },
  selectedTile: { get: () => selectedTile },
});

function render() {
  overlay.setProps({ layers: [
    new MVTLayer({
      id: "mgrs",
      data: "mgrs/{z}/{x}/{y}",     // a key for fetchMvt, never fetched as a URL
      fetch: fetchMvt,
      minZoom: tileZoom.minZoom,
      maxZoom: tileZoom.maxZoom,
      binary: true,
      pickable: false,           // hit-tested on the CPU, see hitIndex
      filled: true,
      stroked: false,            // the outline is MapLibre's mgrs-line
      getFillColor: fillColor,
      updateTriggers: { getFillColor: paintKey },
      beforeId: "mgrs-line",
    }),
    cogPreview,
    cogLayer,
    new GeoJsonLayer({
      id: "mgrs-hover",
      data: hovered ? [hovered] : [],
      stroked: true,
      filled: false,
      getLineColor: HOVER_LINE,
      lineWidthUnits: "pixels",
      getLineWidth: 1.6,
      lineWidthMinPixels: 1.6,
    }),
  ] });
}

function setHovered(poly) {
  const tile = poly?.tile ?? null;
  if (tile === (hovered?.properties?.mgrs_tile ?? null)) return;
  hovered = poly ? hitIndex.feature(poly) : null;
  map.getCanvas().style.cursor = hovered ? "pointer" : "";
  render();
}
// One hit test per animation frame at most, however fast the pointer moves.
let hoverFrame = 0, hoverAt = null;
map.on("mousemove", (e) => {
  hoverAt = e.lngLat;
  if (hoverFrame) return;
  hoverFrame = requestAnimationFrame(() => {
    hoverFrame = 0;
    // .wrap(): on a world copy past ±180 the index is still in -180..180.
    setHovered(hoverAt ? hitIndex.at(hoverAt.wrap().lng, hoverAt.lat) : null);
  });
});
map.on("mouseout", () => { hoverAt = null; setHovered(null); });

function repaint() { paintKey++; render(); }
render();

// ---------------------------------------------------------------------------
// Stats: the choropleth and the timeline.
// ---------------------------------------------------------------------------

// A small remote parquet fetched whole and handed to DuckDB as an in-memory
// file under `name`. Resolves false on a 404, which for a month slice means
// "no tile-months for that month" (the builder writes a slice only for
// months the table has), not a broken bucket.
async function registerRemote(url, name) {
  const res = await fetch(url);
  if (res.status === 404) return false;
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  await db.registerFileBuffer(name, new Uint8Array(await res.arrayBuffer()));
  return true;
}

async function loadTimeline() {
  if (!(await registerRemote(TIMELINE, TIMELINE_FILE))) throw new Error("HTTP 404");
}

// Month slices already registered with DuckDB: ym -> the registered file
// name, or null for a month the bucket has no slice for. The value is the
// in-flight promise, so two callers for the same month share one fetch and
// a revisit costs nothing. A failed fetch is forgotten so the next attempt
// retries rather than replaying the error.
const monthFiles = new Map();
function monthFile(ym) {
  if (!monthFiles.has(ym)) {
    const name = `month-${ym}.parquet`;
    const p = registerRemote(monthUrl(ym), name)
      .then((ok) => (ok ? name : null))
      .catch((err) => { monthFiles.delete(ym); throw err; });
    monthFiles.set(ym, p);
  }
  return monthFiles.get(ym);
}

// How a metric's raw value maps onto the 0..100 ramp (0 = green).
function onRamp(metric, raw) {
  const x = Number(raw);
  // scene_count is rescaled onto the same ramp (12+ scenes = green).
  if (metric === "scene_count") return Math.max(0, 100 - x * 8);
  // coverage is inverted: a fully filled tile (100 %) is green.
  if (metric === "max_cover") return Math.max(0, 100 - x);
  return x;
}

// The month's rows from its slice: [] for a month with no slice. The
// percent columns are UTINYINT and scene_count USMALLINT; they arrive as JS
// numbers and Number() in onRamp and paintMonth handles them like any other.
export async function statsForMonth(y, m) {
  const metric = $("metric").value;
  if (!METRICS.has(metric)) throw new Error(`unknown metric ${metric}`);
  const ym = `${Number(y)}-${String(Number(m)).padStart(2, "0")}`;
  const file = await monthFile(ym);
  if (!file) return [];
  const res = await conn.query(`
    SELECT mgrs_tile, ${metric} AS v, min_cloud_cover AS cc, scene_count AS sc,
           max_cover AS cover
    FROM read_parquet('${file}')`);
  return res.toArray();
}

// The three filter sliders, combined into one sentence for the status line
// (Task 22): "N of M tiles pass (cloud ≤ x, coverage ≥ y, scenes ≥ z)".
function fmtFilters() {
  return `cloud ≤ ${maxCloud}, coverage ≥ ${minCoverage}, scenes ≥ ${minScenes}`;
}
function passCounts() {
  let n = 0;
  for (const s of lookup.values()) {
    if (s.cc !== null && s.cc > maxCloud) continue;
    if (s.cover !== null && s.cover < minCoverage) continue;
    if (s.sc !== null && s.sc < minScenes) continue;
    n++;
  }
  return { n, m: lookup.size };
}
function filterLine() {
  const { n, m } = passCounts();
  return `${n.toLocaleString()} of ${m.toLocaleString()} tiles pass (${fmtFilters()})`;
}

// The scene-count slider's bounds track the loaded month: 0..p99 of that
// month's scene_count, integer step, recomputed every time the month
// changes (a busy month and a quiet one should not share one scale).
function updateScenesBound(rows) {
  const values = rows.map((r) => Number(r.sc)).filter(Number.isFinite).sort((a, b) => a - b);
  const p99 = values.length
    ? values[Math.min(values.length - 1, Math.ceil(0.99 * values.length) - 1)]
    : 0;
  const bound = Math.max(1, Math.round(p99));
  const slider = $("minscenes");
  slider.max = bound;
  if (Number(slider.value) > bound) {
    slider.value = bound;
    minScenes = bound;
  }
  $("minscenes-out").textContent = slider.value;
}

// A month slice is a network fetch now, so two quick month changes can
// resolve out of order; only the latest call may touch the map or the
// status line.
let paintSeq = 0;

async function paintMonth() {
  const [y, m] = ($("month").value || "").split("-").map(Number);
  if (!y || !m) return;
  const ym = `${y}-${String(m).padStart(2, "0")}`;
  const seq = ++paintSeq;
  // Said once, on whichever paintMonth() call happens to be first (the
  // default-month load), then never again.
  const note = lagNote;
  lagNote = "";
  say(`Reading ${ym}…`);
  let rows;
  try {
    rows = await statsForMonth(y, m);
  } catch (err) {
    if (seq === paintSeq) say(`Could not read ${monthUrl(ym)} — ${err.message}`, true);
    return;
  }
  if (seq !== paintSeq) return;
  updateScenesBound(rows);
  const metric = $("metric").value;
  const next = new Map();
  for (const r of rows) {
    // A NULL metric (a cover with no nodata property, a NULL cloud cover)
    // is left unpainted: Number(null) is 0, which would read as "0% filled"
    // or "0% cloud". A NULL clearest-scene cover, coverage or scene count
    // is kept as null so no slider ever dims what it cannot judge.
    if (r.v == null) continue;
    next.set(r.mgrs_tile, {
      v: onRamp(metric, r.v),
      cc: r.cc == null ? null : Number(r.cc),
      cover: r.cover == null ? null : Number(r.cover),
      sc: r.sc == null ? null : Number(r.sc),
    });
  }
  lookup = next;
  repaint();
  markActiveBar();
  if (!rows.length) {
    say(`No tile-months for ${ym} in the published stats. `
      + `Pick a month with bars in the timeline below.`
      + (note ? ` ${note}` : ""));
    return;
  }
  const unpainted = rows.length - next.size;
  say(`${rows.length.toLocaleString()} MGRS tiles imaged in ${ym} — `
    + `one small month slice (months/${ym}.parquet), no API call.`
    + (unpainted ? ` ${unpainted.toLocaleString()} have no ${metric} value and stay grey.` : "")
    + ` ${filterLine()}.`
    + (note ? ` ${note}` : ""));
}

// All tiles: the timeline file, already in memory, one row per month. One
// tile: the full table over httpfs, WHERE mgrs_tile = ... — DuckDB reads
// the footer, keeps only the row groups whose mgrs_tile range covers the
// tile (the table is sorted by tile), and range-reads those. That is
// several network round trips, so like paintMonth() only the latest call
// may touch the bars or the status line: click tile A then B and A's
// answer, landing last, must not replace B's.
let timelineSeq = 0;

export async function timelineFor(tile) {
  const bars = $("bars");
  const seq = ++timelineSeq;
  let rows;
  try {
    if (tile) say(`Reading tile ${tile}'s history…`);
    const res = await conn.query(tile
      ? `SELECT year, month, sum(scene_count)::INT AS n,
                min(min_cloud_cover) AS clearest
         FROM read_parquet('${STATS}')
         WHERE mgrs_tile = '${tile.replace(/'/g, "")}'
         GROUP BY 1, 2 ORDER BY 1, 2`
      : `SELECT year, month, scene_count AS n, min_cloud_cover AS clearest
         FROM read_parquet('${TIMELINE_FILE}') ORDER BY 1, 2`);
    rows = res.toArray();
  } catch (err) {
    if (seq !== timelineSeq) return;
    bars.replaceChildren(el("p", "hint", `Timeline unavailable — ${err.message}`));
    if (tile) say(`Could not read tile ${tile}'s history from ${STATS} — ${err.message}`, true);
    return;
  }
  if (seq !== timelineSeq) return;
  if (tile) {
    const scenes = rows.reduce((t, r) => t + Number(r.n), 0);
    say(`Tile ${tile}: ${rows.length} months, ${scenes.toLocaleString()} scenes — `
      + "range-read from mgrs-monthly.parquet, not the whole file.");
  }
  const scope = tile ? `Tile ${tile}` : "All tiles";
  bars.replaceChildren();
  if (!rows.length) {
    $("timeline-scope").textContent = `${scope} — nothing to plot.`;
    bars.append(el("p", "hint", tile
      ? `No months recorded for tile ${tile}.`
      : "The stats timeline is empty — nothing has been published yet."));
    return;
  }
  const ymOf = (r) => `${r.year}-${String(r.month).padStart(2, "0")}`;
  $("timeline-scope").textContent =
    `${scope}, ${ymOf(rows[0])} → ${ymOf(rows[rows.length - 1])}. `
    + "Click a bar to jump to that month.";
  const max = Math.max(...rows.map((r) => Number(r.n)), 1);
  for (const r of rows) {
    const ym = ymOf(r);
    const d = document.createElement("button");
    d.type = "button";
    d.className = "bar";
    d.dataset.ym = ym;
    d.style.height = `${Math.max(3, (100 * Number(r.n)) / max)}%`;
    d.style.background = rampColor(r.clearest);
    d.title = `${ym}: ${r.n} scenes, clearest ${Number(r.clearest).toFixed(1)}%`;
    d.setAttribute("aria-label", d.title);
    d.onclick = () => {
      $("month").value = ym;
      reboundDateRange(ym);
      paintMonth();
    };
    bars.append(d);
  }
  markActiveBar();
}

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text) n.textContent = text;
  return n;
}

function markActiveBar() {
  const ym = $("month").value;
  for (const b of $("bars").querySelectorAll(".bar")) {
    b.classList.toggle("on", b.dataset.ym === ym);
  }
}

const LEGENDS = {
  scene_count: ["12+ scenes", "1 scene"],
  max_cover: ["100% filled", "0% filled"],
};
function updateLegend() {
  const [lo, hi] = LEGENDS[$("metric").value] ?? ["0% cloud", "100% cloud"];
  $("lo").textContent = lo;
  $("hi").textContent = hi;
}

// The current calendar month is empty until the backfill lands, so the app
// opens on the newest month the stats actually contain: the last row of the
// timeline file, which is also the newest month with a months/ slice.
async function newestMonth() {
  const res = await conn.query(
    `SELECT max(year::INT * 100 + month::INT) AS ym,
            min(year::INT * 100 + month::INT) AS lo
     FROM read_parquet('${TIMELINE_FILE}')`);
  const [row] = res.toArray();
  const fmt = (n) => `${Math.floor(n / 100)}-${String(n % 100).padStart(2, "0")}`;
  return row?.ym ? { newest: fmt(row.ym), oldest: fmt(row.lo) } : null;
}

// The three sliders live-filter the map: tiles that fail any gate go grey.
// One colour recompute per animation frame at most, and no query — every
// threshold is applied inside the fill accessor (fillColor).
let sliderFrame = 0;
function onSlider() {
  $("maxcloud-out").textContent = $("maxcloud").value;
  $("mincoverage-out").textContent = $("mincoverage").value;
  $("minscenes-out").textContent = $("minscenes").value;
  if (sliderFrame) return;
  sliderFrame = requestAnimationFrame(() => {
    sliderFrame = 0;
    const cc = Number($("maxcloud").value);
    const cov = Number($("mincoverage").value);
    const sc = Number($("minscenes").value);
    if (cc === maxCloud && cov === minCoverage && sc === minScenes) return;
    maxCloud = cc; minCoverage = cov; minScenes = sc;
    repaint();
    say(`${filterLine()}.`);
  });
}

let dateRange = null;

// Set once in init() when the stats file lags the newest published item
// year, and appended to the very next paintMonth() status line so the lag
// is said once on load, not repeated on every later month switch.
let lagNote = "";

function lastDayOfMonth(ym) {
  const [yy, mm] = ym.split("-").map(Number);
  return new Date(Date.UTC(yy, mm, 0)).toISOString().slice(0, 10);
}

// The scene query's window is scoped to whichever month is on screen (not
// the whole stats span): re-scoped on every month change so it can never
// reach outside the selected month, and reset to that month's full range.
// The two-handle slider and the two calendar inputs stay the same value,
// either way round.
function reboundDateRange(ym) {
  const from0 = `${ym}-01`, to0 = lastDayOfMonth(ym);
  if (dateRange) {
    dateRange.rebound(from0, to0);
    return;
  }
  // dayRange()'s own construction calls fromDates() once, but that no-ops
  // on a fresh page load: the <input type=date> fields start empty, and
  // fromDates() refuses to compute from an empty value. set() writes the
  // values directly, the same way rebound() does on every later call.
  dateRange = dayRange({ container: $("dayrange"), from: $("date0"), to: $("date1"),
    min: from0, max: to0 });
  dateRange.set(from0, to0);
}

async function init() {
  updateLegend();
  $("metric").addEventListener("change", () => { updateLegend(); paintMonth(); });
  $("month").addEventListener("change", () => {
    reboundDateRange($("month").value);
    paintMonth();
  });
  $("maxcloud").addEventListener("input", onSlider);
  $("mincoverage").addEventListener("input", onSlider);
  $("minscenes").addEventListener("input", onSlider);
  say("Reading the stats timeline…");
  let span;
  try {
    await loadTimeline();
    span = await newestMonth();
  } catch (err) {
    say(`Could not open ${TIMELINE}. The file is missing, unreadable, or the `
      + `bucket refused the read (${err.message}). Until publish-stats `
      + `publishes it, serve a local publish tree and load `
      + `?base=http://localhost:8081`, true);
    return;
  }
  if (!span) {
    say("The stats timeline has no rows yet — the backfill has not published "
      + "any tile-months. The map and timeline will fill in once it does.");
    await timelineFor(null);
    return;
  }
  const month = $("month");
  // The stats can lag the item parts (they are rebuilt on their own
  // schedule, one cached HEAD per year past the stats to find how far
  // publishing has gone, the same probe the search uses): the default
  // month is always span.newest, the newest month the timeline actually
  // has a row for (computed in newestMonth() above with year/month cast to
  // INT before the year*100+month arithmetic — SMALLINT overflows past
  // year 327). Never default to a month the stats haven't reached yet,
  // which would paint an all-grey map with nothing to click. The picker
  // itself still opens as far as the newest published item year (month.max
  // below) so the user can browse ahead of the stats on purpose.
  const statsYear = Number(span.newest.slice(0, 4));
  const newestYear = await newestPublishedYear(statsYear);
  const defaultMonth = span.newest;
  if (newestYear > statsYear) {
    lagNote = `Stats reach ${span.newest}; scenes are published through ${newestYear} `
      + "— the choropleth updates when the stats rebuild lands.";
  }
  month.min = span.oldest;
  month.max = newestYear > statsYear ? `${newestYear}-12` : span.newest;
  month.value = defaultMonth;
  reboundDateRange(defaultMonth);
  await Promise.all([paintMonth(), timelineFor(null)]);
  // paintMonth() already calls markActiveBar() (the same call the timeline
  // bar's own onclick makes), but it can run before timelineFor() has
  // appended the bar buttons; timelineFor() also calls it once its bars
  // exist, so this just guarantees the default month's bar ends up
  // highlighted regardless of which promise settles first.
  markActiveBar();
}

// init() runs at the end of the module: it probes the item years with the
// scene query's constants and helpers, which are defined below.

// ---------------------------------------------------------------------------
// The scene query. Click a tile, pick a window, and DuckDB-WASM range-reads the
// year parts of sentinel-2-l2a directly. The row's `assets` column (a JSON
// string carrying the upstream STAC assets object) is ~18 KB a row and half
// the bytes of a part, so the search leaves it out; every COG the page draws
// or links sits in the scene directory that `thumbnail_url` names (see
// sceneDirOf), so no row ever needs `assets`.
// ---------------------------------------------------------------------------

// An MGRS tile id: 1-2 digit UTM zone, latitude band C..X, then two letters.
// The values come from the tileset, not from a text box, but they are the only
// thing on this page that reaches a SQL string, so they are checked anyway.
const TILE_RE = /^\d{1,2}[C-X][A-Z]{2}$/;
const CURRENT_YEAR = new Date().getUTCFullYear();

map.on("click", (e) => {
  const tile = hitIndex.at(e.lngLat.wrap().lng, e.lngLat.lat)?.tile;
  if (!TILE_RE.test(tile ?? "")) return;
  selectedTile = tile;
  $("query").querySelector(".hint").textContent =
    `Tile ${tile}. Pick a window and search.`;
  $("run").disabled = false;
  timelineFor(tile);
});

// The zone parts of a year: [file stem, first zone, last zone], mirrored
// from tools/s2_build.py because the browser cannot import it (spec
// Amendment 3; tests/test_build.py pins every name and both years to this
// file). A year before ZONE_SPLIT_FROM is one items.parquet; from 2019 it
// is the four ZONE_PARTS quartiles; from ZONE_SPLIT_8_FROM the eight
// ZONE_PARTS_8 octants, whose boundaries nest inside the quartiles'. Every
// tier is split by the UTM zone of the tile id, so a query for one tile
// needs exactly one file of whichever tier the year has.
const ZONE_PARTS = [["z01-20", 1, 20], ["z21-35", 21, 35],
  ["z36-46", 36, 46], ["z47-60", 47, 60]];
const ZONE_SPLIT_FROM = 2019;
const ZONE_PARTS_8 = [["z01-15", 1, 15], ["z16-20", 16, 20],
  ["z21-31", 21, 31], ["z32-35", 32, 35], ["z36-40", 36, 40],
  ["z41-46", 41, 46], ["z47-52", 47, 52], ["z53-60", 53, 60]];
const ZONE_SPLIT_8_FROM = 2021;

// The tier a year is published in, as s2_build.zone_parts_for(year):
// null before the split, then the quartiles, then the octants.
function zonePartsFor(year) {
  if (year >= ZONE_SPLIT_8_FROM) return ZONE_PARTS_8;
  if (year >= ZONE_SPLIT_FROM) return ZONE_PARTS;
  return null;
}

// The archive file holding a tile in a year: its UTM zone is the leading
// one or two digits of the id ("1VCJ" is zone 1, "31UFU" is zone 31), and
// the year picks the tier. items.parquet before the split.
function archivePartFor(tile, year) {
  const parts = zonePartsFor(year);
  if (!parts) return "items";
  const digits = tile.match(/^\d{1,2}/);
  const zone = digits ? Number(digits[0]) : NaN;
  const hit = parts.find(([, lo, hi]) => zone >= lo && zone <= hi);
  return hit ? hit[0] : null;
}

// Which parts actually exist. `read_parquet` over a list fails outright on a
// missing file, and the parts are genuinely optional: live.parquet only exists
// for the current year once the daily refresh has run, and a year that is not
// published yet has no archive part at all. So each candidate is probed once
// with a HEAD (a CORS-simple request, no preflight) and the answer is cached.
const partProbes = new Map();
const partExists = (url) => {
  if (!partProbes.has(url)) {
    partProbes.set(url, fetch(url, { method: "HEAD" })
      // The empty body is drained so Chrome does not log the probe as an
      // aborted request in the network panel.
      .then(async (r) => { await r.arrayBuffer().catch(() => {}); return r.ok; })
      .catch(() => false));
  }
  return partProbes.get(url);
};

// Every part that could hold `tile` in the years y0..y1, filtered to the ones
// that exist. Per year that is exactly one archive file -- items.parquet, or
// the one zone part of the year's tier the tile's zone falls in; the other
// parts of the year are never probed, let alone read -- plus live.parquet for
// the current year. The tier comes from the year (the thresholds are
// constants mirrored above); the probe only asks whether the year is
// published yet.
// The last year with any published archive part, from `from` upward: the
// zone-1 part of each year's tier is probed (a year is published whole, so
// one part stands for the year), plus live.parquet for the current year.
// Years are probed in order and the walk stops at the first unpublished
// one, so a page load costs one 404 (which Chrome logs), not one per
// future year.
async function newestPublishedYear(from) {
  let newest = from;
  for (let y = from + 1; y <= CURRENT_YEAR; y++) {
    const dir = `${BASE}/sentinel-2-l2a/year=${y}`;
    const probes = [`${dir}/${archivePartFor("1CDK", y)}.parquet`];
    if (y === CURRENT_YEAR) probes.push(`${dir}/live.parquet`);
    if (!(await Promise.all(probes.map(partExists))).some(Boolean)) break;
    newest = y;
  }
  return newest;
}

async function partUrls(y0, y1, tile) {
  const candidates = [];
  for (let y = y0; y <= y1; y++) {
    const dir = `${BASE}/sentinel-2-l2a/year=${y}`;
    const archive = archivePartFor(tile, y);
    if (archive) candidates.push(`${dir}/${archive}.parquet`);
    if (y === CURRENT_YEAR) candidates.push(`${dir}/live.parquet`);
  }
  const present = await Promise.all(candidates.map(partExists));
  return candidates.filter((_, i) => present[i]);
}

const partList = (urls) => `[${urls.map((u) => `'${u}'`).join(", ")}]`;

function sceneSql(urls, tile, d0, d1, cc, cov) {
  // `_month` is the cheap row-group filter, but it only narrows anything while
  // the window stays inside one calendar year — across a year boundary
  // (2023-11 → 2024-02) months 11..2 is empty, so it widens to the whole year.
  const sameYear = d0.slice(0, 4) === d1.slice(0, 4);
  const m0 = sameYear ? Number(d0.slice(5, 7)) : 1;
  const m1 = sameYear ? Number(d1.slice(5, 7)) : 12;
  // The coverage gate is the item-level twin of the stats file's max_cover
  // (100 - s2:nodata_pixel_percentage), projected straight from the column
  // the item parts always carry — never from `assets`. Omitted at 0 (the
  // slider's no-op value, and its state whenever the slider is hidden).
  const coverClause = cov > 0
    ? `\n  AND (100 - "s2:nodata_pixel_percentage") >= ${cov}` : "";
  // `datetime` is TIMESTAMPTZ; comparing it against a bare literal would be
  // read in the session's zone, so both sides are pinned to UTC. The same
  // conversion formats the label, rather than guessing at the epoch units
  // Arrow hands back. `assets` is deliberately not selected (see above);
  // `bbox` is, for "Show on map", and the processing baseline (a short
  // dictionary-coded string) for the band mapper's index offset (bands.js).
  return `SELECT id,
       strftime(datetime AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS ts,
       "eo:cloud_cover" AS cloud,
       thumbnail_url,
       bbox,
       "s2:processing_baseline" AS baseline
FROM read_parquet(${partList(urls)}, union_by_name=true)
WHERE "s2:mgrs_tile" = '${tile}'
  AND _month BETWEEN ${m0} AND ${m1}
  AND (datetime AT TIME ZONE 'UTC')
      BETWEEN TIMESTAMP '${d0} 00:00:00' AND TIMESTAMP '${d1} 23:59:59'
  AND "eo:cloud_cover" <= ${cc}${coverClause}
ORDER BY "eo:cloud_cover", id
LIMIT 30`;
}

// The request a STAC API would have been asked for the same answer. Shown in
// full because not making it is the point of this page.
function apiMirror(tile, d0, d1, cc, cov) {
  const query = {
    "eo:cloud_cover": { lte: cc },
    "s2:mgrs_tile": { eq: tile },
  };
  // Mirrors sceneSql's coverage gate: coverage = 100 - nodata, so
  // coverage >= cov is nodata <= 100 - cov. Omitted at the slider's inert
  // value, same as the real query.
  if (cov > 0) query["s2:nodata_pixel_percentage"] = { lte: 100 - cov };
  return JSON.stringify({
    note: "The STAC API request this page did NOT need to make. "
      + "Earth Search would answer it; the panel above is the same answer, "
      + "range-read out of static Parquet.",
    method: "POST",
    url: "https://earth-search.aws.element84.com/v1/search",
    body: {
      collections: ["sentinel-2-l2a"],
      datetime: `${d0}T00:00:00Z/${d1}T23:59:59Z`,
      query,
      sortby: [{ field: "properties.eo:cloud_cover", direction: "asc" }],
      limit: 30,
    },
  }, null, 2);
}

const bboxOf = (r) => {
  try {
    const b = Array.from(r.bbox ?? []).map(Number);
    return b.length === 4 && b.every(Number.isFinite) ? b : null;
  } catch {
    return null;
  }
};

// Every COG of a scene sits in the directory the thumbnail is in (checked on
// 2020 thumbnail.jpg and 2026 preview.jpg rows alike: TCI.tif, B02.tif …
// SCL.tif), and thumbnail_url is already in the search projection — so the
// map derives each href instead of reading the row's `assets`, which cost
// ~2 MB and ~6 s of range reads per click (docs/query-performance.md)
// before anything appeared. https, and a host the page expects, or it
// refuses: these strings are fetched, not just linked.
const COG_HOST_RE = /(^|\.)(amazonaws\.com|source\.coop)$/;
function sceneDirOf(r) {
  const thumb = r.thumbnail_url;
  if (typeof thumb !== "string" || !thumb) throw new Error("the item has no thumbnail_url to locate its COGs by");
  let u;
  try { u = new URL(thumb); } catch { throw new Error(`thumbnail_url is not a URL: ${thumb}`); }
  if (u.protocol !== "https:") throw new Error(`thumbnail_url is not https: ${thumb}`);
  if (!COG_HOST_RE.test(u.hostname)) throw new Error(`thumbnail_url is on an unexpected host: ${u.hostname}`);
  u.pathname = u.pathname.replace(/\/[^/]*$/, "");
  u.search = ""; u.hash = "";
  return u.href;
}
// The same, or null, for a card's download links.
const sceneDirOrNull = (r) => { try { return sceneDirOf(r); } catch { return null; } };

// The BOA offset an index must subtract (bands.js): 1000 from processing
// baseline 04.00 on, 0 before, null when the row does not say.
function offsetOf(r) {
  const b = r.baseline;
  if (typeof b !== "string" || !/^\d\d\.\d\d$/.test(b)) return null;
  return b >= "04.00" ? 1000 : 0;
}

// The thumbnail as an ImageBitmap, or null when it cannot be had (the tiles
// still come; only the instant preview is lost).
async function thumbnailBitmap(url) {
  try {
    const res = await fetch(url, { mode: "cors" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await createImageBitmap(await res.blob());
  } catch (err) {
    console.warn(`no preview for the map: ${url} — ${err.message}`);
    return null;
  }
}

// The cogbar's states: "loading" spins until every tile in view has loaded,
// "full" is the tiles alone, "partial" keeps the preview under tiles that
// failed, or says which band is missing. The spinner is CSS on data-state.
function cogbar(id, state, text) {
  $("cog-id").textContent = id;
  $("cog-state").dataset.state = state;
  $("cog-state").textContent = text;
  $("cogbar").hidden = false;
  $("imgpanel").hidden = false;
}
// The image panel at the map's lower right (Task 29) holds the cogbar and
// the band mapper; it shows with the first cogbar and goes with the scene.
function hideImagePanel() {
  $("imgpanel").hidden = true;
  $("cogbar").hidden = true;
  $("bandbox").hidden = true;
}

// ---------------------------------------------------------------------------
// The band mapper (Task 28). `ui` is what the panel says: the preset, the
// R/G/B and single-band picks, curve, gamma, nodata. The shown scene adds
// what only its data can say: the min/max per channel (defaulting to the
// overview's 2nd..98th percentiles the first time a band is seen), the
// index offset from its baseline, and which bands failed. bandSpec() joins
// the two into the spec cog.js/bands.js paint from.
// ---------------------------------------------------------------------------
const ui = { preset: "tci", rgb: ["B04", "B03", "B02"], single: "B04", curve: "linear", gamma: 1, nodata: 0 };
for (const [key, p] of Object.entries(PRESETS)) $("preset").append(new Option(p.label, key));
for (const id of ["sel-r", "sel-g", "sel-b"]) {
  for (const b of Object.keys(BANDS)) {
    const o = new Option(`${b} ${BANDS[b].label}`, b);
    o.title = bandTitle(b);
    $(id).append(o);
  }
}
for (const [v, name, color] of SCL_CLASSES) {
  const sw = el("span", null, `${v} ${name}`);
  sw.style.setProperty("--sw", color);
  $("scl-legend").append(sw);
}
const chanKey = (ch) => ch.index ?? ch.band;

function bandSpec(me) {
  const p = PRESETS[ui.preset];
  const spec = { kind: p.kind, preset: ui.preset, curve: ui.curve,
    gamma: ui.gamma, nodata: p.kind === "scl" ? 0 : ui.nodata, offset: me?.offset ?? 0,
    label: p.kind === "gray" ? `Single band ${ui.single}` : p.label };
  if (p.kind === "index") {
    spec.index = p.index;
    spec.bands = bandsOf(spec);
    spec.channels = [{ index: p.index }];
  } else {
    spec.bands = p.kind === "gray" ? [ui.single] : ui.preset === "custom" ? [...ui.rgb] : [...p.bands];
    spec.channels = p.kind === "rgb" || p.kind === "gray" ? spec.bands.map((band) => ({ band })) : [];
  }
  for (const ch of spec.channels) Object.assign(ch, me?.ranges.get(chanKey(ch)) ?? { min: 0, max: 1 });
  return spec;
}
const styleKeyOf = (spec) => JSON.stringify([spec.channels, spec.curve, spec.gamma, spec.nodata, spec.offset]);

// The panel follows the spec: which selects show, which blocks apply.
function syncPanel(spec, me) {
  const { kind } = spec;
  $("preset").value = spec.preset;
  // The selects show what is drawn, so a change under a preset starts
  // Custom from that preset's other two bands.
  if (kind === "rgb") ui.rgb = [...spec.bands];
  $("sel-r").value = kind === "gray" ? ui.single : ui.rgb[0];
  $("sel-g").value = ui.rgb[1]; $("sel-b").value = ui.rgb[2];
  $("rgbsel").hidden = !(kind === "rgb" || kind === "gray");
  $("rgbsel").classList.toggle("three", kind === "rgb");
  $("sel-g").parentElement.hidden = $("sel-b").parentElement.hidden = kind !== "rgb";
  $("sel-r").previousElementSibling.textContent = kind === "gray" ? "Band" : "R";
  $("stretchopts").hidden = kind === "tci" || kind === "scl";
  // An index is linear between its handles (bands.js): no curve, no gamma.
  $("stretchopts").querySelector(".bandrow").hidden = kind === "index";
  $("gamma").parentElement.hidden = kind === "index";
  $("channels").hidden = kind === "tci" || kind === "scl";
  $("scl-legend").hidden = kind !== "scl";
  const notes = [];
  if (kind === "tci") notes.push("TCI is ESA's own stretch of B04/B03/B02 — pick a composite or a band for stretch controls.");
  if (kind === "index") {
    const ix = INDICES[spec.index];
    notes.push(`${ix.label} = (${ix.a} − ${ix.b}) / (${ix.a} + ${ix.b}) on DN; ramp fixed over −1..1, handles narrow it.`);
    if (me?.offset === null) notes.push("No processing baseline in the row — the ≥ 04.00 BOA offset (−1000) is not applied.");
    else if (me?.offset) notes.push(`Baseline ${me.r.baseline}: −1000 BOA offset applied before the ratio.`);
  }
  if (me?.missing.length) notes.push(`${me.missing.join(", ")} could not be opened — shown without.`);
  $("bandnote").textContent = notes.join(" ");
}

// One block per channel: the overview's histogram (64 bins over the data's
// own min..max, sqrt-scaled heights so the tail shows), the two handles, the
// numeric min/max, and the 2–98 % / Min/Max buttons. Rebuilt per band set.
function buildChannels(me, spec) {
  const box = $("channels");
  box.replaceChildren();
  for (const ch of spec.channels) {
    const key = chanKey(ch);
    const stats = ch.index
      ? sceneIndexStats(me.scene, INDICES[ch.index].a, INDICES[ch.index].b, me.offset ?? 0)
      : me.scene.overviews.get(ch.band)?.value?.stats ?? null;
    // No stats (band unreadable): the same 0..10000 the range was seeded with.
    const lo = ch.index ? -1 : stats?.min ?? 0, hi = ch.index ? 1 : stats?.max ?? 10000;
    const block = el("div", "chan");
    const head = el("div", "chan-head");
    head.append(el("b", null, ch.index ? INDICES[ch.index].label : ch.band),
      el("span", "hint", ch.index ? `${INDICES[ch.index].a} − ${INDICES[ch.index].b}` : bandTitle(ch.band).slice(4)));
    const canvas = document.createElement("canvas");
    canvas.className = "hist"; canvas.width = 320; canvas.height = 36;
    canvas.title = "Histogram of the overview (nodata left out); the lit bins are inside the handles";
    const range = el("div", "dayrange vrange");
    const mm = el("div", "minmax");
    const minIn = document.createElement("input"), maxIn = document.createElement("input");
    for (const i of [minIn, maxIn]) { i.type = "number"; i.step = ch.index ? 0.01 : 1; }
    mm.append(minIn, "–", maxIn);
    const draw = () => {
      const r = me.ranges.get(key);
      drawHist(canvas, stats, lo, hi, r.min, r.max);
      minIn.value = ch.index ? r.min.toFixed(2) : Math.round(r.min);
      maxIn.value = ch.index ? r.max.toFixed(2) : Math.round(r.max);
    };
    const slider = valueRange({ container: range, lo, hi, onInput: (min, max) => {
      me.ranges.set(key, { min, max }); draw(); scheduleRestyle();
    } });
    const setRange = (min, max) => {
      if (!(max > min)) return;
      me.ranges.set(key, { min, max }); slider.set(min, max); draw(); scheduleRestyle();
    };
    const fromInputs = () => setRange(Number(minIn.value), Number(maxIn.value));
    minIn.addEventListener("change", fromInputs); maxIn.addEventListener("change", fromInputs);
    if (stats) {
      const b1 = el("button", "mini", "2–98 %"), b2 = el("button", "mini", "Min/Max");
      b1.type = b2.type = "button";
      b1.title = "Handles to the overview's 2nd and 98th percentiles";
      b2.title = "Handles to the overview's minimum and maximum";
      b1.addEventListener("click", () => setRange(stats.p2, stats.p98));
      b2.addEventListener("click", () => setRange(stats.min, stats.max));
      head.append(b1, b2);
    }
    block.append(head, canvas, range, mm);
    box.append(block);
    slider.set(me.ranges.get(key).min, me.ranges.get(key).max);
    draw();
  }
}

function drawHist(canvas, stats, lo, hi, min, max) {
  const ctx = canvas.getContext("2d"), W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  if (!stats) {
    ctx.fillStyle = "#93a2c0"; ctx.font = "11px system-ui, sans-serif";
    ctx.fillText("no histogram (overview unreadable)", 6, 22);
    return;
  }
  const { hist } = stats;
  let peak = 1;
  for (let i = 0; i < HIST_BINS; i++) if (hist[i] > peak) peak = hist[i];
  const bw = W / HIST_BINS;
  for (let i = 0; i < HIST_BINS; i++) {
    const v = lo + ((i + 0.5) / HIST_BINS) * (hi - lo);
    const h = Math.sqrt(hist[i] / peak) * (H - 2);
    ctx.fillStyle = v >= min && v <= max ? "#6ea8ff" : "#3a4666";
    ctx.fillRect(i * bw, H - h, Math.max(1, bw - 0.6), h);
  }
}

// The scene being shown: its row, the click's clock, the band-COG memory
// (cog.js openScene), which band set is on the map, whether its tiles have
// settled or one has failed, whether a band set is still loading, and a
// serial that a later load bumps so a superseded load's late overviews
// never draw. Replaced by every "Show on
// map" click and dropped by Clear.
let shown = null;
Object.defineProperties(window.S2, { shown: { get: () => shown }, ui: { value: ui } });

// The preview comes off once the tile layer has every tile of the resting
// viewport. onViewportLoad fires mid-flight too (each coarse view the camera
// passes through loads), so while the map moves this waits for its moveend
// and asks the layer itself. A later pan re-fires onViewportLoad; once
// settled there is nothing left to do.
function tilesSettled() {
  if (!shown || shown.settled || shown.failed || !cogLayer) return;
  if (map.isMoving() || !cogLayer.isLoaded) return;
  shown.settled = true;
  cogPreview = null;
  render();
  const { id, spec, missing } = shown;
  const read = spec.bands.filter((b) => !missing.includes(b));
  const what = spec.kind === "tci" ? "TCI overviews" : `${read.join(", ")} overviews`;
  const how = spec.kind === "tci" ? "reprojected" : "reprojected and stretched";
  const without = missing.length ? ` without ${missing.join(", ")}` : "";
  cogbar(id, missing.length ? "partial" : "full", `Full resolution${without}`);
  say(`${id} on the map at full resolution (${spec.label}${without}): ${what} range-read `
    + `straight from the COG${read.length > 1 ? "s" : ""}, ${how} in the browser. `
    + "No tile server, no API.");
}
map.on("moveend", tilesSettled);

const stale = (me, serial) => me !== shown || serial !== me.serial;

// Put the panel's spec on the map for the shown scene: the TCI path (Task
// 27: thumbnail under the visual COG's tiles), a new band set (overviews
// first — preview and histograms — then the tiles), or, when only the
// stretch changed, a repaint of what is already there. Only a load bumps
// the serial: a gamma or curve change while a band set is still loading
// must not make that load stale (its layer would never appear and the
// spinner never clear); showBands re-derives the spec from `ui` once the
// overviews are in, so the change is not lost either. Any failure takes
// the scene off the map and hides the bar, so nothing is left without a
// Clear.
async function applySpec(me = shown) {
  if (!me) return;
  let serial = me.serial;
  try {
    const spec = bandSpec(me);
    if (spec.kind === "tci") {
      if (me.bandsKey === "TCI") { syncPanel(spec, me); return; }
      serial = ++me.serial;
      me.spec = spec;
      syncPanel(spec, me);
      await showTci(me, spec, serial);
    } else if (me.bandsKey !== spec.bands.join("+")) {
      serial = ++me.serial;
      await showBands(me, spec, serial);
    } else if (!me.loading) {
      restyle(me, spec);
    }
  } catch (err) {
    if (stale(me, serial)) return;
    shown = null;
    cogLayer = null; cogPreview = null; render();
    hideImagePanel();
    say(`Could not show ${me.id} — ${err.message}`, true);
  }
}
let restyleFrame = 0;
function scheduleRestyle() {
  if (restyleFrame) return;
  restyleFrame = requestAnimationFrame(() => { restyleFrame = 0; applySpec(); });
}

// A new band set on the map: reset the loading state, take the old layers
// off, say what is loading, read every band's overview (parallel; a band
// that cannot be opened is reported and left out), seed the handles at
// 2–98 % for a band not seen before, then the preview and the tiles in one
// render. Nothing here waits on a tile.
async function showBands(me, spec, serial) {
  const { id } = me;
  me.bandsKey = spec.bands.join("+"); me.spec = spec;
  me.settled = false; me.failed = false;
  me.loading = true;
  cogLayer = null; cogPreview = null; render();
  syncPanel(spec, me);
  // The old channel blocks go now: a handle dragged during the load would
  // write into ranges the new blocks are about to be built from.
  $("channels").replaceChildren();
  cogbar(id, "loading", "Loading preview…");
  say(`Preview of ${id} (${spec.label}) — loading ${spec.bands.join(", ")} at full resolution…`);
  try {
    await showBandsLoaded(me, spec, serial, await loadOverviews(me.scene, spec.bands));
  } finally {
    // A newer load owns the flag; only this load's own end clears it.
    if (me.serial === serial) me.loading = false;
  }
}
async function showBandsLoaded(me, spec, serial, failed) {
  const { id } = me;
  if (stale(me, serial)) return;
  me.missing = failed.map(([b]) => b);
  if (me.missing.length === spec.bands.length) {
    throw new Error(`${me.missing.join(", ")} could not be opened — ${failed[0][1].message}`);
  }
  for (const ch of spec.channels) {
    if (me.ranges.has(chanKey(ch))) continue;
    const st = ch.index
      ? sceneIndexStats(me.scene, INDICES[ch.index].a, INDICES[ch.index].b, me.offset ?? 0)
      : me.scene.overviews.get(ch.band)?.value?.stats;
    me.ranges.set(chanKey(ch), ch.index ? { min: -1, max: 1 } : st ? { min: st.p2, max: st.p98 } : { min: 0, max: 10000 });
  }
  spec = me.spec = bandSpec(me);
  syncPanel(spec, me);
  buildChannels(me, spec);
  const preview = bandPreviewImage(me.scene, spec);
  cogLayer = bandTileLayer(me.scene, spec, styleKeyOf(spec), `cog-${id}-${me.bandsKey}`, me.eventsFor(me.bandsKey));
  cogPreview = preview ? previewLayer(preview, me.scene, `cog-preview-${id}`) : null;
  render();
  cogbar(id, "loading", preview ? "Preview shown — loading full resolution…" : "Loading full resolution…");
  debug(`[cog] ${id} ${me.bandsKey} preview shown at ${(performance.now() - me.t0).toFixed(0)} ms`);
  if (me.missing.length) {
    say(`${me.missing.join(", ")} of ${id} could not be opened (${failed[0][1].message}) — `
      + `showing ${spec.label} without ${me.missing.length > 1 ? "them" : "it"}.`, true);
  }
  tilesSettled();
}

// Only the stretch changed: the same tiles repainted from their cached
// planes (a new layer instance with the same id and a new style key), and
// the preview repainted if it is still under them.
function restyle(me, spec) {
  me.spec = spec;
  syncPanel(spec, me);
  if (cogLayer) cogLayer = bandTileLayer(me.scene, spec, styleKeyOf(spec), cogLayer.id, me.eventsFor(me.bandsKey));
  if (cogPreview) {
    const img = bandPreviewImage(me.scene, spec);
    cogPreview = img ? previewLayer(img, me.scene, cogPreview.id) : null;
  }
  render();
}

// The visual COG (Task 27): its thumbnail over the scene as soon as the
// headers say where it goes, and the tiles replace it. Both start at once;
// whichever lands second draws the preview under tiles that may already be
// arriving.
async function showTci(me, spec, serial) {
  const { id, r } = me;
  me.bandsKey = "TCI"; me.missing = [];
  me.settled = false; me.failed = false;
  cogLayer = null; cogPreview = null; render();
  cogbar(id, "loading", "Loading preview…");
  say(`Preview of ${id} (True color) — loading full-resolution tiles…`);
  me.bitmapP ??= thumbnailBitmap(r.thumbnail_url);
  const cog = await sceneCog(me.scene, "TCI");
  if (stale(me, serial)) return;
  cogLayer = cogTileLayer(cog, `cog-${id}-TCI`, me.eventsFor("TCI"));
  render();
  cogbar(id, "loading", "Loading full resolution…");
  const bitmap = await me.bitmapP;
  if (stale(me, serial)) return;
  // Only thumbnail.jpg paints nodata white; preview.jpg and the .jp2 of
  // some 2018 rows (which Chrome and Firefox cannot decode, Safari can)
  // paint it black (cog.js, jpegNodataMask). The file name says which.
  const white = /\/thumbnail\.jpg$/i.test(new URL(r.thumbnail_url).pathname);
  const preview = bitmap && previewImage(cog, bitmap, { white });
  // Under the tiles unless they have all settled already; a failed tile
  // keeps it, as the bar says.
  if (preview && !me.settled) {
    cogPreview = previewLayer(preview, cog, `cog-preview-${id}`);
    render();
    if (!me.failed) cogbar(id, "loading", "Preview shown — loading full resolution…");
    debug(`[cog] ${id} preview shown at ${(performance.now() - me.t0).toFixed(0)} ms`);
    // The tiles may have all landed while the JPEG was still coming.
    tilesSettled();
  } else if (!preview && !me.settled) {
    say(`${id}: no preview (thumbnail unreadable) — loading full-resolution tiles…`);
  }
}

// "Show on map" and the card's band chips: fly to the scene's footprint,
// remember the scene, set the panel to the asked preset and apply it.
async function showOnMap(r, button, preset = "tci", band = null) {
  const id = String(r.id);
  // A chip on the scene already shown keeps what it has read and set.
  const prev = shown?.id === id ? shown : null;
  const me = shown = { id, r, t0: performance.now(), serial: 0, bandsKey: null, spec: null,
    settled: false, failed: false, missing: [], ranges: prev?.ranges ?? new Map(),
    offset: offsetOf(r), scene: prev?.scene ?? null, bitmapP: prev?.bitmapP ?? null,
    loading: false, eventsFor: null };
  const bbox = bboxOf(r);
  if (bbox) map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, duration: 1200 });
  button.disabled = true;
  // A scene already on the map comes off now, not when this one is ready:
  // the bar names this scene from here on and the map must not contradict it.
  if (cogLayer || cogPreview) { cogLayer = null; cogPreview = null; render(); }
  cogbar(id, "loading", "Loading preview…");
  // The tile events of one layer, bound to its band set: a layer taken off
  // the map for another band set may still fire while its tiles drain,
  // and must not settle or fail the one that replaced it.
  me.eventsFor = (key) => ({
    onTileError: (err) => {
      if (me !== shown || key !== me.bandsKey || me.failed) return;
      me.failed = true;
      console.warn(`[cog] tile failed for ${id}:`, err);
      cogbar(id, "partial", "Preview under the tiles — a full-resolution tile failed to load");
      say(`A full-resolution tile of ${id} failed to load — ${err?.message ?? err}. `
        + "The preview stays under the tiles that did.", true);
    },
    onViewportLoad: () => {
      if (me !== shown || key !== me.bandsKey) return;
      debug(`[cog] ${id} ${key} viewport loaded at ${(performance.now() - me.t0).toFixed(0)} ms`);
      tilesSettled();
    },
  });
  try {
    me.scene ??= openScene(id, sceneDirOf(r));
    ui.preset = preset;
    if (band) ui.single = band;
    $("bandbox").hidden = false;
    await applySpec(me);
  } catch (err) {
    if (me !== shown) return;
    shown = null;
    cogLayer = null; cogPreview = null; render();
    hideImagePanel();
    say(`Could not show ${id} — ${err.message}`, true);
  } finally {
    button.disabled = false;
  }
}

$("cog-clear").addEventListener("click", () => {
  shown = null;
  cogLayer = null;
  cogPreview = null;
  render();
  hideImagePanel();
});

// The panel's controls. A band select under a preset switches it to Custom
// (the single-band pick stays Single band); the rest apply as they are.
$("preset").addEventListener("change", () => { ui.preset = $("preset").value; applySpec(); });
for (const [i, id] of ["sel-r", "sel-g", "sel-b"].entries()) {
  $(id).addEventListener("change", () => {
    if (PRESETS[ui.preset].kind === "gray") { ui.single = $(id).value; }
    else { ui.rgb[i] = $(id).value; ui.preset = "custom"; }
    applySpec();
  });
}
for (const radio of document.querySelectorAll('input[name="curve"]')) {
  radio.addEventListener("change", () => { if (radio.checked) { ui.curve = radio.value; scheduleRestyle(); } });
}
$("gamma").addEventListener("input", () => {
  ui.gamma = Number($("gamma").value);
  $("gamma-out").textContent = ui.gamma.toFixed(2);
  scheduleRestyle();
});
$("nodata").addEventListener("change", () => { ui.nodata = $("nodata").value === "" ? null : 0; scheduleRestyle(); });

// The preview JPEGs carry opaque white nodata around the swath. A blend mode
// cannot key that out on a dark panel (multiply keeps the white as the panel
// but darkens the imagery by the panel's own brightness, ~8x; screen keeps
// the white), so the near-white pixels are made transparent on a canvas
// instead — in the browser, no server-side work. Needs the host's CORS
// header to read pixels back; when it is missing the plain image is shown.
function keyOutWhite(img) {
  const c = document.createElement("canvas");
  c.width = img.naturalWidth;
  c.height = img.naturalHeight;
  const ctx = c.getContext("2d");
  ctx.drawImage(img, 0, 0);
  const px = ctx.getImageData(0, 0, c.width, c.height);
  const d = px.data;
  for (let i = 0; i < d.length; i += 4) {
    if (d[i] >= 250 && d[i + 1] >= 250 && d[i + 2] >= 250) d[i + 3] = 0;
  }
  ctx.putImageData(px, 0, 0);
  img.src = c.toDataURL("image/png");
}

function thumbnail(r) {
  const img = document.createElement("img");
  img.loading = "lazy";
  img.decoding = "async";
  img.alt = `Preview of ${r.id}`;
  img.crossOrigin = "anonymous";
  img.addEventListener("load", () => {
    // Once: the keyed PNG's own load must not be keyed again, and the plain
    // (no-CORS) fallback cannot be read back at all.
    if (img.dataset.keyed || img.crossOrigin === null) return;
    img.dataset.keyed = "1";
    try { keyOutWhite(img); } catch { /* tainted canvas: keep the image as is */ }
  });
  img.addEventListener("error", () => {
    if (img.crossOrigin !== null) {
      // No CORS on this host: reload it as a plain image, white and all.
      img.crossOrigin = null;
      img.removeAttribute("crossorigin");
      img.src = r.thumbnail_url;
      return;
    }
    // A dead preview must not leave a broken-image box in the card.
    img.remove();
  });
  img.src = r.thumbnail_url;
  return img;
}

function sceneCard(r, i) {
  const card = el("div", "scene" + (i === 0 ? " best" : ""));
  if (typeof r.thumbnail_url === "string" && r.thumbnail_url) card.append(thumbnail(r));
  const cap = document.createElement("div");
  cap.append(el("b", null, r.id), document.createElement("br"));
  cap.append(`${String(r.ts).slice(0, 10)} · ${Number(r.cloud).toFixed(1)}% cloud`);
  cap.append(document.createElement("br"));
  const actions = el("span", "actions");
  const show = el("button", "mini", "Show on map");
  show.type = "button";
  show.title = "Fly to the footprint and draw the visual COG on the map";
  show.addEventListener("click", () => showOnMap(r, show));
  actions.append(show);
  // The band chips: each draws that band (TCI as true colour, B04/B08 as a
  // single band, SCL as the classes) and carries a small link to the COG
  // itself, derived from the thumbnail's directory like the map's reads.
  const chips = el("span", "chips");
  const dir = sceneDirOrNull(r);
  for (const [band, preset, single] of [["TCI", "tci"], ["B04", "single", "B04"],
    ["B08", "single", "B08"], ["SCL", "scl"]]) {
    const chip = el("button", "mini", band);
    chip.type = "button";
    chip.title = `Show ${band} on the map`;
    chip.addEventListener("click", () => showOnMap(r, chip, preset, single ?? null));
    chips.append(chip);
    if (!dir) continue;
    const a = el("a", null, "↗");
    a.href = bandHref(dir, band);
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.title = `Download ${band}.tif — ${a.href}`;
    chips.append(a);
  }
  cap.append(actions, chips);
  card.append(cap);
  return card;
}

async function runQuery() {
  const d0 = $("date0").value;
  const d1 = $("date1").value;
  const cc = Number($("maxcloud").value);
  const box = $("results");
  if (!selectedTile || !TILE_RE.test(selectedTile)) {
    say("Click an MGRS tile on the map first.", true);
    return;
  }
  if (!d0 || !d1) {
    say("Set both ends of the date window.", true);
    return;
  }
  if (d1 < d0) {
    say("The window ends before it starts — swap the two dates.", true);
    return;
  }
  $("run").disabled = true;
  box.replaceChildren(el("p", "hint", "Reading the item parts…"));
  try {
    const urls = await partUrls(Number(d0.slice(0, 4)), Number(d1.slice(0, 4)),
      selectedTile);
    if (!urls.length) {
      $("sql").textContent = "";
      $("api").textContent = "";
      box.replaceChildren(el("p", "hint",
        `No published item parts cover ${d0.slice(0, 4)}–${d1.slice(0, 4)}. `
        + "Pick a window the backfill has reached."));
      say(`Nothing published for ${d0.slice(0, 4)}–${d1.slice(0, 4)} yet.`);
      return;
    }
    const sql = sceneSql(urls, selectedTile, d0, d1, cc, minCoverage);
    $("sql").textContent = sql;
    $("api").textContent = apiMirror(selectedTile, d0, d1, cc, minCoverage);
    say(`Range-reading ${urls.length} parquet part`
      + `${urls.length === 1 ? "" : "s"} for tile ${selectedTile}…`);
    const rows = (await conn.query(sql)).toArray();
    box.replaceChildren();
    if (!rows.length) {
      box.append(el("p", "hint",
        `No ${selectedTile} scenes under ${cc}% cloud in that window — `
        + "raise the slider or widen the dates."));
      say(`No scenes matched — still no API call.`);
      return;
    }
    box.append(...rows.map(sceneCard));
    // The results sit below the timeline in the panel; without this the hero
    // flow's answer lands off-screen on a short window.
    box.scrollIntoView({ block: "nearest", behavior: "smooth" });
    say(`${rows.length} scene${rows.length === 1 ? "" : "s"} for ${selectedTile}, `
      + `clearest first — ${urls.length} range-read part`
      + `${urls.length === 1 ? "" : "s"}, no API call.`);
  } catch (err) {
    box.replaceChildren(el("p", "hint", `Query failed — ${err.message}`));
    say(`Could not read the item parts — ${err.message}`, true);
  } finally {
    $("run").disabled = false;
  }
}

$("run").addEventListener("click", runQuery);

await init();
