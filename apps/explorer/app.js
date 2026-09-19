// Sentinel-2 explorer. Everything below talks to static files:
//   stats/mgrs-monthly.parquet  – choropleth + timeline
//   stats/mgrs.pmtiles          – MGRS tile footprints
//   sentinel-2-l2a/year=*/…     – item queries (Task 12; one part per year
//                                 by the tile's UTM zone from 2019, Task 18;
//                                 eight parts from 2021, Task 19)
//   …/TCI.tif                   – a scene's visual COG, drawn on the map
//                                 straight from its overviews (Task 20)
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
import { openCog, cogTileLayer } from "./cog.js";
import { dayRange } from "./rangeslider.js";
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
// year parts of sentinel-2-l2a directly. Every href shown below is read out of
// the row's `assets` column (a JSON string carrying the upstream STAC assets
// object) — this app never builds an object-store URL from a template. The
// column is ~18 KB a row and half the bytes of a part, so the search leaves
// it out and a card fetches its own row's `assets` when a link is wanted.
// ---------------------------------------------------------------------------

// An MGRS tile id: 1-2 digit UTM zone, latitude band C..X, then two letters.
// The values come from the tileset, not from a text box, but they are the only
// thing on this page that reaches a SQL string, so they are checked anyway.
const TILE_RE = /^\d{1,2}[C-X][A-Z]{2}$/;
// An item id as Earth Search mints them; the lazy `assets` lookup puts it in
// a WHERE clause, so it is checked the same way.
const ITEM_RE = /^[A-Za-z0-9_.-]+$/;
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
  // `bbox` is, for "Show on map".
  return `SELECT id,
       strftime(datetime AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS ts,
       "eo:cloud_cover" AS cloud,
       thumbnail_url,
       bbox
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

// Asset hrefs are remote-derived strings going into an href, so only real
// https URLs are linked; anything else (javascript:, data:, http: — which
// this https page could not fetch anyway, missing) is dropped rather than
// rendered. A protocol check only: a mirror on another host must still work.
function assetHref(assets, key) {
  const href = assets?.[key]?.href;
  if (typeof href !== "string") return null;
  try {
    const u = new URL(href);
    if (u.protocol !== "https:") return null;
    if (!/(^|\.)(amazonaws\.com|source\.coop)$/.test(u.hostname)) {
      console.warn(`asset ${key} on an unexpected host: ${u.hostname}`);
    }
    return href;
  } catch {
    return null;
  }
}

// The visual COG first, then the bands.
const ASSET_LINKS = [["visual", "TCI"], ["red", "B04"], ["nir", "B08"],
  ["scl", "SCL"]];

// The parts the last search read, so a card can go back to the same file for
// its row's `assets` without probing again.
let lastParts = [];
const assetCache = new Map();

// One row's `assets`, read lazily from the same part the search read: the
// `_month` row-group filter plus `id`, LIMIT 1. Cached per item.
async function assetsFor(r) {
  const id = String(r.id);
  if (assetCache.has(id)) return assetCache.get(id);
  if (!ITEM_RE.test(id)) throw new Error(`unexpected item id ${id}`);
  const ts = String(r.ts);
  const year = ts.slice(0, 4), month = Number(ts.slice(5, 7));
  const urls = lastParts.filter((u) => u.includes(`/year=${year}/`));
  if (!urls.length) throw new Error(`no part for ${year}`);
  const sql = `SELECT assets FROM read_parquet(${partList(urls)}, union_by_name=true)
WHERE _month = ${month} AND id = '${id}' LIMIT 1`;
  $("sql").textContent = sql;
  const p = conn.query(sql).then((res) => {
    const row = res.toArray()[0];
    if (!row) throw new Error(`${id} is not in ${urls.map((u) => u.split("/").slice(-2).join("/")).join(", ")}`);
    return JSON.parse(row.assets);
  });
  assetCache.set(id, p);
  p.catch(() => assetCache.delete(id));
  return p;
}

const bboxOf = (r) => {
  try {
    const b = Array.from(r.bbox ?? []).map(Number);
    return b.length === 4 && b.every(Number.isFinite) ? b : null;
  } catch {
    return null;
  }
};

// "Show on map": fly to the scene's footprint and draw its visual COG.
async function showOnMap(r, button) {
  const bbox = bboxOf(r);
  if (bbox) map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, duration: 1200 });
  button.disabled = true;
  try {
    say(`Reading ${r.id}'s assets from the item part…`);
    const assets = await assetsFor(r);
    const href = assetHref(assets, "visual");
    if (!href) throw new Error("the item has no https visual asset");
    say(`Opening ${href.split("/").slice(-2).join("/")} — reading its overviews by range…`);
    const cog = await openCog(href);
    cogLayer = cogTileLayer(cog, `cog-${r.id}`);
    render();
    $("cog-id").textContent = String(r.id);
    $("cogbar").hidden = false;
    say(`${r.id} on the map: TCI overviews range-read straight from the COG, `
      + "reprojected in the browser. No tile server, no API.");
  } catch (err) {
    say(`Could not show ${r.id} — ${err.message}`, true);
  } finally {
    button.disabled = false;
  }
}

$("cog-clear").addEventListener("click", () => {
  cogLayer = null;
  render();
  $("cogbar").hidden = true;
});

// The asset links for a card, rendered once its row's `assets` is read.
async function showLinks(r, holder, button) {
  button.disabled = true;
  try {
    const assets = await assetsFor(r);
    holder.replaceChildren();
    for (const [key, label] of ASSET_LINKS) {
      const href = assetHref(assets, key);
      if (!href) continue;
      const a = el("a", null, label);
      a.href = href;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.title = `${label} — ${href}`;
      holder.append(a);
    }
    if (!holder.childElementCount) holder.append(el("span", "hint", "no https asset links"));
    button.remove();
  } catch (err) {
    holder.replaceChildren(el("span", "hint", `links unavailable — ${err.message}`));
    button.disabled = false;
  }
}

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
  const links = el("span", "links");
  const more = el("button", "mini", "Links");
  more.type = "button";
  more.title = "Read this scene's asset hrefs from the item part";
  more.addEventListener("click", () => showLinks(r, links, more));
  actions.append(show, more);
  cap.append(actions, links);
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
    lastParts = urls;
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
