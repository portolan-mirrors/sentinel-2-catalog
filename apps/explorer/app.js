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
// The year is the unit of work. The sidebar's year select picks it. A click
// on an MGRS tile reads that year's parts once through sceneRows (search.js).
// The page caches the rows. The day-range slider and the cloud/coverage/
// scene-count sliders then filter the cached rows. They redraw the result
// cards. No filter change reads the network again. The map paints on its
// own. It never queries the clicked tile-year. It paints from the stats'
// month slices for the current window. The choropleth and the scene list
// read the same window from two different sources.
// The same page shows Earth Search's Collection 1 (sentinel-2-c1-l2a, with
// stats-c1/) under ?collection=: the COLLECTIONS table below is everything
// that differs — directories, the tile column, which parts a year has, the
// two extra mask bands — and the sidebar's select reloads the page with
// the parameter set.
// There is no API, no server and no database behind this page: every
// parquet read — the scene search over the item parts, the stats timeline,
// the month slices and the per-tile history — goes through hyparquet
// (search.js), a small pure-JS reader, as HTTP range or whole-file fetches
// straight at the object store, and the COG reads work the same way. The
// map is MapLibre for the camera; the tiles are drawn by deck.gl
// interleaved into the same canvas (Task 20), because MapLibre's per-feature
// state and filter changes re-parse the 33k-polygon tile on every update.
import maplibregl from "https://esm.sh/maplibre-gl@4.7.1";
import { PMTiles, Protocol } from "https://esm.sh/pmtiles@3.2.0";
import { parse } from "https://esm.sh/@loaders.gl/core@4.5.1";
import { MVTLoader } from "https://esm.sh/@loaders.gl/mvt@4.5.1";
import { cogTileLayer, previewImage, previewLayer, openScene, sceneCog, loadOverviews,
  sceneIndexStats, bandPreviewImage, bandTileLayer } from "./cog.js";
import { BANDS, MASK_BANDS, bandInfo, bandTitle, fixedRange, INDICES, SCL_CLASSES, PRESETS,
  bandsOf, HIST_BINS } from "./bands.js";
import { dayRange, valueRange } from "./rangeslider.js";
import { sceneRows, warmPart, readTable, keyedRows } from "./search.js";
import { SORTS, viewOf, indexOfId, clampIndex, filterKeyOf } from "./results.js";
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

// The collections this page can show and what differs between them; the
// rest — the stats products, the scene query, the COG reads — is the same
// shape under another directory. `sidecars` says whether the collection
// publishes a <stem>.idx.json beside every part; false lets search.js skip
// the 404 probe that would otherwise precede every footer read.
// `parts(year, tile)` are the file stems
// that may hold a tile's scenes in that year, each HEAD-probed before it is
// read (partExists): the first collection is the one zone part of the
// year's tier plus live.parquet for the current year; Collection 1 is one
// items.parquet per year plus a live.parquet any year may carry (its
// refresh appends by `created`, so a reprocessed 2019 scene lands in
// year=2019/live.parquet). `apiTile` is how a STAC API is asked for a tile
// (apiMirror): Collection 1 items have no s2:mgrs_tile, their tile is
// grid:code "MGRS-31UET". `masks` are the extra single bands its scenes
// carry (bands.js MASK_BANDS). `since` is the first year with scenes. It
// sets the sub-header text. It also sets the year select's fallback range
// when the stats are missing.
// DEFAULT_COLLECTION is what loads without ?collection=. Collection 1 is
// the default since 2026-09-22, when its backfill and stats were published;
// ?collection=sentinel-2-l2a opens the first collection.
export const DEFAULT_COLLECTION = "sentinel-2-c1-l2a";
export const COLLECTIONS = {
  "sentinel-2-l2a": {
    label: "Sentinel-2 L2A (Earth Search)", title: "Sentinel-2 L2A", since: 2016,
    dir: "sentinel-2-l2a", statsDir: "stats", tileColumn: "s2:mgrs_tile",
    // No part of this collection has ever had a sidecar: only Collection 1's
    // items.parquet gets one (tools/rails/fold_live.sbatch). Saying so saves
    // every first search on a part the 404 probe it would otherwise pay.
    sidecars: false,
    parts: (year, tile) => {
      const archive = archivePartFor(tile, year);
      return [...(archive ? [archive] : []), ...(year === CURRENT_YEAR ? ["live"] : [])];
    },
    apiTile: (tile) => ({ "s2:mgrs_tile": { eq: tile } }),
    masks: [],
  },
  "sentinel-2-c1-l2a": {
    label: "Sentinel-2 Collection 1 (Earth Search)", title: "Sentinel-2 Collection 1 L2A",
    since: 2015,
    dir: "sentinel-2-c1-l2a", statsDir: "stats-c1", tileColumn: "_tile",
    // items.parquet publishes one; live.parquet does not, and pays the probe.
    sidecars: true,
    parts: () => ["items", "live"],
    // The parts hold every tile, so they can warm before any tile is picked.
    prefetchParts: true,
    apiTile: (tile) => ({ "grid:code": { eq: `MGRS-${tile}` } }),
    masks: Object.keys(MASK_BANDS),
  },
};
const requestedCollection = new URLSearchParams(location.search).get("collection");
export const COLLECTION_ID = COLLECTIONS[requestedCollection] ? requestedCollection : DEFAULT_COLLECTION;
const COL = COLLECTIONS[COLLECTION_ID];
// An unknown ?collection= is said in the first status sentence the page
// settles on (init() appends this to it), not only in the console.
const collectionNote = requestedCollection && !COLLECTIONS[requestedCollection]
  ? ` (?collection=${requestedCollection} is unknown, showing ${COLLECTION_ID})` : "";

// The stats collection is three parquet products cut from one table
// (tools/s2_stats.py), under stats/ or stats-c1/: the app never reads the
// full table whole.
//  - timeline.parquet: one row per month over all tiles, a few KB. Fetched
//    whole on load; it gives the month span and the global timeline.
//  - months/YYYY-MM.parquet: one month's rows with the paint columns, sorted
//    by tile, ~100-150 KB. Fetched whole when that month is shown, then
//    kept registered so a revisit is free.
//  - mgrs-monthly.parquet: the full table, ~21 MB, sorted by tile in 50k-row
//    groups. Only ever range-read over httpfs with WHERE mgrs_tile = ..., the
//    way the scene search reads the year parts: one tile's history is a
//    ~55 KB footer plus one row group's column chunks, not the file.
const STATS = `${BASE}/${COL.statsDir}/mgrs-monthly.parquet`;
const TIMELINE = `${BASE}/${COL.statsDir}/timeline.parquet`;
const monthUrl = (ym) => `${BASE}/${COL.statsDir}/months/${ym}.parquet`;
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

// ---------------------------------------------------------------------------
// The URL hash: the page state, so a view can be shared or returned to.
// The hash is `&`-separated key=value pairs — the camera (map=zoom/lat/lng),
// the year, the window (d=), the three sliders, the metric, the sort, the
// tile and the shown scene. Only a value that is not the default is written,
// so the hash of a page nobody has touched is one `map=` pair.
//
// The hash is read once, here, into WANT. A hash comes from a link somebody
// else wrote, so every value is checked before it is used and a value this
// page cannot use is dropped in silence — a shared link must never leave the
// page in a state its own controls cannot reach. The tile is held raw
// because TILE_RE is declared with the scene query, below this line.
// ?collection= stays a query parameter: the switcher rebuilds the URL from
// location.href, which carries the fragment, so the hash survives its reload.
// ---------------------------------------------------------------------------
const HASH_IN = new URLSearchParams(location.hash.replace(/^#/, ""));
const hashInt = (key, lo, hi) => {
  if (!HASH_IN.has(key)) return null;
  const n = Number(HASH_IN.get(key));
  return Number.isInteger(n) && n >= lo && n <= hi ? n : null;
};
const WANT = {
  map: (() => {
    const m = (HASH_IN.get("map") ?? "").split("/").map(Number);
    return m.length === 3 && m.every(Number.isFinite) && m[0] >= 0 && m[0] <= 24
      && Math.abs(m[1]) <= 90 && Math.abs(m[2]) <= 180
      ? { zoom: m[0], center: [m[2], m[1]] } : null;
  })(),
  year: hashInt("year", 1970, 3000),
  // "YYYY-MM-DD..YYYY-MM-DD". The two days are checked against the restored
  // year later, by restoreControls, which is the only place that knows it.
  d: /^\d{4}-\d{2}-\d{2}\.\.\d{4}-\d{2}-\d{2}$/.test(HASH_IN.get("d") ?? "")
    ? HASH_IN.get("d").split("..") : null,
  cloud: hashInt("cloud", 0, 100),
  cover: hashInt("cover", 0, 100),
  scenes: hashInt("scenes", 0, 10000),
  metric: METRICS.has(HASH_IN.get("metric")) ? HASH_IN.get("metric") : null,
  // hasOwn, not a truthiness test on SORTS[key]: "__proto__", "constructor"
  // and "toString" all find something on a plain object and would reach the
  // sort select as a key it has no option for.
  sort: Object.hasOwn(SORTS, HASH_IN.get("sort") ?? "") ? HASH_IN.get("sort") : null,
  tile: HASH_IN.get("tile"),
  scene: HASH_IN.get("scene"),
};

// The sidebar's collection switch. The select shows the loaded collection;
// a change reloads the page with ?collection= set (the other parameters
// kept), which is how every piece of per-collection state — the cached
// part metadata, the stats, timeline and month buffers, the results, a
// shown scene — starts over rather than being unpicked one by one.
{
  const sel = $("collection");
  for (const [id, c] of Object.entries(COLLECTIONS)) sel.append(new Option(c.label, id));
  sel.value = COLLECTION_ID;
  sel.addEventListener("change", () => {
    const url = new URL(location.href);
    url.searchParams.set("collection", sel.value);
    location.assign(url);
  });
  $("title").textContent = COL.title;
  $("sub").textContent = `${COL.title} scenes since ${COL.since} — every query on this page `
    + "is a range read against static GeoParquet on Source Cooperative; there is no API.";
}

// hideTipFor(el) lets code outside this block close the popover for one
// anchor (setTip calls it when a nav button's caption changes or clears).
// The popover block below replaces it with the real function at load; this
// stub only guards a call that somehow lands before that.
let hideTipFor = () => {};

// The shared info/hover popover. One #tip element serves every
// `button.info` and the image nav's prev/next caption, instead of the one
// tip per section the filter tips started out with. It is position: fixed on
// body, so it covers the map and the panels instead of pushing them open,
// and app.js only ever moves it and fills its text — the box itself never
// moves in the DOM.
// A hover shows it (skipped on touch, which has no hover); a click on a
// pinnable anchor (the .info buttons) pins it open until a click outside
// any .info button, or Escape, takes it down. imgprev/imgnext wire in the
// same way but not pinnable: their own click already steps to a scene, so
// a second, competing click meaning would be confusing.
// Two anchors matter here: `pinned`, set only by a click and cleared only
// by Escape, an outside click, or another pinnable anchor's click; and
// `activeAnchor`, whichever anchor's text is on screen right now, pinned
// or not. A hover never starts, moves, or ends the pin — while one is
// pinned, hovering a different anchor is a no-op, so the pinned box stays
// exactly as it was. Only the anchor that opened a plain hover (activeAnchor)
// may close it on its own mouseleave.
{
  const tip = $("tip");
  let pinned = null;
  let activeAnchor = null;

  // Put the box beside `anchor`: right first, left if the right edge would
  // leave the viewport, under the anchor if neither side fits. Measured
  // after the text is set and the box is shown, so its real size is known.
  function place(anchor) {
    const pad = 8;
    const r = anchor.getBoundingClientRect();
    const w = tip.offsetWidth, h = tip.offsetHeight;
    let left = r.right + pad;
    if (left + w > innerWidth - pad) left = r.left - w - pad;
    let top = r.top;
    if (left < pad) {
      left = Math.max(pad, Math.min(r.left, innerWidth - w - pad));
      top = r.bottom + pad;
    }
    top = Math.min(Math.max(top, pad), innerHeight - h - pad);
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
  }
  function show(anchor) {
    const text = anchor.dataset.tip;
    if (!text) return;
    tip.textContent = text;
    tip.hidden = false;
    activeAnchor = anchor;
    place(anchor);
  }
  function hide() { tip.hidden = true; activeAnchor = null; }
  function unpin() { pinned = null; hide(); }

  function wire(anchor, pinnable) {
    if (matchMedia("(hover: hover)").matches) {
      anchor.addEventListener("mouseenter", () => {
        // A pinned box is only ever moved by a click; a hover elsewhere
        // must not steal it or blank it out from under the pin.
        if (pinned && pinned !== anchor) return;
        show(anchor);
      });
      anchor.addEventListener("mouseleave", () => {
        if (anchor === pinned) return;        // the pin keeps it open
        if (anchor === activeAnchor) hide();  // only its own opener closes it
      });
    }
    if (pinnable) {
      anchor.addEventListener("click", (e) => {
        // The panel tightening moved the "?" button inside its label's text,
        // so a click on it now bubbles toward a <label for=…> around a range
        // input.
        // Stopped here so the label never also forwards the click to the
        // slider it wraps — this button only ever toggles the tip.
        e.stopPropagation();
        if (pinned === anchor) { unpin(); return; }
        pinned = anchor;
        show(anchor);
      });
    }
  }
  for (const btn of document.querySelectorAll("button.info[data-tip]")) wire(btn, true);
  for (const id of ["imgprev", "imgnext"]) wire($(id), false);

  document.addEventListener("keydown", (e) => { if (e.key === "Escape") unpin(); });
  document.addEventListener("click", (e) => {
    if (pinned && !e.target.closest(".info")) unpin();
  });
  // A scroll under a shown tip is simplest read as "take it down": the box
  // is position: fixed, so it would not follow the panel it came from, and
  // no filter row scrolls far enough for the anchor to need re-finding.
  // It unpins rather than merely hiding: a hide leaves `pinned` on the
  // anchor, and the next click on that "?" reads as a toggle-off and looks
  // dead — two clicks to reopen a box the scroll already closed.
  $("panel").addEventListener("scroll", unpin, { passive: true });
  $("imgpanel").addEventListener("scroll", unpin, { passive: true });

  // A disabled button gets no mouseleave, so a caption change or a step to
  // the sort order's edge (syncNavButtons, via setTip) must close its tip
  // itself rather than wait for an event that will never come.
  hideTipFor = (el) => {
    if (el === pinned) pinned = null;
    if (el === activeAnchor) hide();
  };
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

// A console handle saves reaching into the module; the deck.gl overlay and
// the per-month lookup are added below once they exist.
window.S2 = { BASE, collection: COLLECTION_ID, map };

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

// The camera a shared link asked for. jumpTo, not flyTo: the link names the
// view the reader wants, not a trip to it. The choropleth does not read the
// camera, so nothing else in the boot changes.
if (WANT.map) map.jumpTo({ center: WANT.map.center, zoom: WANT.map.zoom });

// ---------------------------------------------------------------------------
// The MGRS choropleth. The fills are drawn by deck.gl: the footprints come
// out of the same PMTiles archive as before, and the colour of each tile is
// looked up in a Map rebuilt per window from the month slices. A window
// change, a metric change or a slider drag bumps `paintKey`, and deck.gl
// recomputes one colour attribute for the 33k polygons and uploads it — no
// per-feature state, no filter change, no tile re-parse. The outlines stay
// a MapLibre line layer: nothing ever changes on it, so its tile is parsed
// once, and a deck.gl PathLayer of the same 33k outlines was measured at
// 0.5-0.9 s a frame under software GL where MapLibre's lines take a few
// ms. The fills are slotted beneath it with beforeId.
// ---------------------------------------------------------------------------
// The footprints come from the collection's own stats tileset. The grid is
// the same 33k MGRS tiles whichever collection indexed them, so while a
// collection's stats are not published yet (Collection 1 during its
// backfill) the other collection's archive stands in, and there is still
// a tile to click and search. Only a 404 of the collection's own archive
// (one HEAD, a CORS-simple request) earns the stand-in: any other failure
// — a refused read, a truncated file, a bad header — is said out loud and
// left alone, so a broken stats tileset is never masked by a working one.
let mgrsUrl = `${BASE}/${COL.statsDir}/mgrs.pmtiles`;
const mgrsFallback = Object.values(COLLECTIONS).map((c) => `${BASE}/${c.statsDir}/mgrs.pmtiles`)
  .find((url) => url !== mgrsUrl);
// One report per failure: a HEAD that fails is said here and the header
// read below is skipped, because it would fail the same way and say it
// again.
let mgrsReachable = true;
try {
  const head = await fetch(mgrsUrl, { method: "HEAD" });
  await head.arrayBuffer().catch(() => {});
  if (head.status === 404 && mgrsFallback) {
    console.warn(`${mgrsUrl} is not published (404); MGRS footprints from ${mgrsFallback} instead`);
    mgrsUrl = mgrsFallback;
  } else if (!head.ok) {
    throw new Error(`HTTP ${head.status}`);
  }
} catch (err) {
  mgrsReachable = false;
  say(`Could not open ${mgrsUrl} — ${err.message}`, true);
}
const archive = new PMTiles(mgrsUrl);
let tileZoom = { minZoom: 0, maxZoom: 0 };
if (mgrsReachable) {
  try {
    const h = await archive.getHeader();
    tileZoom = { minZoom: h.minZoom, maxZoom: h.maxZoom };
  } catch (err) {
    say(`Could not open ${mgrsUrl} — ${err.message}`, true);
  }
}
map.addSource("mgrs", { type: "vector", url: `pmtiles://${mgrsUrl}` });
map.addLayer({ id: "mgrs-line", type: "line", source: "mgrs",
  "source-layer": "mgrs",
  // Subtle on purpose: the choropleth is the picture and the grid only
  // separates the cells, so it sits at a quarter opacity.
  paint: { "line-color": "#8899bb", "line-width": 0.4, "line-opacity": 0.25 } });
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

// Per-tile stats for the shown window: mgrs_tile -> {v: 0..100 on the ramp,
// cc, cover, sc}. `v` is already rescaled per metric.
let lookup = new Map();
let paintKey = 0;
let hovered = null;          // the hovered feature (GeoJSON, WGS84) or null
let cogLayer = null;         // the shown scene's TileLayer, or null
let cogPreview = null;       // its thumbnail warp, beneath the tiles until they load
let scrubLayer = null;       // the scrub bar's live thumbnail preview, or null

// The one state object. Every mutator writes here and calls scheduleApply;
// every renderer reads from here. Nothing else holds filter or search state.
const S = {
  year: null,                 // int, the #year select
  maxCloud: Number($("maxcloud").value),
  minCoverage: Number($("mincoverage").value),
  minScenes: Number($("minscenes").value),
  from: null, to: null,       // ISO days, the date slider's window
  monthLock: null,            // "YYYY-MM" while a bar click clamps the slider
  tile: null,                 // the selected MGRS tile or null
  search: null,               // {tile, year, rows, at} or null
  sort: "cloud",
  shown: 15,                  // cards rendered
  displayedId: null,          // the scene on the map — an id, never an index
  detachedAt: 0,              // its last known position in the view
};

// ---------------------------------------------------------------------------
// The other half of the hash: writing it. One serializer over `map` and `S`,
// called on a 400 ms throttle from the end of applyNow (every state mutator
// funnels through it) and from the map's moveend. A throttle, not a debounce:
// the first call arms the timer and later ones inside the window ride it, so
// a long drag keeps writing every 400 ms instead of waiting for its end. The
// one history entry is replaced, never added to: a slider drag must keep the
// URL current without filling the Back button with a step per frame.
//
// No write happens before finishRestore(): a boot writes the defaults
// through applyNow several times, and those writes would erase an incoming
// hash before it was read.
// ---------------------------------------------------------------------------
let hashRestored = false;
let hashWritten = null;       // the last hash this page wrote (see hashchange)
let hashTimer = 0;

// URLSearchParams.toString() percent-encodes the slashes of `map=`, so the
// pairs are joined by hand. Only the two values that come from data — the
// tile and the scene id — are escaped; the rest are digits and names.
function hashOfState() {
  const parts = [];
  const put = (k, v) => parts.push(`${k}=${v}`);
  const c = map.getCenter();
  put("map", `${map.getZoom().toFixed(2)}/${c.lat.toFixed(4)}/${c.lng.toFixed(4)}`);
  if (S.year !== null) put("year", S.year);
  if (S.from && S.to && !(S.from === `${S.year}-01-01` && S.to === `${S.year}-12-31`)) {
    put("d", `${S.from}..${S.to}`);
  }
  if (S.maxCloud !== 100) put("cloud", S.maxCloud);
  if (S.minCoverage !== 10) put("cover", S.minCoverage);
  if (S.minScenes !== 0) put("scenes", S.minScenes);
  if ($("metric").value !== "min_cloud_cover") put("metric", $("metric").value);
  if (S.sort !== "cloud") put("sort", S.sort);
  if (S.tile) put("tile", encodeURIComponent(S.tile));
  if (S.displayedId) put("scene", encodeURIComponent(S.displayedId));
  return `#${parts.join("&")}`;
}
function writeHash() {
  clearTimeout(hashTimer);
  hashTimer = 0;
  if (!hashRestored) return;
  const h = hashOfState();
  if (h === location.hash) return;
  hashWritten = h;
  // A page served where the history API is refused keeps the URL it has;
  // that is a URL that cannot be shared, not a page that cannot be used.
  try { history.replaceState(null, "", h); } catch { /* left as it stands */ }
}
function scheduleHashWrite() {
  if (!hashRestored || hashTimer) return;
  hashTimer = setTimeout(writeHash, 400);
}
map.on("moveend", scheduleHashWrite);

// A hash the page did not write is one somebody pasted or edited. Re-running
// the restore over a live UI has too many possible orders to be safe, so the
// page loads again and restores the new hash the one order that is certainly
// right. A replaced history entry fires no hashchange, so writeHash cannot
// land here; the compare only guards a browser that reports one anyway.
addEventListener("hashchange", () => {
  if (location.hash === hashWritten) return;
  location.reload();
});

// All three sliders AND together in one accessor; a NULL metric is "unknown"
// rather than "over/under the slider" and is never dimmed for that reason
// (the Task 20 rule, extended to coverage and scene count).
function fillColor(f) {
  const s = lookup.get(f.properties.mgrs_tile);
  if (!s) return UNPAINTED;
  if (s.cc !== null && s.cc > S.maxCloud) return DIMMED;
  if (s.cover !== null && s.cover < S.minCoverage) return DIMMED;
  if (s.sc !== null && s.sc < S.minScenes) return DIMMED;
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
  selectedTile: { get: () => S.tile },
  S: { value: S },
  // The ids the filters admit, in view order. Only the first 15 cards render,
  // so the headless gate reads the view here instead of off the DOM.
  viewIds: { value: () => currentView().map((r) => r.id) },
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
    scrubLayer,
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

// One coalesced apply per animation frame: a slider drag asks for a paint
// and a card render, and both run once, in order, on the next frame.
let applyFlags = null;
function scheduleApply(flags = {}) {
  const first = !applyFlags;
  applyFlags = { ...(applyFlags ?? {}), ...flags };
  if (first) requestAnimationFrame(applyNow);
}
function applyNow() {
  const f = applyFlags ?? {};
  applyFlags = null;
  if (f.paint) paintWindow();
  if (f.cards) renderResults();
  if (f.nav) {
    renderScrubber();
    syncNavButtons();
  }
  updateFilterStatus();
  // Every state mutator ends here, so this one call keeps the URL current.
  scheduleHashWrite();
}

// ---------------------------------------------------------------------------
// Stats: the choropleth and the timeline.
// ---------------------------------------------------------------------------

// A small remote parquet fetched whole and decoded in the page with
// hyparquet (search.js readTable). Resolves null on a 404, which for a
// month slice means "no tile-months for that month" (the builder writes a
// slice only for months the table has), not a broken bucket.
async function fetchParquet(url) {
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.arrayBuffer();
}

// The timeline file's bytes, kept for every later read. False when the
// collection has no stats in the bucket yet (Collection 1 until
// publish-stats first runs for it): init() then keeps the page in its
// no-stats state below rather than treating the 404 as a broken bucket.
let timelineBuf = null;
const loadTimeline = async () => {
  timelineBuf = await fetchParquet(TIMELINE);
  return timelineBuf !== null;
};

// Set by init() when the collection's stats are not published: the map
// stays unpainted, the timeline empty, and the month and tile-history reads
// that would only 404 are not attempted. The scene search is untouched —
// the year parts are their own files. `statsNote` is the status line that
// says so, repeated whenever a window change would otherwise paint.
let statsMissing = false;
let statsNote = "";

// Month slices fetched whole, ym -> the file's bytes, or null for a month
// the bucket has no slice for. The value is the in-flight promise, so two
// callers for the same month share one fetch and a revisit costs nothing. A
// failed fetch is forgotten so the next attempt retries rather than
// replaying the error.
// Capped like the year cache, and for the same reason: a long session
// scrubbing the timeline would otherwise hold every month it ever painted.
// Three years of slices stay warm, which covers any window the user moves
// back and forth over; the oldest insert goes first.
const MONTH_CACHE_MAX = 36;
const monthFiles = new Map();
function monthFile(ym) {
  if (!monthFiles.has(ym)) {
    const p = fetchParquet(monthUrl(ym))
      .catch((err) => { monthFiles.delete(ym); throw err; });
    monthFiles.set(ym, p);
    if (monthFiles.size > MONTH_CACHE_MAX) monthFiles.delete(monthFiles.keys().next().value);
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

// One month slice, decoded once with every paint column, cached. A 404
// (no slice for that month) caches as null; a failed fetch is forgotten
// so the next paint retries.
const monthStats = new Map();
function monthStatsFor(ym) {
  if (!monthStats.has(ym)) {
    monthStats.set(ym, (async () => {
      const buf = await monthFile(ym);
      if (!buf) return null;
      const rows = await readTable(buf, ["mgrs_tile", "min_cloud_cover",
        "scene_count", "max_cover", "median_cloud_cover"]);
      return rows.map((r) => ({ tile: r.mgrs_tile,
        cc: r.min_cloud_cover == null ? null : Number(r.min_cloud_cover),
        sc: r.scene_count == null ? null : Number(r.scene_count),
        cover: r.max_cover == null ? null : Number(r.max_cover),
        med: r.median_cloud_cover == null ? null : Number(r.median_cloud_cover) }));
    })());
    monthStats.get(ym).catch(() => monthStats.delete(ym));
    if (monthStats.size > MONTH_CACHE_MAX) monthStats.delete(monthStats.keys().next().value);
  }
  return monthStats.get(ym);
}

// The months the window [from, to] overlaps, as "YYYY-MM". A month partly
// inside counts wholly: the map quantises to months, the cards do not.
function monthsIn(from, to) {
  const out = [];
  let y = Number(from.slice(0, 4)), m = Number(from.slice(5, 7));
  const end = Number(to.slice(0, 4)) * 100 + Number(to.slice(5, 7));
  while (y * 100 + m <= end) {
    out.push(`${y}-${String(m).padStart(2, "0")}`);
    if (m === 12) { y += 1; m = 1; } else m += 1;
  }
  return out;
}

// Aggregate the window's months per tile: the clearest scene's cloud is a
// min, coverage a max, scene count a sum, and the median-cloud metric the
// best month's median (an approximation; a true median needs the raw
// scenes). Memoised on the month set and the metric, so a drag inside one
// month set costs nothing here. A month that failed to read marks its key
// with "!", so the memo misses once the next paint reads that month, and the
// recovered month paints.
let aggKey = "";
let aggLookup = new Map();
function aggregateMonths(months, metric) {
  const key = months.map((m) => (m.rows ? m.ym : `${m.ym}!`)).join(",") + "|" + metric;
  if (key === aggKey) return aggLookup;
  const acc = new Map();
  for (const m of months) {
    if (!m.rows) continue;
    for (const r of m.rows) {
      const cur = acc.get(r.tile);
      if (!cur) { acc.set(r.tile, { cc: r.cc, sc: r.sc, cover: r.cover, med: r.med }); continue; }
      if (r.cc !== null) cur.cc = cur.cc === null ? r.cc : Math.min(cur.cc, r.cc);
      if (r.sc !== null) cur.sc = (cur.sc ?? 0) + r.sc;
      if (r.cover !== null) cur.cover = cur.cover === null ? r.cover : Math.max(cur.cover, r.cover);
      if (r.med !== null) cur.med = cur.med === null ? r.med : Math.min(cur.med, r.med);
    }
  }
  const next = new Map();
  for (const [tile, s] of acc) {
    const raw = metric === "scene_count" ? s.sc
      : metric === "max_cover" ? s.cover
      : metric === "median_cloud_cover" ? s.med
      : s.cc;
    if (raw == null) continue;
    next.set(tile, { v: onRamp(metric, raw), cc: s.cc, cover: s.cover, sc: s.sc });
  }
  aggKey = key;
  aggLookup = next;
  return next;
}

// The three filter sliders, combined into one sentence for the status line
// (Task 22): "N of M tiles pass (cloud ≤ x, coverage ≥ y, scenes ≥ z)".
function fmtFilters() {
  return `cloud ≤ ${S.maxCloud}, coverage ≥ ${S.minCoverage}, scenes ≥ ${S.minScenes}`;
}
function passCounts() {
  let n = 0;
  for (const s of lookup.values()) {
    if (s.cc !== null && s.cc > S.maxCloud) continue;
    if (s.cover !== null && s.cover < S.minCoverage) continue;
    if (s.sc !== null && s.sc < S.minScenes) continue;
    n++;
  }
  return { n, m: lookup.size };
}
function filterLine() {
  const { n, m } = passCounts();
  return `${n.toLocaleString()} of ${m.toLocaleString()} tiles pass (${fmtFilters()})`;
}

// The scene-count slider's bounds track the shown window: 0..p99 of the
// window's aggregated scene_count, integer step, recomputed on every window
// change (a busy window and a quiet one must not share one scale).
function updateScenesBound(rows) {
  const values = rows.map((r) => Number(r.sc)).filter(Number.isFinite).sort((a, b) => a - b);
  const p99 = values.length
    ? values[Math.min(values.length - 1, Math.ceil(0.99 * values.length) - 1)]
    : 0;
  const bound = Math.max(1, Math.round(p99));
  const slider = $("minscenes");
  slider.max = bound;
  // A range input re-clamps its own value the moment its max is assigned, so
  // the value here is already inside the new bound. S takes what the input
  // now holds — the input is the only source of this number, and a clamp
  // that S did not hear about would dim the map by a figure no control shows.
  S.minScenes = Number(slider.value);
  $("minscenes-out").textContent = slider.value;
}

// A month slice is a network fetch, so two quick window changes can resolve
// out of order; only the latest call may touch the map or the status line.
let paintSeq = 0;

async function paintWindow() {
  if (!S.from || !S.to) return;
  if (statsMissing) { say(statsNote); return; }
  const metric = $("metric").value;
  if (!METRICS.has(metric)) throw new Error(`unknown metric ${metric}`);
  const seq = ++paintSeq;
  const yms = monthsIn(S.from, S.to);
  const settled = await Promise.allSettled(yms.map(monthStatsFor));
  if (seq !== paintSeq) return;
  // Only the paint that lands consumes the note. A paint that loses the race
  // above must leave it for the winner, or the load-time lag note is lost.
  const note = lagNote;
  lagNote = "";
  const months = yms.map((ym, i) => ({ ym,
    rows: settled[i].status === "fulfilled" ? settled[i].value : null }));
  lookup = aggregateMonths(months, metric);
  updateScenesBound([...lookup.values()]);
  repaint();
  markActiveBars();
  const have = months.filter((m) => m.rows).length;
  // A slice that failed to read leaves its month unpainted, which is by
  // design, but it must not be silent: the next paint retries the month.
  const failed = settled.filter((s) => s.status === "rejected").length;
  const trouble = failed
    ? ` ${failed} month slice${failed === 1 ? "" : "s"} failed to read — `
      + "drag a handle to retry."
    : "";
  if (!lookup.size) {
    say(`No tile-months in the published stats for ${S.from} → ${S.to}. `
      + "Pick a window with bars in the timeline below."
      + trouble + (note ? ` ${note}` : ""));
    return;
  }
  say(`${lookup.size.toLocaleString()} MGRS tiles imaged in ${S.from} → ${S.to} — `
    + `${have} month slice${have === 1 ? "" : "s"}, no API call. ${filterLine()}.`
    + trouble + (note ? ` ${note}` : ""));
}

// All tiles: the timeline file, already in memory, one row per month. One
// tile: keyedRows (search.js) range-reads mgrs-monthly.parquet — footer
// once per session, then only the row groups whose mgrs_tile range covers
// the tile (the table is tile-sorted), aggregated here per month. That is
// still a network read, so like paintWindow() only the latest call may
// touch the bars or the status line: click tile A then B and A's answer,
// landing last, must not replace B's.
let timelineSeq = 0;

export async function timelineFor(tile) {
  const bars = $("bars");
  const seq = ++timelineSeq;
  if (statsMissing) {
    const scope = tile ? `Tile ${tile}` : "All tiles";
    $("timeline-scope").textContent = `${scope} — no stats published yet for ${COLLECTION_ID}.`;
    bars.replaceChildren(el("p", "hint",
      `No stats published yet for ${COLLECTION_ID} (${COL.statsDir}/timeline.parquet is not in the bucket).`));
    return;
  }
  let rows;
  try {
    if (tile) say(`Reading tile ${tile}'s history…`);
    if (tile) {
      // No stats product publishes a sidecar (only a year's items.parquet
      // gets one, in fold_live.sbatch), so telling search.js so saves the
      // 404 probe this read would otherwise pay before its footer.
      const raw = await keyedRows({ url: STATS, keyColumn: "mgrs_tile",
        key: tile, columns: ["year", "month", "scene_count", "min_cloud_cover"],
        sidecars: false });
      const byMonth = new Map();
      for (const r of raw) {
        const k = Number(r.year) * 100 + Number(r.month);
        const cur = byMonth.get(k) ?? { year: Number(r.year), month: Number(r.month),
          n: 0, clearest: Infinity };
        cur.n += Number(r.scene_count);
        cur.clearest = Math.min(cur.clearest, Number(r.min_cloud_cover));
        byMonth.set(k, cur);
      }
      rows = [...byMonth.keys()].sort((a, b) => a - b).map((k) => byMonth.get(k));
    } else {
      rows = (await readTable(timelineBuf,
        ["year", "month", "scene_count", "min_cloud_cover"]))
        .map((r) => ({ year: Number(r.year), month: Number(r.month),
          n: Number(r.scene_count), clearest: Number(r.min_cloud_cover) }))
        .sort((a, b) => a.year - b.year || a.month - b.month);
    }
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
    + "Click a bar to search that month.";
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
    d.onclick = () => onBarClick(ym);
    bars.append(d);
  }
  markActiveBars();
}

function el(tag, cls, text) {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text) n.textContent = text;
  return n;
}

function markActiveBars() {
  const lo = (S.from ?? "").slice(0, 7), hi = (S.to ?? "").slice(0, 7);
  for (const b of $("bars").querySelectorAll(".bar")) {
    b.classList.toggle("on", b.dataset.ym >= lo && b.dataset.ym <= hi);
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
  const rows = await readTable(timelineBuf, ["year", "month"]);
  if (!rows.length) return null;
  const yms = rows.map((r) => Number(r.year) * 100 + Number(r.month));
  const fmt = (n) => `${Math.floor(n / 100)}-${String(n % 100).padStart(2, "0")}`;
  return { newest: fmt(Math.max(...yms)), oldest: fmt(Math.min(...yms)) };
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
    if (cc === S.maxCloud && cov === S.minCoverage && sc === S.minScenes) return;
    S.maxCloud = cc; S.minCoverage = cov; S.minScenes = sc;
    repaint();
    scheduleApply({ cards: true, nav: true });
  });
}

// The filter state a card or a scene read needs, in one object: the two
// slider gates and the window as epoch milliseconds, ends included.
function currentFilters() {
  return { maxCloud: S.maxCloud, minCoverage: S.minCoverage,
    t0: Date.parse(`${S.from}T00:00:00Z`),
    t1: Date.parse(`${S.to}T23:59:59.999Z`) };
}

let dateRange = null;

// Set once in init() when the stats file lags the newest published item
// year, and appended to the very next paintWindow() status line so the lag
// is said once on load, not repeated on every later window change.
let lagNote = "";

function lastDayOfMonth(ym) {
  const [yy, mm] = ym.split("-").map(Number);
  return new Date(Date.UTC(yy, mm, 0)).toISOString().slice(0, 10);
}

// Create the two-handle day slider or move its bounds. onChange fires on
// every handle drag step and calendar edit, and drives the map paint and
// the card filter through one apply.
// dayRange()'s own construction calls fromDates() once, but that no-ops on
// a fresh page load: the <input type=date> fields start empty, and
// fromDates() refuses to compute from an empty value. set() writes the
// values directly, the same way rebound() does on every later call.
function buildDateSlider(from0, to0) {
  if (!dateRange) {
    dateRange = dayRange({ container: $("dayrange"), from: $("date0"), to: $("date1"),
      min: from0, max: to0,
      onChange: (d0, d1) => {
        S.from = d0;
        S.to = d1;
        scheduleApply({ paint: true, cards: true, nav: true });
      } });
    dateRange.set(from0, to0);
  } else {
    dateRange.rebound(from0, to0);
  }
  S.from = from0;
  S.to = to0;
  warmWindowParts();
}
function setWindow(from0, to0) {
  buildDateSlider(from0, to0);
  scheduleApply({ paint: true, cards: true, nav: true });
}

// Switch the whole page to a different year: clear any bar-click month lock,
// rebound the date slider to Jan 1 - Dec 31, and repaint. A tile with an
// active search is searched again, in the new year.
function setYear(year) {
  S.year = year;
  S.monthLock = null;
  $("datelock").hidden = true;
  $("year").value = String(year);
  setWindow(`${year}-01-01`, `${year}-12-31`);
  if (S.tile && S.search) startSearch(S.tile, year);
}

// A month bar clamps the slider to that month and searches it. Three
// exits: the chip's ✕, a second click on the active bar, a year change.
function lockToMonth(ym) {
  S.monthLock = ym;
  $("datelock").hidden = false;
  $("datelock-label").textContent = ym;
  setWindow(`${ym}-01`, lastDayOfMonth(ym));
  if (S.tile) startSearch(S.tile, Number(ym.slice(0, 4)));
}
function unlockMonth() {
  S.monthLock = null;
  $("datelock").hidden = true;
  setWindow(`${S.year}-01-01`, `${S.year}-12-31`);
}
$("datereset").addEventListener("click", unlockMonth);

// English month names for the widen-button label. No other code in this
// file names a month (bars and titles print "YYYY-MM" throughout), and
// Intl.DateTimeFormat's month name follows the browser locale, so a fixed
// table keeps the label the same string in every browser.
const MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"];
function monthYearLabel(isoDay) {
  const [y, m] = isoDay.slice(0, 7).split("-").map(Number);
  return `${MONTH_NAMES[m - 1]} ${y}`;
}

// The window one month wider than S.from/S.to, or null when it already
// spans the whole year. The start gives way first, one month at a time,
// down to the year's first day; only once it is pinned there does the end
// start moving. Both ends stay inside S.year — lastDayOfMonth resolves a
// "YYYY-MM" string to that month's own last day.
function widerWindow() {
  const first = `${S.year}-01-01`, last = `${S.year}-12-31`;
  if (S.from > first) {
    const m = Number(S.from.slice(5, 7));
    const from = m === 1 ? first : `${S.year}-${String(m - 1).padStart(2, "0")}-01`;
    return { from, to: S.to };
  }
  if (S.to < last) {
    const m = Number(S.to.slice(5, 7));
    const to = m === 12 ? last : lastDayOfMonth(`${S.year}-${String(m + 1).padStart(2, "0")}`);
    return { from: S.from, to };
  }
  return null;
}

// The footer's "widen" button shell, appended to whatever list calls it —
// the full card list once every match is already shown, or the empty
// view's hint. One builder for both so the label math lives in one place;
// each call site wires its own click listener to widenWindow.
function widenButton() {
  if (!S.search) return null;
  const next = widerWindow();
  if (!next) return null;
  const label = next.from !== S.from ? monthYearLabel(next.from) : monthYearLabel(next.to);
  const b = el("button", "mini", `Show more — widen to ${label}`);
  b.id = "more";
  b.type = "button";
  return b;
}

// Grow the window by one month and sync the date slider to it. The year's
// rows are already in memory (readTable/keyedRows cache them), so this
// re-filters and repaints from cache — no new read. A month lock is dropped
// first: the widened window is no longer one calendar month.
function widenWindow() {
  const next = widerWindow();
  if (!next) return;
  S.monthLock = null;
  $("datelock").hidden = true;
  setWindow(next.from, next.to);
}

function onBarClick(ym) {
  if (S.monthLock === ym) { unlockMonth(); return; }
  const year = Number(ym.slice(0, 4));
  if (year !== S.year) {
    S.year = year;
    $("year").value = String(year);
  }
  lockToMonth(ym);
}

// Warm the window's parts the moment the window is known, for the
// collections whose parts hold every tile (prefetchParts). The metadata —
// sidecar or footer — then sits in the session cache while the user is
// still looking at the map, and the first tile click skips the 5-10 s it
// used to pay for it.
function warmWindowParts() {
  if (!COL.prefetchParts) return;
  const d0 = $("date0").value;
  const d1 = $("date1").value;
  if (!d0 || !d1) return;
  for (let y = Number(d0.slice(0, 4)); y <= Number(d1.slice(0, 4)); y++) {
    for (const url of partUrlsFor(y, "")) {
      partExists(url).then((ok) => { if (ok) warmPart(url, COL.sidecars !== false); });
    }
  }
}

// True once restoreControls has run. init()'s early returns (an unreadable
// timeline, a timeline with no rows) reach finishRestore without it, and a
// link the boot could not apply must stay in the URL rather than be written
// back as the defaults it never got to replace.
let restoredControls = false;

// The first half of the restore: the year, the window, the sliders, the
// metric and the sort. Both boot paths call it once, after the year select
// holds its options (a year outside them is not restorable) and before the
// first paintWindow, which reads the window and the metric. With no hash it
// does exactly what the two lines and the buildDateSlider call it replaced
// did.
//
// A day is only taken if it is in the restored year and it is a day that
// exists: the shape check on `d=` admits 2023-99-99 and 2023-02-31, and an
// <input type="date"> answers an impossible day with an empty value, which
// would leave the page with no window at all.
function restoreControls(defaultYear) {
  restoredControls = true;
  const goodDay = (s, year) => {
    const t = Date.parse(`${s}T00:00:00Z`);
    return s.slice(0, 4) === String(year) && !Number.isNaN(t)
      && new Date(t).toISOString().slice(0, 10) === s;
  };
  const year = WANT.year !== null
    && [...$("year").options].some((o) => o.value === String(WANT.year))
    ? WANT.year : defaultYear;
  S.year = year;
  $("year").value = String(year);
  if (WANT.metric) { $("metric").value = WANT.metric; updateLegend(); }
  if (WANT.sort) { S.sort = WANT.sort; $("sort").value = WANT.sort; }
  // The scene-count slider's bound follows the window (updateScenesBound),
  // and it starts at 1: raise it far enough to hold the asked value, or the
  // input would clamp it away here. The first paint then re-bounds the slider
  // to the window's own p99, and takes S from the re-clamped input, so a link
  // that asks for more scenes than the window has settles on the bound. Each
  // value below is read back from the input, never from the hash, so a clamp
  // reaches S here as well.
  if (WANT.scenes !== null) {
    $("minscenes").max = String(Math.max(Number($("minscenes").max), WANT.scenes));
    $("minscenes").value = String(WANT.scenes);
  }
  if (WANT.cloud !== null) $("maxcloud").value = String(WANT.cloud);
  if (WANT.cover !== null) $("mincoverage").value = String(WANT.cover);
  S.maxCloud = Number($("maxcloud").value);
  S.minCoverage = Number($("mincoverage").value);
  S.minScenes = Number($("minscenes").value);
  $("maxcloud-out").textContent = $("maxcloud").value;
  $("mincoverage-out").textContent = $("mincoverage").value;
  $("minscenes-out").textContent = $("minscenes").value;
  buildDateSlider(`${year}-01-01`, `${year}-12-31`);
  // A window inside the restored year only. set() clamps to the slider's
  // bounds and writes the clamped days back, and its onChange is what puts
  // them in S; the two reads below just say so out loud.
  const [d0, d1] = WANT.d ?? [];
  if (d0 && goodDay(d0, year) && goodDay(d1, year) && d0 <= d1) {
    dateRange.set(d0, d1);
    S.from = $("date0").value;
    S.to = $("date1").value;
  }
}

// The second half: the tile search and the shown scene, which both need the
// network. One shot, and the user has the last word. Any search that already
// ran is a tile the user clicked while the stats were loading, and it stands;
// a click during the await below takes a higher searchSeq, and the scene
// restore then stands down. Nothing re-applies WANT after this returns.
async function restoreSearch() {
  if (searchSeq > 0 || !WANT.tile || !TILE_RE.test(WANT.tile)) return;
  const seq = searchSeq + 1;            // the number selectTile's search takes
  // A link that named a camera keeps it: neither the auto-show of the best
  // result nor the scene restore below may fly away from the `map=` the boot
  // applied. A link without one still gets the framing it always did.
  await selectTile(WANT.tile, { flyFirst: !WANT.map });
  if (seq !== searchSeq || !S.search || !WANT.scene) return;
  // The id is matched inside the rows, so the view lookup can use the row's
  // own id and no id type has to be assumed.
  const row = S.search.rows.find((r) => String(r.id) === WANT.scene);
  if (!row) return;                     // not in this tile-year: say nothing
  const at = indexOfId(currentView(), row.id);
  if (at >= 0) { showIndex(at, !WANT.map); return; }
  // The scene is in the tile-year but the filters hide it. The link asked
  // for it, so it goes on the map anyway, and the scrub bar reports it as
  // detached the same way a filter change that hides the shown scene does.
  S.displayedId = row.id;
  showOnMap(row, null, ui.preset);
  scheduleApply({ cards: true, nav: true });
}

// The restore is over, whichever way the boot went: from here the URL
// follows the page. A link that came in is written back once, so the URL
// holds what the page actually settled on and not what was asked for — but
// only when restoreControls ran. A link the boot could not apply stays in the
// URL: the early-return paths never read it, so writing the page's state back
// would replace a shared link with the bare `#map=…` of a page that failed.
function finishRestore() {
  hashRestored = true;
  if (location.hash && restoredControls) writeHash();
}

async function init() {
  updateLegend();
  $("metric").addEventListener("change", () => {
    updateLegend();
    paintWindow();
    scheduleHashWrite();      // metric= is in the hash, so it must reach it
  });
  $("year").addEventListener("change", () => setYear(Number($("year").value)));
  $("maxcloud").addEventListener("input", onSlider);
  $("mincoverage").addEventListener("input", onSlider);
  $("minscenes").addEventListener("input", onSlider);
  // Populate and wire the result sort control. Reset the shown card count on
  // every sort change to restart pagination at card 1.
  for (const [key, s] of Object.entries(SORTS)) $("sort").append(new Option(s.label, key));
  $("sort").value = S.sort;
  function setSort(key) {
    S.sort = key;
    S.shown = 15;
    scheduleApply({ cards: true, nav: true });
  }
  $("sort").addEventListener("change", () => setSort($("sort").value));
  say("Reading the stats timeline…");
  let span, found;
  try {
    found = await loadTimeline();
    span = found ? await newestMonth() : null;
  } catch (err) {
    say(`Could not open ${TIMELINE}. The file is unreadable or the bucket `
      + `refused the read (${err.message}). Until publish-stats publishes `
      + `it, serve a local publish tree and load ?base=http://localhost:8081`
      + collectionNote, true);
    return;
  }
  if (!found) {
    await initWithoutStats();
    return;
  }
  if (!span) {
    say("The stats timeline has no rows yet — the backfill has not published "
      + "any tile-months. The map and timeline will fill in once it does."
      + collectionNote);
    await timelineFor(null);
    return;
  }
  // The stats can lag the item parts (they are rebuilt on their own
  // schedule, one cached HEAD per year past the stats to find how far
  // publishing has gone, the same probe the search uses). Never default to
  // a year the stats haven't reached yet, which would paint an all-grey map
  // with nothing to click; the select itself still lists years through the
  // newest published item year so the user can browse ahead on purpose.
  const statsYear = Number(span.newest.slice(0, 4));
  const newestYear = await newestPublishedYear(statsYear);
  if (newestYear > statsYear) {
    lagNote = `Stats reach ${span.newest}; scenes are published through ${newestYear} `
      + "— the choropleth updates when the stats rebuild lands.";
  }
  // Said with the lag note, once, on the first painted year.
  lagNote = (lagNote + collectionNote).trim();
  const y0 = Number(span.oldest.slice(0, 4));
  const y1 = Math.max(newestYear, statsYear);
  for (let y = y0; y <= y1; y++) $("year").append(new Option(String(y), String(y)));
  // Open on the newest month the stats have, expanded to its whole year:
  // the year is the unit now, and the newest year is partly empty ahead of
  // the backfill, which paintWindow tolerates month by month. A hash that
  // names another year, another window or other filters is seeded here
  // instead, before the paint that reads them.
  restoreControls(statsYear);
  await Promise.all([paintWindow(), timelineFor(null)]);
  // paintWindow() already calls markActiveBars() (the same call the timeline
  // bar's own onclick makes), but it can run before timelineFor() has
  // appended the bar buttons; timelineFor() also calls it once its bars
  // exist, so this just guarantees the default window's bars end up
  // highlighted regardless of which promise settles first.
  markActiveBars();
  await restoreSearch();
}

// The page without stats (a 404 on the collection's timeline): the year
// select opens on the current year over the collection's whole span so
// the search window can be set, the timeline says why it is empty, and the
// status line says what is and is not published. The year parts are
// probed from the current year downward and the walk stops at the first
// published one: with no stats there is no year to start an upward walk
// from, and a backfill fills years in its own order (newest first, or with
// gaps while it runs), which an upward walk from the first mission year
// would stop short of. The cost is bounded by the mission's span — one
// HEAD per part per year, twelve years at most — and only paid on a page
// without stats.
async function initWithoutStats() {
  statsMissing = true;
  for (let y = COL.since; y <= CURRENT_YEAR; y++) $("year").append(new Option(String(y), String(y)));
  // A hash can still name a year in COL.since..CURRENT_YEAR, a window and
  // the filters here; there is only no paint for them to change.
  restoreControls(CURRENT_YEAR);
  await timelineFor(null);
  let newestYear = null;
  for (let y = CURRENT_YEAR; y >= COL.since && newestYear === null; y--) {
    if ((await Promise.all(partUrlsFor(y, "1CDK").map(partExists))).some(Boolean)) newestYear = y;
  }
  statsNote = `No stats published yet for ${COLLECTION_ID} (${COL.statsDir}/timeline.parquet `
    + "is not in the bucket): the map stays unpainted and the timeline empty until "
    + "publish-stats runs for it. "
    + (newestYear !== null
      ? `Scenes are published through ${newestYear} — click a tile and search.`
      : "No year parts are published yet either; a search will say so.")
    + collectionNote;
  say(statsNote);
  // A hash's tile still searches: startSearch says it itself when the year
  // has no published parts.
  await restoreSearch();
}

// init() runs at the end of the module: it probes the item years with the
// scene query's constants and helpers, which are defined below.

// ---------------------------------------------------------------------------
// The scene query. A tile click is the whole search — there is no Search
// button. hyparquet (search.js) range-reads the clicked year's parts
// directly: the footer once per part per session, then the admitted row
// groups' search columns in parallel. The read is per tile-year and the rows
// stay in the page, so the sliders and the date window filter them with no
// further network read. Every COG the page draws or links sits in the scene
// directory that `thumbnail_url` names (see sceneDirOf), so no row ever
// needs the parts' ~18 KB-a-row `assets` column.
// ---------------------------------------------------------------------------

// An MGRS tile id: 1-2 digit UTM zone, latitude band C..X, then two letters.
// The values come from the tileset, not from a text box, but they reach the
// stats SQL string (timelineFor), so they are checked anyway.
const TILE_RE = /^\d{1,2}[C-X][A-Z]{2}$/;
const CURRENT_YEAR = new Date().getUTCFullYear();

// A whole tile-year of scene rows, cached as the in-flight promise so two
// clicks share one fetch and a revisit is instant. A failure is forgotten
// so the next click retries. The cap only bounds a long session; a
// tile-year is tens of KB decoded.
const YEAR_CACHE_MAX = 8;
const yearCache = new Map();
function yearRows(tile, year) {
  const key = `${COLLECTION_ID}|${tile}|${year}`;
  if (!yearCache.has(key)) {
    const p = (async () => {
      const urls = await partUrls(year, year, tile);
      if (!urls.length) return { rows: [], plan: "", urls };
      const got = await sceneRows({ urls, tileColumn: COL.tileColumn, tile,
        sidecars: COL.sidecars !== false });
      return { ...got, urls };
    })();
    p.catch(() => yearCache.delete(key));
    yearCache.set(key, p);
    if (yearCache.size > YEAR_CACHE_MAX) yearCache.delete(yearCache.keys().next().value);
  }
  return yearCache.get(key);
}

// The filtered, sorted view of the active search, memoised on every input.
let viewKey = "";
let viewRows = [];
function currentView() {
  if (!S.search) return [];
  const f = currentFilters();
  const key = filterKeyOf(f, S.sort, S.search);
  if (key !== viewKey) {
    viewRows = viewOf(S.search.rows, f, S.sort);
    viewKey = key;
  }
  return viewRows;
}

function updateFilterStatus() {
  if (!S.search) { say(`${filterLine()}.`); return; }
  // The mirror shows the request the page did not make, so it tracks the
  // filters the user sees, not the ones the last read ran under. A slider
  // drag rewrites it with the rest of the apply.
  $("api").textContent = apiMirror(S.search.tile, S.from, S.to, S.maxCloud, S.minCoverage);
  const view = currentView();
  say(`${view.length} of ${S.search.rows.length} ${S.search.tile} scenes in `
    + `${S.search.year} pass (${S.from} → ${S.to}, cloud ≤ ${S.maxCloud}, `
    + `coverage ≥ ${S.minCoverage}) — one year of parts range-read once, `
    + `filters run in the page.`);
}

map.on("click", (e) => {
  const tile = hitIndex.at(e.lngLat.wrap().lng, e.lngLat.lat)?.tile;
  if (!TILE_RE.test(tile ?? "")) return;
  selectTile(tile);
});

// The click is the search. The date inputs, not S, carry the window here:
// the headless gate writes their .value directly with no events, and a
// calendar edit lands the same way.
function selectTile(tile, opts) {
  S.tile = tile;
  timelineFor(tile);
  const d0 = $("date0").value, d1 = $("date1").value;
  if (!d0 || !d1) {
    $("query").querySelector(".hint").textContent = `Tile ${tile}. Pick a window first.`;
    return;
  }
  if (d1 < d0) { say("The window ends before it starts — swap the two dates.", true); return; }
  S.from = d0;
  S.to = d1;
  const year = Number(d0.slice(0, 4));
  if (year !== S.year) { S.year = year; $("year").value = String(year); }
  $("query").querySelector(".hint").textContent = `Tile ${tile}.`;
  // The search is returned, not only started, so the hash restore can wait
  // for the rows before it looks for its scene. A click ignores the promise,
  // the way it always has. `opts` only carries the restore's flyFirst: false
  // through to the auto-show; a click passes nothing and keeps the default.
  return startSearch(tile, year, opts);
}

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

// The URLs of the parts that may hold `tile` in `year`, per the collection
// (COLLECTIONS[..].parts). For the first collection that is exactly one
// archive file -- items.parquet, or the one zone part of the year's tier
// the tile's zone falls in; the other parts of the year are never probed,
// let alone read -- plus live.parquet for the current year. For Collection
// 1 it is items.parquet plus the year's live.parquet. The probe only asks
// whether the file is published yet.
const partUrlsFor = (year, tile) =>
  COL.parts(year, tile).map((stem) => `${BASE}/${COL.dir}/year=${year}/${stem}.parquet`);

// The last year with any published part, from `from` upward: a zone-1
// tile's parts of each year are probed (a year is published whole, so one
// part stands for the year; live.parquet where the collection may have
// one). Years are probed in order and the walk stops at the first
// unpublished one, so a page load costs one 404 (which Chrome logs), not
// one per future year.
async function newestPublishedYear(from) {
  let newest = from;
  for (let y = from + 1; y <= CURRENT_YEAR; y++) {
    if (!(await Promise.all(partUrlsFor(y, "1CDK").map(partExists))).some(Boolean)) break;
    newest = y;
  }
  return newest;
}

async function partUrls(y0, y1, tile) {
  const candidates = [];
  for (let y = y0; y <= y1; y++) candidates.push(...partUrlsFor(y, tile));
  const present = await Promise.all(candidates.map(partExists));
  return candidates.filter((_, i) => present[i]);
}

// The read itself lives in search.js (sceneRows): hyparquet range-reads the
// parts' footers once per session, admits row groups by the tile column's
// statistics, and fetches the admitted groups' search columns in parallel.
// Every gate — the date window, the cloud ceiling, the coverage floor — then
// runs over those rows in the page (results.js filterRows). The coverage
// gate is the item-level twin of the stats file's max_cover
// (100 - s2:nodata_pixel_percentage), and is a no-op at 0 (the slider's
// inert value, and its state whenever the slider is hidden). `assets`
// (~half the bytes of a part) is never fetched; `bbox` is, for "Show on
// map", and the processing baseline for the band mapper's index offset.

// The request a STAC API would have been asked for the same answer. Shown in
// full because not making it is the point of this page.
function apiMirror(tile, d0, d1, cc, cov) {
  const query = {
    "eo:cloud_cover": { lte: cc },
    ...COL.apiTile(tile),
  };
  // Mirrors the page's coverage gate: coverage = 100 - nodata, so
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
      collections: [COLLECTION_ID],
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

// ---------------------------------------------------------------------------
// The bottom sheet. On a phone (style.css, max-width 760px) the map
// fills the viewport and one sheet holds the image controls and the search
// controls. The sheet has three heights: "peek" is the handle, one line, and
// the image nav strip, "half" is what a fresh page shows, "full" is nearly
// the screen. style.css
// owns the heights, in dvh, so they follow the iOS URL bar; this code only
// says which stop is current, in data-snap, and sets a pixel height while a
// finger is on the handle. On a desktop the handle is display: none and the
// heights are not in that media query, so every call below is a no-op.
// ---------------------------------------------------------------------------
const SNAPS = ["peek", "half", "full"];
// The same three heights as style.css, for picking the nearest stop on
// release. The safe-area inset is left out: it shifts all three equally.
const snapHeight = (name) =>
  ({ peek: 150, half: 0.5 * innerHeight, full: 0.88 * innerHeight })[name];
let snap = "half";
// A sheet only on a phone: on a desktop #sheet is display: contents and has
// no height to snap. Asking the style, not the viewport width, keeps the one
// breakpoint in style.css where it can be read.
const isSheet = () => getComputedStyle($("sheet")).display !== "contents";
function setSnap(name) {
  if (!isSheet()) return;
  snap = name;
  const sheet = $("sheet");
  sheet.style.height = "";              // back to the CSS height for the stop
  sheet.dataset.snap = name;
  // peek shows the top of the sheet and nothing else, so the top must be the
  // part in view — otherwise a scrolled sheet peeks at the middle of itself.
  if (name === "peek") $("sheetbody").scrollTop = 0;
}
setSnap("half");
{
  const grip = $("grip");
  const cycle = () => setSnap(SNAPS[(SNAPS.indexOf(snap) + 1) % SNAPS.length]);
  // A pointer gesture settles itself on pointerup: a move of a few pixels is
  // a drag and snaps to the nearest stop, anything shorter is a tap and
  // cycles. The click that a browser sends after that would cycle a second
  // time, so it is swallowed once. A click with no pointer gesture before it
  // is the keyboard (Enter or Space on the handle), and that cycles too.
  let swallowClick = false;
  grip.addEventListener("pointerdown", (e) => {
    // Never take a drag that belongs to a control. The handle holds nothing
    // today; the sliders, selects and buttons of the sheet are all below it
    // and must keep their own pointer events, and this keeps that true if
    // anything is ever put in the handle.
    if (e.target.closest("input, select, a, summary")) return;
    const sheet = $("sheet");
    const h0 = sheet.getBoundingClientRect().height;
    const y0 = e.clientY;
    let moved = 0;
    e.preventDefault();
    sheet.dataset.drag = "";
    grip.setPointerCapture(e.pointerId);
    const move = (ev) => {
      const dy = ev.clientY - y0;
      moved = Math.max(moved, Math.abs(dy));
      sheet.style.height =
        `${Math.min(0.88 * innerHeight, Math.max(56, h0 - dy))}px`;
    };
    const up = (ev) => {
      grip.removeEventListener("pointermove", move);
      grip.removeEventListener("pointerup", up);
      grip.removeEventListener("pointercancel", up);
      delete sheet.dataset.drag;
      // A cancelled gesture (the browser took the pointer) is not a gesture:
      // put the sheet back on its stop and leave the next click alone.
      if (ev.type !== "pointerup") { setSnap(snap); return; }
      swallowClick = true;
      if (moved < 6) { cycle(); return; }                  // a tap, not a drag
      const h = sheet.getBoundingClientRect().height;
      setSnap(SNAPS.reduce((a, b) =>
        Math.abs(snapHeight(b) - h) < Math.abs(snapHeight(a) - h) ? b : a));
    };
    grip.addEventListener("pointermove", move);
    grip.addEventListener("pointerup", up);
    grip.addEventListener("pointercancel", up);
  });
  grip.addEventListener("click", () => {
    if (swallowClick) { swallowClick = false; return; }
    cycle();
  });
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
  $("imgnav").hidden = true;
  $("imgnav-label").hidden = true;
  $("bandbox").hidden = true;
  // No scene is left to bridge to — Clear and a failed load both call this,
  // so the scrub thumbnail (if one is still up) comes off the map too.
  if (scrubLayer) { scrubLayer = null; render(); }
  // peek exists to show the "Showing …" line; with no scene there is nothing
  // to peek at, so the sheet goes back to what a fresh page shows.
  if (snap === "peek") setSnap("half");
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
// The selectable bands: the L2A set, plus the collection's masks (Collection
// 1's cloud and snow probabilities) after them.
const SELECTABLE_BANDS = [...Object.keys(BANDS), ...COL.masks];
for (const id of ["sel-r", "sel-g", "sel-b"]) {
  for (const b of SELECTABLE_BANDS) {
    const o = new Option(`${b} ${bandInfo(b).label}`, b);
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
  if (kind === "gray" && fixedRange(ui.single)) {
    const [lo, hi] = fixedRange(ui.single);
    notes.push(`${ui.single} is ${bandInfo(ui.single).label.toLowerCase()} in percent, `
      + `stretched over the whole ${lo}–${hi} scale (the handles can narrow it). `
      + `Under Auto nodata a ${lo} % pixel is transparent — set Nodata to None to draw it black.`);
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
    // The slider's span: a mask's fixed scale; else the data's own min..max,
    // or with no stats (band unreadable) the same 0..10000 the range was
    // seeded with.
    const fixed = ch.index ? null : fixedRange(ch.band);
    const lo = ch.index ? -1 : fixed?.[0] ?? stats?.min ?? 0;
    const hi = ch.index ? 1 : fixed?.[1] ?? stats?.max ?? 10000;
    const block = el("div", "chan");
    const head = el("div", "chan-head");
    head.append(el("b", null, ch.index ? INDICES[ch.index].label : ch.band),
      el("span", "hint", ch.index ? `${INDICES[ch.index].a} − ${INDICES[ch.index].b}`
        : bandTitle(ch.band).slice(ch.band.length + 1)));
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
  scrubLayer = null;
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

// The image nav strip's zoom-to: re-frame the shown scene's footprint.
// A separate listener from tilesSettled — this one only reads the camera,
// it never touches the tile layer.
function flyToImage(bbox) {
  map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, duration: 800 });
}

// How much of the shown image's bbox the viewport holds, 0..1. The button
// enables under 0.5 and disables over 0.65: the gap stops it from
// flickering while the camera settles.
function imageFraction() {
  const b = shown ? bboxOf(shown.r) : null;
  if (!b) return 1;
  const mb = map.getBounds();
  const w = Math.max(0, Math.min(b[2], mb.getEast()) - Math.max(b[0], mb.getWest()));
  const h = Math.max(0, Math.min(b[3], mb.getNorth()) - Math.max(b[1], mb.getSouth()));
  const area = (b[2] - b[0]) * (b[3] - b[1]);
  return area > 0 ? (w * h) / area : 1;
}
let zoomtoOn = false;
function syncZoomTo() {
  if (!shown) { zoomtoOn = false; $("zoomto").disabled = true; return; }
  const frac = imageFraction();
  if (!zoomtoOn && frac < 0.5) zoomtoOn = true;
  else if (zoomtoOn && frac > 0.65) zoomtoOn = false;
  $("zoomto").disabled = !zoomtoOn;
}
let zoomFrame = 0;
map.on("move", () => {
  if (zoomFrame) return;
  zoomFrame = requestAnimationFrame(() => { zoomFrame = 0; syncZoomTo(); });
});
map.on("moveend", syncZoomTo);
$("zoomto").addEventListener("click", () => {
  const b = shown ? bboxOf(shown.r) : null;
  if (b) flyToImage(b);
});

// Step through the current sort order, from the image nav strip. stepImage
// moves from where the map stands, not from the scrub bar, so a click after
// a manual pan still steps from the shown scene.
function stepImage(delta) {
  const view = currentView();
  if (!view.length) return;
  const at = indexOfId(view, S.displayedId);
  const next = clampIndex(view, (at >= 0 ? at : S.detachedAt) + delta);
  if (next < 0 || next === at) return;
  showIndex(next);
}
$("imgprev").addEventListener("click", () => stepImage(-1));
$("imgnext").addEventListener("click", () => stepImage(1));

function syncNavButtons() {
  const view = currentView();
  const at = shown ? indexOfId(view, S.displayedId) : -1;
  const pos = at >= 0 ? at : clampIndex(view, S.detachedAt);
  $("imgnav").hidden = !shown;
  $("imgprev").disabled = !shown || pos <= 0;
  $("imgnext").disabled = !shown || pos < 0 || pos >= view.length - 1;
  const cap = (r) => (r ? `${r.day} · ${r.cloud.toFixed(1)}% cloud` : "");
  setTip($("imgprev"), pos > 0 ? `← ${cap(view[pos - 1])}` : "");
  setTip($("imgnext"), pos >= 0 && pos < view.length - 1 ? `→ ${cap(view[pos + 1])}` : "");
  syncZoomTo();
}
// data-tip feeds the shared popover (wired above); an empty string is
// removed rather than kept, so a button with nothing to say never anchors
// an empty box. hideTipFor closes a tip already on screen for `el` before
// its caption changes under it — needed because syncNavButtons can disable
// imgprev/imgnext (or blank their caption) while the pointer is still over
// one, and a disabled element never fires the mouseleave that would
// otherwise close it.
function setTip(el, text) {
  hideTipFor(el);
  if (text) el.dataset.tip = text; else delete el.dataset.tip;
}

// The thumbnail bitmap alone, decoded once per scene id and cached: the
// warp builder below needs it, and so does the neighbour prefetch.
const thumbBitmaps = new Map();
const THUMB_BITMAPS_MAX = 40;
function thumbBitmapFor(row) {
  if (!thumbBitmaps.has(row.id)) {
    thumbBitmaps.set(row.id, thumbnailBitmap(row.thumbnail_url));
    if (thumbBitmaps.size > THUMB_BITMAPS_MAX) {
      thumbBitmaps.delete(thumbBitmaps.keys().next().value);
    }
  }
  return thumbBitmaps.get(row.id);
}

// The scrub preview is the same warped image the committed scene shows
// (cog.js's previewImage, over the scene's UTM grid via previewLayer): a
// partial granule registers exactly instead of stretching flat over the
// full footprint square, and nodata is masked the same way. The map below
// caches the warp per scene id, so the cost — one ~64 KiB TCI header read —
// falls once per scene the user pauses on, not once per drag frame.
const scrubPreviews = new Map();
const SCRUB_PREVIEWS_MAX = 20;
function scrubPreviewFor(row) {
  const id = String(row.id);
  if (!scrubPreviews.has(id)) {
    scrubPreviews.set(id, (async () => {
      try {
        const scene = openScene(id, sceneDirOf(row));
        const cog = await sceneCog(scene, "TCI");
        const bitmap = await thumbBitmapFor(row);
        if (!bitmap) return null;
        const white = /\/thumbnail\.jpg$/i.test(new URL(row.thumbnail_url).pathname);
        const img = previewImage(cog, bitmap, { white });
        return img ? { img, cog } : null;
      } catch {
        // A missing/odd thumbnail_url (sceneDirOf) or a failed header read
        // (sceneCog) resolves to null rather than rejecting, so an awaiter
        // never sees an unhandled rejection — just no preview this time.
        return null;
      }
    })());
    scrubPreviews.get(id).catch(() => scrubPreviews.delete(id));
    if (scrubPreviews.size > SCRUB_PREVIEWS_MAX) {
      scrubPreviews.delete(scrubPreviews.keys().next().value);
    }
  }
  return scrubPreviews.get(id);
}

let scrubSeq = 0;
async function previewIndex(i) {
  const view = currentView();
  const row = view[i];
  if (!row) return;
  const seq = ++scrubSeq;
  $("imgnav-label").hidden = false;
  $("imgnav-label").textContent =
    `${i + 1} of ${view.length} · ${row.day} · ${row.cloud.toFixed(1)}% cloud`;
  for (const [id, card] of cardNodes) card.classList.toggle("peek", id === row.id);
  const p = await scrubPreviewFor(row);
  if (seq !== scrubSeq) return;
  // A null build (no thumbnail, a CORS failure, a COG open failure) leaves
  // whatever the scrub layer already shows — never a raw, unwarped bitmap;
  // that mismatch is the jitter this warp exists to remove.
  if (p) scrubLayer = previewLayer(p.img, p.cog, "scrub-preview");
  render();
}
$("imgscrub").addEventListener("input", () => previewIndex(Number($("imgscrub").value)));

// The scrub layer is a live bridge while a committed scene is still
// loading and nothing else covers the map yet; the hand-off points
// (showTci, showBandsLoaded, tilesSettled, hideImagePanel) already null
// it the moment any of those flip, so this only asks whether that
// hand-off has happened yet.
const scrubBridging = () => !!(scrubLayer && shown && !shown.settled && !cogLayer && !cogPreview);

// A gesture ends one of two ways: "change" commits it (release, or once per
// keypress on a held arrow key), or nothing fires at all — a drag back to
// the start value, Esc mid-drag (Firefox rolls the value back with no
// change), a cancelled touch. Either way the card outline and the label
// must not outlive the gesture; the preview comes off too, unless a
// commit's load is already bridging, in which case the bridge is left for
// the hand-off points to clear and only the cosmetics come off here.
let scrubCommitting = false;
let commitTimer = 0;
function clearScrubCosmetics() {
  for (const card of cardNodes.values()) card.classList.remove("peek");
  $("imgnav-label").hidden = true;
  delete $("imgnav-label").dataset.detached;
}
function clearScrubPreview() {
  scrubSeq += 1;
  scrubLayer = null;
  render();
  clearScrubCosmetics();
}
function endScrubGesture() {
  // A "change" for a real commit fires, and sets scrubCommitting, before
  // the matching pointerup settles — so the deferred check below always
  // sees the flag a commit already raised, and never strips the bridging
  // layer a commit is waiting to hand off.
  setTimeout(() => { if (!scrubCommitting) clearScrubPreview(); }, 0);
}
$("imgscrub").addEventListener("pointerup", endScrubGesture);
$("imgscrub").addEventListener("pointercancel", endScrubGesture);
$("imgscrub").addEventListener("blur", () => {
  // A pending debounce settles on its own; scrubCommitting still says so.
  if (scrubCommitting) return;
  // Past that window scrubCommitting is already reset, whether or not a
  // load is still in flight — scrubBridging is the only reliable signal.
  clearScrubCosmetics();
  if (!scrubBridging()) { scrubLayer = null; render(); }
});
$("imgscrub").addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  if (commitTimer) {
    // The debounce has not fired: nothing has loaded, so the gesture
    // aborts whole and no load follows.
    clearTimeout(commitTimer);
    commitTimer = 0;
    scrubCommitting = false;
    clearScrubPreview();
    return;
  }
  // The debounce already fired: a load may be in flight, so only the
  // cosmetics come off — the bridge, if live, is the hand-off points' job.
  scrubCommitting = false;
  clearScrubCosmetics();
  if (!scrubBridging()) { scrubLayer = null; render(); }
});

// The release itself: scrubSeq bumps at once, so no in-flight bitmap can
// draw after this point no matter how the heavy part below is scheduled.
// That heavy part — the card/label cleanup and the load — waits out a
// 200 ms trailing debounce, so a held arrow key settles into one commit
// instead of stacking a full COG load per keypress. scrubLayer is left
// alone here: showTci, showBandsLoaded and tilesSettled drop it once the
// new scene's preview or tiles actually land, so the old thumbnail
// bridges the fly-and-load gap instead of the map going blank.
function commitScrub() {
  commitTimer = 0;
  scrubCommitting = false;
  clearScrubCosmetics();
  const i = Number($("imgscrub").value);
  const row = currentView()[i];
  // Already the shown scene: a track click at the current position must
  // not re-fly the camera or reload it either way. The bridge only comes
  // down here when nothing is actually loading for it to bridge to.
  if (!row || row.id === S.displayedId) {
    if (!scrubBridging()) { scrubLayer = null; render(); }
    return;
  }
  showIndex(i);
}
$("imgscrub").addEventListener("change", () => {
  scrubCommitting = true;
  scrubSeq += 1;
  clearTimeout(commitTimer);
  commitTimer = setTimeout(commitScrub, 200);
});

// The scrubber's position and range follow the view. When the shown scene
// no longer passes the filters the thumb detaches: the image stays on the
// map, the label says why, and prev/next step in from the last position.
// One label serves two writers: this function's detached notice and
// previewIndex's "N of M · day · cloud" while a scrub drag is in flight. The
// non-detached branch therefore hides it only when the detached branch was
// the one that last wrote it (the data-detached marker), so a re-render mid
// drag never blanks the position the user is reading.
function renderScrubber() {
  const view = currentView();
  const scrub = $("imgscrub");
  const label = $("imgnav-label");
  scrub.max = String(Math.max(0, view.length - 1));
  scrub.disabled = !shown || view.length < 2;
  const at = shown ? indexOfId(view, S.displayedId) : -1;
  if (at >= 0) S.detachedAt = at;
  scrub.value = String(at >= 0 ? at : Math.max(0, clampIndex(view, S.detachedAt)));
  const detached = !!shown && view.length > 0 && at < 0;
  scrub.toggleAttribute("data-detached", detached);
  if (detached) {
    label.hidden = false;
    label.textContent = `${shown.id} · outside the current filters`;
    label.dataset.detached = "";
  } else if (label.dataset.detached !== undefined) {
    label.hidden = true;
    delete label.dataset.detached;
  }
}

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
    // A mask opens on its fixed scale; a band on its 2–98 %.
    const fixed = ch.index ? null : fixedRange(ch.band);
    me.ranges.set(chanKey(ch), ch.index ? { min: -1, max: 1 }
      : fixed ? { min: fixed[0], max: fixed[1] }
        : st ? { min: st.p2, max: st.p98 } : { min: 0, max: 10000 });
  }
  spec = me.spec = bandSpec(me);
  syncPanel(spec, me);
  buildChannels(me, spec);
  const preview = bandPreviewImage(me.scene, spec);
  cogLayer = bandTileLayer(me.scene, spec, styleKeyOf(spec), `cog-${id}-${me.bandsKey}`, me.eventsFor(me.bandsKey));
  cogPreview = preview ? previewLayer(preview, me.scene, `cog-preview-${id}`) : null;
  // The real bands are on the map now; the flat scrub thumbnail that
  // bridged the release has done its job.
  scrubLayer = null;
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
  // The real tiles are registered; the flat scrub thumbnail that bridged
  // the release has done its job.
  scrubLayer = null;
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
    scrubLayer = null;
    render();
    if (!me.failed) cogbar(id, "loading", "Preview shown — loading full resolution…");
    debug(`[cog] ${id} preview shown at ${(performance.now() - me.t0).toFixed(0)} ms`);
    // The tiles may have all landed while the JPEG was still coming.
    tilesSettled();
  } else if (!preview && !me.settled) {
    say(`${id}: no preview (thumbnail unreadable) — loading full-resolution tiles…`);
  }
}

// "Show on map" button: fly to the scene's footprint,
// remember the scene, set the panel to the asked preset and apply it.
// `button` is the control to disable while the read runs. It is null when
// the page itself asks for a scene (showIndex), because no control was hit.
// `fly` frames the footprint; a step or a scrub release leaves the camera
// where it stands and lets "Zoom to" (syncZoomTo) re-frame on demand.
async function showOnMap(r, button, preset = "tci", band = null, fly = true) {
  const id = String(r.id);
  // A chip on the scene already shown keeps what it has read and set.
  const prev = shown?.id === id ? shown : null;
  const me = shown = { id, r, t0: performance.now(), serial: 0, bandsKey: null, spec: null,
    settled: false, failed: false, missing: [], ranges: prev?.ranges ?? new Map(),
    offset: offsetOf(r), scene: prev?.scene ?? null, bitmapP: prev?.bitmapP ?? null,
    loading: false, eventsFor: null };
  const bbox = bboxOf(r);
  if (fly && bbox) map.fitBounds([[bbox[0], bbox[1]], [bbox[2], bbox[3]]], { padding: 40, duration: 1200 });
  if (button) button.disabled = true;
  // A scene already on the map comes off now, not when this one is ready:
  // the bar names this scene from here on and the map must not contradict it.
  if (cogLayer || cogPreview) { cogLayer = null; cogPreview = null; render(); }
  cogbar(id, "loading", "Loading preview…");
  // The user asked to see an image: on a phone the sheet drops to peek, so
  // the map, and the scene now being drawn on it, is what fills the screen.
  // The "Showing …" line this cogbar just wrote is what peek still shows.
  setSnap("peek");
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
    if (button) button.disabled = false;
  }
}

$("cog-clear").addEventListener("click", () => {
  shown = null;
  S.displayedId = null;
  cogLayer = null;
  cogPreview = null;
  render();
  hideImagePanel();
  // The one state change that does not go through applyNow. Without this the
  // URL would keep a scene the page no longer shows.
  scheduleHashWrite();
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

// Thumbnails load when their card nears the scroll viewport, not when the
// card is built: a 200-row year would otherwise fetch 200 JPEGs at once.
// The root is the element that scrolls — the sidebar on a desktop, the
// sheet body on a phone — and the observer rebuilds when that flips.
let thumbObserver = null;
const scrollRoot = () => (isSheet() ? $("sheetbody") : $("panel"));
function ensureThumbObserver() {
  thumbObserver ??= new IntersectionObserver((entries) => {
    for (const e of entries) {
      if (!e.isIntersecting) continue;
      thumbObserver.unobserve(e.target);
      e.target.src = e.target.dataset.src;
    }
  }, { root: scrollRoot(), rootMargin: "300px 0px" });
  return thumbObserver;
}
matchMedia("(max-width: 760px)").addEventListener("change", () => {
  thumbObserver?.disconnect();
  thumbObserver = null;
  // Read the live card set from cardNodes, not from #results: a card the
  // filter has removed from #results is still a real card, and a DOM query
  // would miss it and drop it from observation for good.
  for (const card of cardNodes.values()) {
    const img = card.querySelector("img[data-src]:not([src])");
    if (img) ensureThumbObserver().observe(img);
  }
});

function thumbnail(r) {
  const img = document.createElement("img");
  img.loading = "lazy";
  img.decoding = "async";
  img.alt = `Preview of ${r.id}`;
  img.crossOrigin = "anonymous";
  img.addEventListener("load", () => {
    // The white base waits for the pixels.
    img.classList.add("loaded");
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
  img.dataset.src = r.thumbnail_url;
  ensureThumbObserver().observe(img);
  return img;
}

function buildCard(r) {
  const card = el("div", "scene");
  if (typeof r.thumbnail_url === "string" && r.thumbnail_url) card.append(thumbnail(r));
  const cap = document.createElement("div");
  cap.append(el("b", null, r.id), document.createElement("br"));
  cap.append(`${String(r.ts).slice(0, 10)} · ${Number(r.cloud).toFixed(1)}% cloud`);
  cap.append(document.createElement("br"));
  const actions = el("span", "actions");
  const show = el("button", "mini", "Show on map");
  show.type = "button";
  show.title = "Fly to the footprint and draw the visual COG on the map";
  // The card click goes through the nav state so every indicator follows it:
  // showOnMap alone leaves S.displayedId on the previously shown scene, and
  // the .current outline, the scrub thumb, the ‹/› steps and the hash's
  // scene= all keep pointing there. A card click is an explicit "frame this
  // scene", so it flies. The fallback covers a row that fell out of the view
  // between the render that built this card and the click on it.
  show.addEventListener("click", () => {
    const at = indexOfId(currentView(), r.id);
    if (at >= 0) showIndex(at, true);
    else showOnMap(r, show);
  });
  actions.append(show);
  cap.append(actions);
  card.append(cap);
  return card;
}

// One card per scene id, built once and reused across every re-filter:
// the thumbnail never reloads and the keyed-white canvas pass never
// repeats. The map resets with each new search.
let cardNodes = new Map();
function cardFor(row) {
  let card = cardNodes.get(row.id);
  if (!card) {
    card = buildCard(row);
    cardNodes.set(row.id, card);
  }
  return card;
}

// A tile click starts a search, so two can overlap when clicks come
// fast. Each run takes a sequence number; a run that awoke from an
// await to find a newer number leaves the page to the newer run.
let searchSeq = 0;

// `flyFirst` is how a restore keeps the camera a shared link asked for: the
// auto-show below frames the best result, which would overwrite the `map=`
// the boot just applied. Only the restore path passes false; a tile click, a
// year change and a timeline-bar click all keep the default.
async function startSearch(tile, year, { flyFirst = true } = {}) {
  const seq = ++searchSeq;
  const box = $("results");
  // The old search dies with the click that replaces it. It must not outlive
  // this line: an apply between here and the new rows would otherwise render
  // the previous tile-year's cards over the hint below, and a search that
  // finds no published parts would leave them there. renderResults returns
  // on a null search, currentView is empty, and the status line falls back
  // to the map's own filter sentence.
  S.search = null;
  box.replaceChildren(el("p", "hint", "Reading the item parts…"));
  $("sql").textContent = "Range-reading…";
  // The mirror while the read runs. updateFilterStatus keeps it current from
  // the first rows on, but it cannot write it yet: S.search is null here.
  $("api").textContent = apiMirror(tile, S.from, S.to, S.maxCloud, S.minCoverage);
  say(`Range-reading tile ${tile}'s ${year} scenes…`);
  let got;
  try {
    got = await yearRows(tile, year);
  } catch (err) {
    if (seq !== searchSeq) return;
    box.replaceChildren(el("p", "hint", `Query failed — ${err.message}`));
    say(`Could not read the item parts — ${err.message}`, true);
    return;
  }
  if (seq !== searchSeq) return;
  if (!got.urls.length) {
    $("sql").textContent = "";
    $("api").textContent = "";
    box.replaceChildren(el("p", "hint",
      `No published ${COLLECTION_ID} parts cover ${year}. Pick a year the backfill has reached.`));
    say(`Nothing published for ${year} in ${COLLECTION_ID} yet.`);
    return;
  }
  $("sql").textContent = got.plan;
  cardNodes = new Map();
  S.search = { tile, year, rows: got.rows, at: Date.now() };
  S.shown = 15;
  S.displayedId = null;
  S.detachedAt = 0;
  renderResultsNow();
  scheduleApply({ nav: true });
  const view = currentView();
  if (view.length) {
    showIndex(0, flyFirst);
    if (snap !== "peek") box.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
}

// Commit the view's position i to the map: the auto-show of the best
// result, and the target of a nav-strip step or a scrub drag. Grows the
// shown slice so a step past the loaded cards still has one to outline,
// renders that card set now (not on the next frame, so the outline and
// the scroll land together), and scrolls the target card into view —
// except in peek, where the sheet is collapsed and there is nothing to see.
// `fly` only frames the camera for the first result after a search; a step
// or a scrub commit leaves the camera where the user left it (feedback 4).
function showIndex(i, fly = false) {
  const view = currentView();
  const row = view[i];
  if (!row) return;
  if (i >= S.shown) S.shown = Math.ceil((i + 1) / 15) * 15;
  S.displayedId = row.id;
  S.detachedAt = i;
  showOnMap(row, null, ui.preset, null, fly);
  renderResultsNow();
  scheduleApply({ nav: true });
  if (snap !== "peek") {
    cardNodes.get(row.id)?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
  const view2 = currentView();
  const at2 = indexOfId(view2, row.id);
  (window.requestIdleCallback ?? setTimeout)(() => {
    for (const n of [view2[at2 - 1], view2[at2 + 1]]) if (n) scrubPreviewFor(n);
  });
}

// A slider drag re-renders on a short trailing debounce; the map repaint
// stays per-frame. renderResultsNow reconciles the card list in place.
let cardTimer = 0;
function renderResults() {
  clearTimeout(cardTimer);
  cardTimer = setTimeout(renderResultsNow, 60);
}
function renderResultsNow() {
  const box = $("results");
  if (!S.search) return;
  const view = currentView();
  for (const n of [...box.children]) if (!n.classList.contains("scene")) n.remove();
  if (!view.length) {
    // "0 of 0 scenes pass" reads as a filter the user set too tight, and the
    // widen button promises a wider window would find something. Neither is
    // true of a tile-year the parts hold no scenes for: nothing to widen to.
    if (!S.search.rows.length) {
      box.replaceChildren(el("p", "hint",
        `No ${S.search.tile} scenes are published for ${S.search.year}. `
        + "Pick another year or another tile."));
      return;
    }
    box.replaceChildren(el("p", "hint",
      `0 of ${S.search.rows.length} scenes pass — widen a slider or the date window.`));
    const b = widenButton();
    if (b) { b.addEventListener("click", widenWindow); box.append(b); }
    return;
  }
  const want = view.slice(0, S.shown).map(cardFor);
  let node = box.firstElementChild;
  for (const w of want) {
    if (node === w) { node = node.nextElementSibling; continue; }
    box.insertBefore(w, node);
  }
  while (node) { const next = node.nextElementSibling; node.remove(); node = next; }
  for (const [i, w] of want.entries()) {
    w.classList.toggle("best", i === 0 && S.sort === "cloud");
    w.classList.toggle("current", view[i].id === S.displayedId);
  }
  renderMore(box, view.length);
}
function renderMore(box, total) {
  document.getElementById("more")?.remove();
  if (total > S.shown) {
    const b = el("button", "mini", `Show ${Math.min(15, total - S.shown)} more (${total - S.shown} left)`);
    b.id = "more";
    b.type = "button";
    b.addEventListener("click", () => { S.shown += 15; renderResultsNow(); });
    box.append(b);
    return;
  }
  const b = widenButton();
  if (b) { b.addEventListener("click", widenWindow); box.append(b); }
}

await init();
// Every boot path ends here, the ones that gave up early included, so the
// URL starts following the page whether or not a restore ran.
finishRestore();
