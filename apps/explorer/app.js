// Sentinel-2 explorer. Everything below talks to static files:
//   stats/mgrs-monthly.parquet  – choropleth + timeline
//   stats/mgrs.pmtiles          – MGRS tile footprints
//   sentinel-2-l2a/year=*/…     – item queries (Task 12; one part per year
//                                 by the tile's UTM zone from 2019, Task 18;
//                                 eight parts from 2021, Task 19)
// There is no API, no server and no database behind this page: DuckDB-WASM
// issues HTTP range reads straight at the object store.
import maplibregl from "https://esm.sh/maplibre-gl@4.7.1";
import { Protocol } from "https://esm.sh/pmtiles@3.2.0";
// 1.32.0 (DuckDB v1.4.3) is a floor, not a preference: the item parts are
// GeoParquet 2.0.0, and 1.29.0 (DuckDB v1.1.1) refuses them outright with
// "Geoparquet version 2.0.0 is not supported".
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm";

// ?base=http://localhost:8081 points the whole app at a local publish tree,
// which is how it is developed before the bucket is populated.
export const BASE = new URLSearchParams(location.search).get("base")
  ?? "https://data.source.coop/portolan-mirrors/sentinel-2-catalog";

const STATS = `${BASE}/stats/mgrs-monthly.parquet`;
// Only these three may reach the SQL string; the <select> is not trusted input.
const METRICS = new Set(["min_cloud_cover", "scene_count", "median_cloud_cover"]);

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
// `map` is extra, but a console handle to it saves reaching into the module.
window.S2 = { BASE, db, conn, map };

// The choropleth ramp, shared by the map paint expression, the legend and the
// timeline bars so one colour always means one thing.
const RAMP = [[0, "#1a9850"], [25, "#fee08b"], [60, "#d73027"], [100, "#4d0013"]];
const rampColor = (v) => {
  const x = Math.min(100, Math.max(0, Number(v)));
  for (let i = 1; i < RAMP.length; i++) {
    const [a, ca] = RAMP[i - 1], [b, cb] = RAMP[i];
    if (x > b) continue;
    const t = (x - a) / (b - a);
    const mix = (j) => Math.round(parseInt(ca.slice(1 + j, 3 + j), 16) * (1 - t)
      + parseInt(cb.slice(1 + j, 3 + j), 16) * t);
    return `rgb(${mix(0)},${mix(2)},${mix(4)})`;
  }
  return RAMP[RAMP.length - 1][1];
};

await mapReady;

map.addSource("mgrs", {
  type: "vector", url: `pmtiles://${BASE}/stats/mgrs.pmtiles`,
  promoteId: "mgrs_tile",
});
map.addLayer({
  id: "mgrs-fill", type: "fill", source: "mgrs", "source-layer": "mgrs",
  paint: {
    "fill-color": ["case", ["!=", ["feature-state", "v"], null],
      ["interpolate", ["linear"], ["feature-state", "v"],
        ...RAMP.flat()],
      "#223"],
    "fill-opacity": 0.55,
  },
});
map.addLayer({ id: "mgrs-line", type: "line", source: "mgrs",
  "source-layer": "mgrs",
  paint: { "line-color": "#8899bb", "line-width": 0.4 } });
map.addLayer({ id: "mgrs-hover", type: "line", source: "mgrs",
  "source-layer": "mgrs", filter: ["==", ["get", "mgrs_tile"], ""],
  paint: { "line-color": "#e8f0ff", "line-width": 1.6 } });
map.on("mousemove", "mgrs-fill", (e) => {
  map.getCanvas().style.cursor = "pointer";
  map.setFilter("mgrs-hover", ["==", ["get", "mgrs_tile"], e.features[0].properties.mgrs_tile]);
});
map.on("mouseleave", "mgrs-fill", () => {
  map.getCanvas().style.cursor = "";
  map.setFilter("mgrs-hover", ["==", ["get", "mgrs_tile"], ""]);
});

export async function statsForMonth(y, m) {
  const metric = $("metric").value;
  if (!METRICS.has(metric)) throw new Error(`unknown metric ${metric}`);
  const res = await conn.query(`
    SELECT mgrs_tile, ${metric} AS v
    FROM read_parquet('${STATS}')
    WHERE year = ${Number(y)} AND month = ${Number(m)}`);
  return res.toArray();
}

// Which tiles currently carry a feature-state, so the next month can clear
// exactly the ones it does not repaint.
let painted = new Set();

async function paintMonth() {
  const [y, m] = ($("month").value || "").split("-").map(Number);
  if (!y || !m) return;
  const ym = `${y}-${String(m).padStart(2, "0")}`;
  say(`Reading ${ym} from mgrs-monthly.parquet…`);
  let rows;
  try {
    rows = await statsForMonth(y, m);
  } catch (err) {
    say(`Could not read ${STATS} — ${err.message}`, true);
    return;
  }
  const isCount = $("metric").value === "scene_count";
  const next = new Set();
  // Clearing is an explicit `v: null` rather than removeFeatureState: the
  // bulk remove is coalesced against the writes that follow it and left a
  // handful of tiles holding last month's colour.
  for (const r of rows) next.add(r.mgrs_tile);
  for (const id of painted) {
    if (!next.has(id)) map.setFeatureState({ source: "mgrs", sourceLayer: "mgrs", id }, { v: null });
  }
  for (const r of rows) {
    // scene_count is rescaled onto the same 0-100 ramp (12+ scenes = green).
    const v = isCount ? Math.max(0, 100 - Number(r.v) * 8) : Number(r.v);
    map.setFeatureState({ source: "mgrs", sourceLayer: "mgrs", id: r.mgrs_tile }, { v });
  }
  painted = next;
  markActiveBar();
  if (!rows.length) {
    say(`No tile-months for ${ym} in the published stats. `
      + `Pick a month with bars in the timeline below.`);
    return;
  }
  say(`${rows.length.toLocaleString()} MGRS tiles imaged in ${ym} — `
    + `one range read, no API call.`);
}

export async function timelineFor(tile) {
  const bars = $("bars");
  const where = tile ? `WHERE mgrs_tile = '${tile.replace(/'/g, "")}'` : "";
  let rows;
  try {
    const res = await conn.query(`
      SELECT year, month, sum(scene_count)::INT AS n,
             min(min_cloud_cover) AS clearest
      FROM read_parquet('${STATS}')
      ${where} GROUP BY 1, 2 ORDER BY 1, 2`);
    rows = res.toArray();
  } catch (err) {
    bars.replaceChildren(el("p", "hint", `Timeline unavailable — ${err.message}`));
    return;
  }
  const scope = tile ? `Tile ${tile}` : "All tiles";
  bars.replaceChildren();
  if (!rows.length) {
    $("timeline-scope").textContent = `${scope} — nothing to plot.`;
    bars.append(el("p", "hint", tile
      ? `No months recorded for tile ${tile}.`
      : "The stats file is empty — nothing has been published yet."));
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

function updateLegend() {
  const isCount = $("metric").value === "scene_count";
  $("lo").textContent = isCount ? "12+ scenes" : "0% cloud";
  $("hi").textContent = isCount ? "1 scene" : "100% cloud";
}

// The current calendar month is empty until the backfill lands, so the app
// opens on the newest month the stats file actually contains.
async function newestMonth() {
  const res = await conn.query(
    `SELECT max(year::INT * 100 + month::INT) AS ym,
            min(year::INT * 100 + month::INT) AS lo
     FROM read_parquet('${STATS}')`);
  const [row] = res.toArray();
  const fmt = (n) => `${Math.floor(n / 100)}-${String(n % 100).padStart(2, "0")}`;
  return row?.ym ? { newest: fmt(row.ym), oldest: fmt(row.lo) } : null;
}

async function init() {
  updateLegend();
  $("metric").addEventListener("change", () => { updateLegend(); paintMonth(); });
  $("month").addEventListener("change", paintMonth);
  $("maxcloud").addEventListener("input", (e) => {
    $("maxcloud-out").textContent = e.target.value;
  });
  say("Reading the stats file…");
  let span;
  try {
    span = await newestMonth();
  } catch (err) {
    say(`Could not open ${STATS}. The file is missing, unreadable, or the `
      + `bucket refused the range read (${err.message}). Until the backfill `
      + `publishes, serve a local publish tree and load `
      + `?base=http://localhost:8081`, true);
    return;
  }
  if (!span) {
    say("The stats file has no rows yet — the backfill has not published any "
      + "tile-months. The map and timeline will fill in once it does.");
    await timelineFor(null);
    return;
  }
  const month = $("month");
  month.min = span.oldest;
  month.max = span.newest;
  month.value = span.newest;
  // Give the Task 12 scene query a sane default window: the shown month.
  const [y, m] = span.newest.split("-").map(Number);
  $("date0").value = `${span.newest}-01`;
  $("date1").value = new Date(Date.UTC(y, m, 0)).toISOString().slice(0, 10);
  await Promise.all([paintMonth(), timelineFor(null)]);
}

await init();

// ---------------------------------------------------------------------------
// The scene query. Click a tile, pick a window, and DuckDB-WASM range-reads the
// year parts of sentinel-2-l2a directly. Every href shown below is read out of
// the row's `assets` column (a JSON string carrying the upstream STAC assets
// object) — this app never builds an object-store URL from a template.
// ---------------------------------------------------------------------------

// An MGRS tile id: 1-2 digit UTM zone, latitude band C..X, then two letters.
// The values come from the tileset, not from a text box, but they are the only
// thing on this page that reaches a SQL string, so they are checked anyway.
const TILE_RE = /^\d{1,2}[C-X][A-Z]{2}$/;
const CURRENT_YEAR = new Date().getUTCFullYear();

let selectedTile = null;

map.on("click", "mgrs-fill", (e) => {
  const tile = e.features[0]?.properties?.mgrs_tile;
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

function sceneSql(urls, tile, d0, d1, cc) {
  // `_month` is the cheap row-group filter, but it only narrows anything while
  // the window stays inside one calendar year — across a year boundary
  // (2023-11 → 2024-02) months 11..2 is empty, so it widens to the whole year.
  const sameYear = d0.slice(0, 4) === d1.slice(0, 4);
  const m0 = sameYear ? Number(d0.slice(5, 7)) : 1;
  const m1 = sameYear ? Number(d1.slice(5, 7)) : 12;
  // `datetime` is TIMESTAMPTZ; comparing it against a bare literal would be
  // read in the session's zone, so both sides are pinned to UTC. The same
  // conversion formats the label, rather than guessing at the epoch units
  // Arrow hands back.
  return `SELECT id,
       strftime(datetime AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS ts,
       "eo:cloud_cover" AS cloud,
       thumbnail_url,
       assets
FROM read_parquet([${urls.map((u) => `'${u}'`).join(", ")}], union_by_name=true)
WHERE "s2:mgrs_tile" = '${tile}'
  AND _month BETWEEN ${m0} AND ${m1}
  AND (datetime AT TIME ZONE 'UTC')
      BETWEEN TIMESTAMP '${d0} 00:00:00' AND TIMESTAMP '${d1} 23:59:59'
  AND "eo:cloud_cover" <= ${cc}
ORDER BY "eo:cloud_cover", id
LIMIT 30`;
}

// The request a STAC API would have been asked for the same answer. Shown in
// full because not making it is the point of this page.
function apiMirror(tile, d0, d1, cc) {
  return JSON.stringify({
    note: "The STAC API request this page did NOT need to make. "
      + "Earth Search would answer it; the panel above is the same answer, "
      + "range-read out of static Parquet.",
    method: "POST",
    url: "https://earth-search.aws.element84.com/v1/search",
    body: {
      collections: ["sentinel-2-l2a"],
      datetime: `${d0}T00:00:00Z/${d1}T23:59:59Z`,
      query: {
        "eo:cloud_cover": { lte: cc },
        "s2:mgrs_tile": { eq: tile },
      },
      sortby: [{ field: "properties.eo:cloud_cover", direction: "asc" }],
      limit: 30,
    },
  }, null, 2);
}

// Asset hrefs are remote-derived strings going into an href, so only real
// http(s) URLs are linked; anything else (javascript:, data:, missing) is
// dropped rather than rendered.
function assetHref(assets, key) {
  const href = assets?.[key]?.href;
  if (typeof href !== "string") return null;
  try {
    const u = new URL(href);
    return u.protocol === "https:" || u.protocol === "http:" ? href : null;
  } catch {
    return null;
  }
}

const ASSET_LINKS = [["visual", "TCI"], ["red", "B04"], ["nir", "B08"],
  ["scl", "SCL"]];

function sceneCard(r, i) {
  const card = el("div", "scene" + (i === 0 ? " best" : ""));
  if (typeof r.thumbnail_url === "string" && r.thumbnail_url) {
    const img = document.createElement("img");
    img.loading = "lazy";
    img.decoding = "async";
    img.alt = `Preview of ${r.id}`;
    // A dead preview must not leave a broken-image box in the card.
    img.addEventListener("error", () => img.remove(), { once: true });
    img.src = r.thumbnail_url;
    card.append(img);
  }
  const cap = document.createElement("div");
  cap.append(el("b", null, r.id), document.createElement("br"));
  cap.append(`${String(r.ts).slice(0, 10)} · ${Number(r.cloud).toFixed(1)}% cloud`);
  cap.append(document.createElement("br"));
  // 30 rows, so parsing the whole assets object per row is free; a bad row is
  // shown without its links instead of killing the render.
  let assets = null;
  try {
    assets = JSON.parse(r.assets);
  } catch { /* leave the links off this card */ }
  for (const [key, label] of ASSET_LINKS) {
    const href = assetHref(assets, key);
    if (!href) continue;
    const a = el("a", null, label);
    a.href = href;
    a.target = "_blank";
    a.rel = "noopener noreferrer";
    a.title = `${label} — ${href}`;
    cap.append(a);
  }
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
    const sql = sceneSql(urls, selectedTile, d0, d1, cc);
    $("sql").textContent = sql;
    $("api").textContent = apiMirror(selectedTile, d0, d1, cc);
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
