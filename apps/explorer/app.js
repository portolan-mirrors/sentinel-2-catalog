// Sentinel-2 explorer. Everything below talks to static files:
//   stats/mgrs-monthly.parquet  – choropleth + timeline
//   stats/mgrs.pmtiles          – MGRS tile footprints
//   sentinel-2-l2a/year=*/…     – item queries (Task 12)
// There is no API, no server and no database behind this page: DuckDB-WASM
// issues HTTP range reads straight at the object store.
import maplibregl from "https://esm.sh/maplibre-gl@4.7.1";
import { Protocol } from "https://esm.sh/pmtiles@3.2.0";
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

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
