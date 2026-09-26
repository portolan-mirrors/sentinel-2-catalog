# Explorer Year Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the explorer's search flow around a year: a year selector on top, a year-spanning date slider that live-recolors the map and live-refilters result cards, tile-click-initiated whole-year searches, a 15-card lazy result list with sort, and an image nav strip (zoom-to, prev/next, scrubber) on the loaded image.

**Architecture:** One state object `S` in `app.js` is the single source of truth; a new pure module `apps/explorer/results.js` derives filtered/sorted views from the cached whole-year scene rows; `search.js` gains a raw `sceneRows()` export while `sceneSearch()` keeps its frozen contract for the harnesses and the gate. Map recoloring aggregates the pre-built monthly stats slices across the slider's window.

**Tech Stack:** Vanilla ES modules, hyparquet (existing), MapLibre + deck.gl dist bundle (existing), `node --test` for the pure module, `tools/rails/experiments/check_app.py` (headless Chrome + DuckDB ground truth) as the integration gate.

**Spec:** `docs/superpowers/plans/2026-09-26-explorer-year-redesign-spec.md` — read it first; it carries the user's requirements, the three facts that shape the design, and the open questions.

**Worktree:** all work happens in `.claude/worktrees/s2-explorer-year-redesign` on branch `worktree-s2-explorer-year-redesign`. All paths below are relative to that worktree root.

## Global Constraints

- `sceneSearch()` in `apps/explorer/search.js` keeps its exact signature `{ urls, tileColumn, tile, d0, d1, cc, cov, sidecars }` and behavior (date window, cloud ≤ cc, coverage gate with `cov <= 0` escape, ORDER BY cloud then id, `slice(0, 30)`, row keys `{id, ts, cloud, thumbnail_url, bbox, baseline}`, `plan` string whose first line contains `hyparquet range-read plan`). `harness.html:78`, `search_harness.html:83`, `measure_layout.py:198`, `measure_search.py:106` and `check_app.py` all consume it.
- `app.js` must keep the literal spellings `const map = new maplibregl.Map({` and `const hitIndex = {` — `check_app.py:192-200` exits if either changes.
- The gate's DOM contract survives every task: `#date0`/`#date1` inputs exist and are read at search time (the gate writes `.value` directly, no events); `#maxcloud`/`#mincoverage` respond to `input` events; a map click writes `Tile <id>` into `#query .hint`; `#sql` receives text containing `range-read plan`; `#results` ends with card `<b>` ids or a `.hint`.
- No new dependencies or CDN imports. `BitmapLayer` comes from the already-loaded `window.deck` dist bundle.
- Lasting code comments follow ASD-STE100 (repo rule): short sentences, active voice, present tense, no gerunds. Do not rewrite existing comments except where a task says to.
- Never commit data files (GeoParquet, COG, PMTiles). Nothing in this plan touches `catalog/`.
- Every commit message ends with the line `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Gate commands: **fast gates** are `node --test apps/explorer/` (pure module) and a manual serve (`python3 -m http.server 8000` at the worktree root, open `http://localhost:8000/apps/explorer/`, which reads the live bucket). The **slow gate** is `python3 tools/rails/experiments/check_app.py` (~10-25 min, headless Chrome + DuckDB against the live bucket); run it only where a task says to.

## Review Focus

Five failure modes the spec implies but no requirement names, each pinned to the task that owns it:

1. **The gate writes `#date0`/`#date1` with no events.** If the tile-click search reads `S.from`/`S.to` instead of the inputs, the gate searches the wrong window and fails against DuckDB. Pinned in Task 6 (the `selectTile` code reads the inputs first; the task ends with a full `check_app.py` run).
2. **A window over months with no stats slice** (`months/YYYY-MM.parquet` 404s, e.g. January of a year whose stats start in March). The map must show those tiles unpainted, not crash, and the status line must stay sane. Pinned in Task 3 (`paintWindow` uses `Promise.allSettled` and a null-rows month contributes nothing; manual test step drags the slider into an empty span).
3. **The displayed scene stops passing the filters** during a slider drag. The image must stay on the map; the scrubber detaches; prev/next step into the filtered list from the last known position. Pinned in Task 11 (explicit test step narrows max-cloud below the shown scene's value).
4. **A tile-year with zero scenes or no published parts** (e.g. a 2015 L2A year, or a tile outside coverage). `#results` must show a `.hint` (the gate requires `b, .hint` to appear) and nothing may throw. Pinned in Task 6 (empty-result and no-parts branches with their own test step).
5. **A month-bar click from another year while a search is active.** The year select must switch, the date slider must clamp to that month, and the search must re-run for the bar's year (cache hit if visited). Pinned in Task 9's test steps.

---

### Task 1: `sceneRows()` split in search.js

**Files:**
- Modify: `apps/explorer/search.js:260-296`
- Gate: `tools/rails/experiments/check_app.py`

**Interfaces:**
- Consumes: existing `searchPart(url, tileColumn, tile, tally, sidecars)` (search.js:219).
- Produces: `export async function sceneRows({ urls, tileColumn, tile, sidecars = true }) -> Promise<{ rows: Row[], plan: string }>` where `Row = { id, ts, day, t, cloud, cover, thumbnail_url, bbox, baseline }` (`t` epoch ms; `day` `"YYYY-MM-DD"`; `cover` number or `null`; rows sorted by `t` then id, unfiltered, unlimited). `sceneSearch` unchanged externally. Tasks 2, 6 rely on `Row` exactly as written here.

- [ ] **Step 1: Replace `sceneSearch` (search.js:260-296) with the split.** Keep the header comment at 260-264, reworded to say `sceneRows` is the raw read and `sceneSearch` the DuckDB-shaped wrapper (STE):

```js
// The raw read: every row of `tile` in the given parts, projected to the
// card fields, sorted by time. sceneRows applies no date, cloud or
// coverage filter and no limit; the page filters in memory so a slider
// drag costs no network read.
export async function sceneRows({ urls, tileColumn, tile, sidecars = true }) {
  const tally = { parts: 0, groups: 0, gets: 0, bytes: 0, misses: 0 };
  const t0 = performance.now();
  const raw = (await Promise.all(
    urls.map((u) => searchPart(u, tileColumn, tile, tally, sidecars)))).flat();
  const rows = raw.map((r) => {
    const t = r.datetime instanceof Date ? r.datetime.getTime() : Date.parse(r.datetime);
    const nodata = Number(r["s2:nodata_pixel_percentage"]);
    return {
      id: r.id,
      ts: new Date(t).toISOString().slice(0, 19) + "Z",
      day: new Date(t).toISOString().slice(0, 10),
      t,
      cloud: Number(r["eo:cloud_cover"]),
      cover: Number.isFinite(nodata) ? 100 - nodata : null,
      thumbnail_url: r.thumbnail_url,
      bbox: Array.from(r.bbox ?? []),
      baseline: r["s2:processing_baseline"],
    };
  }).sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const secs = ((performance.now() - t0) / 1000).toFixed(1);
  const plan = `hyparquet range-read plan (no SQL engine, no API):\n`
    + `  ${tally.parts} part(s) held ${tile}, ${tally.groups} row group(s) admitted by their`
    + ` ${tileColumn} ranges\n`
    + `  ${tally.gets} parallel range GETs, ${(tally.bytes / 1024).toFixed(0)} KiB`
    + ` (footers cached per session), ${secs} s`
    + (tally.misses ? `\n  ${tally.misses} read(s) fell outside the prefetched chunks` : "");
  return { rows, plan };
}

// The full search, shaped exactly like the DuckDB query it replaces:
// tile, UTC day window, cloud ceiling, coverage floor; ORDER BY cloud, id;
// LIMIT 30. The harnesses and check_app.py pin this contract.
export async function sceneSearch({ urls, tileColumn, tile, d0, d1, cc, cov,
                                    sidecars = true }) {
  const { rows: all, plan } = await sceneRows({ urls, tileColumn, tile, sidecars });
  const lo = Date.parse(`${d0}T00:00:00Z`);
  const hi = Date.parse(`${d1}T23:59:59.999Z`);
  const rows = all
    .filter((r) => r.t >= lo && r.t <= hi && r.cloud <= cc
      && (cov <= 0 || (r.cover !== null && r.cover >= cov)))
    .sort((a, b) => a.cloud - b.cloud || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    .slice(0, 30)
    .map((r) => ({ id: r.id, ts: r.ts, cloud: r.cloud,
      thumbnail_url: r.thumbnail_url, bbox: r.bbox, baseline: r.baseline }));
  return { rows, plan };
}
```

Note the coverage clause: the old code's `100 - Number(nodata) >= cov` is false for a missing nodata value (NaN comparison), so `(r.cover !== null && r.cover >= cov)` reproduces it exactly.

- [ ] **Step 2: Smoke the page.** `python3 -m http.server 8000` at the worktree root, open `http://localhost:8000/apps/explorer/`, click a tile, confirm cards render and `#sql` shows the plan. Stop the server.

- [ ] **Step 3: Run the slow gate.** Run: `python3 tools/rails/experiments/check_app.py`. Expected: `all … case(s) on both collections` and exit 0 (the candidate must match DuckDB exactly; the baseline variant also passes because `sceneSearch`'s behavior is unchanged).

- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/search.js
git commit -m "refactor: split sceneRows out of sceneSearch

sceneRows returns every row of a tile in the given parts, with no
filter and no limit. sceneSearch keeps its exact contract as a thin
wrapper. The page can now cache a whole year and filter in memory.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `results.js` — the pure derived pipeline (TDD)

**Files:**
- Create: `apps/explorer/results.js`
- Create: `apps/explorer/results.test.mjs`

**Interfaces:**
- Consumes: `Row` objects from Task 1 (`{ id, day, t, cloud, cover, … }`).
- Produces (Tasks 6-12 import all of these):
  - `SORTS: { cloud: {label, cmp}, coverage: {label, cmp}, date: {label, cmp} }`
  - `filterRows(rows, f)` with `f = { maxCloud, minCoverage, t0, t1 }` (numbers; `t0`/`t1` epoch ms)
  - `sortRows(rows, key)` (returns a new array)
  - `viewOf(rows, f, key)` = `sortRows(filterRows(rows, f), key)`
  - `indexOfId(view, id) -> number` (-1 when absent)
  - `clampIndex(view, i) -> number` (0..len-1, or -1 when empty)
  - `filterKeyOf(f, key, search) -> string` (memo key; `search` may be null)

- [ ] **Step 1: Write the failing tests** in `apps/explorer/results.test.mjs`:

```js
import test from "node:test";
import assert from "node:assert/strict";
import { SORTS, filterRows, sortRows, viewOf, indexOfId, clampIndex, filterKeyOf }
  from "./results.js";

const day = (d) => Date.parse(`${d}T12:00:00Z`);
const rows = [
  { id: "S2A_1", day: "2024-01-05", t: day("2024-01-05"), cloud: 40, cover: 100 },
  { id: "S2A_2", day: "2024-03-10", t: day("2024-03-10"), cloud: 5, cover: 30 },
  { id: "S2A_3", day: "2024-07-01", t: day("2024-07-01"), cloud: 5, cover: null },
  { id: "S2A_4", day: "2024-11-20", t: day("2024-11-20"), cloud: 80, cover: 90 },
];
const all = { maxCloud: 100, minCoverage: 0,
  t0: Date.parse("2024-01-01T00:00:00Z"), t1: Date.parse("2024-12-31T23:59:59.999Z") };

test("filterRows applies cloud, coverage and date window", () => {
  assert.equal(filterRows(rows, all).length, 4);
  assert.deepEqual(filterRows(rows, { ...all, maxCloud: 10 }).map((r) => r.id),
    ["S2A_2", "S2A_3"]);
  // a null cover is never excluded by the coverage floor
  assert.deepEqual(filterRows(rows, { ...all, minCoverage: 50 }).map((r) => r.id),
    ["S2A_1", "S2A_3", "S2A_4"]);
  assert.deepEqual(filterRows(rows, { ...all,
    t0: Date.parse("2024-03-01T00:00:00Z"),
    t1: Date.parse("2024-08-31T23:59:59.999Z") }).map((r) => r.id),
    ["S2A_2", "S2A_3"]);
});

test("sortRows: cloud ties break on id, coverage sorts nulls last, date is newest first", () => {
  assert.deepEqual(sortRows(rows, "cloud").map((r) => r.id),
    ["S2A_2", "S2A_3", "S2A_1", "S2A_4"]);
  assert.deepEqual(sortRows(rows, "coverage").map((r) => r.id),
    ["S2A_1", "S2A_4", "S2A_2", "S2A_3"]);
  assert.deepEqual(sortRows(rows, "date").map((r) => r.id),
    ["S2A_4", "S2A_3", "S2A_2", "S2A_1"]);
  assert.notEqual(sortRows(rows, "date"), rows); // never mutates its input
  assert.equal(rows[0].id, "S2A_1");
});

test("viewOf composes, indexOfId and clampIndex behave at the edges", () => {
  const view = viewOf(rows, { ...all, maxCloud: 10 }, "cloud");
  assert.deepEqual(view.map((r) => r.id), ["S2A_2", "S2A_3"]);
  assert.equal(indexOfId(view, "S2A_3"), 1);
  assert.equal(indexOfId(view, "S2A_1"), -1);
  assert.equal(clampIndex(view, 5), 1);
  assert.equal(clampIndex(view, -3), 0);
  assert.equal(clampIndex([], 0), -1);
});

test("filterKeyOf changes when any input changes", () => {
  const search = { tile: "31UFU", year: 2024, at: 1 };
  const a = filterKeyOf(all, "cloud", search);
  assert.notEqual(a, filterKeyOf({ ...all, maxCloud: 99 }, "cloud", search));
  assert.notEqual(a, filterKeyOf(all, "date", search));
  assert.notEqual(a, filterKeyOf(all, "cloud", { ...search, at: 2 }));
  assert.equal(typeof filterKeyOf(all, "cloud", null), "string");
});
```

- [ ] **Step 2: Run tests, verify they fail.** Run: `node --test apps/explorer/` — expected: `ERR_MODULE_NOT_FOUND` for `./results.js`.

- [ ] **Step 3: Write `apps/explorer/results.js`:**

```js
// The derived pipeline of the search results: raw year rows in, the
// filtered and sorted view out. Pure functions, no DOM, no imports, so
// `node --test` runs them (results.test.mjs).
const cmpId = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

export const SORTS = {
  cloud: { label: "least cloud (clearest first)",
    cmp: (a, b) => a.cloud - b.cloud || cmpId(a, b) },
  coverage: { label: "most coverage (fullest first)",
    cmp: (a, b) => (b.cover ?? -1) - (a.cover ?? -1) || a.cloud - b.cloud || cmpId(a, b) },
  date: { label: "newest first",
    cmp: (a, b) => b.t - a.t || cmpId(a, b) },
};

// A null cover means "unknown", and the floor never excludes what it
// cannot judge — the same rule fillColor applies on the map.
export function filterRows(rows, f) {
  return rows.filter((r) => r.t >= f.t0 && r.t <= f.t1
    && r.cloud <= f.maxCloud
    && (f.minCoverage <= 0 || r.cover === null || r.cover >= f.minCoverage));
}

export function sortRows(rows, key) {
  return [...rows].sort((SORTS[key] ?? SORTS.cloud).cmp);
}

export function viewOf(rows, f, key) {
  return sortRows(filterRows(rows, f), key);
}

export function indexOfId(view, id) {
  return view.findIndex((r) => r.id === id);
}

export function clampIndex(view, i) {
  return view.length ? Math.min(view.length - 1, Math.max(0, i)) : -1;
}

export function filterKeyOf(f, key, search) {
  return [search?.tile, search?.year, search?.at,
    f.t0, f.t1, f.maxCloud, f.minCoverage, key].join("|");
}
```

- [ ] **Step 4: Run tests, verify they pass.** Run: `node --test apps/explorer/` — expected: 4 passing tests, exit 0.

- [ ] **Step 5: Commit.**

```bash
git add apps/explorer/results.js apps/explorer/results.test.mjs
git commit -m "feat: add results.js, the pure result-view pipeline

filterRows, sortRows, viewOf, indexOfId, clampIndex and filterKeyOf
derive the visible result list from cached year rows. node --test
covers the filter rules, the three sorts and the edge indexes.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: the `S` state object, the apply scheduler, and the year-window paint

This is the core refactor: one state object, one coalesced apply path, and the choropleth painted from an **aggregate of the window's months** instead of one month. The month input stays in the DOM until Task 4 swaps it for the year select, but stops driving anything.

**Files:**
- Modify: `apps/explorer/app.js` (state block 367-375; `fillColor` 380-387; filter helpers 533-549; `updateScenesBound` 554-567; `paintMonth` 574-624; `onSlider` 756-772; `reboundDateRange` 791-806; `init` 825-889; `initWithoutStats` 902-923; map-click block 943-960 — only the `selectedTile` rename here; `window.S2` 397-402, 1410)

**Interfaces:**
- Consumes: `readTable` (search.js), `monthFile` (app.js:497), `onRamp` (app.js:507), `dayRange` (rangeslider.js), Task 2's `filterKeyOf` (imported now for later tasks).
- Produces (later tasks call these exact names):
  - `const S = { year, maxCloud, minCoverage, minScenes, from, to, monthLock, tile, search, sort, shown, displayedId, detachedAt }`
  - `scheduleApply(flags)` with flags `{ paint?, cards?, nav? }`, coalesced to one rAF
  - `paintWindow()` (async, seq-guarded, replaces `paintMonth`)
  - `monthsIn(from, to) -> ["YYYY-MM", …]`
  - `setWindow(from0, to0)` (rebounds the slider and applies)
  - `buildDateSlider(from0, to0)` (creates or rebounds `dateRange` with `onChange` wired)
  - `currentFilters() -> { maxCloud, minCoverage, t0, t1 }`

- [ ] **Step 1: Introduce `S` and delete the scattered locals.** Replace app.js:367-375 with:

```js
// Per-tile stats for the shown window: mgrs_tile -> {v: 0..100 on the ramp,
// cc, cover, sc}. `v` is already rescaled per metric.
let lookup = new Map();
let paintKey = 0;
let hovered = null;          // the hovered feature (GeoJSON, WGS84) or null
let cogLayer = null;         // the shown scene's TileLayer, or null
let cogPreview = null;       // its thumbnail warp, beneath the tiles until they load

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
```

Then rename every read of the old locals: `maxCloud` → `S.maxCloud`, `minCoverage` → `S.minCoverage`, `minScenes` → `S.minScenes` (`fillColor` 380-387, `fmtFilters`/`passCounts` 533-549, `updateScenesBound` 554-567, `apiMirror`'s caller in runQuery 1798/1801-1803 — runQuery still exists until Task 6), and `selectedTile` → `S.tile` (943-960, 1770). Update `window.S2` (397-402): `selectedTile: { get: () => S.tile }`, and add `S: { value: S }`.

- [ ] **Step 2: Add the apply scheduler** (below `repaint()`, app.js:455):

```js
// One coalesced apply per animation frame: a slider drag asks for a paint
// and a card render, and both run once, in order, on the next frame.
// renderResults, renderScrubber and syncNavButtons arrive in later tasks;
// the optional calls keep this file loadable in between.
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
  if (f.cards) globalThis.renderResults?.();
  if (f.nav) { globalThis.renderScrubber?.(); globalThis.syncNavButtons?.(); }
  if (!f.cards) say(`${filterLine()}.`);
}
```

(Task 6 replaces the status-line branch with `updateFilterStatus()`. `globalThis.renderResults` etc. are module functions by Task 7/11; the `?.` guards only bridge the tasks in between — Task 12 removes the guards by which time all three exist. If the executor prefers, declare `let renderResults = null` style hooks instead; the plan's later tasks assign plain functions.)

- [ ] **Step 3: Replace `paintMonth` with `paintWindow` + month aggregation.** Replace app.js:519-529 (`statsForMonth`) and 574-624 (`paintMonth`) with:

```js
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
// month set costs nothing here.
let aggKey = "";
let aggLookup = new Map();
function aggregateMonths(months, metric) {
  const key = months.map((m) => m.ym).join(",") + "|" + metric;
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

async function paintWindow() {
  if (!S.from || !S.to) return;
  if (statsMissing) { say(statsNote); return; }
  const metric = $("metric").value;
  if (!METRICS.has(metric)) throw new Error(`unknown metric ${metric}`);
  const seq = ++paintSeq;
  const note = lagNote;
  lagNote = "";
  const yms = monthsIn(S.from, S.to);
  const settled = await Promise.allSettled(yms.map(monthStatsFor));
  if (seq !== paintSeq) return;
  const months = yms.map((ym, i) => ({ ym,
    rows: settled[i].status === "fulfilled" ? settled[i].value : null }));
  lookup = aggregateMonths(months, metric);
  updateScenesBound([...lookup.values()]);
  repaint();
  markActiveBars();
  const have = months.filter((m) => m.rows).length;
  if (!lookup.size) {
    say(`No tile-months in the published stats for ${S.from} → ${S.to}. `
      + "Pick a window with bars in the timeline below."
      + (note ? ` ${note}` : ""));
    return;
  }
  say(`${lookup.size.toLocaleString()} MGRS tiles imaged in ${S.from} → ${S.to} — `
    + `${have} month slice${have === 1 ? "" : "s"}, no API call. ${filterLine()}.`
    + (note ? ` ${note}` : ""));
}
```

`updateScenesBound` (554-567) keeps its body; its rows now come from `lookup.values()` whose objects carry the same `.sc` field. Change its slider write-back to `S.minScenes = bound;`. `markActiveBar` (725-730) becomes:

```js
function markActiveBars() {
  const lo = (S.from ?? "").slice(0, 7), hi = (S.to ?? "").slice(0, 7);
  for (const b of $("bars").querySelectorAll(".bar")) {
    b.classList.toggle("on", b.dataset.ym >= lo && b.dataset.ym <= hi);
  }
}
```

Update its two other call sites (timelineFor 715, init 888) to the new name. The timeline bar `onclick` (708-712) temporarily becomes `d.onclick = () => setWindow(`${ym}-01`, lastDayOfMonth(ym));` (Task 9 replaces it). Delete the `export` on the old `statsForMonth` (nothing imports it).

- [ ] **Step 4: Wire the slider `onChange` and rework `reboundDateRange`.** Replace app.js:791-806 with:

```js
// Create the two-handle day slider or move its bounds. onChange fires on
// every handle drag step and calendar edit, and drives the map paint and
// the card filter through one apply.
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
```

In `init` (828-831) the `#month` listener becomes `$("month").addEventListener("change", () => setWindow(`${$("month").value}-01`, lastDayOfMonth($("month").value)));` (Task 4 deletes it). `reboundDateRange(defaultMonth)` at 881 → `buildDateSlider(`${defaultMonth}-01`, lastDayOfMonth(defaultMonth))`; same at 909 for `initWithoutStats`. `paintMonth()` call sites (827, 882) → `paintWindow()`. `onSlider` (756-772) becomes:

```js
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
```

Also add `currentFilters()` next to it (used from Task 6 on):

```js
function currentFilters() {
  return { maxCloud: S.maxCloud, minCoverage: S.minCoverage,
    t0: Date.parse(`${S.from}T00:00:00Z`),
    t1: Date.parse(`${S.to}T23:59:59.999Z`) };
}
```

- [ ] **Step 5: Manual test.** Serve and open the page. Expected: identical behavior to before (the month input still drives the window through `setWindow`), slider drags recolor tiles live, month switch repaints, tile click still searches (runQuery reads `$("date0")`/`$("date1")` and `S.maxCloud`/`S.minCoverage`). Drag nothing new. Then set the month to one month and drag a date handle: the map must now recolor when the handle crosses out of the month — it cannot yet (window is one month), so instead verify no console errors on drag and that `#status` shows the filter line.

- [ ] **Step 6: Commit.**

```bash
git add apps/explorer/app.js
git commit -m "refactor: one state object and a window-aggregated paint

S replaces the scattered filter locals. scheduleApply coalesces paint,
card and nav updates into one frame. paintWindow aggregates the month
slices the date window overlaps, so the paint follows the slider.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: the year selector replaces the month input

**Files:**
- Modify: `apps/explorer/index.html:71`
- Modify: `apps/explorer/app.js` (`init` 858-888, `initWithoutStats` 902-923, remaining `$("month")` references)

**Interfaces:**
- Consumes: Task 3's `setWindow`, `buildDateSlider`, `paintWindow`.
- Produces: `setYear(year)` — clears `monthLock`, rebounds the slider to the full year, repaints, and (from Task 6 on) re-searches the selected tile. `#year` is a `<select>` of ints.

- [ ] **Step 1: Swap the control.** index.html:71: replace `<label>Month <input type="month" id="month" min="2015-07"></label>` with `<label>Year <select id="year"></select></label>`.

- [ ] **Step 2: Add `setYear` and populate the select.** In app.js, next to `setWindow`:

```js
function setYear(year) {
  S.year = year;
  S.monthLock = null;
  $("datelock")?.toggleAttribute("hidden", true);
  $("year").value = String(year);
  setWindow(`${year}-01-01`, `${year}-12-31`);
  if (S.tile && S.search) startSearch(S.tile, year);   // no-op until Task 6
}
```

(Until Task 6 defines `startSearch`, guard the last line with `typeof startSearch === "function" &&` or add it in Task 6 — executor's choice; Task 6 owns the final line.)

In `init`, replace the month block (858-881) with:

```js
const statsYear = Number(span.newest.slice(0, 4));
const newestYear = await newestPublishedYear(statsYear);
if (newestYear > statsYear) {
  lagNote = `Stats reach ${span.newest}; scenes are published through ${newestYear} `
    + "— the choropleth updates when the stats rebuild lands.";
}
lagNote = (lagNote + collectionNote).trim();
const y0 = Number(span.oldest.slice(0, 4));
const y1 = Math.max(newestYear, statsYear);
for (let y = y0; y <= y1; y++) $("year").append(new Option(String(y), String(y)));
S.year = statsYear;
$("year").value = String(statsYear);
// Open on the newest month the stats have, expanded to its whole year:
// the year is the unit now, and the newest year is partly empty ahead of
// the backfill, which paintWindow tolerates month by month.
buildDateSlider(`${statsYear}-01-01`, `${statsYear}-12-31`);
```

and its listener registration (replacing the `#month` one at 828-831):

```js
$("year").addEventListener("change", () => setYear(Number($("year").value)));
```

In `initWithoutStats` (902-923): options `COL.since`..`CURRENT_YEAR`, `S.year = CURRENT_YEAR`, `$("year").value = String(CURRENT_YEAR)`, `buildDateSlider(`${CURRENT_YEAR}-01-01`, `${CURRENT_YEAR}-12-31`)`. Delete every remaining `$("month")` reference (grep for `"month"` — the `#metric` listener at 827 keeps `paintWindow()`).

- [ ] **Step 3: Manual test.** Serve, open. Expected: year select shows the stats span, defaults to the newest stats year; the map paints the whole year's aggregate; dragging a date handle across a month boundary recolors tiles live (this is Review Focus 2's smoke: drag `From` into January of the newest year, where slices may 404 — tiles unpainted, no console error). Switch year: slider rebounds to Jan-Dec, map repaints.

- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/index.html apps/explorer/app.js
git commit -m "feat: year selector with a year-wide interactive date slider

The month input becomes a year select. The date slider spans the year
and repaints the choropleth on drag from the aggregated month slices.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: the filter hints collapse into an info icon

**Files:**
- Modify: `apps/explorer/index.html:84-101`
- Modify: `apps/explorer/app.js` (one listener block, near the collection-switch block)
- Modify: `apps/explorer/style.css` (new `.info`/`.tip` rules, after the `.hint` rule)

**Interfaces:** none consumed by later tasks. `#query .hint` (index.html:86) **stays** — the gate reads it.

- [ ] **Step 1: Edit the markup.** In index.html `#query` (84-103): delete the three hint paragraphs (94, 97, 101), the `title` on `#dayrange` (91) and the `title` on `#minscenes` (99-100). Replace the `<h2>Find scenes</h2>` line with:

```html
    <div class="secthead"><h2>Find scenes</h2>
      <button type="button" class="info" id="query-info" aria-expanded="false"
        aria-label="How the filters work">i</button></div>
    <div class="tip" id="query-tip" hidden>
      <p><b>Max cloud</b>: tiles whose clearest scene in the window is cloudier go grey.</p>
      <p><b>Min coverage</b>: tiles whose fullest scene fills less of the tile go grey.</p>
      <p><b>Min scenes</b>: tiles imaged fewer times across the window go grey.
        The scene list below is not gated by this slider.</p>
      <p><b>Dates</b>: drag either handle; the calendars follow. The map recolors
        by whole months; the scene list follows to the day.</p>
    </div>
```

- [ ] **Step 2: Wire it** (app.js, near the collection-switch block at 147-159):

```js
// The filter help: a hover shows it, a click pins it (touch has no hover).
{
  const info = $("query-info"), tip = $("query-tip");
  let pinned = false;
  const show = (on) => { tip.hidden = !on; info.setAttribute("aria-expanded", String(on)); };
  info.addEventListener("click", () => { pinned = !pinned; show(pinned); });
  if (matchMedia("(hover: hover)").matches) {
    info.addEventListener("mouseenter", () => show(true));
    info.addEventListener("mouseleave", () => { if (!pinned) show(false); });
  }
}
```

- [ ] **Step 3: Style it** (style.css, after the `.hint` rule):

```css
.secthead { display: flex; align-items: center; gap: 6px; }
.secthead h2 { margin: 0; }
.info { width: 18px; height: 18px; border-radius: 50%; padding: 0;
  font: 600 11px/16px system-ui, sans-serif; font-style: italic;
  background: #223; color: #93a2c0; border: 1px solid #3a4666; cursor: pointer; }
.tip { background: #131a30; border: 1px solid #3a4666; border-radius: 6px;
  padding: 8px 10px; margin: 6px 0; }
.tip p { margin: 4px 0; font-size: 12px; color: #93a2c0; }
```

(Match the file's existing color tokens if they differ — read the surrounding rules first and reuse their values.)

- [ ] **Step 4: Manual test.** Serve, open. Hover the icon: tip shows and hides. Click: it pins, click again unpins. Keyboard: tab to it, Enter toggles. At 390 px width: tap toggles. The sliders keep their `<output>` numbers; `#query .hint` ("Click the map…") is still present.

- [ ] **Step 5: Commit.**

```bash
git add apps/explorer/index.html apps/explorer/app.js apps/explorer/style.css
git commit -m "feat: fold the filter hints into one info tooltip

The three slider hint paragraphs and two title attributes move into a
tip behind an info icon. Hover shows the tip; a click pins it.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: tile click fetches the whole year; the Search button goes; the gate learns the new shape

**Files:**
- Modify: `apps/explorer/app.js` (imports at 34-39; map click 943-960; `runQuery` 1764-1838 replaced; `$("run")` at 947, 1782, 1836, 1840; `showOnMap` 1590-1642 null-button; `window.S2`)
- Modify: `apps/explorer/index.html:102` (remove `#run`)
- Modify: `apps/explorer/style.css:81-82` (remove `#run` rules)
- Modify: `tools/rails/experiments/check_app.py` (the driver's `out.ids` read)

**Interfaces:**
- Consumes: Task 1's `sceneRows`, Task 2's `viewOf`/`filterKeyOf`, Task 3's `S`/`currentFilters`/`scheduleApply`.
- Produces (Tasks 7-12 rely on these):
  - `yearRows(tile, year) -> Promise<{ rows, plan, urls }>` (cached per collection|tile|year, cap 8)
  - `startSearch(tile, year)` (seq-guarded; fills `S.search`, renders, auto-shows index 0)
  - `currentView() -> Row[]` (memoised on `filterKeyOf`)
  - `showIndex(i)` (commits view position i to the map via `showOnMap(row, null, …)`)
  - `updateFilterStatus()`
  - `renderResults()` (plain full re-render here; Task 7 upgrades it)
  - `window.S2.viewIds() -> string[]`

- [ ] **Step 1: Import the pipeline.** Add to app.js imports: `import { SORTS, viewOf, indexOfId, clampIndex, filterKeyOf } from "./results.js";` and change the search.js import to `import { sceneRows, sceneSearch, warmPart, readTable, keyedRows } from "./search.js";` (keep `sceneSearch` imported only if still referenced; if not, drop it).

- [ ] **Step 2: The year cache and view accessor** (replace the comment block 1047-1056 area, keeping `apiMirror`):

```js
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
  const view = currentView();
  say(`${view.length} of ${S.search.rows.length} ${S.search.tile} scenes in `
    + `${S.search.year} pass (${S.from} → ${S.to}, cloud ≤ ${S.maxCloud}, `
    + `coverage ≥ ${S.minCoverage}) — one year of parts range-read once, `
    + `filters run in the page.`);
}
```

In `applyNow` (Task 3), replace the `if (!f.cards) say(…)` line with `updateFilterStatus();`.

- [ ] **Step 3: Replace the click handler and `runQuery`.** Replace app.js:943-960 with:

```js
map.on("click", (e) => {
  const tile = hitIndex.at(e.lngLat.wrap().lng, e.lngLat.lat)?.tile;
  if (!TILE_RE.test(tile ?? "")) return;
  selectTile(tile);
});

// The click is the search. The date inputs, not S, carry the window here:
// the headless gate writes their .value directly with no events, and a
// calendar edit lands the same way.
function selectTile(tile) {
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
  startSearch(tile, year);
}
```

Replace `runQuery` (1764-1838) and its `$("run")` listener (1840) with:

```js
async function startSearch(tile, year) {
  const seq = ++searchSeq;
  const box = $("results");
  box.replaceChildren(el("p", "hint", "Reading the item parts…"));
  $("sql").textContent = "Range-reading…";
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
  S.search = { tile, year, rows: got.rows, at: Date.now() };
  S.shown = 15;
  S.displayedId = null;
  S.detachedAt = 0;
  renderResults();
  scheduleApply({ nav: true });
  const view = currentView();
  if (view.length) {
    showIndex(0);
    if (snap !== "peek") box.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
}

// Commit the view's position i to the map. Task 11 extends this with the
// card outline scroll; here it is the auto-show of the best result.
function showIndex(i) {
  const view = currentView();
  const row = view[i];
  if (!row) return;
  S.displayedId = row.id;
  S.detachedAt = i;
  showOnMap(row, null, ui.preset);
  scheduleApply({ cards: true, nav: true });
}

// Plain render for now: the visible slice of the view, plus a Show-more
// footer. Task 7 turns this into a keyed reconcile.
function renderResults() {
  const box = $("results");
  if (!S.search) return;
  const view = currentView();
  if (!view.length) {
    box.replaceChildren(el("p", "hint",
      `0 of ${S.search.rows.length} scenes pass — widen a slider or the date window.`));
    return;
  }
  box.replaceChildren(...view.slice(0, S.shown).map(sceneCard));
  renderMore(box, view.length);
}
function renderMore(box, total) {
  document.getElementById("more")?.remove();
  if (total <= S.shown) return;
  const b = el("button", "mini", `Show ${Math.min(15, total - S.shown)} more (${total - S.shown} left)`);
  b.id = "more";
  b.type = "button";
  b.addEventListener("click", () => { S.shown += 15; renderResults(); });
  box.append(b);
}
```

`sceneCard(r, i)` is called with one argument now — change its signature to `sceneCard(r)` and drop the `" best"` class from it (Task 7 owns `.best`; interim absence is fine). In `showOnMap` (1590, 1600, 1640): `button?.disabled` guards (`if (button) button.disabled = true;` and `finally { if (button) button.disabled = false; }`). Delete index.html:102 (`#run`), style.css:81-82, and every `$("run")` reference. In `setYear` (Task 4), the last line becomes unguarded: `if (S.tile && S.search) startSearch(S.tile, year);`. Add to the `window.S2` definitions:

```js
viewIds: { value: () => currentView().map((r) => r.id) },
```

Also update the file's header comment block that describes the flow (app.js:943-960's old comment) — say the click is the search and the button is gone (STE).

- [ ] **Step 4: Update the gate driver.** In `check_app.py`, the driver's ids read (`out.ids = [...$("results")…`) becomes:

```js
    out.ids = (w.S2 && w.S2.viewIds ? w.S2.viewIds()
      : [...$("results").querySelectorAll(".card b, b")].map((b) => b.textContent.trim()))
      .filter((t) => /^S2[A-Z]/.test(t)).slice(0, 30);
```

The fallback keeps the **baseline** variant (new app.js + HEAD search.js) readable — but note the baseline variant cannot boot at all now (`import { sceneRows }` fails against HEAD's search.js), which the gate tolerates: a failed baseline prints `committed client: failed` and does not fail the run (`check_app.py:365-376` only fails on `DIFFERENT`).

- [ ] **Step 5: Manual test.** Serve, open. Click a tile: the whole year loads (~1-2 s warm), 15 cards render, "Show 15 more" appends, the clearest scene auto-shows on the map, `#sql` shows the plan. Click a tile with no scenes for the year (e.g. far ocean edge if clickable, or set the year to one before the backfill): a `.hint` renders, no console error (Review Focus 4). Drag max-cloud down: cards re-filter live and the status line counts down.

- [ ] **Step 6: Run the slow gate.** Run: `python3 tools/rails/experiments/check_app.py`. Expected: `candidate … matches DuckDB: True` on every case (the candidate's `viewIds()` under the driver's window and cc=100/cov=0 equals DuckDB's `ORDER BY cloud, id LIMIT 30` after the cap); `committed client: failed` lines are expected and non-fatal.

- [ ] **Step 7: Commit.**

```bash
git add apps/explorer/app.js apps/explorer/index.html apps/explorer/style.css tools/rails/experiments/check_app.py
git commit -m "feat: tile click fetches the whole year; remove the Search button

A tile click reads the year's parts once through sceneRows and caches
the rows per tile-year. The sliders and the date window filter the
cached rows in the page. The gate reads the view ids from window.S2
because only 15 cards render at first.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: keyed card reconcile and lazy thumbnails

**Files:**
- Modify: `apps/explorer/app.js` (`sceneCard` 1721-1757 → `buildCard` + `cardFor`; `thumbnail` 1693-1719; `renderResults` from Task 6)
- Modify: `apps/explorer/style.css` (`.current`, `.peek` outlines near the `.scene.best` rule at 199-205)

**Interfaces:**
- Consumes: Task 6's `renderResults` slot, `currentView`, `S`.
- Produces: `cardNodes: Map<id, HTMLElement>`, `cardFor(row)`, `renderResultsNow()` (Tasks 11-12 toggle `.current`/`.peek` through `cardNodes`). `renderResults()` becomes a 60 ms trailing debounce over `renderResultsNow()`.

- [ ] **Step 1: Lazy thumbnails.** In `thumbnail(r)` (1693-1719), replace the final `img.src = r.thumbnail_url;` with:

```js
  img.dataset.src = r.thumbnail_url;
  ensureThumbObserver().observe(img);
```

and add above it:

```js
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
  for (const img of document.querySelectorAll("#results img[data-src]:not([src])")) {
    ensureThumbObserver().observe(img);
  }
});
```

(The existing `error` fallback path that sets `img.src = r.thumbnail_url` directly is untouched — by then the real load already started.)

- [ ] **Step 2: Keyed cards and reconcile.** Rename `sceneCard(r)` to `buildCard(r)` (body unchanged apart from the Task 6 signature change) and add:

```js
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
```

Replace Task 6's `renderResults` with:

```js
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
    box.replaceChildren(el("p", "hint",
      `0 of ${S.search.rows.length} scenes pass — widen a slider or the date window.`));
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
```

In `startSearch`, add `cardNodes = new Map();` right before `S.search = …`, and change its `renderResults()` call to `renderResultsNow()` (a fresh search renders now, not after a debounce). `renderMore`'s click handler calls `renderResultsNow()`.

- [ ] **Step 3: Outline styles.** In style.css, next to `.scene.best` (199-205):

```css
.scene.current { outline: 2px solid #6ea8ff; outline-offset: -2px; }
.scene.peek { outline: 2px dashed #6ea8ff; outline-offset: -2px; }
```

- [ ] **Step 4: Manual test.** Serve, open, search a tile. Open devtools Network, filter images: only the ~6 on-screen thumbnails load; scrolling loads more in batches. Drag max-cloud from 100 to 20 slowly: cards leave and return with **zero** new image requests for already-seen cards, and the drag stays smooth. The shown scene's card carries the blue outline. "Show more" extends by 15.

- [ ] **Step 5: Commit.**

```bash
git add apps/explorer/app.js apps/explorer/style.css
git commit -m "feat: keyed card reconcile and lazy thumbnails

Cards build once per scene id and reconcile in place on a re-filter.
Thumbnails load through an IntersectionObserver on the scroll root.
The shown scene's card carries an outline.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: the sort control

**Files:**
- Modify: `apps/explorer/index.html` (one line above `#results`, index.html:106)
- Modify: `apps/explorer/app.js` (populate + listener, next to the Task 6 block)

**Interfaces:**
- Consumes: `SORTS` (Task 2), `S.sort`, `scheduleApply`.
- Produces: `#sort` select; `setSort(key)`.

- [ ] **Step 1: Markup.** Above `<section id="results">`:

```html
  <div id="sortrow"><label>Sort <select id="sort"></select></label></div>
```

- [ ] **Step 2: Wire it** (app.js):

```js
for (const [key, s] of Object.entries(SORTS)) $("sort").append(new Option(s.label, key));
$("sort").value = S.sort;
function setSort(key) {
  S.sort = key;
  S.shown = 15;
  scheduleApply({ cards: true, nav: true });
}
$("sort").addEventListener("change", () => setSort($("sort").value));
```

- [ ] **Step 3: Manual test.** Search a tile; switch sort to "newest first": cards reorder, the shown scene keeps its outline wherever it lands (or none if beyond 15), the `best` badge disappears (it is cloud-sort-only). Switch to "most coverage": fullest scenes first. Back to "least cloud": badge returns on card 1.

- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/index.html apps/explorer/app.js
git commit -m "feat: expose the result sort

Least cloud stays the default and says so. Most coverage and newest
first are the other two orders, from results.js SORTS.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: a month bar starts a month-locked search

**Files:**
- Modify: `apps/explorer/app.js` (bar `onclick` from Task 3; new `lockToMonth`/`unlockMonth`/`onBarClick` next to `setYear`)
- Modify: `apps/explorer/index.html` (the lock chip, inside `.dates` at 87-90)
- Modify: `apps/explorer/style.css` (`.chip`)

**Interfaces:**
- Consumes: `setWindow`, `startSearch`, `S`, `lastDayOfMonth` (app.js:781).
- Produces: `lockToMonth(ym)`, `unlockMonth()`; `S.monthLock`; `#datelock` chip.

- [ ] **Step 1: Markup.** Inside the `.dates` div (index.html:87-90), after the two labels:

```html
      <span id="datelock" class="chip" hidden><span id="datelock-label"></span>
        <button id="datereset" class="mini" type="button"
          aria-label="Back to the whole year">✕</button></span>
```

style.css: `.chip { display: inline-flex; align-items: center; gap: 4px; background: #223; border: 1px solid #3a4666; border-radius: 10px; padding: 1px 6px; font-size: 12px; }` (reuse the file's tokens).

- [ ] **Step 2: The lock** (app.js, next to `setYear`):

```js
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

function onBarClick(ym) {
  if (S.monthLock === ym) { unlockMonth(); return; }
  const year = Number(ym.slice(0, 4));
  if (year !== S.year) {
    S.year = year;
    $("year").value = String(year);
  }
  lockToMonth(ym);
}
```

The bar's `onclick` (in `timelineFor`, from Task 3) becomes `d.onclick = () => onBarClick(ym);`. In `setYear`, the two `monthLock` lines already clear the lock (Task 4) — make sure the chip hides there too: `$("datelock").hidden = true;` replaces the optional-chained line from Task 4.

Also update `#timeline-scope`'s texts ("Click a bar to jump to that month." at app.js 694-696) to "Click a bar to search that month." and the default in index.html:105 to "All tiles. Click a tile on the map, then a bar to search a month."

- [ ] **Step 3: Manual test** (Review Focus 5). Search a tile in the newest year. The timeline shows the tile's whole history. Click a bar from an **earlier** year: the year select switches, the date slider rebounds to that month (calendars show its first/last day), the chip shows `YYYY-MM ✕`, a search for that year runs (network shows one new year read), cards are that month's scenes. Click the same bar again: back to the full year. Click the bar, then ✕: same. Click the bar, then change the year select: lock clears, full new year.

- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/app.js apps/explorer/index.html apps/explorer/style.css
git commit -m "feat: a month bar starts a month-locked search

A bar click switches the year when needed, clamps the date slider to
the bar's month behind a dismissable chip, and searches. The active
bar, the chip and a year change all release the lock.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: the image nav strip — zoom-to and prev/next

**Files:**
- Modify: `apps/explorer/index.html` (after `#cogbar`, line 29)
- Modify: `apps/explorer/app.js` (`showOnMap`'s fitBounds 1599 extracted; near `tilesSettled` 1417-1433; `hideImagePanel` 1231-1238; `cog-clear` 1644-1650)
- Modify: `apps/explorer/style.css` (`#imgnav` row)

**Interfaces:**
- Consumes: `currentView`, `indexOfId`, `clampIndex`, `showIndex`, `bboxOf` (1084), `shown` (1409), `S`.
- Produces: `flyToImage(bbox)`, `stepImage(delta)`, `syncNavButtons()` (called from `applyNow`'s `nav` flag), `#imgnav` with `#imgprev` `#imgnext` `#zoomto` `#imgscrub` (the scrubber input exists here, wired in Task 11), `#imgnav-label`.

- [ ] **Step 1: Markup.** After `#cogbar` (index.html:29):

```html
  <div id="imgnav" hidden>
    <button id="imgprev" class="mini" type="button" aria-label="Previous scene">‹</button>
    <input type="range" id="imgscrub" min="0" max="0" step="1" value="0"
      aria-label="Scrub through the search results" disabled>
    <button id="imgnext" class="mini" type="button" aria-label="Next scene">›</button>
    <button id="zoomto" class="mini" type="button" disabled>Zoom to</button>
  </div>
  <p id="imgnav-label" class="hint" hidden></p>
```

style.css: `#imgnav { display: flex; align-items: center; gap: 6px; } #imgnav input[type=range] { flex: 1; min-width: 60px; }`.

- [ ] **Step 2: Zoom-to.** Extract the fly (app.js:1598-1599 stays as is inside `showOnMap`, plus):

```js
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
```

- [ ] **Step 3: Prev/next.**

```js
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
  $("imgprev").title = pos > 0 ? `← ${cap(view[pos - 1])}` : "";
  $("imgnext").title = pos >= 0 && pos < view.length - 1 ? `→ ${cap(view[pos + 1])}` : "";
  syncZoomTo();
}
// The native title is slow and invisible on touch; a hover names the
// target in the label line at once.
for (const id of ["imgprev", "imgnext"]) {
  $(id).addEventListener("pointerenter", () => {
    if ($(id).title) { $("imgnav-label").hidden = false; $("imgnav-label").textContent = $(id).title; }
  });
  $(id).addEventListener("pointerleave", () => { $("imgnav-label").hidden = true; });
}
```

Extend `showIndex` (Task 6) — before `showOnMap`, grow the list if needed and scroll the card into view after:

```js
function showIndex(i) {
  const view = currentView();
  const row = view[i];
  if (!row) return;
  if (i >= S.shown) S.shown = Math.ceil((i + 1) / 15) * 15;
  S.displayedId = row.id;
  S.detachedAt = i;
  showOnMap(row, null, ui.preset);
  renderResultsNow();
  scheduleApply({ nav: true });
  if (snap !== "peek") {
    cardNodes.get(row.id)?.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }
}
```

In `hideImagePanel` (1231-1238) add `$("imgnav").hidden = true; $("imgnav-label").hidden = true;`, and in the `cog-clear` handler (1644-1650) add `S.displayedId = null;` before `render()`.

- [ ] **Step 4: Manual test.** Search a tile. Pan the map far away: "Zoom to" enables; click it: the camera re-frames the image and the button disables. Step forward: the next scene in sort order loads, its card outlines and scrolls into view; hover over ‹/›: the label line shows the target's date and cloud. Step to the ends: buttons disable. Clear: the strip hides.

- [ ] **Step 5: Commit.**

```bash
git add apps/explorer/index.html apps/explorer/app.js apps/explorer/style.css
git commit -m "feat: image nav strip with zoom-to and prev/next

Zoom to enables when under half the image is in view and re-frames it.
Prev/next step through the current sort order, name the target scene
on hover, and outline its card.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 11: the scrubber

**Files:**
- Modify: `apps/explorer/app.js` (`render` 404-433 gains a `scrubLayer` slot; scrubber wiring next to Task 10's block; `renderScrubber` called from `applyNow`)
- Modify: `apps/explorer/style.css` (detached thumb style)

**Interfaces:**
- Consumes: `#imgscrub` (Task 10 markup), `thumbnailBitmap` (1125-1134), `currentView`, `showIndex`, `cardNodes`.
- Produces: `renderScrubber()`, `previewIndex(i)`; a `scrubLayer` drawn above `cogLayer` in `render()`.

- [ ] **Step 1: The preview layer slot.** In `render()` (404-433), insert `scrubLayer,` between `cogLayer,` and the `GeoJsonLayer`, and declare `let scrubLayer = null;` next to `cogLayer` (373). deck.gl ignores a null entry, same as `cogPreview`.

- [ ] **Step 2: Wire the scrubber.**

```js
// The scrub preview is the scene's thumbnail as a flat BitmapLayer over
// its bbox: one ~40 KB JPEG per step, no COG read. The full showOnMap
// path runs only on release. Bitmaps cache so a back-and-forth is free.
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
  const b = bboxOf(row);
  const bitmap = b ? await thumbBitmapFor(row) : null;
  if (seq !== scrubSeq) return;
  scrubLayer = bitmap ? new window.deck.BitmapLayer({
    id: "scrub-preview", image: bitmap, bounds: [b[0], b[1], b[2], b[3]] }) : null;
  render();
}
$("imgscrub").addEventListener("input", () => previewIndex(Number($("imgscrub").value)));
$("imgscrub").addEventListener("change", () => {
  scrubSeq += 1;
  scrubLayer = null;
  render();
  for (const card of cardNodes.values()) card.classList.remove("peek");
  $("imgnav-label").hidden = true;
  showIndex(Number($("imgscrub").value));
});

// The scrubber's position and range follow the view. When the shown scene
// no longer passes the filters the thumb detaches: the image stays on the
// map, the label says why, and prev/next step in from the last position.
function renderScrubber() {
  const view = currentView();
  const scrub = $("imgscrub");
  scrub.max = String(Math.max(0, view.length - 1));
  scrub.disabled = !shown || view.length < 2;
  const at = shown ? indexOfId(view, S.displayedId) : -1;
  if (at >= 0) S.detachedAt = at;
  scrub.value = String(at >= 0 ? at : Math.max(0, clampIndex(view, S.detachedAt)));
  const detached = !!shown && view.length > 0 && at < 0;
  scrub.toggleAttribute("data-detached", detached);
  if (detached) {
    $("imgnav-label").hidden = false;
    $("imgnav-label").textContent = `${shown.id} · outside the current filters`;
  }
}
```

style.css: `#imgscrub[data-detached] { opacity: 0.5; }` (a dashed-thumb style per engine is not portable; reduced opacity says "detached" everywhere).

Prefetch neighbours on commit — at the end of `showIndex`:

```js
  const view2 = currentView();
  const at2 = indexOfId(view2, row.id);
  (window.requestIdleCallback ?? setTimeout)(() => {
    for (const n of [view2[at2 - 1], view2[at2 + 1]]) if (n) thumbBitmapFor(n);
  });
```

- [ ] **Step 2b: Confirm `applyNow`'s `nav` flag calls both.** `renderScrubber` and `syncNavButtons` now exist as module functions; remove the `globalThis.…?.()` guards from Task 3's `applyNow` and call them (and `renderResults`) directly.

- [ ] **Step 3: Manual test** (Review Focus 3). Search a tile. Drag the scrubber: the label counts through, thumbnails flash over the map live, the matching card gets a dashed outline; release: the full COG loads for that scene. Now narrow max-cloud below the shown scene's cloud: the image **stays**, the scrubber dims (detached), the label says "outside the current filters", and › steps to a passing scene near the old position. Widen the filter: the thumb re-attaches at the scene's index.

- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/app.js apps/explorer/style.css
git commit -m "feat: scrub the search results on the image strip

A drag previews each scene's thumbnail over its footprint and the
release loads the COG. A filtered-out shown scene detaches the thumb
and keeps the image; prev/next re-enter from the last position.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 12: remove the Image #13 elements (default: the card band chips)

**Default assumption (spec, Open questions): the TCI/B04/B08/SCL chip buttons and their ↗ links on every card. Confirm with the plan reviewer before executing; if the target is different, this task's shape (delete one block + its styles) still applies.**

**Files:**
- Modify: `apps/explorer/app.js` (`buildCard`: the chips block, formerly 1737-1753)
- Modify: `apps/explorer/style.css` (`.chips` rules, if any remain unused)

- [ ] **Step 1: Delete the chips block** in `buildCard` (the `const chips = el("span", "chips");` block through `cap.append(actions, chips);` → `cap.append(actions);`). Keep `sceneDirOrNull` only if still referenced (it is not — delete it and its comment; `sceneDirOf` stays, `showOnMap` needs it).
- [ ] **Step 2: Remove dead CSS** (`.chips` selectors in style.css).
- [ ] **Step 3: Manual test.** Cards show thumbnail, id, date · cloud, and "Show on map" only. The band mapper on the image panel still switches bands for the shown scene.
- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/app.js apps/explorer/style.css
git commit -m "feat: drop the per-card band chips

The band mapper on the image panel covers band switching. Cards keep
the thumbnail, the id, the date and cloud line, and Show on map.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 13: mobile pass

**Files:**
- Modify: `apps/explorer/style.css` (peek height ~248; `#imgnav` in the sheet; `#imgscrub` touch-action)
- Modify: `apps/explorer/app.js:1150` (`snapHeight` peek 110 → 150)

- [ ] **Step 1: Peek shows the nav.** style.css phone block: the `peek` height 110px → 150px (find the `data-snap="peek"` rule near line 248). app.js:1150: `peek: 110` → `peek: 150`. Add `#imgscrub { touch-action: none; }` so the sheet body cannot steal a horizontal scrub.
- [ ] **Step 2: Manual test at 390×844** (devtools device mode). All three snaps work; at peek the "Showing …" line **and** the nav strip are visible; the scrubber drags without moving the sheet; the info icon toggles on tap; a bar tap runs the month search; "show more" works in the sheet scroller; `scrollIntoView` still skips at peek.
- [ ] **Step 3: Commit.**

```bash
git add apps/explorer/style.css apps/explorer/app.js
git commit -m "fix: mobile pass for the year redesign

Peek grows to 150 px so the image nav strip shows with the cogbar.
The scrubber owns its horizontal drags on touch.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 14: comments, docs, and the full gates

**Files:**
- Modify: `apps/explorer/app.js:1-29` (header comment) and any stale section comments (the old `#controls`/`#query`/scene-query blocks)
- Gate: `python3 tools/rails/experiments/check_app.py`, `node --test apps/explorer/`, `CI_LIGHT=1 python3 tests/run_all.py`

- [ ] **Step 1: Rewrite the stale comments** (STE). The app.js header must say: the year is the unit; a tile click reads the year's parts once through `sceneRows`; the sliders and the date window filter cached rows in the page; the map paints from aggregated month slices. The old month-slice and "the button stays for re-runs" sentences go.
- [ ] **Step 2: Check the docs.** `docs/query-performance.md` and `docs/mobile-layout-report.md`: add a short note only where they describe the removed Search button or the month-scoped flow (read them first; do not rewrite them).
- [ ] **Step 3: Run everything.**
  - `node --test apps/explorer/` — expected: pass.
  - `python3 tools/rails/experiments/check_app.py` — expected: all cases match DuckDB.
  - `CI_LIGHT=1 python3 tests/run_all.py` — expected: `all gates passed`.
- [ ] **Step 4: Commit.**

```bash
git add apps/explorer/app.js docs/
git commit -m "docs: rewrite the explorer comments for the year flow

The header and section comments describe the year-based search, the
in-page filters and the aggregated paint. The month-slice wording and
the Search button references go.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Execution notes

- Tasks 1-2 are independent of each other; everything after is sequential.
- The slow gate runs three times total (Tasks 1, 6, 14). Every other task verifies with `node --test` and/or the served page.
- If `dayRange().rebound()` turns out to fire `onChange` (rangeslider.js) — making the explicit `S.from`/`S.to` writes in `buildDateSlider`/`setWindow` redundant — keep the explicit writes anyway; `scheduleApply` coalesces the double call for free and the state is then correct even if `rebound` changes.
- If the `#imgnav` strip crowds the desktop image panel, the `#imgnav-label` line may move inside `#cogbar`; keep the ids.
