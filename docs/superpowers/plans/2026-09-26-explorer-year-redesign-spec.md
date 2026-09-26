# Explorer year redesign — spec

Date: 2026-09-26. Source: user request (verbatim requirements below), plus two
design investigations (a code-mapping pass and an interaction-architecture
pass) whose conclusions are folded in.

## What the user asked for

1. The top selector becomes **year** (it is month today). Below it the same
   "Color by" selector, and below that the **date slider spanning the whole
   year**. Dragging the date slider updates the map tile colors live, exactly
   like the max-cloud / min-coverage sliders do today.
2. The explanatory text under the filter sliders becomes a small **info icon**
   you can hover (or tap) to read, so the text stops taking vertical space
   (user's Image #12).
3. **Remove the "Search scenes" button.** A search starts by clicking a map
   tile (which also builds the monthly scenes graph). Clicking a bar in the
   monthly scenes graph starts a search for that month: it switches the top
   year selector if the bar is from another year and constrains the date
   slider to that month.
4. When a search is active, the **result cards re-filter live from the
   sliders** (max cloud, min coverage, and the date slider, which narrows
   within the year). The search fetches the **whole year** of scenes for the
   tile so narrowing is client-side and instant.
5. Results render at most **15 cards initially** with a "show more"
   interaction and lazy thumbnail loading.
6. Expose a **sort** control: default is min cloud (labeled clearly), plus
   min coverage and date.
7. Remove the elements in the user's Image #13 (see Open questions).
8. The loaded-image control strip (Image #14) gains:
   - a **"Zoom to" button**, active only when the image is not clearly
     visible (after panning or zooming away);
   - **forward/back buttons** stepping through results in the current sort
     order, showing the adjacent scene's date on hover, and outlining the
     corresponding result card;
   - a **scrubber slider** across the active (filtered, sorted) results that
     changes the displayed image on drag, with forward/back advancing one
     step.

## Facts that shape the design

- **A whole-year search costs the same as a one-month search today.** Scene
  data is one GeoParquet per year (Collection 1: `items.parquet` +
  `live.parquet`; L2A: one zone part per year). `sceneSearch` admits row
  groups by the tile column only and applies the date window client-side
  after decode (`search.js:273-276`). The date window has never influenced a
  byte fetched. Fetching the year = removing the date predicate and the
  `slice(0, 30)` from what the app consumes.
- **`tools/rails/experiments/check_app.py` is the gate.** It drives the real
  page headless and compares the rendered card ids exactly against DuckDB's
  `ORDER BY cloud, id LIMIT 30`. It requires: the literal spellings
  `const map = new maplibregl.Map({` and `const hitIndex = {` in app.js; the
  `#date0`/`#date1` inputs (set by direct `.value` writes, no events);
  `#maxcloud`/`#mincoverage` responding to `input` events; a map click
  writing `Tile <id>` into `#query .hint`; `#sql` receiving text containing
  `range-read plan`; and `#results` containing card `<b>` ids or a `.hint`.
  It never touches `#run` or `#month`, so both can go. `sceneSearch`'s
  signature and output contract are also used by `harness.html`,
  `search_harness.html`, `measure_layout.py`, `measure_search.py` — the
  export must keep its exact behavior.
- **The choropleth has month granularity.** Map colors come from
  `stats/months/YYYY-MM.parquet` (one row per tile per month). A day-precise
  date slider therefore recolors the map at **month** granularity: the paint
  aggregates the months the window overlaps (a month partly inside counts
  wholly), while the card filter is exact to the day. The info tooltip and
  status line say so, or it reads as a bug.

## Design decisions

- One state object `S` in app.js is the single source of truth; a pure
  derived pipeline in a new `apps/explorer/results.js`:
  rawYearRows → filtered (cloud/coverage/date) → sorted → visible (first
  `S.shown`) → displayed index.
- The displayed image's identity is `S.displayedId` (a scene id), never an
  index. When filters exclude the displayed scene, the image **stays on the
  map**, the scrubber shows a detached state, and forward/back step into the
  filtered list from the last known position (`S.detachedAt`).
- Year stats aggregation per tile across the window's months:
  `min_cloud_cover = min`, `max_cover = max`, `scene_count = sum`,
  `median_cloud_cover = min` (best month — an approximation, noted in code).
- Year scene rows are cached per `(collection, tile, year)` keyed promise,
  capped at 8 entries; a failed fetch is forgotten so a retry works.
- The scrubber's drag shows a cheap thumbnail `BitmapLayer` preview over the
  scene bbox; the full COG loads only on release (`change` event).
- Month lock (from a bar click) rebounds the date slider to that month and
  shows a dismissable chip; exits: the chip's ✕, clicking the active bar
  again, or changing the year.
- The search reads the date window from the `#date0`/`#date1` inputs at tile
  click time (the gate writes them directly with no events).
- The gate driver is updated to read the full filtered view's ids from
  `window.S2` (capped at 30) instead of counting rendered cards, because only
  15 cards render initially.

## Open questions (flagged for review; defaults chosen)

- **Image #13** (the "just get rid of these" elements) could not be
  identified with certainty from the screenshots' text. Default assumption:
  the per-card band chip buttons (TCI / B04 / B08 / SCL and their ↗ download
  links) — plural, card-cluttering, and redundant once the band mapper is
  one click away. The removal is an isolated task so the target is cheap to
  swap if the assumption is wrong. Alternative candidates: the
  `#cogbar` "Showing <id> on the map" sentence, the `#apibox` panel, the
  legend ticks.
- "Sort by min coverage" ships as **most coverage first** ("fullest first"),
  since least-coverage-first orders the worst scenes first.
- The `#minscenes` slider stays, computed over the aggregated window, with
  its caveat moved into the info tooltip.
