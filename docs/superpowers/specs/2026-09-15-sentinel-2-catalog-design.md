# Sentinel-2 Portolan Catalog — Design

**Date:** 2026-09-15
**Status:** Approved (design review in chat, 2026-09-15)
**Repository:** github.com/portolan-mirrors/sentinel-2-catalog
**Published to:** https://source.coop/portolan-mirrors/sentinel-2-catalog

## Purpose

A git-backed Portolan catalog mirroring the **Earth Search / AWS Sentinel-2 L2A
archive as STAC-GeoParquet**. The catalog carries no imagery: the COGs stay on
AWS (`sentinel-cogs` bucket, Registry of Open Data); this catalog publishes the
*item index* as partitioned GeoParquet plus small aggregate products, so that
clients query the full archive with plain Parquet reads — no STAC API, no rate
limits.

The driving use case is Fields of the World (FTW): given a field location and a
planting or harvest date window, find cloud-free Sentinel-2 scenes — from a
script or from an interactive app — without hitting a rate-limited API.

A second, explicit goal: an **explorer app that acts like there is an API
behind it but is powered only by static STAC-GeoParquet** (DuckDB-WASM range
reads + PMTiles).

## Decisions (settled in design review)

| Question | Decision |
|---|---|
| Asset hrefs | **Derive client-side.** Keep the slim schema (no `assets` struct). Document the deterministic COG href template in collection metadata and README; app and snippets build hrefs from `id` / `s2:mgrs_tile` / `datetime`. |
| Partitioning | **`year=YYYY/items.parquet` + `live.parquet`** for the current year (firms-catalog pattern). Sorted `(_month, _hilbert)`, zstd, 100k row groups. |
| Update cadence | **Daily** cron pulls recent items into the live part; monthly consolidation merges live into the year file. |
| Aggregates | **MGRS tile × month stats** (count, min/median cloud cover, best-item id) as `mgrs-monthly.parquet`, plus a geometry-only `mgrs.pmtiles` of tile footprints; the app joins stats to tiles by id. |

## Source data

* **Seed archive:** `https://data.source.coop/cholmes/stac-geoparquet-public/slim/s2-stac.parquet`
  — 28,146,662 items, single collection `sentinel-2-l2a`, covering
  **2015-07-04 → 2024-06-24**, ~7.0 GB. Slim schema: `thumbnail_url`,
  `geometry` (GEOMETRY, OGC:CRS84), STAC core fields, `datetime`,
  `eo:cloud_cover`, `proj:epsg`, `s2:*` scene metadata, `sat:*`. No `assets`.
* **Gap + ongoing:** Earth Search STAC API
  (`https://earth-search.aws.element84.com/v1`), collection `sentinel-2-l2a`.
  Gap to backfill: **2024-06-24 → present**, roughly 12M items (~450k/month).
* **Imagery:** referenced, not copied. COG template:
  `https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/{utm_zone}/{lat_band}/{grid_square}/{year}/{month}/{id}/{asset}.tif`
  where `{utm_zone}/{lat_band}/{grid_square}` parse from `s2:mgrs_tile`,
  `{year}/{month}` from `datetime` (month unpadded), `{id}` is the item id,
  and `{asset}` ∈ B01…B12, B8A, AOT, SCL, TCI, WVP (`.tif`) plus
  `thumbnail.jpg`. The template is verified against live Earth Search items in
  a test before being documented.

## Repository layout

Created from `portolan-sdi/portolan-catalog-template` (Mode A of the
`git-backed-catalog` skill; work through the template's `SETUP.md`). Since
`portolan-mirrors` is outside `portolan-sdi`, follow SETUP step 10 unless the
org already carries the ops contract — firms-catalog (same org) is the
reference for what to keep.

```
catalog/
  catalog.json                  # root; vcs + issues links to this repo
  README.md, AGENTS.md
  sentinel-2-l2a/
    collection.json             # item index: partition glob over year files + live
    README.md, AGENTS.md
  stats/
    collection.json             # mgrs-monthly.parquet, mgrs.pmtiles, styles/
    README.md
    styles/                     # MapLibre styles for the MGRS layer
apps/
  explorer/                     # static app, deployed via GitHub Pages
tools/                          # pipeline (below)
tests/                          # template gates + project tests
docs/superpowers/specs/         # this document
catalog.publish.yaml            # write_prefix/public_base/region for source.coop
```

Data lives only in the bucket:

```
sentinel-2-l2a/year=2015/items.parquet
...
sentinel-2-l2a/year=2026/items.parquet   # consolidated through last month-end
sentinel-2-l2a/year=2026/live.parquet    # daily-updated tail since consolidation
stats/mgrs-monthly.parquet
stats/mgrs.pmtiles
```

## Item index schema

Byte-for-byte the slim schema, plus two documented helper columns:

* `_month` (int, 1–12) and `_hilbert` — sort key `(_month, _hilbert)` so month
  pruning and spatial row-group pruning both work (firms-catalog rationale;
  documented in `catalog/sentinel-2-l2a/AGENTS.md`).

Newly fetched Earth Search items are normalized to exactly this schema; the
fetch tool asserts schema equality against the archive so drift fails loudly
instead of silently forking the schema. GeoParquet output settings (geometry
encoding, CRS, zstd level, row-group size) follow firms-catalog via
`geoparquet-io`.

## Pipeline

| Tool | Does |
|---|---|
| `tools/s2_fetch.py` | POST search against Earth Search for one datetime window; normalize features to the slim schema; write chunk Parquet. Resumable: existing chunks are skipped. |
| `tools/s2_repartition.py` | One-time: slice the 7 GB seed archive into sorted `year=YYYY/items.parquet` files (runs locally, not in CI). |
| `tools/s2_build.py` | Compact chunks into a sorted year or live part (shared by backfill, daily refresh, consolidation). |
| `tools/s2_stats.py` | Aggregate MGRS tile × month → `mgrs-monthly.parquet`; emit MGRS footprints (union of item geometries per tile, or the nominal MGRS polygon) → tippecanoe → `mgrs.pmtiles`. Supports touching only affected months so the daily run is cheap. |
| `tools/make_collection.py` | Regenerate collection extents, row counts, `updated` timestamps from the published parts (uploaded, not committed — firms pattern). |
| `tools/publish.py`, `tools/upload_data.py` | From the template, unmodified. |

## Workflows

* **`ci.yml`** (template): `rashid`, `stac-check`, `tests/run_all.py` on every PR.
* **`backfill.yml`**: month-slice matrix from 2024-06 to now. Each slice
  fetches + builds and uploads a workflow artifact (resumable across
  interruptions, firms pattern); modest parallelism (Earth Search has no hard
  transaction budget, but stay polite: max-parallel 2, retries with backoff).
  **`publish-backfill.yml`** then does the short, credentialed merge + upload.
* **`refresh-daily.yml`** (cron, daily): fetch a ~5-day lookback window (S2
  publishes with hours-to-days latency), rebuild `year=<Y>/live.parquet`,
  rebuild stats for touched months, regenerate extents/counts, upload.
  **Nothing is committed**; the repo keeps only the stable catalog definition.
* **`consolidate-month.yml`** (cron, monthly + manual): merge live into
  `year=<Y>/items.parquet`, reset live. Shares a concurrency group with the
  daily refresh so two writers never interleave.
* **`pages.yml`**: build and deploy `apps/explorer` to GitHub Pages.

## Aggregates (`stats/`)

`mgrs-monthly.parquet`: one row per MGRS tile × month —
`mgrs_tile, year, month, scene_count, min_cloud_cover, median_cloud_cover,
best_item_id (lowest cloud cover), best_item_datetime`. ~56k tiles × ~135
months ≈ small enough for the app to read whole columns for a selected month.

`mgrs.pmtiles`: tile-footprint geometries only, keyed by `mgrs_tile`. Stats
stay in Parquet; the app joins by id. This keeps the tileset static (rebuilt
rarely) while stats update daily.

S2's regular grid means spatial binning (H3/A5) adds nothing; the MGRS tile
*is* the bin, and the aggregation's job is to drive the timeline and filtering.

## Explorer app (`apps/explorer`)

Static site, MapLibre GL + pmtiles + DuckDB-WASM. No server, no API.

* **Map:** MGRS grid from `mgrs.pmtiles`, choropleth from `mgrs-monthly.parquet`
  for the selected month (scene count or min cloud cover).
* **Timeline:** monthly histogram for the current view / selected tile,
  driven entirely by the stats parquet.
* **Query panel (the hero):** click a point or pick a tile, set a date window
  (e.g. planting or harvest) and a max-cloud slider → DuckDB-WASM reads only
  the relevant `year=YYYY` files (`s2:mgrs_tile` predicate + month row-group
  pruning) → ranked scene list with `thumbnail_url` previews and derived COG
  links. The panel displays the equivalent "API request" it is answering, to
  make the point that no API exists.
* The same query ships in the collection README as a plain DuckDB snippet for
  FTW scripting.

## Testing

* Template gates: `tests/run_all.py` (setup, links, conformance) + `rashid` +
  `stac-check` in CI. Never widen the conformance allow-list to pass.
* `test_schema.py`: a live-fetched Earth Search item normalizes to exactly the
  archive schema.
* `test_hrefs.py`: the documented COG href template resolves (HTTP 200 HEAD)
  for a handful of items sampled across years.
* `test_stats.py`: aggregation correctness on a small fixture.
* App: smoke-tested against published data; no test infrastructure of its own
  in v1.

## Sequencing

1. Repo from template; catalog skeleton green in CI.
2. Repartition + upload the seed archive (2015 → 2024-06), locally.
3. Backfill 2024-06 → present via `backfill.yml`.
4. Stats + PMTiles; `stats/` collection.
5. Explorer app + Pages deploy.
6. Daily refresh + monthly consolidation live.
7. Register the catalog (portolan `register-catalog` skill).

## Needs from the user

* Push rights on `github.com/portolan-mirrors` (repo creation).
* Source Cooperative credentials for the
  `portolan-mirrors/sentinel-2-catalog` prefix, as repo secrets for the
  publish/refresh workflows and a local profile for the seed upload.

## Out of scope (v1)

* Copying or re-hosting imagery; L1C; other collections.
* A full `assets` struct in the parquet.
* H3/A5 spatial aggregation.
* Item-level PMTiles of all 40M footprints (the 60-day footprint tileset was
  considered and deferred; MGRS stats won the aggregation question).
