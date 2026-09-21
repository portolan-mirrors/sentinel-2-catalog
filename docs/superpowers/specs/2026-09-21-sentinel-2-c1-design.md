# Sentinel-2 Collection 1 — second collection design

**Date:** 2026-09-21
**Status:** Approved (design review in chat, 2026-09-21)
**Extends:** `2026-09-15-sentinel-2-catalog-design.md` (the first collection;
everything not restated here is unchanged)

## Purpose

Add Earth Search's **`sentinel-2-c1-l2a`** (ESA Collection 1 reprocessing) as
a second collection of this catalog, built on the TGI RAILS Slurm cluster
instead of GitHub runners, with the current best layout for range-read
queries. The explorer switches to Collection 1 once its backfill is complete;
the original `sentinel-2-l2a` collection stays published and refreshed as is.

## Facts about the source (measured 2026-09-21)

* Earth Search API `collections=sentinel-2-c1-l2a`; 30,391,138 items. By
  year: 2015 200 · 2016 222 · 2017 24,664 · 2018 1,329,973 · 2019 3,166,919 ·
  2020 4,013,725 · 2021 4,055,717 · **2022 283,705** · 2023 4,249,728 ·
  2024 4,369,942 · 2025 5,065,361 · 2026 3,830,982 (to date). ESA's
  reprocessing is ongoing: old years grow, with recent `created` timestamps.
* Item ids `S2B_T31UET_20260921T105030_L2A`; tile from `grid:code`
  (`MGRS-31UET`) or `mgrs:utm_zone` + `mgrs:latitude_band` +
  `mgrs:grid_square`. No `s2:mgrs_tile`.
* Properties beyond the first collection: `grid:code`, `mgrs:*`,
  `s2:tile_id`, `s2:product_uri`, `processing:software`, `view:*`,
  `storage:*`, `earthsearch:payload_id`, `created`, `updated`, plus the
  `s2:*` percentages. Assets: the bands, `visual` (TCI), `cloud`, `snow`,
  `preview`, `thumbnail` (`L2A_PVI.jpg`), metadata XML/JSON. All at
  `https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/<zone>/<band>/<sq>/<year>/<month>/<id>/`.
* The bucket lists anonymously; each scene directory holds `<id>.json`
  (the canonical STAC item), so the bucket-crawl repair lane works. The
  inventory bucket `e84-earth-search-sentinel-data-inventory` exists.
* RAILS: Slurm, account `bgtj-tgirails`, partitions `cpu`/`cpu_amd`,
  `--exclusive` nodes, outbound S3, micromamba env on shared `/u`, login by
  ssh ControlMaster with Kerberos + Duo; work on the login node is reaped.

## Decisions

| Question | Decision |
|---|---|
| Collection id | `sentinel-2-c1-l2a`, directory `catalog/sentinel-2-c1-l2a/`. |
| Schema | Faithful Collection 1 property names, frozen from a sample into `tools/s2c1_schema.py` (the same discipline as `s2_schema.py`); `assets` verbatim JSON string; `links` self/canonical; helper columns `_month`, `_hilbert`, **`_tile`** (5-char MGRS id from `grid:code`); `geometry` last. |
| Tile column | Tools take the tile column as a parameter: `s2:mgrs_tile` for the first collection, `_tile` for C1. A `CollectionConfig` (id, schema module, tile column, id regex, bucket base + key pattern, thumbnail asset key, API collection) drives fetch, repair, audit, build, stats and the app. |
| Layout | **One `items.parquet` per year** (no zone parts), plus `year=YYYY/live.parquet` for the tail. Sort `(_month, _tile, _hilbert)`. Row groups **cut on `_month` boundaries** at a target of 20,000 rows (a group never spans two months). zstd 18. GeoParquet 2.0 via `gpio` on a whole compute node. |
| Layout experiments | Before the big years: measure on 2018 (1.3M rows) the footer size with and without column statistics on `assets`, and gpio vs DuckDB parallel COPY wall time; record in `docs/query-performance.md`. Further row-group/partition experiments are a separate RAILS job (`tools/rails/experiments/`) after the backfill. |
| Fetch | API lane first on RAILS: a Slurm array, one task per month, each task runs `s2_fetch` into node-local scratch and uploads `slices/YYYY-MM.parquet` to S3; the day-granular bucket-repair lane (`s2_repair`) fills API holes; `s2_audit` against the inventory closes the year. |
| Publish | RAILS uploads year files and live to source.coop directly with a `source-coop` AWS profile (an IAM user in the bucket's account, policy scoped to the catalog prefix; the user creates it). Metadata stays in git and is published by `publish-catalog.yml` as today. |
| Sync after backfill | GitHub `refresh-daily` gains a C1 job: query Earth Search on **`created`/`updated`** over the lookback window (not `datetime`, so late-reprocessed old scenes are caught), append into `year=YYYY/live.parquet` at **zstd 3** (one live per year touched; a live may hold a whole year), dedupe against the archive by id + generation time. **No monthly consolidation.** A RAILS **fold** job, run by the user every month or two and at year end, merges each live into its year file at zstd 18 and empties live. |
| Docs | `tools/README.md` gets a **Sync & backfill** section: the GitHub technique (backfill → publish-backfill → refresh-daily → consolidate-month, kept unchanged, marked "not used for C1", with the steps to switch C1 back to it), and the RAILS technique with the periodic fold duty and exact commands. |
| Stats | Second stats collection `catalog/stats-c1/` with the same products (`mgrs-monthly.parquet`, `months/YYYY-MM.parquet`, `timeline.parquet`, `mgrs.pmtiles`) built by the same tools with the tile-column parameter; `publish-stats` and `refresh-daily` run it for both collections. |
| Explorer | Collection switch: `?collection=` query param and a small toggle; default `sentinel-2-c1-l2a` once its backfill and stats are complete (until then, the first collection). Per-collection config in the app: parts glob/year list, tile column, thumbnail/COG URL derivation (`L2A_PVI.jpg`, `TCI.tif`, band files, `cloud`/`snow` masks selectable in the band mapper). |
| Reprocessed duplicates | C1 reprocesses a scene under the same id with a newer `s2:generation_time`; the existing dedupe rule (highest generation time wins) applies unchanged. |

## Repository additions

```
tools/s2c1_schema.py          frozen C1 schema (+ helper columns)
tools/collections.py          CollectionConfig for both collections
tools/rails/                  Slurm lane
  README.md                   login, deploy, env, credentials, run order
  env.sh                      PATH/AWS/scratch conventions (sourced by every sbatch)
  fetch_months.sbatch         array job: one month per task → slices/YYYY-MM.parquet on S3
  repair_month.sbatch         bucket-crawl repair for one month
  build_year.sbatch           slices → year file (gpio, zstd 18) → upload
  fold_live.sbatch            merge live into year files, empty live, restamp
  audit_year.sbatch           inventory audit for one year
  experiments/                layout experiments (after backfill)
catalog/sentinel-2-c1-l2a/    collection.json, README.md, AGENTS.md, year items
catalog/stats-c1/             stats collection
```

`tools/s2_build.py` gains the month-aligned row-group writer and the
`--collection` switch; `s2_fetch`, `s2_repair`, `s2_audit`, `s2_stats`,
`make_items`, `make_collection`, `make_stats_collection` gain `--collection`.
Nothing about the first collection's outputs changes.

## Month-aligned row groups

`gpio sort column` writes uniform row groups. To cut groups on `_month`
boundaries the build stages the sorted year, then writes it as one Parquet
file whose row groups are the concatenation of per-month chunks of ≤ 20,000
rows (a month with 45,000 rows becomes groups of 20,000 / 20,000 / 5,000). The
writer keeps gpio's GeoParquet 2.0 file metadata (copied from a gpio-written
probe of the same table) and column order. `gpio check all` gates the result
as before. Verified by a test that reads `parquet_metadata` and asserts every
row group's `_month` min equals its max.

## Testing

* Unit: schema freeze for C1 (sample fixture in C1 shape); `CollectionConfig`
  resolution; `normalize` for C1 ids/tiles; `created`-based lookback query
  body; month-aligned writer (group boundaries, metadata preserved, gpio check
  passes); fold merges and empties live; stats with `_tile`.
* RAILS: every sbatch supports `--dry-run` (prints the plan, touches
  nothing) and a `SMOKE=1` mode that processes one small month (2017-07) end
  to end into a scratch prefix.
* First real year: 2017 (24,664 items) through fetch → build → upload →
  items/collection metadata → stats → explorer, before any large year.

## Sequencing

1. Tooling (`collections.py`, C1 schema, `--collection` everywhere,
   month-aligned writer, `created` lookback) with tests — GitHub CI green.
2. RAILS lane scripts + README; user applies the IAM policy and places the
   profile on `/u`; smoke month.
3. 2017 end to end; then 2018 with the layout experiment; then the array
   fetch for all months and year builds largest-last.
4. Catalog metadata for C1 committed and published; stats-c1; explorer
   switch (default flips to C1 when complete).
5. `refresh-daily` C1 job enabled; first fold on RAILS documented as done.

## Needs from the user

* IAM user/keys for RAILS (policy in the plan); Duo login for each RAILS
  session and for submitting jobs (the controller cannot log in).
* Confirmation to flip the explorer default when C1 is complete.

## Out of scope

* Reprocessing the first collection (issue #9) — the month-aligned writer
  built here is what that job will use.
* EOPF Zarr (issue #1).
