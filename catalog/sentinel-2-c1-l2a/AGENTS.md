# AGENTS.md — sentinel-2-c1-l2a

Guidance for AI agents and automated clients querying this collection.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file. An agent acting on an invented
column name or an invented join key produces a confident wrong answer, and
nothing downstream catches it.

Every year from 2015 on is published (the collection.json counted
30,402,025 rows, 2015-10 to 2026-09, when read on 2026-09-22). This is the
uniform record: query it first, and use
[`sentinel-2-l2a`](../sentinel-2-l2a/AGENTS.md) when you need Earth
Search's original index instead.

## What this is

One row per Sentinel-2 Collection 1 L2A scene in the AWS Earth Search item
index (collection `sentinel-2-c1-l2a`: ESA's reprocessing of the archive to
one processing baseline), republished as partitioned GeoParquet. The imagery
is not here: it is in the public `e84-earth-search-sentinel-data` bucket on
AWS, and every COG URL is already in the `assets` column of the row that
describes it.

```
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=YYYY/items.parquet   every year, one file
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=YYYY/live.parquet    any year may have one
```

A year is one `items.parquet`. There is no zone split and no zone column:
the archive of a year, whatever its size, is that one file. Any year may
also carry `live.parquet`, the tail the daily refresh has appended since the
last fold; a fold every month or two merges it into `items.parquet` and
empties it. The two files of a year do not overlap, with one exception: a
scene ESA reprocessed keeps its id with a newer `s2:generation_time`, and
live holds the newer copy beside the archive's older one until the next
fold. Dedupe on `id` keeping the highest `s2:generation_time` when you glob
a year; it is safe and removes nothing else. The `QUALIFY` below is that
dedupe; without it the count is high by the number of scenes reprocessed
since the last fold, which is a fine approximation for a coverage question
and the wrong answer for an inventory one:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_url_style = 'path';
SET TimeZone = 'UTC';

SELECT year, count(*) AS scenes, min(datetime) AS first, max(datetime) AS last
FROM (
  SELECT year, id, datetime
  FROM read_parquet('s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=*/*.parquet',
                    hive_partitioning = true)
  WHERE year IN (2017, 2018)
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY "s2:generation_time" DESC NULLS LAST) = 1
)
GROUP BY year ORDER BY year;
```

`hive_partitioning = true` exposes `year` as an INTEGER column that is not
stored in the files. Filter on it first; it is the only filter that skips
whole files. The glob goes through the anonymous `s3://` door because DuckDB
expands `*` only where it can list the store: over plain `https://` it stops
with "Globs (`*`) for generic HTTP file are not supported", so an `https://`
read names the file (below).

## Query pattern

Filter in this order. Each step removes more data than the next one can.

1. `year IN (…)` — partition pruning, skips whole files.
2. `_tile = '31UFU'` — the spatial join key, the first sort key, and the
   cheapest spatial filter there is: a tile's year is one contiguous run,
   so this filter prunes to the one or two row groups that hold it. A tile
   id is stable for the life of the mission.
3. A `datetime` range — the second sort key, so it trims the tile's run;
   on its own (no tile) it prunes nothing, because every month is in
   nearly every row group. `_month` still works as a filter, with the same
   caveat.
4. `"eo:cloud_cover" < 10` — the usual last cut.

```sql
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover",
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=2021/items.parquet')
WHERE _tile = '31UFU'
  AND datetime BETWEEN '2021-08-01' AND '2021-10-31 23:59:59'
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover", id
LIMIT 20;
```

Column names with a colon are not identifiers. Quote them: `"eo:cloud_cover"`,
not `eo:cloud_cover`. `_tile`, `_month` and `_hilbert` need no quotes.

**Row groups.** Every file is sorted `(_tile, datetime)` — tile-major — in
uniform row groups at a target of 6,000 rows (6,144 as DuckDB writes them).
One tile's year is a single contiguous run in acquisition order, so any
tile-and-window query admits the one or two groups that hold the run: one
range request each, a few hundred KB, whatever the window. This is the
layout the search-latency experiments behind issue #9 measured as fastest
for tile-window searches; the month-major order of the older
`sentinel-2-l2a` parts scatters a tile's year across its twelve month
sections and admits a group per month instead. A month filter alone does
not prune here (every month is in nearly every group), and a bbox filter
inside a tile has no Hilbert locality to lean on; both are correct, just
not cheap. Lead with the tile.

Use an `ST_Intersects` filter on `geometry` when you have a real polygon and no
tile id. It works, and it is slower than the tile filter, because it has to
decode geometries the tile filter never reads. `proj:centroid` (a struct of
`lat`, `lon`) is a cheap point stand-in when the footprint is more than you
need.

`datetime` is TIMESTAMP WITH TIME ZONE and every value is UTC. DuckDB renders
it in the session time zone, so run `SET TimeZone='UTC'` before you compare a
rendered string to a date, or a scene acquired at 23:50 UTC reports the next
day west of Greenwich and the previous day east of it.

## Schema

The published column list is `tools/s2c1_schema.py` in the source repository,
and the collection's `table:columns` is generated from it, as is this table.
That is the authority; the notes after it cover what a description cannot
say on its own.

| Column | Type | Description |
| --- | --- | --- |
| `thumbnail_url` | string | Preview JPEG on the e84-earth-search-sentinel-data bucket. |
| `type` | string | Always 'Feature'. |
| `stac_version` | string | STAC version of the source item. |
| `stac_extensions` | list<string> | Extension schema URIs of the source item. |
| `id` | string | Earth Search item id, e.g. S2B_T31UET_20260921T105030_L2A. |
| `bbox` | list<double> | Item bounding box [w, s, e, n], CRS84. |
| `links` | STRUCT(href VARCHAR, rel VARCHAR, title VARCHAR, "type" VARCHAR)[] | Source item links (canonical et al.); paging links are stripped. |
| `collection` | string | Always 'sentinel-2-c1-l2a'. |
| `datetime` | timestamp[us, tz=UTC] | Acquisition datetime, UTC. |
| `created` | timestamp[us, tz=UTC] | When Earth Search created the item; the incremental-fetch lookback field. |
| `updated` | timestamp[us, tz=UTC] | When Earth Search last updated the item. |
| `platform` | string | sentinel-2a / sentinel-2b / sentinel-2c. |
| `constellation` | string | Always 'sentinel-2'. |
| `instruments` | list<string> | Always ['msi']. |
| `grid:code` | string | MGRS grid code, e.g. MGRS-31UET. `_tile` is the bare id. |
| `mgrs:utm_zone` | int64 | UTM zone number, 1-60. |
| `mgrs:latitude_band` | string | MGRS latitude band letter. |
| `mgrs:grid_square` | string | MGRS 100 km grid square. |
| `proj:epsg` | int64 | UTM EPSG code of the scene grid. |
| `proj:centroid` | STRUCT(lat DOUBLE, lon DOUBLE) | Scene centroid, CRS84. |
| `eo:cloud_cover` | double | Scene cloud cover percentage, 0-100. |
| `s2:tile_id` | string | ESA tile (granule) id. |
| `s2:degraded_msi_data_percentage` | double | Scene classification percentage. |
| `s2:nodata_pixel_percentage` | double | Nodata share; high values = partial scenes. |
| `s2:saturated_defective_pixel_percentage` | double | Scene classification percentage. |
| `s2:dark_features_percentage` | double | Scene classification percentage. Present on processing baselines <= 05.10; NULL on 05.11+. |
| `s2:cloud_shadow_percentage` | double | Scene classification percentage. |
| `s2:vegetation_percentage` | double | Scene classification percentage. |
| `s2:not_vegetated_percentage` | double | Scene classification percentage. |
| `s2:water_percentage` | double | Scene classification percentage. |
| `s2:unclassified_percentage` | double | Scene classification percentage. |
| `s2:medium_proba_clouds_percentage` | double | Scene classification percentage. |
| `s2:high_proba_clouds_percentage` | double | Scene classification percentage. |
| `s2:thin_cirrus_percentage` | double | Scene classification percentage. |
| `s2:snow_ice_percentage` | double | Scene classification percentage. |
| `s2:product_type` | string | Always 'S2MSI2A'. |
| `s2:processing_baseline` | string | e.g. 05.13. |
| `s2:product_uri` | string | ESA product name. |
| `s2:generation_time` | string | Processing generation time; dedupe tiebreak. |
| `s2:datatake_id` | string | ESA datatake id. |
| `s2:datatake_type` | string | e.g. INS-NOBS. |
| `s2:datastrip_id` | string | ESA datastrip id. |
| `s2:reflectance_conversion_factor` | double | Sun-distance reflectance factor. |
| `view:azimuth` | double | Mean viewing azimuth angle, degrees. |
| `view:incidence_angle` | double | Mean viewing incidence angle, degrees. |
| `view:sun_azimuth` | double | Mean solar azimuth angle, degrees. |
| `view:sun_elevation` | double | Mean solar elevation angle, degrees. |
| `storage:platform` | string | Always 'AWS'. |
| `storage:region` | string | Always 'us-west-2'. |
| `storage:requester_pays` | bool | Always false. |
| `processing:software` | string | The upstream processing:software object (name -> version), verbatim, as a compact JSON string. |
| `earthsearch:payload_id` | string | Earth Search ingest payload id. |
| `assets` | string | The upstream STAC assets object, verbatim, as a compact JSON string. Parse with json_extract or JSON.parse. |
| `_month` | int8 | month(datetime). Query helper, not STAC; not a sort key here (rows are ordered (_tile, datetime)). |
| `_hilbert` | uint32 | ST_Hilbert(geometry, world bounds). Query helper, not STAC; not a sort key here. |
| `_tile` | string | MGRS tile id from grid:code, e.g. 31UET. THE spatial join key and the first sort key; datetime is the second. |
| `geometry` | geometry | Scene footprint, CRS84. |

**Spatial.** `geometry` is the scene footprint in CRS84. `bbox` is the same
footprint as `[w, s, e, n]`. There is no `s2:mgrs_tile` in Collection 1
items: the tile arrives as `grid:code` (`MGRS-31UET`) and as the three
`mgrs:*` parts, and this mirror adds `_tile`, the bare id (`31UET`,
`grid:code` without its `MGRS-` prefix). `_tile` is THE join key: scenes
over one place share it across all years, and it is the same id the
`sentinel-2-l2a` collection calls `s2:mgrs_tile` and the `stats-c1`
collection calls `mgrs_tile`.

**Time.** `datetime` is the acquisition instant, UTC. `s2:generation_time` is
when ESA processed the product, not when the satellite looked. `created`
and `updated` are when Earth Search ingested and last touched the item;
`created` is what the daily refresh looks back on, because reprocessed old
scenes arrive with an old `datetime` and a new `created`.

**Three added columns, and they are helpers, not STAC.** They exist so that
readers can prune and join:

- `_tile` — the MGRS tile id, above. The first sort key; `datetime` (an
  upstream column) is the second, and together they are the whole order.
- `_month` — `month(datetime)`, 1 to 12. A filter convenience only; not a
  sort key in this collection (it is the first sort key in
  `sentinel-2-l2a`, which is why the column is kept with the same meaning).
- `_hilbert` — `ST_Hilbert(geometry, world bounds)`. Kept for parity with
  `sentinel-2-l2a`; not a sort key here.

Nothing upstream publishes these three columns. Do not pass them on as STAC
properties, and do not treat `_hilbert` as meaningful on its own — it is a
position on a space-filling curve, not a measurement.

**Two upstream objects are strings.** `processing:software` is the upstream
name-to-version map as a compact JSON string (its keys change between
baselines, which is why it is not a struct); `assets` is the JSON string
described below. `proj:centroid` and `links` are the only nested columns.

**Everything else** is the upstream STAC property under its upstream name,
unchanged. Nothing is reconstructed or derived from another field.

## NULLs you will meet

`s2:dark_features_percentage` is NULL on every scene processed at baseline
05.11 or later — ESA dropped the class from the scene classification, and
the column is present on baselines 05.00–05.10 only (measured on one item
per year across 2017–2026 when the schema was frozen). A query that requires
it silently drops every recently processed scene. `s2:processing_baseline`
says which case a row is.

A property the schema does not know (something Earth Search adds after the
freeze) is dropped from the row and counted by the fetch, never invented.

## Coverage

Earth Search's `sentinel-2-c1-l2a` held 30,391,138 items when it was counted
on 2026-09-21, by year of acquisition: 2015 200 · 2016 222 · 2017 24,664 ·
2018 1,329,973 · 2019 3,166,919 · 2020 4,013,725 · 2021 4,055,717 · 2022
283,705 · 2023 4,249,728 · 2024 4,369,942 · 2025 5,065,361 · 2026 3,830,982
(to date). The earliest item is acquired 2015-10-22.

Those counts move. ESA's reprocessing is ongoing, so the thin years (2015–
2017, and 2022 at a fraction of its neighbours) are years it has not
reached or finished, and they grow with recent `created` timestamps. That
is the source's state, not a gap introduced here. Do not report "few scenes
in 2022" as an observation about Sentinel-2 — the mission was acquiring at
full rate; the index has not caught up. Check the per-year items
(`year=YYYY/YYYY.json`) for each year's measured row count and time range
before you conclude anything about a period, and expect a year's count to
be higher next month.

## The `assets` column

`assets` is a VARCHAR holding the upstream STAC assets object verbatim, as a
compact JSON string. It is a string on purpose: deeply nested structs make a
Parquet file hard for some readers to open, and a string keeps every reader's
schema flat. The cost is that you parse it.

```sql
json_extract_string(assets, '$.visual.href')   -- one href
json_extract(assets, '$.red')                  -- one asset object
json_keys(assets)                              -- every key on this scene
```

Keys present on a Collection 1 scene (the 23 keys of the upstream
`item_assets` template, all present on the frozen sample item):

| Group | Keys |
| --- | --- |
| Bands, COG | `coastal` `blue` `green` `red` `rededge1` `rededge2` `rededge3` `nir` `nir08` `nir09` `swir16` `swir22` |
| Derived rasters | `visual` (true colour), `scl` (scene classification), `aot`, `wvp`, `cloud` (cloud probability, 20 m), `snow` (snow probability, 20 m), `preview` (`L2A_PVI.tif`) |
| Preview | `thumbnail` (`L2A_PVI.jpg`; also flat in the `thumbnail_url` column) |
| Metadata | `granule_metadata`, `product_metadata`, `tileinfo_metadata` |

There are no JPEG 2000 assets in Collection 1, unlike `sentinel-2-l2a`.
Every href sits under one directory per scene:
`https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/<zone>/<band>/<sq>/<year>/<month>/<id>/`
(zone, latitude band and grid square being the three parts of `_tile`, the
month unpadded), with the band files named `B04.tif`, `TCI.tif`, `SCL.tif`,
`CLD_20m.tif`, `SNW_20m.tif`, `L2A_PVI.jpg` and so on. That directory also
holds `<id>.json`, the canonical STAC item (the row's `links` carry it as
`rel:canonical`).

Each asset object carries `href`, `type`, `title`, `roles`, and for the raster
assets `gsd`, `eo:bands`, `raster:bands`, `proj:shape` and `proj:transform`.
The `proj:*` values are per scene and correct only for that scene.

The collection's `item_assets` block mirrors the stable part of this — the band
metadata per key — for STAC clients that expect it at collection level. It is
documentation. The per-item `assets` value is the authority, and it is the only
place the hrefs exist.

Do not build an asset URL from a template. Read the href.

## Dedupe

A scene can be fetched more than once: the daily refresh re-reads a lookback
window on `created`, and a reprocessed product keeps its id. Rows are
deduped by `id`, keeping the highest `s2:generation_time` (`NULLS LAST`),
when each year file is built and again at each fold. So `id` is unique
within a file. Between folds, a reprocessed scene can sit in `live.parquet`
with a newer `s2:generation_time` than the copy in `items.parquet`, so
dedupe on `id` keeping the highest `s2:generation_time` when you glob a
year (the `QUALIFY` in the glob snippet above). Across the whole table,
treat `id` as unique and report it if you ever
find otherwise.

## Cadence

Three clocks, from the workflows in the repository's `.github/workflows/`
and the cluster scripts in `tools/rails/`:

- **Daily, 03:42 UTC** (`refresh-daily`, its Collection 1 job): the last
  five days of Earth Search by `created`, appended to the `live.parquet` of
  every year the slice touches, with the ids the year's `items.parquet`
  already holds dropped. The year items and the collection are restamped
  from the published files on the same run, so `table:row_count` and the
  extents describe the bucket, not the commit.
- **Daily, on the same run**: the stats collection
  ([`stats-c1`](../stats-c1/AGENTS.md)) has every touched year recomputed
  and spliced into `mgrs-monthly.parquet`.
- **Every month or two, and at year end**, by a person on the RAILS
  cluster (`tools/rails/fold_live.sbatch`): each `live.parquet` is merged
  into its year's `items.parquet` and emptied. There is no monthly
  consolidation on GitHub for this collection. Between folds a year's two
  files together are the year; the dedupe above makes the count exact.

The daily job runs only while the repository variable `C1_LIVE_ENABLED` is
set. With it unset this collection changes only when a person commits and
publishes it.

## What this collection does not do

It does not filter, reclassify or interpolate anything Earth Search publishes.
It adds `_month`, `_hilbert`, `_tile` and the flat `thumbnail_url`, and nothing
else. There is no STAC API in front of it: query the Parquet directly, over
HTTP range requests, with no key and no rate limit.

Structural links resolve relative to the object that carries them. Objects here
carry no `self` link, so a client tracks its own location.
