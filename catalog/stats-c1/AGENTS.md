# AGENTS.md — stats-c1

Guidance for AI agents and automated clients querying this collection.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file.

## What this is

Three parquet products and one tileset, joined by `mgrs_tile`, all generated
from the `sentinel-2-c1-l2a` item index by `tools/s2_stats.py` with
`--collection sentinel-2-c1-l2a` (the tile column it groups on is that
index's `_tile`; the output column is still `mgrs_tile`):

```
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats-c1/mgrs-monthly.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats-c1/months/YYYY-MM.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats-c1/timeline.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats-c1/mgrs.pmtiles
```

`mgrs-monthly.parquet` is one row per MGRS tile per month, the whole record.
`months/YYYY-MM.parquet` is that table filtered to one month and projected
to the paint columns; there is one per month the table has, and they are not
listed as assets -- the name pattern is the contract. `timeline.parquet` is
one row per month over all tiles. `mgrs.pmtiles` is one polygon per MGRS
tile on the vector layer `mgrs`, with an `mgrs_tile` feature attribute
suitable for `promoteId`. The polygon is the envelope of that tile's scene
footprints, not the true MGRS grid cell.

## Query pattern

Pick the file by the shape of the question:

- **One tile over time** -> `mgrs-monthly.parquet` with `WHERE mgrs_tile =`.
  The file is sorted by `(mgrs_tile, year, month)` in 50k-row groups, so
  DuckDB's row-group statistics turn that filter into a range read of one
  group's projected columns plus a small footer, not the whole download.
- **Every tile in one month** -> `months/YYYY-MM.parquet`. One row group,
  sorted by `mgrs_tile`; read it whole. A 404 means the table has no
  tile-months for that month, which is not an error.
- **Which months exist, how much is in each** -> `timeline.parquet`, a few
  KB; read it whole. Its `max(year, month)` row is the newest month that has
  a slice.

```sql
SELECT year, month, scene_count, min_cloud_cover, median_cloud_cover,
       best_item_id, best_item_date, mean_cover, max_cover
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats-c1/mgrs-monthly.parquet')
WHERE mgrs_tile = '31UFU'
ORDER BY year, month;
```

There is no hive partitioning here, unlike `sentinel-2-c1-l2a`: `months/` is
a flat directory of `YYYY-MM.parquet` files, and `read_parquet` over a glob
of them has no `year`/`month` column to filter on. Take the month from the
file name.

## Schema

`mgrs-monthly.parquet`:
`mgrs_tile VARCHAR, year SMALLINT, month TINYINT, scene_count USMALLINT,
min_cloud_cover UTINYINT, median_cloud_cover UTINYINT, mean_cover UTINYINT,
max_cover UTINYINT, best_item_id VARCHAR, best_item_date DATE`. The
collection's `table:columns` carries a description per column; that is the
authority.

`months/YYYY-MM.parquet`: `mgrs_tile, scene_count, min_cloud_cover,
median_cloud_cover, mean_cover, max_cover`, the same types.

`timeline.parquet`: `year SMALLINT, month TINYINT, tile_count INTEGER,
scene_count INTEGER, min_cloud_cover UTINYINT` -- the count of tiles, the
sum of `scene_count` and the minimum of `min_cloud_cover` over the month's
rows in the full table.

Every percent column is `round(x)` stored as an unsigned 8-bit integer, 0
to 100. The source `eo:cloud_cover` has two decimals; if you need them, read
the item in `sentinel-2-c1-l2a`. `NULL` survives the rounding.

`mean_cover`/`max_cover` are the mean and maximum over the tile-month's
scenes of `100 - s2:nodata_pixel_percentage` -- the percent of the MGRS tile
a scene actually fills (an orbit-edge sliver is 5 %, a full tile 100 %).
Both skip a NULL `s2:nodata_pixel_percentage` and are NULL when every scene
of the tile-month lacks it.

`best_item_id`/`best_item_date` name the scene with the lowest
`eo:cloud_cover` for that tile and month -- look it up in
`sentinel-2-c1-l2a` by `id` (a Collection 1 id, e.g.
`S2B_T31UET_20260921T105030_L2A`) to get its footprint, full datetime and
asset hrefs. `best_item_date` is the UTC calendar date. Both fields are read
from a single `arg_min` over a packed `(id, datetime)` pair, so on a
cloud-cover tie they always describe the same arbitrary tied scene, not
necessarily the earliest one. A row with a NULL `eo:cloud_cover` or a NULL
`_tile` is still counted in `scene_count` -- only `min_cloud_cover`,
`median_cloud_cover` and the `best_item_*` pair (all driven by
`eo:cloud_cover`) skip NULLs, per DuckDB's ordinary aggregate behavior.

`mgrs_tile` here is the index's `_tile` (`31UET`), which is the same id the
`sentinel-2-l2a` collection calls `s2:mgrs_tile`, so a tile's rows in
`stats` and `stats-c1` describe the same place in the two indexes.

## Antimeridian exclusion

Scenes whose bbox spans more than 20 degrees of longitude (antimeridian
wraps, reported as `[-180, ..., 180, ...]`) are **excluded from
`mgrs.pmtiles` only** — enveloping such a bbox would draw a false polygon
across the whole globe. They are still counted normally in
`mgrs-monthly.parquet`'s `scene_count` and cloud-cover statistics. A tile
that only ever has antimeridian-wrapping scenes therefore appears in the
stats table but has no polygon in the tileset.

## Refresh model

The daily refresh recomputes the years it names (`--merge-years Y…
--existing <current mgrs-monthly.parquet>`) and splices them into the
existing table rather than rescanning the whole archive. Every run then
rewrites all of `months/*.parquet` and `timeline.parquet` from the merged
table, and deletes from its local output any month slice the table no
longer has, so the three parquet products always describe the same table.
That deletion is local only: publishing never deletes from the bucket.
`mgrs.pmtiles` is not rebuilt on that schedule: the tileset changes only
when a fuller rebuild adds tiles that have never been seen, so the app's
map layer and its statistics update on different cadences by design.

One thing differs from `stats` because of how Collection 1 is synced. The
index's daily refresh looks back on `created`, not `datetime`, so scenes
ESA reprocessed for an old year land in that year's `live.parquet` and, once
the stats for that year are recomputed, in that year's months here. Because
ESA's reprocessing is still filling old years in, a month's `scene_count`
can therefore rise long after the month ended -- an old month is not final
the way it is in `stats`.

## Status

Nothing is published yet (see the top of this file). Once it is, the table
covers every month the `sentinel-2-c1-l2a` index holds, from 2015-10 on
(the index's earliest scene is 2015-10-22). Read the current span from
`timeline.parquet` rather than from this file; the collection's
`extent.temporal`, `table:row_count` and `updated` are measured from that
timeline at each publish by `tools/make_stats_collection.py --collection
sentinel-2-c1-l2a`. Coverage of the early years is thin in the source
itself (2015–2017 hold a few hundred to a few tens of thousands of scenes,
2022 a fraction of its neighbours, as of 2026-09-21) because the
reprocessing has not reached or finished them, so a tile's absence in an
early month is upstream's record, not evidence Sentinel-2 never imaged it.

Structural links resolve relative to the object that carries them. This
collection carries no `self` link, so a client tracks its own location.
