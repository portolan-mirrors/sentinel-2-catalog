# AGENTS.md — stats

Guidance for AI agents and automated clients querying this collection.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file.

## What this is

Two products, joined by `mgrs_tile`, both generated from the `sentinel-2-l2a`
item index by `tools/s2_stats.py`:

```
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs-monthly.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs.pmtiles
```

`mgrs-monthly.parquet` is one row per MGRS tile per month. `mgrs.pmtiles` is
one polygon per MGRS tile on the vector layer `mgrs`, with an `mgrs_tile`
feature attribute suitable for `promoteId`. The polygon is the envelope of
that tile's scene footprints, not the true MGRS grid cell.

## Query pattern

```sql
SELECT year, month, scene_count, min_cloud_cover, median_cloud_cover,
       best_item_id, best_item_datetime
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs-monthly.parquet')
WHERE mgrs_tile = '31UFU'
ORDER BY year, month;
```

Filter on `mgrs_tile` first: the file is sorted by `(mgrs_tile, year, month)`,
so that is the cheapest cut. There is no partitioning here, unlike
`sentinel-2-l2a` — this is a single small file meant to be read whole or
filtered by tile, not globbed by year.

## Schema

`mgrs_tile VARCHAR, year SMALLINT, month TINYINT, scene_count INTEGER,
min_cloud_cover DOUBLE, median_cloud_cover DOUBLE, best_item_id VARCHAR,
best_item_datetime TIMESTAMPTZ`. The collection's `table:columns` carries a
description per column; that is the authority.

`best_item_id`/`best_item_datetime` name the scene with the lowest
`eo:cloud_cover` for that tile and month — look it up in `sentinel-2-l2a` by
`id` to get its footprint and asset hrefs. Both fields are read from a single
`arg_min` over a packed `(id, datetime)` pair, so on a cloud-cover tie they
always describe the same arbitrary tied scene, not necessarily the earliest
one. A row with a NULL `eo:cloud_cover` or a NULL `s2:mgrs_tile` is still
counted in `scene_count` — only `min_cloud_cover`, `median_cloud_cover` and
the `best_item_*` pair (all driven by `eo:cloud_cover`) skip NULLs, per
DuckDB's ordinary aggregate behavior.

## Antimeridian exclusion

`s2:mgrs_tile` scenes whose bbox spans more than 20 degrees of longitude
(antimeridian wraps, reported as `[-180, ..., 180, ...]`) are **excluded from
`mgrs.pmtiles` only** — enveloping such a bbox would draw a false polygon
across the whole globe. They are still counted normally in
`mgrs-monthly.parquet`'s `scene_count` and cloud-cover statistics. A tile
that only ever has antimeridian-wrapping scenes therefore appears in the
stats table but has no polygon in the tileset.

## Refresh model

The daily refresh recomputes only the current year (`--merge-years Y
--existing <current mgrs-monthly.parquet>`) and splices it into the existing
table rather than rescanning the whole archive. `mgrs.pmtiles` is not
rebuilt on that schedule: the tileset changes only when a fuller rebuild adds
tiles that have never been seen, so the app's map layer and its statistics
update on different cadences by design.

## Status

Generated from a pilot slice of the mirrored archive (two days,
2026-09-12 to 2026-09-13) while the full historical backfill runs. Expect
tile counts and cloud-cover statistics to change substantially as more years
land — do not treat a tile's absence here as evidence Sentinel-2 never
imaged it.

Structural links resolve relative to the object that carries them. This
collection carries no `self` link, so a client tracks its own location.
