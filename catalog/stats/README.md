# MGRS coverage and cloud statistics

Three parquet products and one tileset, generated from the `sentinel-2-l2a`
item index by `tools/s2_stats.py`, meant to be joined by `mgrs_tile`:

- **`mgrs-monthly.parquet`** -- one row per MGRS tile per month: scene
  count, minimum and median `eo:cloud_cover`, the id and UTC date of that
  tile-month's least-cloudy scene, and the mean and maximum percent of the
  tile its scenes fill (`mean_cover`/`max_cover`, from
  `100 - s2:nodata_pixel_percentage`; NULL when no scene carried it). Every
  percent is an integer 0-100 (`round()`), which is what keeps the full
  table at ~21 MB for ten years of every tile. Sorted by
  `(mgrs_tile, year, month)` in 50k-row groups, so a filter on one tile is
  a range read of one row group plus a small footer, not the whole file. This is the
  cheap first stop for "which month has a cloud-free scene here".
- **`months/YYYY-MM.parquet`** -- one file per month present in the table
  (not listed as assets; the pattern is the contract), holding that
  month's rows with the paint columns only: `mgrs_tile, scene_count,
  min_cloud_cover, median_cloud_cover, mean_cover, max_cover`. Sorted by
  `mgrs_tile`, one row group, ~100-150 KB. The explorer app fetches one of
  these per month it paints. The newest month that has a slice is the
  `max(year, month)` row of `timeline.parquet`; a month between the oldest
  and newest with no rows has no file (a 404 means "no tile-months").
- **`timeline.parquet`** -- one row per `(year, month)` over all tiles:
  `tile_count`, summed `scene_count`, and the minimum `min_cloud_cover`. A
  few KB; read it whole for the global timeline and the month span.
- **`mgrs.pmtiles`** -- one polygon per MGRS tile, on the vector layer
  `mgrs` with an `mgrs_tile` attribute, generated with `gpio pmtiles create`
  (tippecanoe underneath). The polygon is the envelope of that tile's scene
  footprints, not the true MGRS grid cell, and it never changes on a daily
  stat refresh: only the parquet products are recomputed for the current
  year, so the app's map layer and its statistics update independently.

## Antimeridian exclusion

A Sentinel-2 scene whose bbox spans more than 20 degrees of longitude has
wrapped the antimeridian and is reported as `[-180, ..., 180, ...]` --
averaging or enveloping that bbox draws a false polygon across the whole
globe. Those scenes are dropped from the footprint aggregation that builds
`mgrs.pmtiles`, but they are still counted normally in
`mgrs-monthly.parquet`: the exclusion is a geometry-rendering fix, not a
data-quality filter.

## Query it

One tile's history (a range read of one or two row groups):

```sql
SELECT year, month, scene_count, min_cloud_cover, max_cover, best_item_id, best_item_date
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs-monthly.parquet')
WHERE mgrs_tile = '31UFU'
ORDER BY year, month;
```

Every tile for one month (a ~120 KB file):

```sql
SELECT mgrs_tile, scene_count, min_cloud_cover
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/months/2026-08.parquet')
WHERE min_cloud_cover <= 5;
```

Which months exist, and the newest:

```sql
SELECT year, month, tile_count, scene_count
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/timeline.parquet')
ORDER BY year DESC, month DESC LIMIT 1;
```

## Status

The table covers every month of the mirrored archive. As of 2026-09-19 that
is 119 months, 2016-11 through 2026-09, and 3,135,156 tile-months in
`mgrs-monthly.parquet`; the newest month is the `max(year, month)` row of
`timeline.parquet`. The daily refresh recomputes the current year and
rewrites the month slices and the timeline; `tools/make_stats_collection.py`
measures this collection's temporal extent, row count and `updated` from
the timeline at each publish, so the collection never has to be edited by
hand to follow the table.

## Provenance and license

Aggregated from the [`sentinel-2-l2a`](../sentinel-2-l2a/README.md) item
index, itself mirrored from the
[Earth Search STAC API](https://earth-search.aws.element84.com/v1). Contains
modified Copernicus Sentinel data, under the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice)
(SPDX `CC-BY-SA-3.0-IGO`).
