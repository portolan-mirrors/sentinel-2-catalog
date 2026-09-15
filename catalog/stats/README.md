# MGRS coverage and cloud statistics

Two products, generated from the `sentinel-2-l2a` item index by
`tools/s2_stats.py`, meant to be joined by `mgrs_tile`:

- **`mgrs-monthly.parquet`** -- one row per MGRS tile per month: scene
  count, minimum and median `eo:cloud_cover`, and the id/datetime of that
  tile-month's least-cloudy scene. This is the cheap first stop for "which
  month has a cloud-free scene here", and the timeline/choropleth data
  source for the explorer app.
- **`mgrs.pmtiles`** -- one polygon per MGRS tile, on the vector layer
  `mgrs` with an `mgrs_tile` attribute, generated with `gpio pmtiles create`
  (tippecanoe underneath). The polygon is the envelope of that tile's scene
  footprints, not the true MGRS grid cell, and it never changes on a daily
  stat refresh: only `mgrs-monthly.parquet` is recomputed for the current
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

```sql
SELECT year, month, scene_count, min_cloud_cover, best_item_id
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs-monthly.parquet')
WHERE mgrs_tile = '31UFU'
ORDER BY year, month;
```

## Status

Generated from a pilot slice of the mirrored archive (two days,
2026-09-12 to 2026-09-13: 31,264 scenes across 21,040 MGRS tiles) while the
full historical backfill runs in CI. Both products are refreshed, and this
collection's `updated`/extent restamped, as more of the archive lands.

## Provenance and license

Aggregated from the [`sentinel-2-l2a`](../sentinel-2-l2a/README.md) item
index, itself mirrored from the
[Earth Search STAC API](https://earth-search.aws.element84.com/v1). Contains
modified Copernicus Sentinel data, under the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice)
(SPDX `CC-BY-SA-3.0-IGO`).
