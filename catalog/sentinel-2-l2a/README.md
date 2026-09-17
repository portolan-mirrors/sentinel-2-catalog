# Sentinel-2 L2A scenes (item index)

Every Sentinel-2 L2A scene that AWS Earth Search publishes, as one
year-partitioned GeoParquet table you can query in place. One row per scene:
footprint, acquisition time, MGRS tile, cloud cover, the scene-classification
percentages, and the complete upstream STAC `assets` object.

This catalog carries no imagery. The Cloud-Optimized GeoTIFFs stay in the
public `sentinel-cogs` bucket on AWS, and every one of their URLs is already in
the table.

## Query it

```sql
-- Cloud-free scenes over a field during harvest, no API, no rate limits.
INSTALL spatial; LOAD spatial;
SELECT id, datetime, "eo:cloud_cover", thumbnail_url,
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet', hive_partitioning=true)
WHERE year IN (2021)
  AND "s2:mgrs_tile" = '31UFU'
  AND datetime BETWEEN '2021-08-01' AND '2021-10-15'
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover" LIMIT 20;
```

Every asset href is in the `assets` JSON-string column — no URL templates, no
API. Swap `$.visual.href` for `$.red.href`, `$.scl.href` or any other key; the
[agent guide](AGENTS.md) lists them all.

Three things make that query cheap. `year=*` is a Hive partition, so DuckDB
opens only the years you name. Rows inside each part are sorted by
`(_month, _hilbert)`, so a month filter and a spatial filter both prune row
groups. And the whole answer comes from HTTP range requests against the
Parquet files: there is no API in front of this, so there is nothing to rate
limit.

## Layout

```
sentinel-2-l2a/
  collection.json
  year=2017/items.parquet      one file per year through 2018
  year=2018/items.parquet
  year=2019/z01-20.parquet     four files per year from 2019, by UTM zone
  year=2019/z21-35.parquet
  year=2019/z36-46.parquet
  year=2019/z47-60.parquet
  …
  year=2026/z01-20.parquet … z47-60.parquet
  year=2026/live.parquet       rolling tail since the last consolidation
```

One directory per year Earth Search actually holds items for, so the listing
starts where its record does — see Coverage below.

A year's archive is one file through 2018 and four from 2019, split by the UTM
zone of `s2:mgrs_tile` (the leading digits of the tile id): `z01-20.parquet`
holds zones 1–20, `z21-35.parquet` zones 21–35, `z36-46.parquet` zones 36–46,
`z47-60.parquet` zones 47–60. The split is what keeps a 7-million-scene year
buildable, and it is also a spatial index for free: a tile id names its part,
so a query for `31UFU` in 2021 can open `year=2021/z21-35.parquet` alone and
skip the other three quarters of the year. The current year also carries
`live.parquet`, rebuilt daily from the Earth Search API and folded into the
archive parts once a month. No two parts of a year overlap, so a glob over
`year=*/*.parquet` reads each scene exactly once and includes yesterday,
whichever shape the year has.

Each `year=YYYY/YYYY.json` item states that year's measured row count, time
range, footprint bounds and platforms, so a client can choose a year without
opening a byte of Parquet. Each data asset in the item states the same for the
one part it names.

## Coverage

Coverage before December 2018 is partial: nothing for 2015-2016, part of
2017-2018. That is what Earth Search serves. This mirror adds no rows and
drops none, so the gap is upstream, not here.

`sat:orbit_state` and `s2:granule_id` are NULL on newer items, because Earth
Search stopped populating them. Read the [agent guide](AGENTS.md) before you
write a query that depends on either.

## Provenance and license

Items come from the
[Earth Search STAC API](https://earth-search.aws.element84.com/v1), collection
`sentinel-2-l2a`, which Element 84 runs over the
[Sentinel-2 L2A COGs](https://registry.opendata.aws/sentinel-2-l2a-cogs/) on the
AWS Registry of Open Data. Nothing is filtered, reclassified or interpolated
here. The columns added are two sort helpers, `_month` and `_hilbert`, and they
are documented in the [agent guide](AGENTS.md).

Contains modified Copernicus Sentinel data. The
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice)
give free, full and open access for any use; the collection states them by
their SPDX identifier, `CC-BY-SA-3.0-IGO`. The AWS Registry of Open Data entry
for the upstream COGs asks that you cite it as:

> Sentinel-2 Cloud-Optimized GeoTIFFs, accessed on [DATE] from
> https://registry.opendata.aws/sentinel-2-l2a-cogs.
