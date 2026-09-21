# AGENTS.md — sentinel-2-l2a

Guidance for AI agents and automated clients querying this collection.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file. An agent acting on an invented
column name or an invented join key produces a confident wrong answer, and
nothing downstream catches it.

## What this is

One row per Sentinel-2 L2A scene in the AWS Earth Search item index,
republished as partitioned GeoParquet. The imagery is not here: it is in the
public `sentinel-cogs` bucket on AWS, and every COG URL is already in the
`assets` column of the row that describes it.

```
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/items.parquet   2015-2018
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z01-20.parquet   2019-2020, four parts
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z21-35.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z36-46.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z47-60.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z01-15.parquet   2021 onward, eight parts
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z16-20.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z21-31.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z32-35.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z36-40.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z41-46.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z47-52.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/z53-60.parquet
https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=YYYY/live.parquet    current year only
```

A year's archive is one of three shapes, and the year tells you which.
2015-2018 are a single `items.parquet` each. From 2019 the archive is split
by the UTM zone of `s2:mgrs_tile` (the leading one or two digits of the tile
id): 2019 and 2020 into four files, and from 2021 into eight, whose
boundaries sit inside the four (every quartile edge is also an octant edge):

| 2019–2020 part   | UTM zones | 2021+ part       | UTM zones |
| ---------------- | --------- | ---------------- | --------- |
| `z01-20.parquet` | 1–20      | `z01-15.parquet` | 1–15      |
|                  |           | `z16-20.parquet` | 16–20     |
| `z21-35.parquet` | 21–35     | `z21-31.parquet` | 21–31     |
|                  |           | `z32-35.parquet` | 32–35     |
| `z36-46.parquet` | 36–46     | `z36-40.parquet` | 36–40     |
|                  |           | `z41-46.parquet` | 41–46     |
| `z47-60.parquet` | 47–60     | `z47-52.parquet` | 47–52     |
|                  |           | `z53-60.parquet` | 53–60     |

The boundaries are fixed (both tiers balance the 2018 row distribution: the
four at 27/26/22/25%, the eight at 10–15% each; the eight exist because a
quarter of a 2021-sized year no longer built inside one CI job), so the part
that holds a tile is known from the tile id and the year alone: `31UFU` is
zone 31, in `z21-35.parquet` for 2019–2020 and `z21-31.parquet` from 2021.
The current year also has `live.parquet`, the tail fetched daily since the
last consolidation. Its daily rebuild drops every id the year's archive parts
already hold, so no two parts of a year overlap. A glob over the year reads
each scene once, whichever shape the year has, and a client can always
dedupe on `id` anyway; it is safe and removes nothing:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_url_style = 'path';
SET TimeZone = 'UTC';

SELECT year, count(*) AS scenes, min(datetime) AS first, max(datetime) AS last
FROM read_parquet('s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet',
                  hive_partitioning = true)
WHERE year IN (2016, 2017)
GROUP BY year ORDER BY year;
```

`hive_partitioning = true` exposes `year` as an INTEGER column that is not
stored in the files. Filter on it first; it is the only filter that skips
whole files. The glob goes through the anonymous `s3://` door because DuckDB
expands `*` only where it can list the store: over plain `https://` it stops
with "Globs (`*`) for generic HTTP file are not supported", so an `https://`
read names one part (below). There is no `zone=` directory and no zone
column: the zone split is a file name inside the year, and you use it by
choosing the file.

## Query pattern

Filter in this order. Each step removes more data than the next one can.

1. `year IN (…)` — partition pruning, skips whole files.
2. `"s2:mgrs_tile" = '31UFU'` — the spatial join key, and the cheapest spatial
   filter there is. A tile id is stable for the life of the mission.
3. `_month = 8` or a `datetime` range — rows are sorted by month first, so a
   month filter prunes row groups inside the file.
4. `"eo:cloud_cover" < 10` — the usual last cut.

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_url_style = 'path';
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover",
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet',
                  hive_partitioning = true)
WHERE year = 2021
  AND "s2:mgrs_tile" = '31UFU'
  AND _month BETWEEN 8 AND 10
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover", id
LIMIT 20;
```

Column names with a colon are not identifiers. Quote them: `"eo:cloud_cover"`,
not `eo:cloud_cover`.

**Read only the part containing your tile's zone.** The glob above opens every
part of the year and lets the `s2:mgrs_tile` filter discard the rest of it by
row-group statistics: seven footers read for nothing (measured 2026-09-21,
11.9 s against 11.5 s for the single-part form below, same eight rows). Row-group size differs by vintage: parts published
through 2023 carry ~100k-row groups (a tile lookup reads ~14 MB per group it
touches); parts from 2024 on carry ~6k-row groups (~0.85 MB per hit). Sort order also
differs by vintage: through 2025 rows are ordered (_month, _hilbert); from 2026
they are ordered (_month, s2:mgrs_tile, _hilbert), so one tile's scenes for a
month sit in a single small row group and a tile lookup is one range request.
All vintages read identically; only bytes and requests per hit differ, and the
older years are rebuilt when a larger machine allows. When you know the tile, skip them entirely: pick the
file by zone and year. Zone 31 in 2021 is in `z21-31.parquet`, so the same
query touches one file:

```sql
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover",
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=2021/z21-31.parquet')
WHERE "s2:mgrs_tile" = '31UFU'
  AND _month BETWEEN 8 AND 10
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover", id
LIMIT 20;
```

To choose the file in code, parse the zone with `^\d{1,2}` and take the range
that contains it from the year's tier: the four ranges for 2019–2020, the
eight from 2021; for a year before 2019 the file is `items.parquet` regardless
of zone. Add `live.parquet` for the current year. A bounding box spans the
zone ranges its longitudes fall in (each UTM zone is six degrees wide), so a
regional bbox query names one or two parts; a global query globs.
The per-year item (`year=YYYY/YYYY.json`) lists every part of that year as its
own asset, with its own row count and time range, if you would rather discover
than assume.

Use a `ST_Intersects` filter on `geometry` when you have a real polygon and no
tile id. It works, and it is slower than the tile filter, because it has to
decode geometries the tile filter never reads.

`datetime` is TIMESTAMP WITH TIME ZONE and every value is UTC. DuckDB renders
it in the session time zone, so run `SET TimeZone='UTC'` before you compare a
rendered string to a date, or a scene acquired at 23:50 UTC reports the next
day west of Greenwich and the previous day east of it.

## Schema

The published column list is `tools/s2_schema.py` in the source repository, and
the collection's `table:columns` is generated from it. Both carry a description
per column; that is the authority, and this section covers what a description
cannot say on its own.

**Spatial.** `geometry` is the scene footprint in CRS84. `bbox` is the same
footprint as `[w, s, e, n]`. `s2:mgrs_tile` is the MGRS tile id, for example
`31UFU`, and it is THE join key: scenes over one place share it across all
years.

**Time.** `datetime` is the acquisition instant, UTC. `s2:generation_time` is
when ESA processed the product, not when the satellite looked.

**Two added columns, and they are helpers, not STAC.** They exist so that
readers can prune:

- `_month` — `month(datetime)`, 1 to 12. The first sort key.
- `_hilbert` — `ST_Hilbert(geometry, world bounds)`. The second sort key.

Every part file is written sorted by `(_month, _hilbert)`: month first so that
a month filter prunes row groups inside a year, Hilbert within month so that
each row group's bounding box stays tight and a spatial filter prunes too.
Nothing upstream publishes these two columns. Do not pass them on as STAC
properties, and do not treat `_hilbert` as meaningful on its own — it is a
position on a space-filling curve, not a measurement.

**Everything else** is the upstream STAC property under its upstream name,
unchanged. Two of them are reconstructed when Earth Search omits them, and
never invented: `sat:relative_orbit` is parsed from the `_R(\d{3})_` group of
`s2:product_uri`, and `s2:mean_solar_zenith` is computed as
`90 - view:sun_elevation`.

## NULLs you will meet

`s2:granule_id` and `sat:orbit_state` are NULL on newer items. Earth Search
stopped publishing those two properties, and this mirror leaves an absent value
absent rather than inventing one. A query that requires either silently drops
every recent scene. Use `s2:product_uri` in place of `s2:granule_id`, and
derive the orbit direction from `sat:relative_orbit` if you truly need it.

Several `s2:*_percentage` columns are NULL on some items for the same reason:
the upstream item did not carry them.

## Coverage

Nothing for 2015-2016. Part of 2017-2018. Complete from about December 2018.

That is what Earth Search and the `sentinel-cogs` bucket serve, not a gap
introduced here. Do not report "no scenes in 2016" as an observation about
Sentinel-2 — the mission was acquiring; this index does not carry those items.
Check the per-year items (`year=YYYY/YYYY.json`) for each year's measured row
count and time range before you conclude anything about a period.

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

Keys present on a current L2A scene:

| Group | Keys |
| --- | --- |
| Bands, COG | `coastal` `blue` `green` `red` `rededge1` `rededge2` `rededge3` `nir` `nir08` `nir09` `swir16` `swir22` |
| Bands, JPEG 2000 | the same names with a `-jp2` suffix |
| Derived rasters | `visual` (true colour), `scl` (scene classification), `aot`, `wvp`, `cloud`, `snow` |
| Preview | `thumbnail` (also flat in the `thumbnail_url` column) |
| Metadata | `granule_metadata`, `product_metadata`, `tileinfo_metadata` |

Each asset object carries `href`, `type`, `title`, `roles`, and for the raster
assets `gsd`, `eo:bands`, `raster:bands`, `proj:shape` and `proj:transform`.
The `proj:*` values are per scene and correct only for that scene.

The collection's `item_assets` block mirrors the stable part of this — the band
metadata per key — for STAC clients that expect it at collection level. It is
documentation. The per-item `assets` value is the authority, it is the only
place the hrefs exist, and it carries keys (`cloud`, `snow`,
`product_metadata`) that the upstream `item_assets` template omits.

Do not build an asset URL from a template. Read the href.

## Dedupe

A scene can be fetched more than once: the daily refresh re-reads a five-day
window, and a reprocessed product keeps its id. Rows are deduped by `id`,
keeping the highest `s2:generation_time` (`NULLS LAST`), when each year is
built. So `id` is unique within a part. The parts of a year do not overlap:
a scene has one tile, and a tile one zone, and the daily `live.parquet`
rebuild drops every id the year's archive parts hold. So `id` is unique
within a year. Across the whole table, treat `id` as unique and report it if
you ever find otherwise.

## What this collection does not do

It does not filter, reclassify or interpolate anything Earth Search publishes.
It adds `_month`, `_hilbert` and the flat `thumbnail_url`, and nothing else.
There is no STAC API in front of it: query the Parquet directly, over HTTP
range requests, with no key and no rate limit.

Structural links resolve relative to the object that carries them. Objects here
carry no `self` link, so a client tracks its own location.
