# Sentinel-2 Collection 1 L2A scenes (item index)

Every Sentinel-2 Collection 1 L2A scene that AWS Earth Search publishes, as
one year-partitioned GeoParquet table you can query in place. Collection 1
is ESA's reprocessing of the whole Sentinel-2 archive to one processing
baseline; Earth Search indexes it as `sentinel-2-c1-l2a`, beside the older
`sentinel-2-l2a` this catalog also mirrors. One row per scene: footprint,
acquisition time, MGRS tile, cloud cover, the scene-classification
percentages, the viewing and sun angles, when Earth Search created and last
updated the item, and the complete upstream STAC `assets` object.

This catalog carries no imagery. The Cloud-Optimized GeoTIFFs stay in the
public `e84-earth-search-sentinel-data` bucket on AWS, and every one of
their URLs is already in the table.

**Status: the backfill is in progress.** No year is published yet, so the
collection's `table:row_count` is 0, its temporal extent is the source's,
and there are no per-year items. Years appear as the backfill lands them,
each with its own `year=YYYY/YYYY.json`; check
[`collection.json`](collection.json) for the current list. Every query below
is written for the layout the backfill publishes and runs once its year
exists.

## Query it (after the backfill)

```sql
-- Cloud-free scenes over a field during harvest, no API, no rate limits.
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover", thumbnail_url,
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=2021/items.parquet')
WHERE _tile = '31UFU'
  AND datetime BETWEEN '2021-08-01' AND '2021-10-15 23:59:59'
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover", id LIMIT 20;
```

Every asset href is in the `assets` JSON-string column — no URL templates, no
API. Swap `$.visual.href` for `$.red.href`, `$.scl.href`, `$.cloud.href` or
any other key; the [agent guide](AGENTS.md) lists them all.

Three things make that query cheap. A year names the file (`year=2021/items.parquet`;
there is one per year), so it opens one file. Rows inside it are sorted by
`(_tile, datetime)`, so `31UFU`'s whole year is one contiguous run of rows
and the tile filter lands on the one or two row groups that hold it; the
date window then trims that run. And the whole answer comes from HTTP range
requests against the Parquet file: there is no API in front of this, so
there is nothing to rate limit.

The collection's partition glob, `year=*/*.parquet`, is a Hive layout:
`hive_partitioning = true` exposes `year` as a column that is not stored in
the files, and a filter on it skips whole files. DuckDB expands the glob
when it can list the store, which it can through the anonymous `s3://` door;
over plain `https://` it does not ("Globs (`*`) for generic HTTP file are not
supported"), so name the file as above. A year-pruned count through the glob:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_url_style = 'path';
SET TimeZone = 'UTC';

SELECT year, count(*) AS scenes, min(datetime) AS first, max(datetime) AS last
FROM read_parquet('s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=*/*.parquet',
                  hive_partitioning = true)
WHERE year IN (2017, 2018)
GROUP BY year ORDER BY year;
```

Two files are opened, and only their footers are read. A scan of every file
is minutes, not seconds; the collection's `table:row_count` and temporal
extent give the whole-archive answer without one.

## Layout

```
sentinel-2-c1-l2a/
  collection.json
  year=2015/items.parquet      one file per year, every year
  year=2016/items.parquet
  …
  year=2026/items.parquet
  year=2026/live.parquet       rolling tail since the last fold; any year may have one
```

One directory per year Earth Search holds Collection 1 items for, from 2015
on — see Coverage below for how many, and how that keeps changing.

A year's archive is one `items.parquet`, whatever its size: there is no zone
split here, because this collection is built on a compute cluster rather
than inside a CI job. Rows are sorted by `(_tile, datetime)` — tile-major,
so one tile's year is one contiguous run in acquisition order, and any
tile-and-window query reads the one or two row groups that hold the run
rather than a group per month. Row groups are uniform at a target of 6,000
rows (6,144 as DuckDB writes them), small enough that a tile lookup fetches
little beyond its own rows. This is the layout the search-latency
experiments behind
[issue #9](https://github.com/portolan-mirrors/sentinel-2-catalog/issues/9)
measured as the fastest for tile-window searches (the month-major sort of
the older `sentinel-2-l2a` parts scatters a tile's year across twelve month
sections, so a three-month window admits six or seven row groups where
this layout admits one or two). `_month` and `_hilbert` are still columns, so a month filter or
a Hilbert-range filter still works; they just no longer set the order. The
files are GeoParquet 2.0 (native `GEOMETRY` column with per-row-group geo
statistics), zstd level 18.

Any year — not only the current one — may also carry `live.parquet`. ESA's
reprocessing is still running, so scenes from old years keep appearing with
recent `created` timestamps; the daily refresh looks back on `created`, not
`datetime`, and appends what it finds to the live file of whichever year the
scene belongs to, at zstd level 3 so the daily write stays cheap. A live file
can hold a whole year's worth of late arrivals. Every month or two, and at
year end, a fold job merges each live file into its year's `items.parquet`
(re-sorted by tile and time, zstd 18) and empties it. Between folds, a glob over
`year=YYYY/*.parquet` reads the archive plus the tail; the two do not overlap
except for a reprocessed scene, which keeps its id with a newer
`s2:generation_time` — dedupe on `id` keeping the highest generation time
and you have the year exactly once.

Each `year=YYYY/YYYY.json` item states that year's measured row count, time
range, footprint bounds and platforms, so a client can choose a year without
opening a byte of Parquet. Each data asset in the item states the same for
the one file it names.

## Coverage

Earth Search's `sentinel-2-c1-l2a` held 30,391,138 items when it was counted
on 2026-09-21, by year of acquisition:

| Year | Items | Year | Items |
| --- | ---: | --- | ---: |
| 2015 | 200 | 2021 | 4,055,717 |
| 2016 | 222 | 2022 | 283,705 |
| 2017 | 24,664 | 2023 | 4,249,728 |
| 2018 | 1,329,973 | 2024 | 4,369,942 |
| 2019 | 3,166,919 | 2025 | 5,065,361 |
| 2020 | 4,013,725 | 2026 | 3,830,982 (to date) |

Those are the source's counts, not this mirror's, and they are moving: ESA
reprocesses the archive year by year, so the small years (2015–2017, and
2022 at a fraction of its neighbours) are years the reprocessing has not
reached or finished, not years Sentinel-2 did not acquire. They grow as it
proceeds, and the growth arrives with recent `created` timestamps, which is
why the refresh looks back on that field. Read the per-year items for what
is published; do not report a thin year as an observation about the
mission.

`s2:dark_features_percentage` is NULL on every scene processed at baseline
05.11 or later (ESA dropped the class); it is present on 05.00–05.10.
`created` and `updated` are Earth Search's ingest times, not anything about
the acquisition.

## Provenance and license

Items come from the
[Earth Search STAC API](https://earth-search.aws.element84.com/v1), collection
`sentinel-2-c1-l2a`, which Element 84 runs over the Collection 1 COGs it
produces into `e84-earth-search-sentinel-data`, listed on the
[AWS Registry of Open Data](https://registry.opendata.aws/sentinel-2-l2a-cogs/).
Every asset sits at
`https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/sentinel-2-c1-l2a/<zone>/<band>/<sq>/<year>/<month>/<id>/`
(`31/U/ET/2026/9/S2B_T31UET_20260921T105030_L2A/`), the thumbnail being
`L2A_PVI.jpg` in that directory — but read the href from `assets` rather
than build it. Nothing is filtered, reclassified or interpolated here. The
columns added are four helpers, `thumbnail_url`, `_month`, `_hilbert` and `_tile`,
documented in the [agent guide](AGENTS.md).

Contains modified Copernicus Sentinel data. The
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice)
give free, full and open access for any use; the collection states them by
their SPDX identifier, `CC-BY-SA-3.0-IGO`. The AWS Registry of Open Data entry
for the upstream COGs asks that you cite it as:

> Sentinel-2 Cloud-Optimized GeoTIFFs, accessed on [DATE] from
> https://registry.opendata.aws/sentinel-2-l2a-cogs.
