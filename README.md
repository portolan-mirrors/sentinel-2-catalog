# sentinel-2-catalog

Every Sentinel-2 L2A scene in the AWS Earth Search archive, as a
[Portolan](https://www.portolan-sdi.org/) catalog of partitioned
STAC-GeoParquet you query in place. **51.3 million scenes, November 2016 to
today**, refreshed daily. One row per scene carries the footprint, acquisition
time, MGRS tile, cloud cover, the scene-classification percentages, and the
complete upstream `assets` object. Every Cloud-Optimized GeoTIFF URL is
therefore in the row that describes the scene.

The imagery stays on AWS in the public `sentinel-cogs` bucket. This catalog
carries only the item index and a small set of MGRS-tile aggregates, with
**no API in front of it**. A client filters the whole archive with HTTP range
reads against a public bucket: no key, no server, no rate limit.

- **Explore it**: https://portolan-mirrors.github.io/sentinel-2-catalog/, a
  static page that behaves like there is an API behind it. DuckDB-WASM range
  reads plus PMTiles, nothing else.
- **Published catalog**: https://source.coop/portolan-mirrors/sentinel-2-catalog
- **STAC root**: `https://data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json`
- **Upstream**: [Earth Search](https://earth-search.aws.element84.com/v1) by
  [Element 84](https://element84.com/), over the
  [Sentinel-2 L2A COGs](https://registry.opendata.aws/sentinel-2-l2a-cogs/) on
  the AWS Registry of Open Data
- **Issues and contributions**: https://github.com/portolan-mirrors/sentinel-2-catalog/issues

## Query it

The driving use case: given a field and a planting or harvest window, find the
cloud-free scenes, from a script, without an API. This is the explorer's own
search, as a DuckDB snippet. Because a tile id and a year name the part, it
reads one file, and only the row groups for those months:

```sql
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover" AS cloud, thumbnail_url
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=2021/z21-31.parquet')
WHERE "s2:mgrs_tile" = '31UFU'
  AND _month BETWEEN 8 AND 10
  AND datetime BETWEEN '2021-08-01' AND '2021-10-15 23:59:59'
  AND "eo:cloud_cover" <= 10
ORDER BY "eo:cloud_cover", id
LIMIT 30;
```

The COG URLs are in the `assets` column, a JSON string of the upstream STAC
assets object. It is the widest column in the table. Fetch it for the scenes
you chose rather than for every candidate:

```sql
INSTALL httpfs; LOAD httpfs;

SELECT json_extract_string(assets, '$.visual.href') AS visual,
       json_extract_string(assets, '$.red.href')    AS red,
       json_extract_string(assets, '$.scl.href')    AS scl
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=2021/z21-31.parquet')
WHERE _month = 10 AND id = 'S2B_31UFU_20211004_0_L2A'
LIMIT 1;
```

Swap `$.visual.href` for any key on the scene (`red`, `nir`, `scl`,
`thumbnail`, ...); the
[collection agent guide](catalog/sentinel-2-l2a/AGENTS.md) lists them all.
Column names with a colon need double quotes.

**Choosing the part.** The UTM zone is the leading digits of the tile id
(`31UFU` is zone 31). A year through 2018 is one `items.parquet`; 2019-2020
are four zone parts; 2021 onward are eight. So zone 31 in 2021 is
`year=2021/z21-31.parquet`, and in 2019 it is `year=2019/z21-35.parquet`
(the layout table below has every range). Add `year=YYYY/live.parquet` for the
current year. Each `year=YYYY/YYYY.json` item lists that year's parts with
their row counts and time ranges if you would rather discover than assume.

**The whole archive.** The collection's partition glob is
`year=*/*.parquet`. DuckDB expands it when it can list the store, which it can
over `s3://` anonymously:

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

`hive_partitioning` exposes `year` as a column that is not stored in the
files, and a filter on it skips whole files, so that query opens two parts
out of sixty.

Over plain `https://` DuckDB does not expand a glob. It says so, with
"Globs (`*`) for generic HTTP file are not supported", and the fix is to name
the parts in a list. Two cautions apply to either form.

The parts are 0.1-2.8 GB each, and a full scan of them over HTTP can fail
partway on some networks. Even a `count(*)` over every part, which reads only
footers and row-group statistics, took four minutes on one run here and hit
DuckDB's HTTP timeout on another. The snippets on this page are range-pruned
reads (tile, month and year predicates) by design; raise `http_timeout` and
`http_retries` before you scan wider.

`live.parquet` and the current year's archive parts are disjoint. The daily
rebuild re-reads a five-day window from Earth Search and drops every id the
archive parts already hold. The two can overlap only for the few hours
between a consolidation and the next refresh. Deduping on `id` is always
safe, and removes nothing when nothing overlaps.

## Two collections

| Collection | Holds | Read it |
|---|---|---|
| [`sentinel-2-l2a`](catalog/sentinel-2-l2a/README.md) | The item index: 51,279,608 scenes in 59 archive parts (2016-11-01 to 2026-09-17 at the last restamp) plus the current year's `live.parquet` tail. Complete from December 2018; before that, what Earth Search holds. | [README](catalog/sentinel-2-l2a/README.md), [agent guide](catalog/sentinel-2-l2a/AGENTS.md) |
| [`stats`](catalog/stats/README.md) | MGRS tile x month aggregates over the whole index and a tile-footprint tileset, for the explorer and for planning a query before running it. | [README](catalog/stats/README.md), [agent guide](catalog/stats/AGENTS.md) |

The `stats` collection is four small products, all keyed by `mgrs_tile`:

- `stats/mgrs-monthly.parquet`: one row per tile per month, the whole
  record in ~21 MB. It carries `scene_count`, `min_cloud_cover`,
  `median_cloud_cover`, `mean_cover` and `max_cover` (percent of the tile its
  scenes fill), and `best_item_id`/`best_item_date`, the least-cloudy scene of
  the month. Every percent is an integer 0-100. Because it is sorted by
  `(mgrs_tile, year, month)` in 50k-row groups, one tile's history is a range
  read of one row group.
- `stats/months/YYYY-MM.parquet`: that table cut to one month, paint columns
  only, ~100-150 KB. The slices are not listed as assets; the name pattern is
  the contract, and a 404 means the month has no tile-months.
- `stats/timeline.parquet`: one row per month over all tiles, a few KB.
- `stats/mgrs.pmtiles`: one polygon per MGRS tile on the vector layer `mgrs`.
  The polygon is the envelope of the tile's scene footprints, not the true
  grid cell.

"Which month has a cloud-free scene here" is a stats question, not an archive
scan:

```sql
INSTALL httpfs; LOAD httpfs;

SELECT year, month, scene_count, min_cloud_cover, best_item_id, best_item_date
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/mgrs-monthly.parquet')
WHERE mgrs_tile = '31UFU' AND year = 2021
ORDER BY year, month;
```

And the newest month the table holds:

```sql
INSTALL httpfs; LOAD httpfs;

SELECT year, month, tile_count, scene_count
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/stats/timeline.parquet')
ORDER BY year DESC, month DESC
LIMIT 1;
```

## Layout of `sentinel-2-l2a`

Everything sits under `year=YYYY/` and matches the glob `year=*/*.parquet`.
The shape of a year, and how its parts were written, changed as the mirror
grew; every vintage reads with the same query, and only the bytes per hit
differ.

| Years | Parts per year | Row groups | Sort order |
|---|---|---|---|
| 2016-2018 | one `items.parquet` | ~100k rows | `(_month, _hilbert)` |
| 2019-2020 | four, by UTM zone: `z01-20`, `z21-35`, `z36-46`, `z47-60.parquet` | ~100k rows | `(_month, _hilbert)` |
| 2021-2023 | eight, by UTM zone: `z01-15`, `z16-20`, `z21-31`, `z32-35`, `z36-40`, `z41-46`, `z47-52`, `z53-60.parquet` | ~100k rows | `(_month, _hilbert)` |
| 2024-2025 | the same eight | ~6k rows | `(_month, _hilbert)` |
| 2026- | the same eight, plus `live.parquet` for the current year | ~6k rows | `(_month, s2:mgrs_tile, _hilbert)` |

The eight ranges nest inside the four, and both are fixed for the life of the
catalog, so a tile id and a year always name one file. Because the 2026 sort
puts one tile's month in a single small row group, a tile lookup there is one
range request. Older parts read the same way at more bytes per hit. Rows are
sorted by `_month` first in every vintage, so a month filter prunes row groups
everywhere. The two `_`-prefixed columns are sort helpers added by the mirror,
not STAC properties. Measured costs per vintage are in
[`docs/query-performance.md`](docs/query-performance.md).

**Reader floors.** The parts are GeoParquet 2.0 with native geometry. They
need DuckDB 1.4 or newer, and duckdb-wasm 1.32 or newer in the browser. Older
readers refuse them with "Geoparquet version 2.0.0 is not supported".

## What the numbers mean

Earth Search's item counts roughly halve from 2022 (8.6 million scenes in
2021, 4.2 million in 2022) without the satellites acquiring less. Earlier
years mostly carry two items per scene, with ids ending `_0_L2A` and
`_1_L2A`, for the same tile and acquisition; from 2022 the `_1_` twins largely
stop. Over tile `31UFU`, 2021 has 454 items for 217 distinct acquisitions and
2022 has 226 for 217. The mirror is faithful to the source: it adds no rows
and drops none, so the halving is Earth Search's record, not a gap here.

Coverage before December 2018 is partial for the same reason: nothing for
2015-2016 and part of 2017-2018 is what Earth Search serves. Two properties,
`sat:orbit_state` and `s2:granule_id`, are NULL on newer items because Earth
Search stopped publishing them. The
[agent guide](catalog/sentinel-2-l2a/AGENTS.md) has the details.

## Update cadence

| Workflow | When | Does |
|---|---|---|
| `refresh-daily` | daily, 03:42 UTC | Fetches the last five days from Earth Search into the current year's `live.parquet`, splices the current year into the stats table, restamps counts and extents, uploads. Nothing is committed. |
| `consolidate-month` | the 3rd of each month, 05:17 UTC | Folds `live.parquet` into the year's archive parts, one job per part, deduped by `id`; then empties `live`. |
| `publish-stats` | manual | Full rebuild of `mgrs-monthly.parquet`, the month slices, `timeline.parquet` and `mgrs.pmtiles` from the published parts. |
| `backfill` and `publish-backfill` | manual | Fetch the whole record one month-slice at a time, then the credentialed year-by-year build and upload. How the archive was seeded; also the repair path. |
| `publish-catalog` | manual | Publishes the committed `catalog/` metadata as-is. |
| `pages` | on push to `apps/explorer/` | Deploys the explorer to GitHub Pages. |

`tools/make_collection.py` regenerates the `updated` stamps and each
collection's row count and temporal extent from the published parts on every
refresh. `collection.json` is therefore the authority on what is here now.

## The repository

Catalog metadata lives in git and CI validates every change. The data lives
in object storage next to it, referenced by URL and never committed.

| Kind | Where | Example |
|---|---|---|
| Tracked and published | inside `catalog/` | STAC JSON, `README.md`, `AGENTS.md` |
| Tracked, never published | outside `catalog/` | `tools/`, `tests/`, `apps/`, `docs/`, this README, `catalog.publish.yaml` |
| Neither | gitignored | GeoParquet, COGs, PMTiles, credentials |

`tools/s2_fetch.py` and `tools/s2_build.py` fetch and compact the parts,
`tools/s2_stats.py` builds the aggregates, `tools/make_items.py` and
`tools/make_collection.py` restamp the metadata, and `tools/publish.py` and
`tools/upload_data.py` carry metadata and data to the bucket. The design and
its amendments are in
[`docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md`](docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md).

### Publish

```bash
python3 tools/publish.py            # dry run: what would change
python3 tools/publish.py --confirm  # upload; needs AWS credentials
```

It never deletes. Removing a file from `catalog/` does not unpublish it, so
delete the object yourself if that is what you meant. Data is staged at the
`data_dir` set in `catalog.publish.yaml`. `tools/upload_data.py` uploads it,
with the same dry-run and `--confirm` flags.

### Test

```bash
CI_LIGHT=1 python3 tests/run_all.py
```

| Gate | What it checks |
|---|---|
| `test_links.py` | Every relative link and asset href resolves |
| `test_publish.py` | Nothing outside `catalog/` can be uploaded |
| `test_upload_data.py` | Only staged files with an allowed suffix upload |
| `test_stac_valid.py` | Valid STAC 1.1.0, via `stac-check` |
| `test_conformance.py` | Portolan conformance, via `rashid` |

`CI_LIGHT=1` exempts asset hrefs with a data suffix from `test_links.py`,
which is the normal case when the data bytes are not on this machine. Every
structural link is still checked. The unit tests for the tools run under
`CI_LIGHT=1 python3 -m pytest tests/ -q`. They need `duckdb`, and the build
test needs `geoparquet-io==1.5.0` on the PATH, the version every workflow
pins. `tools/s2_build.py` needs at least 1.4 for `--compression-level` and
`--write-memory`.

CI runs `rashid`, `stac-check`, and `tests/run_all.py` on every pull request.
`docs/conformance.md` records any accepted deviation, with the rule, why, and
the tracking issue. The allow-list in `tests/test_conformance.py` never widens
without a matching row there.

### Contributing

Wrong metadata, a query that should be cheaper, a column that needs
explaining: open an issue at
https://github.com/portolan-mirrors/sentinel-2-catalog/issues, or send a pull
request against `catalog/`. CI runs the gates above on it.

## License

Data: Sentinel-2 imagery and its derived products carry the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice),
free, full, and open (SPDX `CC-BY-SA-3.0-IGO`). Contains modified Copernicus
Sentinel data. See [`catalog/README.md`](catalog/README.md) for the citation
the AWS Registry of Open Data asks for. Repository code: see `LICENSE`.
