# Sentinel-2 L2A STAC-GeoParquet Mirror

Every Sentinel-2 L2A scene in the AWS Earth Search archive, republished as
cloud-native GeoParquet. You query it in place over HTTP. No API or key
sits in front of it.

**[Open this catalog in the Portolan browser](https://browser.portolan-sdi.org/#/external/data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json)**
to explore the collections, or use the
[scene explorer](https://taylor-geospatial.github.io/s2-stac-geoparquet/) to
search scenes on a map and draw any band of a scene from its COGs.

This is a mirror. Earth Search, run by
[Element 84](https://element84.com/) on the
[AWS Registry of Open Data](https://registry.opendata.aws/sentinel-2-l2a-cogs/),
produces the item indexes this catalog republishes. Copernicus Sentinel-2 is
the underlying imagery source.

## What is here

This catalog carries no imagery. The Sentinel-2 Cloud-Optimized GeoTIFFs stay in the
`sentinel-cogs` bucket on AWS, and every one of their URLs is carried
verbatim in the item rows. What publishes here is the item index — one
GeoParquet row per scene — plus small per-MGRS-tile aggregates (scene
counts, cloud cover, coverage per month) that make it practical to plan a
query before running it.

The published index pairs:

- [`sentinel-2-c1-l2a`](sentinel-2-c1-l2a/collection.json) and
  [`stats-c1`](stats-c1/collection.json) mirror Earth Search's **Sentinel-2
  Collection 1** index, ESA's uniform reprocessing of the archive: 30
  million scenes from 2015 on, in one `items.parquet` per year. This is the
  pair to start with, and the pair the scene explorer opens on.
- [`sentinel-2-l2a`](sentinel-2-l2a/collection.json) and
  [`stats`](stats/collection.json) mirror the original Earth Search
  `sentinel-2-l2a` index: 51 million scenes from November 2016 on, in
  zone-partitioned year parts. Its record reflects Earth Search's own
  history, including duplicate items in early years.

Each collection's `table:row_count` and temporal extent state what it holds
right now, and each `year=YYYY/YYYY.json` item states its year's. Those are
the authority; this page describes what is mirrored and how to read it.

## License

Sentinel-2 data and its derivatives carry the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice):
free, full, and open access for any use. The
[AWS Registry of Open Data entry](https://registry.opendata.aws/sentinel-2-l2a-cogs/)
for the upstream COGs asks that you cite it as:

> Sentinel-2 Cloud-Optimized GeoTIFFs, accessed on [DATE] from
> https://registry.opendata.aws/sentinel-2-l2a-cogs.

Cite this catalog itself as the item index derived from that archive.

## Provenance

Every item comes from the
[Earth Search STAC API](https://earth-search.aws.element84.com/v1), fetched
and normalized to one schema per collection. Months the API could not serve
during its outage windows were read instead from the static item JSON in the
`sentinel-cogs` bucket, which contains the same items. Each archive is one
queryable table, and a row means the same thing in 2019 as it does today.

This catalog does not filter, reclassify, or interpolate anything Earth
Search publishes. Column meanings and query patterns are documented per
collection, in
[`sentinel-2-c1-l2a`](sentinel-2-c1-l2a/README.md) and
[`sentinel-2-l2a`](sentinel-2-l2a/README.md), each with an agent guide
(`AGENTS.md`) beside it.

## Access

A client reads the Parquet over HTTP, with range requests, straight from
the bucket:

```sql
INSTALL httpfs; LOAD httpfs;
SET TimeZone = 'UTC';

SELECT id, datetime, "eo:cloud_cover" AS cloud
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=2025/items.parquet')
WHERE _tile = '31UFU'
  AND "eo:cloud_cover" <= 10
ORDER BY "eo:cloud_cover" LIMIT 10;
```

The parts are sorted by tile, so a tile filter reads a few row groups, not
the file. To scan many years at once, use the anonymous `s3://` door with a
glob (`s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=*/items.parquet`,
with `hive_partitioning = true` and `SET s3_region = 'us-west-2'`): DuckDB
lists a bucket but cannot expand a glob over plain `https://`. The
collection READMEs carry the full query patterns, including the one that
finds cloud-free scenes over one field and the COG URL for each.
