# Sentinel-2 L2A STAC-GeoParquet Mirror

An item index for the AWS Earth Search Sentinel-2 L2A archive, republished as
cloud-native GeoParquet that you can query in place without downloading
anything and without a rate-limited API in front of it.

This is a **mirror**. Earth Search, run by
[Element 84](https://element84.com/) on the
[AWS Registry of Open Data](https://registry.opendata.aws/sentinel-2-l2a-cogs/),
produces the item index this catalog republishes. Copernicus Sentinel-2 is the
underlying imagery source.

## What is here

This catalog carries no imagery. Sentinel-2 L2A Cloud-Optimized GeoTIFFs stay
in the `sentinel-cogs` bucket on AWS, and every one of their URLs is carried
verbatim in the item's `assets` column, so nothing has to be derived from a
template. What publishes here is the *item index* — Earth Search's STAC
metadata for the Sentinel-2 L2A archive, which held 51.25 million items when it
was counted on 2026-09-15 — plus small aggregate products (scene counts and
cloud cover per MGRS tile per month) that make it practical to plan a query
before running it.

The mirror carries that whole record and follows it daily. The
[`sentinel-2-l2a` collection](sentinel-2-l2a/collection.json) carries the row
count and time range it holds right now, and each `year=YYYY/YYYY.json` item
carries its year's. Those are the authority on what is here; this page
describes what is mirrored and how to read it.

The record starts in November 2016, when Earth Search produced its first L2A
Cloud-Optimized GeoTIFFs: 2015 and most of 2016 have no COG products, 2017-2018
are partial, and the record is complete from about December 2018. This mirror
adds no items and drops none, so that gap is Earth Search's record, not an
artifact of the mirroring.

[`sentinel-2-l2a`](sentinel-2-l2a/collection.json) holds the item index and
[`stats`](stats/collection.json) holds the MGRS aggregates. A second pair,
[`sentinel-2-c1-l2a`](sentinel-2-c1-l2a/collection.json) and
[`stats-c1`](stats-c1/collection.json), mirrors Earth Search's Sentinel-2
Collection 1 index (ESA's reprocessing of the archive) the same way; its
backfill is in progress, so it holds no published year yet. Check
[`catalog.json`](catalog.json) for the current list of collections.

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
[Earth Search STAC API](https://earth-search.aws.element84.com/v1), collection
`sentinel-2-l2a`, fetched from the API and normalized to one schema. Months
the API could not serve during its outage windows were read instead from the
static item JSON in the `sentinel-cogs` bucket (the repair lane), which holds
the same items. The whole archive is therefore one queryable table, and a row
means the same thing in 2019 as it does today.

This catalog does not filter, reclassify, or interpolate anything Earth Search
publishes. Column meanings, the two added sort-key columns, and the query
patterns are documented on the
[`sentinel-2-l2a` collection](sentinel-2-l2a/README.md) and in its
[agent guide](sentinel-2-l2a/AGENTS.md).

## Access

No API, no key, no rate limit: a client reads the Parquet over HTTP, with
range requests, straight from the bucket.

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';
SET s3_url_style = 'path';
SET TimeZone = 'UTC';

SELECT year, count(*) AS scenes, min(datetime) AS earliest, max(datetime) AS latest
FROM read_parquet('s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet',
                  hive_partitioning = true)
WHERE year IN (2016, 2017)
GROUP BY year ORDER BY year;
```

That measures two of the published files, from their footers. The
collection's `table:row_count` and temporal extent say the same thing for
the whole archive without opening one; a scan of every part is minutes,
not seconds.

Filter on `year` first: it is a Hive partition key, so it skips whole files.
The glob is read through the anonymous `s3://` door because DuckDB lists a
bucket but cannot list a plain `https://` prefix ("Globs (`*`) for generic
HTTP file are not supported"). Over `https://` name the part instead:
`https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=2021/z21-31.parquet`.
The [collection README](sentinel-2-l2a/README.md) has the query that finds
cloud-free scenes over one field, and the COG URL for each of them.
