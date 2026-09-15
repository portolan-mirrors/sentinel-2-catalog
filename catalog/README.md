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
template. What publishes here is the *item index* — the STAC metadata for the
51.25 million items Earth Search held when it was counted on 2026-09-15 — plus
small aggregate products (scene counts and cloud cover per MGRS tile per month)
that make it practical to plan a query before running it.

Coverage is partial before December 2018: nothing for 2015-2016, part of
2017-2018, complete after that. This mirror adds no items and drops none, so
that gap is Earth Search's record, not an artifact of the mirroring.

[`sentinel-2-l2a`](sentinel-2-l2a/collection.json) holds the item index. The
`stats` collection, with the MGRS aggregates, follows. Check
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
`sentinel-2-l2a`, fetched from the API and normalized to one schema. The whole
archive is therefore one queryable table, and a row means the same thing in
2019 as it does today.

This catalog does not filter, reclassify, or interpolate anything Earth Search
publishes. Column meanings, the two added sort-key columns, and the query
patterns are documented on the
[`sentinel-2-l2a` collection](sentinel-2-l2a/README.md) and in its
[agent guide](sentinel-2-l2a/AGENTS.md).

## Access

No API, no key, no rate limit: DuckDB reads the Parquet over HTTP.

```sql
INSTALL httpfs; LOAD httpfs;

SELECT count(*) AS scenes, min(datetime) AS earliest, max(datetime) AS latest
FROM read_parquet(
  'https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet',
  hive_partitioning=true);
```

Filter on `year` first: it is a Hive partition key, so it skips whole files.
The [collection README](sentinel-2-l2a/README.md) has the query that finds
cloud-free scenes over one field, and the COG URL for each of them.
