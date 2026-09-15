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
in the `sentinel-cogs` bucket on AWS; a client derives each COG's URL from an
item's MGRS tile and date rather than downloading it through this catalog.
What publishes here is the *item index* — the STAC metadata for roughly 28
million scenes from 2015 to today — plus small aggregate products (scene
counts and cloud cover per MGRS tile per month) that make it practical to plan
a query before running it.

**No collection is published yet.** This repository currently ships the
catalog skeleton only. Once built out, it will hold `sentinel-2-l2a` (the item
index) and `stats` (the MGRS aggregates); check
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
`sentinel-2-l2a`. The initial archive (2015-07-04 through 2024-06-24, about 28
million items) is a one-time repartition of a
[pre-existing GeoParquet export](https://data.source.coop/cholmes/stac-geoparquet-public/slim/s2-stac.parquet)
of that same collection; everything after that date is fetched from the API
directly and normalized to the same schema, so the whole archive is one
queryable table regardless of which side of that date a scene falls on.

This catalog does not filter, reclassify, or interpolate anything Earth Search
publishes. Column meanings and the two added sort-key columns will be
documented on the `sentinel-2-l2a` collection once it exists.

## Access

The archive this catalog mirrors is queryable today, ahead of this catalog's
own copy landing:

```sql
INSTALL httpfs; LOAD httpfs;

SELECT count(*) AS scenes, min(datetime) AS earliest, max(datetime) AS latest
FROM read_parquet(
  'https://data.source.coop/cholmes/stac-geoparquet-public/slim/s2-stac.parquet');
-- 28146662 scenes, 2015-07-04 through 2024-06-24
```

Once `sentinel-2-l2a` publishes, the same query pattern applies against this
catalog's own partitioned files under
`https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/`,
documented on that collection.
