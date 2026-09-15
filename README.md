# sentinel-2-catalog

A [Portolan](https://www.portolan-sdi.org/) catalog mirroring the **AWS Earth
Search Sentinel-2 L2A archive** (about 28 million scenes, 2015 to today) as
partitioned STAC-GeoParquet, plus MGRS-tile aggregate stats. The imagery stays
on AWS; this catalog carries only the item index, so a client queries the full
archive with plain Parquet reads instead of a rate-limited STAC API.

Catalog metadata lives in this repository. CI validates every change to it.
The data itself lives in object storage next to the metadata, referenced by
URL and never committed.

- **Published catalog (once populated)**: https://source.coop/portolan-mirrors/sentinel-2-catalog
- **STAC root**: https://data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json
- **Upstream**: https://earth-search.aws.element84.com/v1 (Earth Search, by
  [Element 84](https://element84.com/), on the
  [AWS Registry of Open Data](https://registry.opendata.aws/sentinel-2-l2a-cogs/))
- **Design spec**: [`docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md`](docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md)

## Status

This repository currently holds the template scaffold only: a valid, empty
catalog root and the gates that will validate everything added to it. No
collection is published yet.

Once built out, `catalog/` will hold two collections:

| Collection | Will hold |
|---|---|
| `sentinel-2-l2a` | The item index — Earth Search's Sentinel-2 L2A metadata, partitioned `year=YYYY/items.parquet` plus a `live.parquet` tail, sorted `(_month, _hilbert)` |
| `stats` | MGRS tile × month aggregates (scene counts, cloud cover) and a tile-footprint PMTiles layer, for the explorer app and for query planning |

Neither collection nor the pipeline that builds them (`tools/s2_fetch.py`,
`tools/s2_build.py`, `tools/s2_stats.py`, and the GitHub Actions workflows that
run them) exists yet. They land in later work; see the design spec for the
full plan.

## Why no imagery, no API

Sentinel-2 L2A Cloud-Optimized GeoTIFFs already live on AWS in the
`sentinel-cogs` bucket, produced and hosted by Element 84. Re-hosting them
here would duplicate petabytes of data this catalog does not need to own. A
client derives each COG's URL from the item's MGRS tile and date instead —
the derivation is documented on the `sentinel-2-l2a` collection once it
exists. Serving the index as GeoParquet, rather than behind a STAC API, means
a client filters ~28 million scenes with a Parquet range read against a
public bucket: no server to rate-limit, no server to keep running.

## Three kinds of file

| Kind | Where | Example |
|---|---|---|
| Tracked and published | inside `catalog/` | STAC JSON, `README.md`, `AGENTS.md` |
| Tracked, never published | outside `catalog/` | `tools/`, `tests/`, `docs/`, this README, `catalog.publish.yaml` |
| Neither | gitignored | GeoParquet, COGs, PMTiles, credentials |

## Publish

```bash
python3 tools/publish.py            # dry run: what would change
python3 tools/publish.py --confirm  # upload; needs AWS credentials
```

It never deletes. Removing a file from `catalog/` does not unpublish it, so
delete the object yourself if that is what you meant.

## Upload the data

The data is too large for git, so it lives outside `catalog/`, staged at the
`data_dir` set in `catalog.publish.yaml`. `tools/upload_data.py` carries it to
the same bucket prefix.

```bash
python3 tools/upload_data.py            # dry run: what would change
python3 tools/upload_data.py --confirm  # upload; needs AWS credentials
```

## Test

```bash
python3 tests/run_all.py
```

| Gate | What it checks |
|---|---|
| `test_links.py` | Every relative link and asset href resolves |
| `test_publish.py` | Nothing outside `catalog/` can be uploaded |
| `test_upload_data.py` | Only staged files with an allowed suffix upload |
| `test_stac_valid.py` | Valid STAC 1.1.0, via `stac-check` |
| `test_conformance.py` | Portolan conformance, via `rashid` |

CI runs `rashid`, `stac-check`, and `tests/run_all.py` on every pull request.
`docs/conformance.md` records any accepted deviation, with the rule, why, and
the tracking issue — the allow-list in `tests/test_conformance.py` never
widens without a matching row there.

## License

Data: Sentinel-2 imagery and its derived products carry the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice) —
free, full, and open. See `catalog/README.md` for the acknowledgement this
requires once published. Repository code: see `LICENSE`.
