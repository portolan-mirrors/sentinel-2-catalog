# sentinel-2-catalog

A [Portolan](https://www.portolan-sdi.org/) catalog mirroring the **AWS Earth
Search Sentinel-2 L2A archive** — 51.25 million items when Earth Search was
counted on 2026-09-15 — as partitioned STAC-GeoParquet, plus MGRS-tile
aggregate stats. The imagery stays on AWS; this catalog carries only the item
index, so a client queries the archive with plain Parquet reads instead of a
rate-limited STAC API.

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

| Collection | Holds |
|---|---|
| `sentinel-2-l2a` | The item index — Earth Search's Sentinel-2 L2A metadata, partitioned by year (`year=YYYY/items.parquet` through 2018; four UTM-zone parts `z01-20`, `z21-35`, `z36-46`, `z47-60.parquet` for 2019–2020; eight from 2021, `z01-15`, `z16-20`, `z21-31`, `z32-35`, `z36-40`, `z41-46`, `z47-52`, `z53-60.parquet`) plus a `live.parquet` tail, sorted `(_month, _hilbert)` |
| `stats` | MGRS tile × month aggregates (scene counts, cloud cover) and a tile-footprint PMTiles layer, for the explorer app and for query planning. Still to come. |

The backfill fills `sentinel-2-l2a` year by year, so
[`catalog/sentinel-2-l2a/collection.json`](catalog/sentinel-2-l2a/collection.json)
is the authority on how much of the upstream record has landed: it carries the
measured row count and time range, restamped by `tools/make_collection.py` on
every run.

`tools/s2_fetch.py` and `tools/s2_build.py` fetch and compact the parts;
`tools/s2_stats.py` and the GitHub Actions workflows that run everything on a
schedule land in later work. See the design spec for the full plan.

## Why no imagery, no API

Sentinel-2 L2A Cloud-Optimized GeoTIFFs already live on AWS in the
`sentinel-cogs` bucket. Re-hosting them here would duplicate petabytes of data
this catalog does not need to own. Nothing has to be derived to reach them:
each row carries the upstream `assets` object verbatim, so every COG URL for a
scene is in the row that describes it — documented on the
[`sentinel-2-l2a` collection](catalog/sentinel-2-l2a/AGENTS.md). Serving the
index as GeoParquet, rather than behind a STAC API, means a client filters the
whole archive with a Parquet range read against a public bucket: no server to
rate-limit, no server to keep running.

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

Set `CI_LIGHT=1` when the data bytes are not on this machine, which is the
normal case: it exempts asset hrefs with a data suffix from `test_links.py`,
and nothing else. Every structural link is still checked.

Unit tests for the tools (`test_fetch.py`, `test_build.py`, `test_schema.py`,
`test_make_items.py`) run under `CI_LIGHT=1 python3 -m pytest tests/ -q`. They
are not in `run_all.py` because they need `duckdb`, and the build test needs
`geoparquet-io` on the PATH.

Install `geoparquet-io==1.5.0` — the version every workflow pins. Unpinned, it
moved from 1.3.0 to 1.5.0 under this catalog without anyone noticing, and
`gpio sort column`'s `--compression-level` went from a no-op to a flag that
costs real time (parts publish at zstd 18; 22 costs hours per year).
`tools/s2_build.py` needs at least 1.4 for that flag and for
`--write-memory`; testing against an older local install measures a tool CI
does not run.

CI runs `rashid`, `stac-check`, and `tests/run_all.py` on every pull request.
`docs/conformance.md` records any accepted deviation, with the rule, why, and
the tracking issue — the allow-list in `tests/test_conformance.py` never
widens without a matching row there.

## License

Data: Sentinel-2 imagery and its derived products carry the
[Copernicus Sentinel Data Terms and Conditions](https://sentinels.copernicus.eu/documents/247904/690755/Sentinel_Data_Legal_Notice) —
free, full, and open. See `catalog/README.md` for the acknowledgement this
requires once published. Repository code: see `LICENSE`.
