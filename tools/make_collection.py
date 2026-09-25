#!/usr/bin/env python3
"""Generate catalog/<catalog_dir>/collection.json from the published data.

Row counts, the temporal extent and the partition file count are measured,
never hand-written: from each year's committed item where one exists, and from
the Parquet footers of anything staged that no item describes yet. Run this
after tools/make_items.py and before publishing. It is also what lets the
scheduled refresh restamp `updated`, the row count and the temporal extent
without a commit -- the generator runs, the publisher uploads, and the
repository keeps only the stable definition.

The items are the authority for a year, and that is deliberate. --data-dir
sees only what is staged on this machine, which for the daily refresh is one
rolling tail. Reporting that as the collection's extent would tell a client the
archive covers five days and holds a few hundred thousand rows. Each item
carries its whole year, read from the Parquet footer, so the collection is the
union of the items plus whatever is staged that has no item.

    python3 tools/make_collection.py --data-dir ./staging/publish/sentinel-2-l2a
    python3 tools/make_collection.py --data-dir ... --remote-baseline

--remote-baseline means the same thing it means in make_items.py: for a year
that is staged but has no committed item, parts missing from --data-dir are
read from the published copy over HTTP rather than assumed absent.

`table:columns` is generated from the collection's schema module
(tools/s2_schema.py, tools/s2c1_schema.py), which is the single source of
truth for the published schema. `item_assets` comes from the committed cache
for the collection (tools/item_assets.json, tools/item_assets_c1.json), never
the network; refresh it deliberately with --refresh-item-assets.

--collection picks the collection (s2_collections; default the first one, so
every existing call is unchanged). Everything that names the collection
follows the config: id and title, the description's layout and sort-order
sentences (derived from s2_build's rules for that collection, so the prose
cannot say one order when the builder writes another), the columns, the
item_assets cache, the partition glob, the canonical link and the default
--out (catalog/<catalog_dir>/collection.json):

    python3 tools/make_collection.py --collection sentinel-2-c1-l2a \\
        --data-dir ./staging/publish/sentinel-2-c1-l2a --remote-baseline
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))

import s2_collections as cols  # noqa: E402
from make_items import (  # noqa: E402
    DEFAULT_CONFIG, PUBLIC, UA, as_dt, connect, discover, load_httpfs,
    part_stats,
)
from publish import load_config  # noqa: E402
from s2_build import (  # noqa: E402
    ROW_GROUP, TILE_SORT_FROM, ZONE_PARTS, ZONE_PARTS_8, ZONE_SPLIT_8_FROM,
    ZONE_SPLIT_FROM, sort_key,
)
from s2_collections import CollectionConfig  # noqa: E402

S3 = "s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog"
REPO = "https://github.com/taylor-geospatial/s2-stac-geoparquet"
APP = "https://taylor-geospatial.github.io/s2-stac-geoparquet/"

EARTH_SEARCH = "https://earth-search.aws.element84.com/v1"
EARTH_SEARCH_PAGE = "https://element84.com/earth-search/"
# One registry entry covers both: the sentinel-cogs bucket the first
# collection's hrefs point at, and (under "Resources on AWS", read
# 2026-09-21) the e84-earth-search-sentinel-data bucket of Collection 1.
AWS_REGISTRY = "https://registry.opendata.aws/sentinel-2-l2a-cogs/"

# The committed item_assets cache per collection. Named rather than derived
# from the id so that the first collection's file keeps the name every
# workflow and doc already uses.
ITEM_ASSETS_CACHES = {
    "sentinel-2-l2a": HERE / "item_assets.json",
    "sentinel-2-c1-l2a": HERE / "item_assets_c1.json",
}


def item_assets_url(config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The upstream collection whose item_assets block is mirrored; also
    the collection's rel:canonical."""
    return f"{EARTH_SEARCH}/collections/{config.api_collection}"


def item_assets_cache(config: CollectionConfig = DEFAULT_CONFIG) -> Path:
    return ITEM_ASSETS_CACHES[config.id]

# Earth Search's item_assets template carries proj:shape and proj:transform
# taken from one scene. They are true of that scene and of no other, so they
# are dropped here: the per-item `assets` column holds the real values for
# every scene, and a collection-level claim that every Sentinel-2 tile shares
# one UTM origin is simply false.
PER_SCENE_FIELDS = ("proj:shape", "proj:transform", "proj:bbox", "proj:epsg")

# DuckDB type names, lowercased into the table extension's vocabulary.
TYPE_NAMES = {
    "VARCHAR": "string",
    "VARCHAR[]": "list<string>",
    "DOUBLE": "double",
    "DOUBLE[]": "list<double>",
    "BIGINT": "int64",
    "TINYINT": "int8",
    "UINTEGER": "uint32",
    "BOOLEAN": "bool",
    "TIMESTAMP WITH TIME ZONE": "timestamp[us, tz=UTC]",
    "GEOMETRY": "geometry",
}


def zone_parts_text() -> str:
    """The zone-part layout, in prose, from the constants that define it.

    The partition extension has one key here, `year`, because that is the
    only hive directory. The zone split is a second level of file naming
    inside a year, not a `zone=` directory, so it is described in words on
    the year key rather than declared as a key that would imply a path
    segment nobody publishes. Two tiers: the quartiles of ZONE_PARTS for
    2019-2020 and the octants of ZONE_PARTS_8 from 2021, whose boundaries
    nest inside the quartiles'.
    """
    quartiles = ", ".join(f"{label}.parquet (zones {lo}\u2013{hi})"
                          for label, lo, hi in ZONE_PARTS)
    octants = ", ".join(f"{label}.parquet (zones {lo}\u2013{hi})"
                        for label, lo, hi in ZONE_PARTS_8)
    return (f"Years before {ZONE_SPLIT_FROM} are one items.parquet each. "
            f"From {ZONE_SPLIT_FROM} each year is split by the UTM zone of "
            f"s2:mgrs_tile (the leading one or two digits of the tile id): "
            f"{ZONE_SPLIT_FROM}\u2013{ZONE_SPLIT_8_FROM - 1} into "
            f"{len(ZONE_PARTS)} files, {quartiles}; from {ZONE_SPLIT_8_FROM} "
            f"into {len(ZONE_PARTS_8)} files, {octants}. The current year "
            f"adds live.parquet, the tail fetched daily since the last "
            f"consolidation. There is no zone= directory: every part sits "
            f"in year=YYYY/ and matches partition:glob, and a reader with a "
            f"tile id opens only the part whose zone range holds it.")


def year_file_text(config: CollectionConfig) -> str:
    """The layout of a collection that never zone-splits, in prose, from
    the config that defines it: one items.parquet per year, the row-group
    rule of its mode at its target, and the tail."""
    target = config.row_group_size or ROW_GROUP
    if config.row_group_mode == "month_aligned":
        groups = (f"whose row groups are cut on month boundaries at "
                  f"{target:,} rows or fewer -- a group never spans two "
                  f"months, so a month filter reads only that month's groups")
    else:
        groups = (f"in uniform row groups of about {target:,} rows, so a "
                  f"tile's run is a small number of groups and a tile lookup "
                  f"reads only those")
    return (f"Every year is one items.parquet, {groups}. Any year may add "
            f"live.parquet, the tail fetched daily since the last fold, "
            f"which the periodic fold merges back into the year file. There "
            f"is no zone= directory and no zone split: every part sits in "
            f"year=YYYY/ and matches partition:glob.")


def layout_text(config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The part layout for the collection: the zone tiers of the first,
    or the one-file-per-year rule of one that never splits."""
    return zone_parts_text() if config.zone_split else year_file_text(config)


# The sort keys as a reader knows them (s2_build.sort_key names the columns).
_SORT_WORDS = {"_month": "month", "s2:mgrs_tile": "MGRS tile",
               "_tile": "MGRS tile", "datetime": "acquisition time",
               "_hilbert": "Hilbert index"}


def sort_order_text(config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The row order of the parts, in prose, from s2_build.sort_key: the
    first collection's parts published before TILE_SORT_FROM are
    (_month, _hilbert), parts from that year on put the tile between them;
    a collection whose every year sorts the same way gets one sentence,
    worded for a tile-major key (Collection 1's `_tile,datetime`) or a
    month-major one. Derived per vintage so this text cannot say one
    order when the builder writes two."""
    def words(year: int) -> str:
        keys = [_SORT_WORDS[k] for k in sort_key(year, config).split(",")]
        return ", then ".join(keys)
    before, after = words(TILE_SORT_FROM - 1), words(TILE_SORT_FROM)
    if before == after:
        if sort_key(TILE_SORT_FROM, config).startswith(config.tile_column):
            return (f"Rows in every part are ordered by {after}, so one "
                    f"tile's year is one contiguous run and a tile-and-"
                    f"window query reads one or two row groups.")
        return (f"Rows in every part are ordered by {after}, so a reader "
                f"prunes on both time and space, and one tile's month sits "
                f"in a single row group.")
    return (f"Rows in parts published through {TILE_SORT_FROM - 1} are "
            f"ordered by {before}, so a reader prunes on "
            f"both time and space; parts from {TILE_SORT_FROM} are ordered "
            f"by {after}, which also puts one tile's month "
            f"in a single row group.")


def table_columns(config: CollectionConfig = DEFAULT_CONFIG) -> list[dict]:
    """The table:columns array, generated from the canonical schema."""
    out = []
    for name, duck_type, description in config.schema.COLUMNS:
        out.append({
            "name": name,
            # An unmapped compound type (the `links` struct array) is reported
            # as the DuckDB type it is. Guessing a shorter name for it would
            # describe a different column.
            "type": TYPE_NAMES.get(duck_type, duck_type),
            "description": description,
        })
    return out


def refresh_item_assets(config: CollectionConfig = DEFAULT_CONFIG) -> None:
    """Re-fetch the upstream item_assets block into the committed cache."""
    url, path = item_assets_url(config), item_assets_cache(config)
    request = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(request, timeout=60) as response:
        upstream = json.loads(response.read().decode())
    cache = {
        "source": url,
        "fetched": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "item_assets": upstream["item_assets"],
    }
    path.write_text(json.dumps(cache, indent=2) + "\n")
    print(f"refreshed {path.name}: {len(cache['item_assets'])} asset(s) "
          f"from {url}")


def item_assets(config: CollectionConfig = DEFAULT_CONFIG) -> dict:
    """Per-asset band metadata, mirrored for clients that expect it here.

    This is documentation, not a contract: each item's `assets` column carries
    the upstream asset object verbatim and is self-sufficient. What this adds
    is the band metadata (eo:bands, raster:bands, gsd) that the per-item object
    does not repeat for every scene.
    """
    cache = json.loads(item_assets_cache(config).read_text())
    out = {}
    for key, asset in cache["item_assets"].items():
        out[key] = {k: v for k, v in asset.items() if k not in PER_SCENE_FIELDS}
    return out


def collection_assets(collection_dir: Path) -> dict:
    """Assets that ship inside the catalog directory itself.

    Only the thumbnail today, and it is advertised only when the bytes are
    there: an asset naming a file nobody uploaded is worse than no asset. It is
    a render of the data, made by tools/make_thumbnail.py, so it carries a
    checksum as well as a size -- the bytes are small enough to hash and they
    change whenever the archive does.
    """
    thumbnail = collection_dir / "thumbnail.png"
    if not thumbnail.is_file():
        return {}
    raw = thumbnail.read_bytes()
    return {
        "thumbnail": {
            "href": "./thumbnail.png",
            "type": "image/png",
            "title": "Scene density, rendered from the published index",
            "roles": ["thumbnail"],
            "file:size": len(raw),
            # Multihash: '1220' is sha2-256 over 32 bytes, then the digest.
            "file:checksum": "1220" + hashlib.sha256(raw).hexdigest(),
        }
    }


def restamp_root(catalog_path: Path, when: str) -> None:
    """Carry this run's sync time up to the root catalog.

    Portolan makes a mirror record its last sync in a top-level `updated`
    (PORTO-CORE-057, rashid PTL-PRO-003), and requires it on the root catalog
    too when every collection in the tree is a mirror. A hand-written date
    there would be a claim about a sync nobody made, so the tool that measures
    the sync writes it. Nothing else in catalog.json is touched.
    """
    if not catalog_path.is_file():
        return
    root = json.loads(catalog_path.read_text())
    if root.get("updated") == when:
        return
    # Rebuilt rather than assigned so that a new `updated` lands above `links`
    # instead of after it. The diff of a restamp should be one line.
    fields = [(key, value) for key, value in root.items() if key != "updated"]
    where = next((i for i, (key, _) in enumerate(fields) if key == "links"),
                 len(fields))
    fields.insert(where, ("updated", when))
    catalog_path.write_text(json.dumps(dict(fields), indent=2) + "\n")
    print(f"restamped {catalog_path.name}: updated {when}")


def committed_items(collection_dir: Path) -> dict[int, dict]:
    """Every year item already on disk, by year."""
    found = {}
    for path in sorted(collection_dir.glob("year=*/[0-9]*.json")):
        try:
            item = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if item.get("type") != "Feature":
            continue
        try:
            found[int(path.stem)] = item
        except ValueError:
            continue
    return found


def description(config: CollectionConfig, rows: int, span: str) -> str:
    """The collection's description: what the rows are, how the parts are
    laid out and sorted (derived from the build rules), and what the
    record covers. The first collection's text is the one it has always
    published; Collection 1's says what differs -- the tile column, the
    reprocessing that keeps growing old years, and the fold model."""
    if config.id == cols.DEFAULT:
        return (
            f"The AWS Earth Search item index for Sentinel-2 L2A, republished "
            f"as one year-partitioned GeoParquet 2.0 table of {rows:,} rows "
            f"covering {span}. One row per scene, carrying the whole "
            f"STAC item: footprint, acquisition time, MGRS tile, cloud cover, "
            f"the scene-classification percentages, and the complete upstream "
            f"`assets` object as a JSON string. Every Cloud-Optimized GeoTIFF "
            f"URL is therefore in the table -- no API call, no URL template to "
            f"guess -- while the imagery itself stays in the `sentinel-cogs` "
            f"bucket on AWS. {sort_order_text(config)} "
            f"{zone_parts_text()} The record starts in November 2016, "
            f"when Earth Search's first L2A Cloud-Optimized GeoTIFFs were "
            f"produced; 2015 and most of 2016 have no COG products, and "
            f"2017-2018 are partial, which is what Earth Search serves "
            f"rather than a gap introduced here. "
            f"Contains modified Copernicus Sentinel data. "
            f"Read the [agent guide](AGENTS.md) before querying: "
            f"`sat:orbit_state` and `s2:granule_id` are NULL on newer items, "
            f"and `assets` is a JSON string, not a struct."
        )
    return (
        f"The AWS Earth Search item index for Sentinel-2 Collection 1 L2A "
        f"(ESA's reprocessing of the whole archive to one processing "
        f"baseline), republished as one year-partitioned GeoParquet 2.0 "
        f"table of {rows:,} rows covering {span}. One row per scene, "
        f"carrying the whole STAC item: footprint, acquisition time, MGRS "
        f"tile, cloud cover, the scene-classification percentages, the "
        f"viewing and sun angles, `created`/`updated`, and the complete "
        f"upstream `assets` object as a JSON string. Every Cloud-Optimized "
        f"GeoTIFF URL is therefore in the table -- no API call, no URL "
        f"template to guess -- while the imagery itself stays in the "
        f"`{config.bucket}` bucket on AWS. Collection 1 items carry no "
        f"`s2:mgrs_tile`: the tile is `grid:code` (`MGRS-31UET`), and the "
        f"bare id (`31UET`) is the added `{config.tile_column}` column, THE "
        f"spatial join key. {sort_order_text(config)} {year_file_text(config)} "
        f"ESA's reprocessing is still running, so old years keep gaining "
        f"scenes with recent `created` timestamps; the daily refresh looks "
        f"back on `created`, not `datetime`, and appends what it finds to "
        f"the year's live.parquet. Contains modified Copernicus Sentinel "
        f"data. Read the [agent guide](AGENTS.md) before querying: "
        f"`s2:dark_features_percentage` is NULL from processing baseline "
        f"05.11, `processing:software` and `assets` are JSON strings, not "
        f"structs."
    )


def keywords(config: CollectionConfig) -> list[str]:
    base = ["sentinel-2", "l2a", "stac-geoparquet", "earth-search",
            "esa", "copernicus", "satellite imagery", "cloud cover", "mgrs"]
    if config.id == cols.DEFAULT:
        return base
    return [*base[:2], "collection-1", *base[2:]]


def providers(config: CollectionConfig) -> list[dict]:
    """Who made what. "host" belongs to whoever serves THESE files, and
    rashid (PTL-PRV-002) allows exactly one provider to claim it; the
    parties who serve the imagery this index points at are processors,
    because that is a different set of bytes."""
    esa = {"name": "European Space Agency (ESA)",
           "description": "Operates the Sentinel-2 mission and produces the "
                          "L2A surface reflectance products for the "
                          "Copernicus programme.",
           "roles": ["producer", "licensor"],
           "url": "https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2"}
    portolan = {"name": "Portolan Mirrors",
                "description": "Republishes the Earth Search item index as "
                               "partitioned STAC-GeoParquet.",
                "roles": ["processor", "host"],
                "url": REPO}
    if config.id == cols.DEFAULT:
        return [
            esa,
            # Sinergise and AWS serve the imagery this index points at,
            # which is a different set of bytes, so the role that fits is
            # processor: they turn ESA's products into the COGs.
            {"name": "Sinergise and AWS Open Data",
             "description": "Convert the ESA L2A products to Cloud-Optimized "
                            "GeoTIFFs and serve them in the public "
                            "sentinel-cogs bucket that every asset href in "
                            "this table points at.",
             "roles": ["processor"],
             "url": AWS_REGISTRY},
            {"name": "Element 84 (Earth Search)",
             "description": "Runs the Earth Search STAC API whose "
                            "sentinel-2-l2a items this table mirrors.",
             "roles": ["processor"],
             "url": EARTH_SEARCH},
            portolan,
        ]
    return [
        esa,
        # Element 84 both converts the Collection 1 products to COGs (the
        # e84-earth-search-sentinel-data bucket is theirs, listed on the
        # AWS Registry of Open Data as "Collection 1 Level 2A scenes and
        # metadata", managed by Element 84) and runs the API.
        {"name": "Element 84 (Earth Search)",
         "description": "Converts the ESA Collection 1 L2A products to "
                        "Cloud-Optimized GeoTIFFs, serves them in the public "
                        f"{config.bucket} bucket on AWS Open Data that every "
                        "asset href in this table points at, and runs the "
                        f"Earth Search STAC API whose {config.api_collection} "
                        "items this table mirrors.",
         "roles": ["processor"],
         "url": EARTH_SEARCH},
        portolan,
    ]


def via_links(config: CollectionConfig) -> list[dict]:
    """rel:via names the upstream, and Portolan wants a page a person can
    read (PTL-PRO-001 requires text/html on every one). The API endpoint
    itself is the machine-readable upstream, so it is the rel:canonical
    rather than a via link claiming to be HTML."""
    registry_title = ("Sentinel-2 L2A COGs on the AWS Registry of Open Data"
                      if config.id == cols.DEFAULT else
                      "Sentinel-2 Collection 1 L2A COGs on the AWS Registry "
                      "of Open Data")
    return [
        {"rel": "via", "href": EARTH_SEARCH_PAGE, "type": "text/html",
         "title": "Earth Search by Element 84 (upstream source)"},
        {"rel": "via", "href": AWS_REGISTRY, "type": "text/html",
         "title": registry_title},
    ]


def preview_url(config: CollectionConfig) -> str:
    """The explorer, on this collection. The app's default collection is
    the first one; any other is named in the query string."""
    return APP if config.id == cols.DEFAULT else f"{APP}?collection={config.id}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    dirs = ", ".join(cols.get(n).catalog_dir for n in cols.NAMES)
    cols.add_collection_arg(ap)
    ap.add_argument("--data-dir", required=True,
                    help="staged collection directory holding year=*/")
    ap.add_argument("--out",
                    help="collection.json path (default "
                         f"catalog/<catalog_dir>/collection.json: {dirs})")
    ap.add_argument("--remote-baseline", action="store_true",
                    help="for a staged year with no committed item, read parts "
                         "missing from --data-dir from the published catalog")
    ap.add_argument("--refresh-item-assets", action="store_true",
                    help="re-fetch the collection's tools/item_assets*.json "
                         "from Earth Search, then continue")
    a = ap.parse_args()
    config = cols.get(a.collection)

    # The hrefs this tool writes and the bytes tools/publish.py uploads have to
    # name the same place. They are separate constants on purpose -- one is
    # metadata, one is deploy config -- so the drift is checked rather than
    # hoped for.
    deploy = load_config()
    for label, ours, theirs in (("public_base", PUBLIC, deploy["public_base"]),
                                ("write_prefix", S3, deploy["write_prefix"])):
        if ours.rstrip("/") != theirs.rstrip("/"):
            raise SystemExit(
                f"{label} in catalog.publish.yaml is {theirs!r}, but this "
                f"generator writes {ours!r}. Fix one of them.")

    if a.refresh_item_assets:
        refresh_item_assets(config)

    data = Path(a.data_dir).resolve()
    out = (Path(a.out).resolve() if a.out
           else ROOT / "catalog" / config.catalog_dir / "collection.json")
    if not data.is_dir():
        raise SystemExit(f"--data-dir does not exist: {data}")

    items = committed_items(out.parent)
    con = connect()
    if a.remote_baseline:
        load_httpfs(con)

    rows = 0
    files = 0
    starts: list[str] = []
    ends: list[str] = []
    years: set[int] = set()

    for year, item in items.items():
        years.add(year)
        props = item.get("properties") or {}
        rows += props.get("table:row_count") or 0
        for key in ("start_datetime", "end_datetime"):
            value = props.get(key)
            if value:
                (starts if key == "start_datetime" else ends).append(value)
        files += sum(1 for asset in (item.get("assets") or {}).values()
                     if str(asset.get("href", "")).endswith(".parquet"))

    # Anything staged that no item describes. A year WITH an item is skipped
    # whole: that item already counts every part of its year, so measuring the
    # local copy again would report the same rows twice.
    for year_dir in sorted(data.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        if year in items:
            continue
        for part in discover(year_dir, year, a.remote_baseline, config=config):
            stats = part["stats"] or part_stats(con, part["location"])
            if stats is None:
                raise SystemExit(f"year={year}: cannot read {part['location']}")
            years.add(year)
            rows += stats["rows"]
            files += 1
            if stats["t0"]:
                starts.append(stats["t0"])
            if stats["t1"]:
                ends.append(stats["t1"])

    if not years or not starts or not ends:
        raise SystemExit(
            f"nothing to describe: no items under {out.parent} and no readable "
            f"parts under {data}")

    earliest, latest = min(starts, key=as_dt), max(ends, key=as_dt)
    first, last = min(years), max(years)
    span = str(first) if first == last else f"{first} to {last}"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    collection = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://schemas.portolan-sdi.org/portolan/v0.2.0/schema.json",
            "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json",
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        ],
        "id": config.id,
        "title": f"{config.title} (item index)",
        "description": description(config, rows, span),
        # The Copernicus Sentinel Data Terms and Conditions, by its SPDX id.
        "license": "CC-BY-SA-3.0-IGO",
        "keywords": keywords(config),
        "updated": now,
        "providers": providers(config),
        "extent": {
            # Sentinel-2 acquires between roughly 83N and 56S, but the index is
            # global by design and a partial year must not narrow what the
            # collection claims to cover. The per-year items carry the measured
            # footprint bounds.
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[earliest, latest]]},
        },
        "partition:scheme": "hive",
        "partition:strategy": "temporal",
        "partition:keys": [
            {"name": "year", "type": "int32",
             "description": f"Year of acquisition (UTC). {layout_text(config)}"}
        ],
        "partition:file_count": files,
        # The `*` part name covers items.parquet, the z*.parquet zone parts
        # and live.parquet alike, so a reader that globs gets the whole year
        # including today, whichever shape the year has.
        "partition:glob": f"{S3}/{config.catalog_dir}/year=*/*.parquet",
        "table:primary_geometry": "geometry",
        "table:row_count": rows,
        "table:columns": table_columns(config),
        "item_assets": item_assets(config),
        "assets": collection_assets(out.parent),
        # No self link. Portolan forbids one: a static object that hardcodes
        # its own location cannot be mirrored or moved. stac-check nags; rashid
        # is the gate.
        "links": [
            {"rel": "root", "href": "../catalog.json", "type": "application/json",
             "title": "Sentinel-2 L2A STAC-GeoParquet Mirror"},
            {"rel": "parent", "href": "../catalog.json", "type": "application/json",
             "title": "Sentinel-2 L2A STAC-GeoParquet Mirror"},
            {"rel": "describedby", "href": "./README.md", "type": "text/markdown",
             "title": "Collection README"},
            {"rel": "agents", "href": "./AGENTS.md", "type": "text/markdown",
             "title": "Collection agent guide"},
            *via_links(config),
            {"rel": "canonical", "href": item_assets_url(config),
             "type": "application/json",
             "title": "The upstream Earth Search collection"},
            # STAC uses rel:preview for a preview of the data itself, which is
            # what an interactive map is. rel:alternate is reserved by the
            # Language extension.
            {"rel": "preview", "href": preview_url(config), "type": "text/html",
             "title": "Interactive scene explorer"},
        ],
    }

    # One item link per committed year item. Globbing here rather than having
    # make_items.py append means the two tools cannot disagree about which
    # items exist: a year whose JSON was deleted stops being linked, and a year
    # written by a partial run is linked without a full rebuild.
    for year in sorted(committed_items(out.parent)):
        collection["links"].append({
            "rel": "item", "href": f"./year={year}/{year}.json",
            "type": "application/geo+json",
            "title": f"{config.title}, {year}"})

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(collection, indent=2) + "\n")
    print(f"wrote {out}: {rows:,} rows, {files} partition file(s), "
          f"{first}-{last}, {earliest} .. {latest}")

    restamp_root(out.parent.parent / "catalog.json", now)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
