#!/usr/bin/env python3
"""Generate catalog/sentinel-2-l2a/collection.json from the published data.

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

`table:columns` is generated from tools/s2_schema.py, which is the single
source of truth for the published schema. `item_assets` comes from the
committed tools/item_assets.json cache, never the network; refresh it
deliberately with --refresh-item-assets.
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

from make_items import (  # noqa: E402
    PUBLIC, UA, as_dt, connect, discover, load_httpfs, part_stats,
)
from publish import load_config  # noqa: E402
from s2_schema import COLUMNS  # noqa: E402

S3 = "s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog"
REPO = "https://github.com/portolan-mirrors/sentinel-2-catalog"
APP = "https://portolan-mirrors.github.io/sentinel-2-catalog/"

EARTH_SEARCH = "https://earth-search.aws.element84.com/v1"
EARTH_SEARCH_PAGE = "https://element84.com/earth-search/"
AWS_REGISTRY = "https://registry.opendata.aws/sentinel-2-l2a-cogs/"
ITEM_ASSETS_URL = f"{EARTH_SEARCH}/collections/sentinel-2-l2a"
ITEM_ASSETS_CACHE = HERE / "item_assets.json"

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
    "TIMESTAMP WITH TIME ZONE": "timestamp[us, tz=UTC]",
    "GEOMETRY": "geometry",
}


def table_columns() -> list[dict]:
    """The table:columns array, generated from the canonical schema."""
    out = []
    for name, duck_type, description in COLUMNS:
        out.append({
            "name": name,
            # An unmapped compound type (the `links` struct array) is reported
            # as the DuckDB type it is. Guessing a shorter name for it would
            # describe a different column.
            "type": TYPE_NAMES.get(duck_type, duck_type),
            "description": description,
        })
    return out


def refresh_item_assets() -> None:
    """Re-fetch the upstream item_assets block into the committed cache."""
    request = urllib.request.Request(ITEM_ASSETS_URL, headers=UA)
    with urllib.request.urlopen(request, timeout=60) as response:
        upstream = json.loads(response.read().decode())
    cache = {
        "source": ITEM_ASSETS_URL,
        "fetched": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "item_assets": upstream["item_assets"],
    }
    ITEM_ASSETS_CACHE.write_text(json.dumps(cache, indent=2) + "\n")
    print(f"refreshed {ITEM_ASSETS_CACHE.name}: "
          f"{len(cache['item_assets'])} asset(s) from {ITEM_ASSETS_URL}")


def item_assets() -> dict:
    """Per-asset band metadata, mirrored for clients that expect it here.

    This is documentation, not a contract: each item's `assets` column carries
    the upstream asset object verbatim and is self-sufficient. What this adds
    is the band metadata (eo:bands, raster:bands, gsd) that the per-item object
    does not repeat for every scene.
    """
    cache = json.loads(ITEM_ASSETS_CACHE.read_text())
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data-dir", required=True,
                    help="staged sentinel-2-l2a/ directory holding year=*/")
    ap.add_argument("--out",
                    default=str(ROOT / "catalog" / "sentinel-2-l2a" / "collection.json"),
                    help="collection.json path")
    ap.add_argument("--remote-baseline", action="store_true",
                    help="for a staged year with no committed item, read parts "
                         "missing from --data-dir from the published catalog")
    ap.add_argument("--refresh-item-assets", action="store_true",
                    help="re-fetch tools/item_assets.json from Earth Search, "
                         "then continue")
    a = ap.parse_args()

    # The hrefs this tool writes and the bytes tools/publish.py uploads have to
    # name the same place. They are separate constants on purpose -- one is
    # metadata, one is deploy config -- so the drift is checked rather than
    # hoped for.
    config = load_config()
    for label, ours, theirs in (("public_base", PUBLIC, config["public_base"]),
                                ("write_prefix", S3, config["write_prefix"])):
        if ours.rstrip("/") != theirs.rstrip("/"):
            raise SystemExit(
                f"{label} in catalog.publish.yaml is {theirs!r}, but this "
                f"generator writes {ours!r}. Fix one of them.")

    if a.refresh_item_assets:
        refresh_item_assets()

    data = Path(a.data_dir).resolve()
    out = Path(a.out).resolve()
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
        for part in discover(year_dir, year, a.remote_baseline):
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
        "id": "sentinel-2-l2a",
        "title": "Sentinel-2 L2A scenes (item index)",
        "description": (
            f"The AWS Earth Search item index for Sentinel-2 L2A, republished "
            f"as one year-partitioned GeoParquet 2.0 table of {rows:,} rows "
            f"covering {span}. One row per scene, carrying the whole "
            f"STAC item: footprint, acquisition time, MGRS tile, cloud cover, "
            f"the scene-classification percentages, and the complete upstream "
            f"`assets` object as a JSON string. Every Cloud-Optimized GeoTIFF "
            f"URL is therefore in the table -- no API call, no URL template to "
            f"guess -- while the imagery itself stays in the `sentinel-cogs` "
            f"bucket on AWS. Rows are ordered by month and then by a Hilbert "
            f"index, so a reader prunes on both time and space. Coverage before "
            f"December 2018 is partial: nothing for 2015-2016 and part of "
            f"2017-2018, which is what Earth Search serves rather than a gap "
            f"introduced here. Contains modified Copernicus Sentinel data. "
            f"Read the [agent guide](AGENTS.md) before querying: "
            f"`sat:orbit_state` and `s2:granule_id` are NULL on newer items, "
            f"and `assets` is a JSON string, not a struct."
        ),
        # The Copernicus Sentinel Data Terms and Conditions, by its SPDX id.
        "license": "CC-BY-SA-3.0-IGO",
        "keywords": ["sentinel-2", "l2a", "stac-geoparquet", "earth-search",
                     "esa", "copernicus", "satellite imagery", "cloud cover",
                     "mgrs"],
        "updated": now,
        "providers": [
            {"name": "European Space Agency (ESA)",
             "description": "Operates the Sentinel-2 mission and produces the "
                            "L2A surface reflectance products for the "
                            "Copernicus programme.",
             "roles": ["producer", "licensor"],
             "url": "https://sentinels.copernicus.eu/web/sentinel/missions/sentinel-2"},
            # "host" belongs to whoever serves THESE files, and rashid
            # (PTL-PRV-002) allows exactly one provider to claim it. Sinergise
            # and AWS serve the imagery this index points at, which is a
            # different set of bytes, so the role that fits is processor: they
            # turn ESA's products into the COGs.
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
            {"name": "Portolan Mirrors",
             "description": "Republishes the Earth Search item index as "
                            "partitioned STAC-GeoParquet.",
             "roles": ["processor", "host"],
             "url": REPO},
        ],
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
             "description": "Year of acquisition (UTC)."}
        ],
        "partition:file_count": files,
        # The `*` part name covers both items.parquet and live.parquet, so a
        # reader that globs gets the whole year including today.
        "partition:glob": f"{S3}/sentinel-2-l2a/year=*/*.parquet",
        "table:primary_geometry": "geometry",
        "table:row_count": rows,
        "table:columns": table_columns(),
        "item_assets": item_assets(),
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
            # rel:via names the upstream, and Portolan wants a page a person
            # can read (PTL-PRO-001 requires text/html on every one). The API
            # endpoint itself is the machine-readable upstream, so it is the
            # rel:canonical below rather than a via link claiming to be HTML.
            {"rel": "via", "href": EARTH_SEARCH_PAGE, "type": "text/html",
             "title": "Earth Search by Element 84 (upstream source)"},
            {"rel": "via", "href": AWS_REGISTRY, "type": "text/html",
             "title": "Sentinel-2 L2A COGs on the AWS Registry of Open Data"},
            {"rel": "canonical", "href": f"{EARTH_SEARCH}/collections/sentinel-2-l2a",
             "type": "application/json",
             "title": "The upstream Earth Search collection"},
            # STAC uses rel:preview for a preview of the data itself, which is
            # what an interactive map is. rel:alternate is reserved by the
            # Language extension.
            {"rel": "preview", "href": APP, "type": "text/html",
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
            "title": f"Sentinel-2 L2A scenes, {year}"})

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(collection, indent=2) + "\n")
    print(f"wrote {out}: {rows:,} rows, {files} partition file(s), "
          f"{first}-{last}, {earliest} .. {latest}")

    restamp_root(out.parent.parent / "catalog.json", now)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
