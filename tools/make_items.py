#!/usr/bin/env python3
"""Write one STAC item per published year of the sentinel-2-l2a index.

The partition extension allows items where a partition is a user-meaningful
unit, and a year is one. Each item carries that year's real extent, row count
and platform list, so a client can pick a year without opening gigabytes of
Parquet.

Statistics come from the Parquet footer, never a scan: row count from the file
metadata, bounds from the GeoParquet `geo` key, and the time range from
row-group statistics on `datetime`. That is a few range requests per part
instead of a full read. The one thing that IS read from the data is
`s2:platforms` -- a DISTINCT over the `platform` column, which touches one
dictionary-encoded column and states what the file actually holds rather than
what the calendar implies (Sentinel-2C only starts appearing part-way through
2025, and no schedule can tell you where).

Two modes, because the scheduled workflows and a full rebuild need different
things:

    python3 tools/make_items.py --data-dir ./staging/publish/sentinel-2-l2a
        Describe exactly what is staged. Years with no directory under
        --data-dir keep their committed item JSON untouched.

    python3 tools/make_items.py --data-dir ... --remote-baseline
        Same, plus: for a year that IS staged, any standard part missing from
        --data-dir is read from the published copy over HTTP. The daily
        refresh stages only `live.parquet`; without this the year's item would
        be rewritten to describe the rolling tail alone and drop the millions
        of rows sitting in the published `items.parquet`.

Collection item links are not written here. make_collection.py globs the item
files it finds and links every one, so the two tools cannot disagree about
which items exist.
"""
from __future__ import annotations

import argparse
import json
import math
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

PUBLIC = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"

# Source Cooperative's CDN answers 403 to the default Python-urllib agent, so
# every request here names itself. Without this a HEAD looks like a missing
# file rather than a rejected client.
UA = {"User-Agent": "sentinel-2-catalog-tools/1.0 "
                    "(+https://github.com/portolan-mirrors/sentinel-2-catalog)"}

# The two part names a year can hold, in the order they are advertised.
# `items.parquet` is the consolidated archive; `live.parquet` is the rolling
# tail fetched daily and folded back in monthly. They do not overlap: a
# consolidation rewrites items.parquet and resets live.parquet, so a year's
# row count is the sum of its parts.
PARTS = (
    ("data", "items.parquet", "{year} scenes, GeoParquet 2.0",
     ["data"]),
    ("live", "live.parquet",
     "Rolling tail since the last consolidation, refreshed daily", ["data"]),
)

# Rounding a bbox has to widen it. round() can shrink a bound by up to half a
# unit in the last place, which turns a footprint that touches the antimeridian
# into one that does not quite reach it.
PLACES = 4


def floor4(v: float) -> float:
    return math.floor(v * 10**PLACES) / 10**PLACES


def ceil4(v: float) -> float:
    return math.ceil(v * 10**PLACES) / 10**PLACES


def remote_size(url: str) -> int | None:
    """Content-Length for a published part, without fetching the bytes.

    Also the existence probe: None means "not published", which is how
    --remote-baseline decides a year has no live part yet.
    """
    try:
        req = urllib.request.Request(url, method="HEAD", headers=UA)
        with urllib.request.urlopen(req, timeout=30) as response:
            length = response.headers.get("Content-Length")
        return int(length) if length else None
    except (urllib.error.URLError, OSError, ValueError):
        return None


def connect() -> duckdb.DuckDBPyConnection:
    """A connection that renders timestamps in UTC.

    DuckDB renders TIMESTAMP WITH TIME ZONE in the session time zone, and the
    Parquet statistics come back as rendered strings. Left at a local zone this
    silently shifts every extent -- and rolls the date on anything near
    midnight, which is most of an orbit.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def load_httpfs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")


def iso(value) -> str | None:
    """A DuckDB timestamp string as RFC 3339 with a Z offset."""
    if value is None:
        return None
    text = str(value).strip().replace(" ", "T")
    if text.endswith("+00"):
        text = text[:-3] + "+00:00"
    when = datetime.fromisoformat(text)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    when = when.astimezone(timezone.utc)
    # Keep every digit that carries information and no more. Upstream
    # datetimes are millisecond-precision, so truncating to seconds would move
    # an end_datetime backwards past rows the part actually holds, and padding
    # to microseconds would claim precision that is not there.
    if when.microsecond == 0:
        fraction = ""
    elif when.microsecond % 1000 == 0:
        fraction = f".{when.microsecond // 1000:03d}"
    else:
        fraction = f".{when.microsecond:06d}"
    return when.strftime("%Y-%m-%dT%H:%M:%S") + fraction + "Z"


def as_dt(value: str) -> datetime:
    """An RFC 3339 string back to a datetime, for comparisons.

    Extents are compared as instants, never as text. Two timestamps in the
    same second sort the wrong way as strings, because a fractional part
    starts with '.' and an unfractioned one goes straight to 'Z'.
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def part_stats(con: duckdb.DuckDBPyConnection, location: str) -> dict | None:
    """Row count, bbox and time range for one part, from metadata only."""
    try:
        rows = con.execute(
            "SELECT num_rows FROM parquet_file_metadata(?)", [location]
        ).fetchone()[0]
    except duckdb.Error:
        return None
    kv = con.execute(
        "SELECT value FROM parquet_kv_metadata(?) WHERE key='geo'", [location]
    ).fetchall()
    geo = json.loads(kv[0][0]) if kv else {}
    bbox = ((geo.get("columns") or {}).get("geometry") or {}).get("bbox")
    t0, t1 = con.execute(
        "SELECT min(stats_min_value), max(stats_max_value) "
        "FROM parquet_metadata(?) WHERE path_in_schema='datetime'",
        [location],
    ).fetchone()
    return {"rows": rows, "bbox": bbox, "t0": iso(t0), "t1": iso(t1)}


def platforms(con: duckdb.DuckDBPyConnection, locations: list[str]) -> list[str]:
    """Distinct platforms actually present, by reading one column."""
    rows = con.execute(
        "SELECT DISTINCT platform FROM read_parquet(?) "
        "WHERE platform IS NOT NULL ORDER BY platform", [locations]
    ).fetchall()
    return [row[0] for row in rows]


def discover(year_dir: Path, year: int, remote_baseline: bool) -> list[dict]:
    """The parts that make up one year, local first, published as fallback."""
    found = []
    for key, name, title, roles in PARTS:
        local = year_dir / name
        if local.is_file():
            found.append({"key": key, "name": name, "title": title,
                          "roles": roles, "location": str(local),
                          "size": local.stat().st_size})
            continue
        if not remote_baseline:
            continue
        url = f"{PUBLIC}/sentinel-2-l2a/year={year}/{name}"
        size = remote_size(url)
        if size is not None:
            found.append({"key": key, "name": name, "title": title,
                          "roles": roles, "location": url, "size": size})
    return found


def build_item(con: duckdb.DuckDBPyConnection, year: int,
               parts: list[dict]) -> dict:
    """One STAC item describing every part of a year."""
    stats = []
    for part in parts:
        st = part_stats(con, part["location"])
        if st is None:
            raise SystemExit(f"year={year}: cannot read {part['location']}")
        stats.append(st)

    boxes = [st["bbox"] for st in stats if st["bbox"]]
    if not boxes:
        raise SystemExit(
            f"year={year}: no GeoParquet bbox metadata on any part")
    bbox = [floor4(min(b[0] for b in boxes)), floor4(min(b[1] for b in boxes)),
            ceil4(max(b[2] for b in boxes)), ceil4(max(b[3] for b in boxes))]
    rows = sum(st["rows"] for st in stats)
    start = min((st["t0"] for st in stats if st["t0"]), key=as_dt)
    end = max((st["t1"] for st in stats if st["t1"]), key=as_dt)

    assets = {}
    for part in parts:
        assets[part["key"]] = {
            "href": f"./{part['name']}",
            "type": "application/vnd.apache.parquet",
            "title": part["title"].format(year=year),
            "roles": part["roles"],
            "file:size": part["size"],
        }

    return {
        "type": "Feature",
        "stac_version": "1.1.0",
        "stac_extensions": [
            "https://stac-extensions.github.io/table/v1.2.0/schema.json",
            "https://stac-extensions.github.io/file/v2.1.0/schema.json",
        ],
        "id": str(year),
        "collection": "sentinel-2-l2a",
        "bbox": bbox,
        "geometry": {"type": "Polygon", "coordinates": [[
            [bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]],
            [bbox[0], bbox[3]], [bbox[0], bbox[1]]]]},
        "properties": {
            "title": f"Sentinel-2 L2A scenes, {year}",
            # Null with a start/end pair: the item covers a range, and STAC
            # says say so rather than pick a moment inside it.
            "datetime": None,
            "start_datetime": start,
            "end_datetime": end,
            "table:row_count": rows,
            "s2:platforms": platforms(
                con, [part["location"] for part in parts]),
        },
        "assets": assets,
        # No self link. Portolan forbids one: a static object that hardcodes
        # its own location cannot be mirrored or moved.
        "links": [
            {"rel": "root", "href": "../../catalog.json",
             "type": "application/json",
             "title": "Sentinel-2 L2A STAC-GeoParquet Mirror"},
            {"rel": "parent", "href": "../collection.json",
             "type": "application/json",
             "title": "Sentinel-2 L2A scenes (item index)"},
            {"rel": "collection", "href": "../collection.json",
             "type": "application/json",
             "title": "Sentinel-2 L2A scenes (item index)"},
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data-dir", required=True,
                    help="staged sentinel-2-l2a/ directory holding year=*/")
    ap.add_argument("--out", default=str(ROOT / "catalog" / "sentinel-2-l2a"),
                    help="tracked collection directory (default: catalog/sentinel-2-l2a)")
    ap.add_argument("--remote-baseline", action="store_true",
                    help="for a staged year, read parts missing from --data-dir "
                         "from the published catalog over HTTP")
    ap.add_argument("--years", help="comma list; default = every staged year")
    a = ap.parse_args()

    data = Path(a.data_dir).resolve()
    out = Path(a.out).resolve()
    if not data.is_dir():
        raise SystemExit(f"--data-dir does not exist: {data}")

    wanted = {int(y) for y in a.years.split(",")} if a.years else None
    years = []
    for year_dir in sorted(data.glob("year=*")):
        try:
            year = int(year_dir.name.split("=", 1)[1])
        except ValueError:
            continue
        if wanted is None or year in wanted:
            years.append((year, year_dir))
    if not years:
        raise SystemExit(f"no year partitions under {data}")

    con = connect()
    if a.remote_baseline:
        load_httpfs(con)

    for year, year_dir in years:
        parts = discover(year_dir, year, a.remote_baseline)
        if not parts:
            print(f"  {year}: no parts, skipped")
            continue
        item = build_item(con, year, parts)
        target = out / f"year={year}"
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{year}.json").write_text(json.dumps(item, indent=2) + "\n")
        props = item["properties"]
        print(f"  {year}: {props['table:row_count']:>12,} rows  "
              f"{len(parts)} part(s)  {props['start_datetime']} .. "
              f"{props['end_datetime']}  "
              f"{','.join(props['s2:platforms'])}")
    print(f"\n  {len(years)} item(s) written under {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
