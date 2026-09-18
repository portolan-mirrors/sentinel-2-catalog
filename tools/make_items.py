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
        Same, plus: for a year that IS staged, any candidate part missing from
        --data-dir is read from the published copy over HTTP. The daily
        refresh stages only `live.parquet`; without this the year's item would
        be rewritten to describe the rolling tail alone and drop the millions
        of rows sitting in the published archive parts.

        A probe that fails is not a part that is missing. A 404 means the part
        was never published; anything else -- a timeout, a 5xx, a broken link
        -- means the question went unanswered, and the run falls back to what
        the committed item recorded for that part rather than writing a
        smaller year. See discover() for the three cases.

A year takes one of three shapes (spec Amendment 3), and all are discovered
from the same candidate list: 2015-2018 are one `items.parquet`; 2019-2020
are four zone quartiles `z01-20.parquet` .. `z47-60.parquet`
(s2_build.ZONE_PARTS); from 2021 eight zone octants `z01-15.parquet` ..
`z53-60.parquet` (ZONE_PARTS_8), split by the UTM zone of `s2:mgrs_tile`.
Any shape may add `live.parquet`. Every part present becomes its own asset,
with its own row count, time range and size, so a client with a tile id can
pick the one part its zone lives in and the year's totals are the sum of
the parts.

Collection item links are not written here. make_collection.py globs the item
files it finds and links every one, so the two tools cannot disagree about
which items exist.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_build import ZONE_PARTS, ZONE_PARTS_8  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

PUBLIC = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"

# Source Cooperative's CDN answers 403 to the default Python-urllib agent, so
# every request here names itself. Without this a HEAD looks like a missing
# file rather than a rejected client.
UA = {"User-Agent": "sentinel-2-catalog-tools/1.0 "
                    "(+https://github.com/portolan-mirrors/sentinel-2-catalog)"}

# Every part name a year can hold, in the order they are advertised:
# (asset key, file name, title template, roles). `items.parquet` is the
# consolidated archive of a year published before the zone split; the four
# quartile `z*.parquet` files are the archive of 2019-2020, one per
# ZONE_PARTS range, and the eight octants the archive of a year from 2021,
# one per ZONE_PARTS_8 range; `live.parquet` is the rolling tail fetched
# daily and folded back in monthly. A year holds one archive shape, never
# two, and the archive and the tail do not overlap: a consolidation
# rewrites the archive parts and resets live.parquet, so a year's row count
# is the sum of its parts. Fourteen candidates, of which a year has at most
# nine; a probe is one HEAD, so the misses cost nothing worth a table of
# which year has which.
PARTS = (
    ("data", "items.parquet", "{year} scenes, GeoParquet 2.0",
     ["data"]),
    *((f"data-{label}", f"{label}.parquet",
       f"{{year}} scenes, UTM zones {lo}\u2013{hi}", ["data"])
      for label, lo, hi in (*ZONE_PARTS, *ZONE_PARTS_8)),
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


#: What a probe found. The third value is the point of the type: "I could not
#: ask" is not the same answer as "it is not there", and collapsing the two is
#: how a five-minute outage turns into a published record that lost a year.
PRESENT, ABSENT, UNKNOWN = "present", "absent", "unknown"

# Source Cooperative answers a missing object with 404 (verified against
# data.source.coop, 2026-09-16), so 404 and 410 are the only statuses that mean
# "not published". A 403, a 5xx, a timeout, a DNS failure: those mean the
# question was not answered.
ABSENT_STATUSES = (404, 410)


def remote_probe(url: str) -> tuple[str, int | None]:
    """Ask whether a published part exists, and how big it is.

    Returns (PRESENT, size), (ABSENT, None) or (UNKNOWN, None). A HEAD, so the
    bytes are never fetched. Size is worth one round trip; a checksum would be
    worth gigabytes of download, which is why published data assets carry
    file:size and no file:checksum.
    """
    request = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            length = response.headers.get("Content-Length")
        return PRESENT, (int(length) if length else None)
    except urllib.error.HTTPError as exc:
        return (ABSENT, None) if exc.code in ABSENT_STATUSES else (UNKNOWN, None)
    except (urllib.error.URLError, OSError, ValueError):
        return UNKNOWN, None


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


def read_item(path: Path) -> dict | None:
    """The item already on disk for a year, when there is a readable one."""
    try:
        item = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return item if isinstance(item, dict) and item.get("type") == "Feature" else None


def year_platforms(con: duckdb.DuckDBPyConnection, parts: list[dict],
                   committed: dict | None) -> list[str]:
    """Platforms across a year's parts, keeping what a skipped part recorded."""
    locations = [part["location"] for part in parts if part["location"]]
    found = set(platforms(con, locations)) if locations else set()
    if any(part["source"] == "committed" for part in parts) and committed:
        found |= set((committed.get("properties") or {}).get("s2:platforms") or [])
    return sorted(found)


def recorded_part(committed: dict | None, key: str) -> dict | None:
    """One part's own measurements, as the committed item recorded them.

    Every data asset carries the row count and time range of the part it names,
    which is what makes a part-level fallback possible at all: the item's
    properties are the year's totals and cannot be decomposed back into parts.
    """
    if not committed:
        return None
    asset = (committed.get("assets") or {}).get(key)
    if not asset or asset.get("table:row_count") is None:
        return None
    return {"rows": asset["table:row_count"],
            # The committed item's bbox covers every part of the year, so using
            # it for one part widens the result at worst. A bbox is allowed to
            # be a superset of its geometry; it is never allowed to be short.
            "bbox": committed.get("bbox"),
            "t0": asset.get("start_datetime"),
            "t1": asset.get("end_datetime"),
            "size": asset.get("file:size")}


def discover(year_dir: Path, year: int, remote_baseline: bool,
             committed: dict | None = None, probe=remote_probe) -> list[dict]:
    """The parts that make up one year: staged, published, or last recorded.

    A part is read from --data-dir when it is staged there. Otherwise, and only
    under --remote-baseline, the published copy is probed. What happens next
    depends on which answer came back, and the three answers are not
    interchangeable:

    PRESENT  read the published part over HTTP.
    ABSENT   it was never published (404). Skip it -- unless the committed item
             names it, in which case the published record has lost a file and
             this run must not paper over that by rewriting the year smaller.
    UNKNOWN  the probe failed: a timeout, a 5xx, a broken link. Fall back to
             what the committed item recorded for that part, so a bad minute on
             the network cannot shrink the record. A part the committed item
             never recorded is skipped when that item records the year's other
             parts: the year cannot come out smaller than its record, and a
             part no successful run has seen is not made real by a probe that
             could not answer. With nothing committed at all, stop: an item
             built from the parts that happened to answer is worse than no new
             item at all.
    """
    recorded_keys = {key for key, _, _, _ in PARTS
                     if recorded_part(committed, key) is not None}
    found = []
    for key, name, title, roles in PARTS:
        common = {"key": key, "name": name, "title": title, "roles": roles}
        local = year_dir / name
        if local.is_file():
            found.append({**common, "source": "local", "location": str(local),
                          "size": local.stat().st_size, "stats": None})
            continue
        if not remote_baseline:
            continue

        url = f"{PUBLIC}/sentinel-2-l2a/year={year}/{name}"
        state, size = probe(url)
        recorded = recorded_part(committed, key)

        if state == PRESENT:
            found.append({**common, "source": "remote", "location": url,
                          "size": size, "stats": None})
        elif state == ABSENT:
            if recorded is not None:
                raise SystemExit(
                    f"year={year}: {url} is gone (404), but the committed item "
                    f"describes it as {recorded['rows']:,} rows. Refusing to "
                    f"rewrite the year without it -- restore the file, or "
                    f"delete the asset from the item on purpose.")
        else:
            if recorded is None:
                if recorded_keys:
                    print(f"  {year}: cannot reach {name} and the committed "
                          f"item does not record it; left out",
                          file=sys.stderr)
                    continue
                raise SystemExit(
                    f"year={year}: cannot reach {url}, and no committed item "
                    f"records what it holds. Refusing to write an item from "
                    f"the parts that answered.")
            found.append({**common, "source": "committed", "location": None,
                          "size": recorded["size"], "stats": recorded})
    return found


def build_item(con: duckdb.DuckDBPyConnection, year: int, parts: list[dict],
               committed: dict | None = None) -> dict:
    """One STAC item describing every part of a year."""
    stats = []
    for part in parts:
        st = part["stats"] or part_stats(con, part["location"])
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

    # Each asset states what its own part holds, not just how big it is. The
    # item's properties are the year's totals and cannot be split back up, so
    # without this a part that cannot be read has nothing to fall back to.
    assets = {}
    for part, st in zip(parts, stats):
        assets[part["key"]] = {
            "href": f"./{part['name']}",
            "type": "application/vnd.apache.parquet",
            "title": part["title"].format(year=year),
            "roles": part["roles"],
            "start_datetime": st["t0"],
            "end_datetime": st["t1"],
            "table:row_count": st["rows"],
            **({"file:size": part["size"]} if part["size"] else {}),
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
            # A part that fell back to the committed item cannot be scanned, so
            # its platforms come from what that item recorded. Dropping them
            # would state that a platform stopped flying.
            "s2:platforms": year_platforms(con, parts, committed),
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
        target = out / f"year={year}"
        committed = read_item(target / f"{year}.json")
        parts = discover(year_dir, year, a.remote_baseline, committed)
        if not parts:
            print(f"  {year}: no parts, skipped")
            continue
        item = build_item(con, year, parts, committed)
        target.mkdir(parents=True, exist_ok=True)
        (target / f"{year}.json").write_text(json.dumps(item, indent=2) + "\n")
        props = item["properties"]
        kept = [part["name"] for part in parts if part["source"] == "committed"]
        note = f"   KEPT FROM LAST RUN: {','.join(kept)}" if kept else ""
        print(f"  {year}: {props['table:row_count']:>12,} rows  "
              f"{len(parts)} part(s)  {props['start_datetime']} .. "
              f"{props['end_datetime']}  "
              f"{','.join(props['s2:platforms'])}{note}")
    print(f"\n  {len(years)} item(s) written under {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
