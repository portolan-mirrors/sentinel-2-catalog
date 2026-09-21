#!/usr/bin/env python3
"""Stamp catalog/<stats_dir>/collection.json from the stats table's timeline.

The stats collection is a committed, mostly static file: its columns, assets
and links do not change when the table is rebuilt. Three fields do, and they
are measured here rather than typed: `extent.temporal.interval`, `updated`
and `table:row_count`. Nothing else restamped them, which is how the
published collection still claimed the two-day pilot extent
(2026-09-12 .. 2026-09-13) months after `publish-stats` built the table
from 2016-11 onward.

    python3 tools/make_stats_collection.py --data-dir ./staging/publish/stats
    python3 tools/make_stats_collection.py --data-dir ... --remote-baseline
    python3 tools/make_stats_collection.py --collection sentinel-2-c1-l2a

The measurement comes from `timeline.parquet` (one row per (year, month)
over every tile, a few KB): the extent runs from the first day of the
earliest month at 00:00:00Z to the last day of the latest month at
23:59:59Z, and the row count is the sum of `tile_count`, which is one row
per tile-month in `mgrs-monthly.parquet`. `--data-dir` names the staged
stats directory; when it holds no `timeline.parquet` and `--remote-baseline`
is given, the published copy is read over HTTP instead (publish-stats and
refresh-daily both stage one, so the fallback is for a run from a clean
checkout). Every other key of the collection is preserved in place, in its
order, with the same 2-space indent, so a diff of a restamp is the three
fields and nothing else; mirror of make_collection.py's restamp_root().

`--collection` picks which stats collection (s2_collections; default the
first one, so every existing call is unchanged). Everything that names a
place follows config.stats_dir (`stats`, `stats-c1`): the collection.json
stamped, the default staged directory, and the published timeline that
`--remote-baseline` falls back to.
"""
from __future__ import annotations

import argparse
import calendar
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))

import s2_collections as cols  # noqa: E402
from s2_collections import CollectionConfig  # noqa: E402

DEFAULT_CONFIG = cols.get(cols.DEFAULT)
TIMELINE = "timeline.parquet"


def collection_path(config: CollectionConfig = DEFAULT_CONFIG) -> Path:
    """The committed collection.json this tool stamps."""
    return ROOT / "catalog" / config.stats_dir / "collection.json"


def staged_dir(config: CollectionConfig = DEFAULT_CONFIG) -> Path:
    """Where the workflows stage the collection's stats (s2_stats --out)."""
    return Path("./staging/publish") / config.stats_dir


def published_timeline(config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The published timeline.parquet, read when --remote-baseline is given
    and nothing is staged."""
    return f"{cols.PUBLIC}/{config.stats_dir}/{TIMELINE}"


PUBLISHED_TIMELINE = published_timeline()


def connect() -> duckdb.DuckDBPyConnection:
    """UTC session, httpfs loaded lazily by the caller when a URL is read.
    The timeline's year/month are integers, so no bucketing happens here,
    but every DuckDB connection in this repo pins UTC as a matter of
    course."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


def month_span(con: duckdb.DuckDBPyConnection, location: str
               ) -> tuple[int, int, int, int, int]:
    """(first year, first month, last year, last month, rows) of the
    timeline at `location` (a path or URL); rows is the sum of tile_count,
    one per tile-month of the full table."""
    if "://" in location:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    row = con.execute(f"""
        SELECT min(year::INTEGER * 100 + month), max(year::INTEGER * 100 + month),
               sum(tile_count), count(*)
        FROM read_parquet('{location}')""").fetchone()
    first, last, rows, months = row
    if not months:
        raise SystemExit(f"{location}: the timeline has no rows; nothing "
                         f"to stamp an extent from")
    return (first // 100, first % 100, last // 100, last % 100, int(rows))


def interval(first_year: int, first_month: int,
             last_year: int, last_month: int) -> list[str]:
    """First day of the earliest month at 00:00:00Z to the last day of the
    latest month at 23:59:59Z: the table buckets by month, so a month is
    the finest thing its extent can say."""
    last_day = calendar.monthrange(last_year, last_month)[1]
    return [f"{first_year:04d}-{first_month:02d}-01T00:00:00Z",
            f"{last_year:04d}-{last_month:02d}-{last_day:02d}T23:59:59Z"]


def stamp(collection: dict, span: list[str], rows: int, when: str) -> dict:
    """The collection with its three measured fields replaced in place.
    Keys keep their order; nothing else is touched."""
    collection["extent"]["temporal"]["interval"] = [span]
    collection["table:row_count"] = rows
    collection["updated"] = when
    return collection


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    dirs = ", ".join(cols.get(n).stats_dir for n in cols.NAMES)
    cols.add_collection_arg(ap)
    ap.add_argument("--data-dir",
                    help="staged stats directory holding timeline.parquet "
                         f"(default ./staging/publish/<stats_dir>: {dirs})")
    ap.add_argument("--out",
                    help="collection.json path (default "
                         f"catalog/<stats_dir>/collection.json: {dirs})")
    ap.add_argument("--remote-baseline", action="store_true",
                    help="when --data-dir holds no timeline.parquet, read the "
                         f"published one at {cols.PUBLIC}/<stats_dir>/{TIMELINE}")
    a = ap.parse_args()
    config = cols.get(a.collection)

    out = Path(a.out).resolve() if a.out else collection_path(config)
    data_dir = Path(a.data_dir) if a.data_dir else staged_dir(config)
    staged = data_dir.resolve() / TIMELINE
    if staged.is_file():
        location = str(staged)
    elif a.remote_baseline:
        location = published_timeline(config)
    else:
        raise SystemExit(f"{staged} does not exist and --remote-baseline was "
                         f"not given; nothing to stamp the extent from")

    collection = json.loads(out.read_text())
    if collection.get("type") != "Collection":
        raise SystemExit(f"{out} is not a STAC Collection")
    con = connect()
    fy, fm, ly, lm, rows = month_span(con, location)
    span = interval(fy, fm, ly, lm)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp(collection, span, rows, now)
    out.write_text(json.dumps(collection, indent=2) + "\n")
    print(f"wrote {out}: {rows:,} tile-months, {span[0]} .. {span[1]} "
          f"(from {location})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
