#!/usr/bin/env python3
"""Fetch Earth Search items for one collection and a date window into chunk
Parquet matching that collection's canonical schema (s2_collections.py
names the collection, its API id and its schema module; --collection picks
one, default sentinel-2-l2a).

Resumable: a window whose chunk file already exists is skipped, and a window
with zero matches leaves a zero-byte sentinel so the next run skips it too.
limit=200 because the API 500s at limit=500 (measured 2026-09-15).

Two lookback fields (--field). `datetime` windows the acquisition time,
which is what a backfill and the first collection's daily refresh want.
`created` windows the time Earth Search created the item: Collection 1
(sentinel-2-c1-l2a) is being back-processed, so on any given day it gains
items whose acquisition dates are years old, and a refresh that only
looked at recent `datetime`s would never see them. Its daily refresh
therefore fetches by `created` and lets the build fold the rows into
whichever year each item's `datetime` belongs to. The first collection's
schema has no `created` column, so `--field created` is refused for it.

The `created` form that works (verified live 2026-09-21, 15,053 matches for
one day; the query extension, not CQL2 `filter`):
  {"collections": ["sentinel-2-c1-l2a"],
   "query": {"created": {"gte": "2026-09-20T00:00:00Z", "lte": "2026-09-20T23:59:59Z"}},
   "limit": 200}
The next link comes back with merge=false and a self-contained body that
carries the `query` through, so paging needs nothing collection-specific.
A `query` on a property the API does not index silently matches nothing
rather than erroring, which is why the CLI test asserts on `created`
bounds and not just on row count.

Each collection's normalize() lives with its schema (s2_schema.py,
s2c1_schema.py; the first collection's is re-exported here for
s2_repair.py). A run ends with one warning line if the schema module saw
upstream properties it does not know (Collection 1's drift guard).
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import s2_collections as cols  # noqa: E402
import s2_schema  # noqa: E402
from s2_schema import USER_AGENT, normalize  # noqa: E402,F401  normalize: s2_repair's import

API = "https://earth-search.aws.element84.com/v1/search"
PAGE = 200
UA = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
DEFAULT_CONFIG = cols.get(cols.DEFAULT)
FIELDS = ("datetime", "created")


def data_columns(schema) -> list[tuple]:
    """The columns a chunk parquet carries for `schema` (a module with
    COLUMNS and normalize()): everything normalize() emits. The two sort
    helpers are computed at build time, so they are the only ones left
    out."""
    return [c for c in schema.COLUMNS if c[0] not in ("_month", "_hilbert")]


# The first collection's chunk columns: the default for the writers below,
# which is what s2_repair.py (first collection only) relies on.
DATA_COLUMNS = data_columns(s2_schema)


def with_retries(fn, tries: int = 8):
    """Call fn() with exponential backoff (capped at 300s) on network/HTTP
    errors. Shared by the API POST here and s2_repair.py's bucket GETs so
    both retry the same way against different transports.

    Catches http.client.HTTPException and ConnectionError alongside
    urllib.error.URLError/HTTPError and TimeoutError: urllib only wraps
    connect-phase failures into URLError. A connection that drops during
    the response-read phase surfaces as a raw http.client.HTTPException
    subclass (RemoteDisconnected, BadStatusLine, IncompleteRead) or a raw
    ConnectionError subclass (ConnectionResetError), neither of which is a
    URLError -- so without this those escaped uncaught and killed a whole
    month's repair job (production, run 35097702125, job repair (2018-09)).
    MissingItem (s2_repair.py) is a plain Exception, not one of these, so
    it stays outside this tuple by construction and keeps propagating
    immediately with zero retries."""
    for i in range(tries):
        try:
            return fn()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
                http.client.HTTPException, ConnectionError) as e:
            if i == tries - 1:
                raise
            wait = min(2 ** i * 5, 300)
            print(f"  retry {i + 1} in {wait}s: {e}", file=sys.stderr)
            time.sleep(wait)
    raise AssertionError("unreachable")


def _post(body: dict, tries: int = 8) -> dict:
    data = json.dumps(body).encode()

    def call():
        req = urllib.request.Request(API, data=data, headers=UA)
        return json.load(urllib.request.urlopen(req, timeout=120))
    return with_retries(call, tries)


def search_body(config: cols.CollectionConfig, start: str, end: str,
                field: str = "datetime") -> dict:
    """The POST /search body for one inclusive day window [start, end] on
    `field`. Key order is deliberate: the datetime form is byte-identical
    to what the first collection has always sent."""
    if field not in FIELDS:
        raise ValueError(f"field must be one of {FIELDS}, not {field!r}")
    if field != "datetime" and config.lookback_field != field:
        raise ValueError(
            f"{config.id} does not look back on {field!r}; "
            f"its lookback field is {config.lookback_field!r}")
    lo, hi = f"{start}T00:00:00Z", f"{end}T23:59:59Z"
    body: dict = {"collections": [config.api_collection]}
    if field == "datetime":
        body["datetime"] = f"{lo}/{hi}"
    else:
        # The STAC query extension; see the module docstring for the
        # live-verified form.
        body["query"] = {field: {"gte": lo, "lte": hi}}
    body["limit"] = PAGE
    return body


def fetch_window(start: str, end: str, out_dir: Path,
                 limit_pages: int | None = None,
                 config: cols.CollectionConfig = DEFAULT_CONFIG,
                 field: str = "datetime") -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{start}_{end}.parquet"
    if dest.exists():
        print(f"  {dest.name}: exists, skipping")
        return 0
    body = search_body(config, start, end, field)
    norm = config.schema.normalize
    # Rows for a window are held in memory before being written; bounded
    # because the CLI runs with --days-per-chunk=1 (~5k items/day).
    rows, pages, next_body = [], 0, body
    while True:
        resp = _post(next_body)
        rows.extend(norm(f) for f in resp.get("features", []))
        pages += 1
        nxt = [l for l in resp.get("links", []) if l.get("rel") == "next"]
        if not nxt or (limit_pages and pages >= limit_pages):
            break
        # STAC paging contract: merge=True means the link's body is a partial
        # body to merge OVER the original request; merge=False/absent means
        # the link's body is self-contained and used as-is. A next link with
        # no body at all is a contract change we've never seen from Earth
        # Search -- fail loudly rather than loop forever or stop silently.
        link = nxt[0]
        if link.get("merge"):
            next_body = {**body, **(link.get("body") or {})}
        elif link.get("body"):
            next_body = link["body"]
        else:
            raise SystemExit(
                f"{start}_{end}: next link with no body: "
                "pagination contract changed")
    if not rows:
        dest.touch()          # sentinel: fetched, zero matches
        return 0
    write_rows(rows, dest, data_columns(config.schema))
    print(f"  {dest.name}: {len(rows):,} rows in {pages} page(s)", flush=True)
    return len(rows)


def write_rows(rows: list[dict], dest: Path,
               columns: list[tuple] = DATA_COLUMNS) -> None:
    """Write normalize()d rows to a canonical-schema chunk parquet. Shared
    with s2_repair.py so the bucket repair path produces byte-identical
    chunk schema to the API fetch path. Holds all of `rows` in memory at
    once -- fine here because fetch_window's own rows list is already
    bounded by --days-per-chunk; s2_repair.py's per-month row count is not
    bounded the same way, so it streams rows straight to an NDJSON file
    instead of building a list and calls copy_ndjson_to_parquet() itself."""
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as tf:
        for r in rows:
            tf.write(json.dumps(r) + "\n")
        nd = tf.name
    try:
        copy_ndjson_to_parquet(nd, dest, columns)
    finally:
        Path(nd).unlink()


def copy_ndjson_to_parquet(nd_path: str, dest: Path,
                           columns: list[tuple] = DATA_COLUMNS) -> None:
    """The COPY/cast step shared by write_rows() above and s2_repair.py's
    streaming writer: turn an NDJSON file of normalize()d rows (one JSON
    object per line, `_geometry_json` instead of `geometry`) into a
    canonical-schema chunk parquet. `columns` is the collection's
    data_columns(); the default is the first collection's.

    COPYs to a same-directory temp name first, then os.replace()s it onto
    `dest` -- a same-filesystem rename, atomic on POSIX and Windows alike.
    Without this, a process killed mid-COPY (a real event: see
    s2_repair.py's runner-eviction handling) would leave a PARTIAL file at
    `dest`, and every caller's skip-if-exists check (`dest.exists()`) would
    then treat that half-written chunk as finished and never retry it."""
    tmp = dest.with_name(dest.name + ".tmp")
    try:
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        cast = ", ".join(
            f'CAST("{n}" AS {t}) AS "{n}"'
            for n, t, _ in columns if n != "geometry")
        con.execute(f"""
            COPY (
              SELECT {cast},
                     ST_GeomFromGeoJSON(_geometry_json) AS geometry
              FROM read_ndjson('{nd_path}', maximum_object_size=20000000)
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
        """)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--days-per-chunk", type=int, default=1)
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="test hook: stop after N pages")
    cols.add_collection_arg(ap)
    ap.add_argument("--field", choices=FIELDS, default="datetime",
                    help="which item property the window bounds: datetime "
                         "(acquisition; backfills) or created (when Earth "
                         "Search made the item; Collection 1's refresh)")
    a = ap.parse_args()
    config = cols.get(a.collection)
    try:
        search_body(config, a.start, a.end, a.field)
    except ValueError as e:
        ap.error(str(e))
    out = Path(a.out) / "api"
    total = 0
    d0, d1 = date.fromisoformat(a.start), date.fromisoformat(a.end)
    cur = d0
    while cur <= d1:
        end = min(cur + timedelta(days=a.days_per_chunk - 1), d1)
        total += fetch_window(cur.isoformat(), end.isoformat(), out,
                              a.limit_pages, config, a.field)
        cur = end + timedelta(days=1)
    print(f"TOTAL {total:,} rows fetched")
    unknown = getattr(config.schema, "UNKNOWN_PROPERTIES", None)
    if unknown:
        # Drift guard, not an error: the frozen schema dropped these.
        print(f"WARNING: {config.id}: {len(unknown)} upstream propert"
              f"{'y' if len(unknown) == 1 else 'ies'} not in the schema, "
              "dropped from every row: "
              + ", ".join(f"{k} (x{n})" for k, n in sorted(unknown.items())),
              file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
