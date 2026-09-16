#!/usr/bin/env python3
"""Fetch Earth Search sentinel-2-l2a items for a date window into chunk
Parquet matching the seed archive's slim schema.

Resumable: a window whose chunk file already exists is skipped, and a window
with zero matches leaves a zero-byte sentinel so the next run skips it too.
limit=200 because the API 500s at limit=500 (measured 2026-09-15).

Normalization notes (newer items moved off the s2 extension for several
fields; the archive keeps the seed schema):
  s2:mgrs_tile        <- props or mgrs:utm_zone + mgrs:latitude_band + mgrs:grid_square
  sat:relative_orbit  <- props or _R(\\d{3})_ in s2:product_uri
  s2:mean_solar_zenith  <- props or 90 - view:sun_elevation
  s2:mean_solar_azimuth <- props or view:sun_azimuth
Absent values stay NULL rather than being invented (s2:granule_id,
sat:orbit_state on newer items).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_schema import COLUMNS

API = "https://earth-search.aws.element84.com/v1/search"
PAGE = 200
UA = {"User-Agent": "sentinel-2-catalog-tools/1.0 "
      "(+https://github.com/portolan-mirrors/sentinel-2-catalog)",
      "Content-Type": "application/json"}
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]
_REL_ORBIT = re.compile(r"_R(\d{3})_")


def with_retries(fn, tries: int = 8):
    """Call fn() with exponential backoff (capped at 300s) on network/HTTP
    errors. Shared by the API POST here and s2_repair.py's bucket GETs so
    both retry the same way against different transports."""
    for i in range(tries):
        try:
            return fn()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
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


def normalize(f: dict) -> dict:
    p = f["properties"]
    tile = p.get("s2:mgrs_tile")
    if not tile:
        try:
            tile = (f"{p['mgrs:utm_zone']}{p['mgrs:latitude_band']}"
                    f"{p['mgrs:grid_square']}")
        except KeyError as e:
            raise ValueError(
                f"{f.get('id', '<unknown id>')}: missing mgrs field {e} "
                "and no s2:mgrs_tile") from e
    rel = p.get("sat:relative_orbit")
    if rel is None and p.get("s2:product_uri"):
        m = _REL_ORBIT.search(p["s2:product_uri"])
        rel = int(m.group(1)) if m else None
    zen = p.get("s2:mean_solar_zenith")
    if zen is None and p.get("view:sun_elevation") is not None:
        zen = 90.0 - p["view:sun_elevation"]
    azi = p.get("s2:mean_solar_azimuth", p.get("view:sun_azimuth"))
    row = {
        "assets": json.dumps(f.get("assets", {}), separators=(",", ":")),
        "thumbnail_url": (f.get("assets", {}).get("thumbnail") or {}).get("href"),
        "type": "Feature",
        "stac_version": f.get("stac_version"),
        "stac_extensions": f.get("stac_extensions") or [],
        "id": f["id"],
        "bbox": f.get("bbox"),
        "links": [{"href": l.get("href"), "rel": l.get("rel"),
                   "title": l.get("title"), "type": l.get("type")}
                  for l in f.get("links", [])
                  if l.get("rel") not in ("next", "prev", "root", "parent")],
        "collection": "sentinel-2-l2a",
        "datetime": p["datetime"],
        "platform": p.get("platform"),
        "proj:epsg": p.get("proj:epsg"),
        "instruments": p.get("instruments") or [],
        "s2:mgrs_tile": tile,
        "constellation": p.get("constellation"),
        "s2:granule_id": p.get("s2:granule_id"),
        "eo:cloud_cover": p.get("eo:cloud_cover"),
        "sat:orbit_state": p.get("sat:orbit_state"),
        "sat:relative_orbit": rel,
        "s2:mean_solar_zenith": zen,
        "s2:mean_solar_azimuth": azi,
        "_geometry_json": json.dumps(f["geometry"]),
    }
    # Every remaining s2:* column comes straight from properties.
    for name, _, _ in DATA_COLUMNS:
        if name not in row and name != "geometry":
            row[name] = p.get(name)
    return {k: row[k] for k in
            [c[0] for c in DATA_COLUMNS if c[0] != "geometry"] + ["_geometry_json"]}


def fetch_window(start: str, end: str, out_dir: Path,
                 limit_pages: int | None = None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{start}_{end}.parquet"
    if dest.exists():
        print(f"  {dest.name}: exists, skipping")
        return 0
    body = {"collections": ["sentinel-2-l2a"],
            "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
            "limit": PAGE}
    # Rows for a window are held in memory before being written; bounded
    # because the CLI runs with --days-per-chunk=1 (~5k items/day).
    rows, pages, next_body = [], 0, body
    while True:
        resp = _post(next_body)
        rows.extend(normalize(f) for f in resp.get("features", []))
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
    write_rows(rows, dest)
    print(f"  {dest.name}: {len(rows):,} rows in {pages} page(s)", flush=True)
    return len(rows)


def write_rows(rows: list[dict], dest: Path) -> None:
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
        copy_ndjson_to_parquet(nd, dest)
    finally:
        Path(nd).unlink()


def copy_ndjson_to_parquet(nd_path: str, dest: Path) -> None:
    """The COPY/cast step shared by write_rows() above and s2_repair.py's
    streaming writer: turn an NDJSON file of normalize()d rows (one JSON
    object per line, `_geometry_json` instead of `geometry`) into a
    canonical-schema chunk parquet.

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
            for n, t, _ in DATA_COLUMNS if n != "geometry")
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
    a = ap.parse_args()
    out = Path(a.out) / "api"
    total = 0
    d0, d1 = date.fromisoformat(a.start), date.fromisoformat(a.end)
    cur = d0
    while cur <= d1:
        end = min(cur + timedelta(days=a.days_per_chunk - 1), d1)
        total += fetch_window(cur.isoformat(), end.isoformat(), out,
                              a.limit_pages)
        cur = end + timedelta(days=1)
    print(f"TOTAL {total:,} rows fetched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
