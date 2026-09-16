#!/usr/bin/env python3
"""Fetch one month of sentinel-2-l2a items straight from the sentinel-cogs
bucket, bypassing the Earth Search API entirely.

Why: the API backfill (s2_fetch.py) lost several month-slices to sustained
Earth Search 502 windows. The sentinel-cogs bucket carries a static STAC
item JSON per scene at
    s3://sentinel-cogs/sentinel-s2-l2a-cogs/{zone}/{band}/{square}/{yyyy}/{m}/{id}/{id}.json
(verified shape-identical to API features -- 43 properties incl. mgrs:*, 38
assets with absolute https hrefs, links self/canonical/license/derived_from)
served over plain, unauthenticated HTTPS with no outage windows. {m} in the
bucket path is NOT zero-padded (".../2018/1/...", never ".../2018/01/...")
even though this tool's --month argument and output filenames are.

Discovery is two-level:
  1. A ONE-TIME cache of every MGRS tile prefix under sentinel-s2-l2a-cogs/
     (zone -> band -> square, ~3 levels), committed as
     tools/mgrs_prefixes.txt. Rebuild with --refresh-prefixes. A real run
     (2026-09-16) took ~1,050 anonymous LISTs / ~16s and found 35,433 tile
     prefixes across 60 real UTM zones -- see build_prefix_cache()'s
     _is_valid_zone() note for one bucket-root anomaly excluded along the
     way.
  2. Per month: for each cached tile prefix, LIST "{prefix}{yyyy}/{m}/" for
     scene directories, then GET "{id}.json" for each -- both anonymous
     (boto3 UNSIGNED for LIST, plain HTTPS for GET), both threaded.

Resumable exactly like s2_fetch.py: an existing non-empty chunk is skipped,
and a month with zero matches leaves a zero-byte sentinel.
"""
from __future__ import annotations

import argparse
import calendar
import json
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_fetch import UA, normalize, with_retries, write_rows  # noqa: E402

BUCKET = "sentinel-cogs"
PREFIX_ROOT = "sentinel-s2-l2a-cogs/"
HTTPS_BASE = f"https://{BUCKET}.s3.us-west-2.amazonaws.com"
DEFAULT_PREFIX_CACHE = Path(__file__).resolve().parent / "mgrs_prefixes.txt"


def _s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="us-west-2")


def _list_common_prefixes(s3, prefix: str) -> list[str]:
    """Every CommonPrefix directly under `prefix` (one path segment down),
    anonymous ListObjectsV2 with Delimiter='/', paginated."""
    prefixes: list[str] = []
    token = None
    while True:
        kwargs = dict(Bucket=BUCKET, Prefix=prefix, Delimiter="/", MaxKeys=1000)
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        prefixes.extend(p["Prefix"] for p in resp.get("CommonPrefixes", []))
        if not resp.get("IsTruncated"):
            return prefixes
        token = resp["NextContinuationToken"]


def _is_valid_zone(prefix: str) -> bool:
    """UTM zone segments are numeric strings 1-60. The bucket root also
    holds one stray, non-tile-structured prefix (discovered live,
    2026-09-16: "sentinel-s2-l2a-cogs/2019/..." holds ~64k scene
    directories placed directly at the root, bypassing zone/band/square
    entirely -- a legacy/duplicate artifact: every scene checked there also
    exists complete under its real zone/band/square/yyyy/m path, so nothing
    is lost by excluding it. Without this filter that one bogus "zone"
    balloons the crawl by ~65k needless LISTs for zero new tiles."""
    seg = prefix.rstrip("/").rsplit("/", 1)[-1]
    return seg.isdigit() and 1 <= int(seg) <= 60


def build_prefix_cache(out_path: Path = DEFAULT_PREFIX_CACHE,
                       workers: int = 16) -> list[str]:
    """Traverse zone -> band -> square once and cache every MGRS tile
    prefix. Band and square levels are parallelized (zone level is a single
    call). See the task report for a real run's numbers."""
    s3 = _s3_client()
    zones = [z for z in _list_common_prefixes(s3, PREFIX_ROOT) if _is_valid_zone(z)]
    print(f"  {len(zones)} zone prefixes", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        bands = [b for bl in pool.map(lambda z: _list_common_prefixes(s3, z), zones)
                 for b in bl]
    print(f"  {len(bands)} zone/band prefixes", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        squares = [sq for sl in pool.map(lambda b: _list_common_prefixes(s3, b), bands)
                   for sq in sl]
    squares.sort()
    out_path.write_text("\n".join(squares) + ("\n" if squares else ""))
    print(f"  {len(squares)} tile prefixes -> {out_path}", file=sys.stderr)
    return squares


def load_prefixes(path: Path = DEFAULT_PREFIX_CACHE) -> list[str]:
    if not path.exists():
        raise SystemExit(
            f"{path} not found; run with --refresh-prefixes first")
    return [line for line in path.read_text().splitlines() if line]


def discover_scenes(prefixes: list[str], yyyy: str, m: str, list_fn) -> list[tuple[str, str]]:
    """[(scene_id, item_json_url), ...] for one month across every cached
    tile prefix. `m` must already be the bucket's non-padded form ("9", not
    "09"). `list_fn(prefix)` returns the CommonPrefixes directly under
    `prefix` -- injected so this is testable without S3."""
    out: list[tuple[str, str]] = []
    for prefix in prefixes:
        for scene_prefix in list_fn(f"{prefix}{yyyy}/{m}/"):
            scene_id = scene_prefix.rstrip("/").rsplit("/", 1)[-1]
            out.append((scene_id, f"{HTTPS_BASE}/{scene_prefix}{scene_id}.json"))
    return out


def _get_json(url: str, tries: int = 8) -> dict:
    def call():
        req = urllib.request.Request(url, headers={"User-Agent": UA["User-Agent"]})
        return json.load(urllib.request.urlopen(req, timeout=120))
    return with_retries(call, tries)


def fetch_items(scenes: list[tuple[str, str]], get_fn, workers: int = 16) -> list[dict]:
    """GET + normalize() every scene's item JSON. `get_fn(url)` returns the
    parsed item dict -- injected so this is testable without S3, and swapped
    for a real HTTPS GET (with retry) in production."""
    rows = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(get_fn, url) for _, url in scenes]
        for f in as_completed(futures):
            rows.append(normalize(f.result()))
    return rows


def repair_month(month: str, out_dir: Path, prefixes: list[str], workers: int = 16,
                 list_fn=None, get_fn=None) -> int:
    y, m_pad = month.split("-")
    m = str(int(m_pad))          # bucket path segment is not zero-padded
    last_day = calendar.monthrange(int(y), int(m_pad))[1]
    dest_dir = out_dir / "repair"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{y}-{m_pad}-01_{y}-{m_pad}-{last_day:02d}.parquet"
    if dest.exists():
        print(f"  {dest.name}: exists, skipping")
        return 0

    if list_fn is None or get_fn is None:
        s3 = _s3_client()
        if list_fn is None:
            list_fn = lambda p: _list_common_prefixes(s3, p)  # noqa: E731
        if get_fn is None:
            get_fn = _get_json

    scenes = discover_scenes(prefixes, y, m, list_fn)
    if not scenes:
        dest.touch()          # sentinel: fetched, zero matches
        return 0
    rows = fetch_items(scenes, get_fn, workers)
    write_rows(rows, dest)
    print(f"  {dest.name}: {len(rows):,} rows", flush=True)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM")
    ap.add_argument("--out", help="output DIR; writes DIR/repair/<chunk>.parquet")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--refresh-prefixes", action="store_true",
                    help="rebuild tools/mgrs_prefixes.txt (~1k anonymous LISTs)")
    ap.add_argument("--prefix-cache", default=str(DEFAULT_PREFIX_CACHE))
    a = ap.parse_args()

    cache_path = Path(a.prefix_cache)
    if a.refresh_prefixes:
        build_prefix_cache(cache_path, a.workers)

    if not a.month:
        return 0
    if not a.out:
        raise SystemExit("--month requires --out")
    prefixes = load_prefixes(cache_path)
    n = repair_month(a.month, Path(a.out), prefixes, a.workers)
    print(f"TOTAL {n:,} rows fetched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
