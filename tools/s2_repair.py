#!/usr/bin/env python3
"""Fetch one month of items straight from a collection's source bucket,
bypassing the Earth Search API entirely.

Why: the API backfill (s2_fetch.py) lost several month-slices to sustained
Earth Search 502 windows. Each source bucket carries a static STAC item
JSON per scene at
    s3://{config.bucket}/{config.key_root}{zone}/{band}/{square}/{yyyy}/{m}/{id}/{id}.json
served over plain, unauthenticated HTTPS with no outage windows. The
collection (--collection, default sentinel-2-l2a) picks the bucket, key
root, HTTPS base, scene-id regex, prefix cache and schema from
s2_collections; nothing bucket-specific lives in this file.
  * sentinel-2-l2a: s3://sentinel-cogs/sentinel-s2-l2a-cogs/... (verified
    shape-identical to API features -- 43 properties incl. mgrs:*, 38
    assets with absolute https hrefs, links self/canonical/license/
    derived_from).
  * sentinel-2-c1-l2a: s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/...
    (anonymous LIST and GET verified 2026-09-21; the item JSON normalizes
    onto s2c1_schema exactly like an API feature -- tests/test_repair.py's
    live C1 test).
{m} in the bucket path is NOT zero-padded (".../2018/1/...", never
".../2018/01/...") in either bucket, even though this tool's --month
argument and output filenames are.

Discovery is two-level:
  1. A ONE-TIME cache of every MGRS tile prefix under the key root
     (zone -> band -> square, ~3 levels), committed per collection as
     tools/mgrs_prefixes.txt (sentinel-2-l2a) and tools/mgrs_prefixes_c1.txt
     (sentinel-2-c1-l2a) -- see prefix_cache_path(). Rebuild with
     --refresh-prefixes. A real run for the first collection (2026-09-16)
     took ~1,050 anonymous LISTs / ~16s and found 35,433 tile prefixes
     across 60 real UTM zones -- see build_prefix_cache()'s
     _is_valid_zone() note for one bucket-root anomaly excluded along the
     way. The C1 run (2026-09-21, --workers 32) took ~1,030 LISTs / ~9s
     and found 32,811 tile prefixes across 60 zones; that bucket root has
     no stray prefix (its only sibling, sentinel-2-pre-c1-l2a/, is another
     collection and sits outside the key root).
  2. Per month: for each cached tile prefix (threaded, --workers), LIST
     "{prefix}{yyyy}/{m}/" for scene directories, then GET "{id}.json" for
     each (also threaded) -- both anonymous: boto3 UNSIGNED for LIST, plain
     HTTPS for GET.

Chunking and eviction resilience (production evidence, 2026-09-16): a
single-parquet-per-month run of repair-slices.yml was killed by GitHub
Actions runner evictions in 11/11 attempts -- worse at higher --workers
(~20-25min to eviction at 48 vs ~1h at 16, i.e. kill rate tracks request
rate, so raising parallelism cannot outrun it) -- and Earth Search
persistently 502s 2018-12..2019-04 from any IP, so this bucket path is the
ONLY source for those months and must survive being evicted mid-run.
repair_month() therefore writes one chunk PER DAY
("YYYY-MM-DD_YYYY-MM-DD.parquet", same naming s2_fetch.py uses at
--days-per-chunk=1, so publish-backfill's `${Y}-*.parquet` glob already
matches), finalizing each day's parquet as soon as that day's items are
fetched -- so a mid-month eviction still leaves every already-finished
day's chunk on disk (and, per the workflow, already uploaded). Discovery
stays a single month-level LIST pass (splitting it per day would multiply
LISTs by ~30 for no benefit); scenes are grouped into days by the acquisition
date embedded in the scene id (S2A_31UFU_20180905_0_L2A -> 2018-09-05), not
by a second LIST per day. Skip-if-exists is per day, exactly like
s2_fetch.py: an existing non-empty day chunk is skipped (no re-fetch), and
a day with zero matches leaves a zero-byte sentinel -- so re-running
repair_month() for a month that was interrupted mid-way (the workflow
downloads whatever partial slice-YYYY-MM artifact already exists before
re-invoking this tool) only fetches the days that never finished.

Memory: a day is at most a few thousand items (unlike a whole month, up to
~450k) -- fetch_and_write() still streams each normalize()d row straight to
a temp NDJSON file as its GET future completes rather than building a
Python list, so peak memory per day is bounded by in-flight HTTP responses
(~--workers of them) plus DuckDB's own read_ndjson buffering during that
day's COPY, not by the item count.
"""
from __future__ import annotations

import argparse
import calendar
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import s2_collections as cols  # noqa: E402
from s2_collections import CollectionConfig  # noqa: E402
from s2_fetch import UA, copy_ndjson_to_parquet, with_retries  # noqa: E402

DEFAULT_CONFIG = cols.get(cols.DEFAULT)

# One committed prefix cache per collection, next to this file. The first
# collection keeps the name it has always had (repair-slices.yml and the
# README refer to it); later collections get a suffix.
_PREFIX_CACHE_NAMES = {
    "sentinel-2-l2a": "mgrs_prefixes.txt",
    "sentinel-2-c1-l2a": "mgrs_prefixes_c1.txt",
}


def prefix_cache_path(config: CollectionConfig = DEFAULT_CONFIG) -> Path:
    try:
        name = _PREFIX_CACHE_NAMES[config.id]
    except KeyError:
        raise SystemExit(f"no prefix cache name registered for collection {config.id!r}")
    return Path(__file__).resolve().parent / name


def _s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="us-west-2")


def _list_common_prefixes(s3, prefix: str,
                          config: CollectionConfig = DEFAULT_CONFIG) -> list[str]:
    """Every CommonPrefix directly under `prefix` (one path segment down)
    in the collection's bucket, anonymous ListObjectsV2 with Delimiter='/',
    paginated."""
    prefixes: list[str] = []
    token = None
    while True:
        kwargs = dict(Bucket=config.bucket, Prefix=prefix, Delimiter="/", MaxKeys=1000)
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


def build_prefix_cache(out_path: Path | None = None, workers: int = 16,
                       config: CollectionConfig = DEFAULT_CONFIG) -> list[str]:
    """Traverse zone -> band -> square once under config.key_root and cache
    every MGRS tile prefix. Band and square levels are parallelized (zone
    level is a single call). See the task report for a real run's numbers."""
    if out_path is None:
        out_path = prefix_cache_path(config)
    s3 = _s3_client()
    zones = [z for z in _list_common_prefixes(s3, config.key_root, config)
             if _is_valid_zone(z)]
    print(f"  {len(zones)} zone prefixes", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        bands = [b for bl in pool.map(lambda z: _list_common_prefixes(s3, z, config), zones)
                 for b in bl]
    print(f"  {len(bands)} zone/band prefixes", file=sys.stderr)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        squares = [sq for sl in pool.map(lambda b: _list_common_prefixes(s3, b, config), bands)
                   for sq in sl]
    squares.sort()
    out_path.write_text("\n".join(squares) + ("\n" if squares else ""))
    print(f"  {len(squares)} tile prefixes -> {out_path}", file=sys.stderr)
    return squares


def load_prefixes(path: Path) -> list[str]:
    if not path.exists():
        raise SystemExit(
            f"{path} not found; run with --refresh-prefixes first")
    return [line for line in path.read_text().splitlines() if line]


def discover_scenes(prefixes: list[str], yyyy: str, m: str, list_fn,
                    workers: int = 16,
                    config: CollectionConfig = DEFAULT_CONFIG) -> list[tuple[str, str]]:
    """[(scene_id, item_json_url), ...] for one month across every cached
    tile prefix (~35k of them -- threaded across `workers`, matching
    build_prefix_cache's pattern; a serial loop over that many LISTs would
    dominate a month's wall clock). `m` must already be the bucket's
    non-padded form ("9", not "09"). `list_fn(prefix)` returns the
    CommonPrefixes directly under `prefix` -- injected so this is testable
    without S3. Results are gathered in prefix order (not completion
    order), which also keeps behavior deterministic for tests."""
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(list_fn, f"{p}{yyyy}/{m}/") for p in prefixes]
        results = [f.result() for f in futures]
    out: list[tuple[str, str]] = []
    for scene_prefixes in results:
        for scene_prefix in scene_prefixes:
            scene_id = scene_prefix.rstrip("/").rsplit("/", 1)[-1]
            out.append((scene_id, f"{config.https_base}/{scene_prefix}{scene_id}.json"))
    return out


def scene_day(scene_id: str, config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The ISO acquisition date (YYYY-MM-DD) embedded in a scene id, e.g.
    S2A_31UFU_20180905_0_L2A -> "2018-09-05" (first collection) or
    S2B_T31UET_20260921T105030_L2A -> "2026-09-21" (Collection 1), parsed
    with config.id_re's `day` group. Grouping by this avoids a second,
    per-day LIST pass: one month-level discover_scenes() call already names
    every scene, and the date is right there in its id."""
    m = config.id_re.match(scene_id)
    if not m:
        raise ValueError(f"cannot parse acquisition date from scene id: {scene_id!r}")
    d = m.group("day")
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


def group_scenes_by_day(scenes: list[tuple[str, str]],
                        config: CollectionConfig = DEFAULT_CONFIG,
                        ) -> dict[str, list[tuple[str, str]]]:
    """scenes -> {"YYYY-MM-DD": [(scene_id, url), ...]}.

    A scene id that doesn't parse is logged and SKIPPED, not raised: this
    runs before any day is fetched, so an unhandled exception here would
    kill the whole month deterministically on every retry (discovery is
    re-run from scratch each attempt) over one bad id, and no local check
    can tell "harmless bucket oddity" from "expected item, malformed" --
    the inventory audit (s2_audit.py) is the net that catches any resulting
    shortfall against bucket ground truth, not this function."""
    by_day: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for scene_id, url in scenes:
        try:
            day = scene_day(scene_id, config)
        except ValueError:
            print(f"  skipping unparsable scene id: {scene_id!r}", file=sys.stderr)
            continue
        by_day[day].append((scene_id, url))
    return dict(by_day)


class MissingItem(Exception):
    """A scene's item JSON genuinely does not exist in the bucket (HTTP 404
    or 410) -- a permanent condition, not a transient failure. Deliberately
    NOT a subclass of urllib.error.URLError/HTTPError: with_retries()'s
    except clause only matches those two plus TimeoutError, so raising this
    instead makes it propagate out of with_retries() immediately, with zero
    retries. (Production evidence, 2026-09-16, run 35090066508: some scenes
    in 2018-12..2019-04 have no item JSON at all -- confirmed with direct
    HEAD requests, not a URL-construction bug -- and retrying each one
    burned the FULL 8-attempt/~11-minute ladder before the future's
    unhandled exception killed the whole month.)"""


def _get_json(url: str, tries: int = 8) -> dict:
    def call():
        req = urllib.request.Request(url, headers={"User-Agent": UA["User-Agent"]})
        try:
            return json.load(urllib.request.urlopen(req, timeout=120))
        except urllib.error.HTTPError as e:
            if e.code in (404, 410):
                raise MissingItem(f"{url}: HTTP {e.code}") from e
            raise          # 5xx, etc. -- let with_retries's ladder handle it
    return with_retries(call, tries)


def fetch_and_write(scenes: list[tuple[str, str]], get_fn, nd_path: str,
                    workers: int = 16,
                    config: CollectionConfig = DEFAULT_CONFIG) -> tuple[int, int]:
    """GET + config.schema.normalize() every scene's item JSON, writing each
    row straight to the NDJSON file at `nd_path` as its future completes -- never holds
    more than one month's worth of in-flight requests in memory, unlike
    accumulating a Python list of ~450k rows. The writes happen in THIS
    thread as as_completed() yields (fetching is threaded, consuming is
    not), so no lock is needed even though GETs run in worker threads.
    `get_fn(url)` returns the parsed item dict -- injected so this is
    testable without S3, and swapped for a real HTTPS GET (with retry) in
    production.

    Two kinds of per-scene skip, both logged and counted rather than
    raised (a single bad scene must never crash the whole day's, let alone
    month's, fetch):
      - `get_fn` raises MissingItem for a scene with no item JSON in the
        bucket at all (permanent, HTTP 404/410 -- NOT retried).
      - normalize() raises ValueError for a scene whose item JSON exists
        but is missing required fields (production evidence, 2026-09-16,
        run 35092766147: S2B_35NKA_20180901_0_L2A had neither
        s2:mgrs_tile nor the mgrs:* fields normalize() falls back to --
        one such exotic straggler killed the whole month before this fix;
        a follow-up survey of 88 scenes across 3 tiles x 2 months found
        zero other occurrences, so these are rare, not systemic).

    Returns (rows written, scenes skipped -- either reason, one counter:
    the inventory audit is the tool for distinguishing "how many" from
    "why", not this return value)."""
    n = 0
    skipped = 0
    with open(nd_path, "w") as nd, ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(get_fn, url): scene_id for scene_id, url in scenes}
        for f in as_completed(futures):
            try:
                item = f.result()
            except MissingItem:
                print(f"  skipping scene with missing item JSON: {futures[f]}",
                     file=sys.stderr)
                skipped += 1
                continue
            try:
                row = config.schema.normalize(item)
            except ValueError as e:
                print(f"  skipping unnormalizable item: {e}", file=sys.stderr)
                skipped += 1
                continue
            nd.write(json.dumps(row) + "\n")
            n += 1
    return n, skipped


def _fetch_one_day(day: str, day_scenes: list[tuple[str, str]], dest_dir: Path,
                   get_fn, workers: int,
                   config: CollectionConfig = DEFAULT_CONFIG) -> tuple[int, int]:
    """Fetch+write one day's chunk (or a zero-byte sentinel), skipping if
    its destination already exists -- the unit of eviction resilience: this
    is the piece of work that either fully lands on disk or never starts,
    so an eviction mid-month never corrupts or loses an already-finished
    day. Returns (rows written, scenes skipped -- missing item JSON or
    unnormalizable; see fetch_and_write())."""
    dest = dest_dir / f"{day}_{day}.parquet"
    if dest.exists():
        print(f"  {dest.name}: exists, skipping")
        return 0, 0
    if not day_scenes:
        dest.touch()          # sentinel: fetched, zero matches
        return 0, 0
    fd, nd = tempfile.mkstemp(suffix=".ndjson")
    os.close(fd)
    try:
        n, skipped = fetch_and_write(day_scenes, get_fn, nd, workers, config)
        if n:
            copy_ndjson_to_parquet(nd, dest, config.schema.DATA_COLUMNS)
        else:
            dest.touch()      # every scene that day was skipped
    finally:
        Path(nd).unlink(missing_ok=True)
    if n:
        print(f"  {dest.name}: {n:,} rows", flush=True)
    return n, skipped


def repair_month(month: str, out_dir: Path, prefixes: list[str], workers: int = 16,
                 list_fn=None, get_fn=None,
                 config: CollectionConfig = DEFAULT_CONFIG) -> int:
    """Discover one month's scenes with a single month-level LIST pass, then
    fetch and write ONE PARQUET CHUNK PER DAY (skip-if-exists per day, same
    resumability contract as s2_fetch.py) -- see the module docstring for
    why: a mid-month eviction must not lose already-finished days. A scene
    with no item JSON in the bucket, or one that exists but fails
    normalize()'s field checks, is skipped and counted rather than
    crashing the month (see fetch_and_write()); the running total is
    printed as a month-end summary so the shortfall is visible in the run
    log and comparable against the inventory audit. `prefixes` must be the
    cache built for the same `config` (prefix_cache_path(config))."""
    y, m_pad = month.split("-")
    m = str(int(m_pad))          # bucket path segment is not zero-padded
    last_day = calendar.monthrange(int(y), int(m_pad))[1]
    dest_dir = out_dir / "repair"
    dest_dir.mkdir(parents=True, exist_ok=True)

    if list_fn is None or get_fn is None:
        s3 = _s3_client()
        if list_fn is None:
            list_fn = lambda p: _list_common_prefixes(s3, p, config)  # noqa: E731
        if get_fn is None:
            get_fn = _get_json

    scenes = discover_scenes(prefixes, y, m, list_fn, workers, config)
    by_day = group_scenes_by_day(scenes, config)

    total = 0
    total_skipped = 0
    for day_num in range(1, last_day + 1):
        day = f"{y}-{m_pad}-{day_num:02d}"
        n, skipped = _fetch_one_day(day, by_day.get(day, []), dest_dir, get_fn,
                                    workers, config)
        total += n
        total_skipped += skipped
    print(f"month {month}: {total:,} scenes fetched, "
         f"{total_skipped:,} scenes skipped (missing or unnormalizable)")
    return total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM")
    ap.add_argument("--out", help="output DIR; writes DIR/repair/<day>_<day>.parquet"
                                  " per day, one per day in the month")
    ap.add_argument("--workers", type=int, default=16)
    cols.add_collection_arg(ap)
    ap.add_argument("--refresh-prefixes", action="store_true",
                    help="rebuild the collection's prefix cache (~1k anonymous LISTs)")
    ap.add_argument("--prefix-cache",
                    help="default: tools/mgrs_prefixes.txt (sentinel-2-l2a) or "
                         "tools/mgrs_prefixes_c1.txt (sentinel-2-c1-l2a)")
    a = ap.parse_args()
    config = cols.get(a.collection)

    cache_path = Path(a.prefix_cache) if a.prefix_cache else prefix_cache_path(config)
    if a.refresh_prefixes:
        build_prefix_cache(cache_path, a.workers, config)

    if not a.month:
        return 0
    if not a.out:
        raise SystemExit("--month requires --out")
    prefixes = load_prefixes(cache_path)
    n = repair_month(a.month, Path(a.out), prefixes, a.workers, config=config)
    print(f"TOTAL {n:,} rows fetched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
