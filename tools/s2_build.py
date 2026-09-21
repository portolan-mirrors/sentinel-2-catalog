#!/usr/bin/env python3
"""Compact fetched chunks into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)
    sentinel-2-l2a/year=<YYYY>/z01-20.parquet  (--split zones, 2019-2020: four
                          z21-35.parquet   parts by UTM zone of s2:mgrs_tile,
                          z36-46.parquet   ZONE_PARTS; spec Amendment 3)
                          z47-60.parquet
    sentinel-2-l2a/year=<YYYY>/z01-15.parquet  (--split zones, 2021 onward:
                          z16-20.parquet   eight parts, ZONE_PARTS_8, nested
                          z21-31.parquet   inside the quartile boundaries)
                          z32-35.parquet
                          z36-40.parquet
                          z41-46.parquet
                          z47-52.parquet
                          z53-60.parquet

Without --split the year is one file, which is how 2015-2018 are published
and how `live.parquet` is always built. With `--split zones` the year is
staged ONCE (dedupe + helper columns, exactly as without), then each zone
range of zone_parts_for(year) is copied out of that staged file into its own
staged part and put through the same gpio sort/write/check/rename pipeline
on its own -- so the sort, the spill and the output only ever hold a
fraction of the year. That is what lets a 7M-row year build on a free
runner: 2019 as one file spilled past the runner's 51.9 GB disk in gpio's
sort. Four parts were enough for 2019-2020; 2021 (8.65M rows, ~94 min per
quartile at zstd 18) ran past the 6-hour job ceiling with three of four
parts built, so from 2021 a year is eight parts of ~50 min each. A range
with no rows writes no file. A row whose tile has no parseable zone belongs
to no part, and rather than drop it the build stops.

Two flags make a build resumable across job timeouts, so a part that is
finished is never lost: `--skip-existing-url BASE` HEADs
BASE/year=YYYY/<part>.parquet before each part and skips one that is already
published (200); `--on-part-done CMD` runs CMD with the finished part's path
after its gpio check, which is how the workflow uploads each part the moment
it exists rather than after the whole year. A CMD that fails stops the
build: an upload that did not happen is not a part that is published.

`--only-parts LABEL[,LABEL...]` (with `--split zones` only) builds just the
named parts of the tier: the year is staged as usual, the named ranges are
copied out and written, and the rows of every other range are dropped with
a log line that names the count. This is how consolidate-month.yml folds
live.parquet into a year in one job per part -- eight octants re-sorted and
re-compressed in one job is 8-12 hours, past every runner ceiling -- and
each job only ever has its own part's rows to sort. A label that is not in
the year's tier stops the build naming the valid ones.

`--exclude-ids-from URL [URL ...]` drops, before the sort, every staged row
whose `id` a listed published part already holds at the same or a newer
`s2:generation_time`; a reprocessed product (same id, newer generation)
that arrives after its predecessor was consolidated is kept, since it would
otherwise never reach the archive. This is how
refresh-daily.yml keeps `live.parquet` disjoint from the same year's archive
parts: the five-day lookback re-fetches days the last consolidation already
folded into the octants, and without this every one of those scenes was
published twice for up to five days after each consolidation (49,942 of
live's 84,675 rows on 2026-09-19), which broke the "a glob reads each scene
once" promise and inflated the current month's stats. Only the `id` column
is read, and only the row groups whose `_month` is among the months the
staged rows span, so the read is a few range requests per part, not the
part. A URL that answers 404 is skipped with a log line (a year whose
archive is not published yet has nothing to exclude); any other answer
stops the build, because a silently skipped archive part recreates the
overlap it exists to remove. When the exclusion leaves no row at all, the
part is still written, with zero rows -- the shape consolidate-month.yml
publishes for an emptied live.parquet -- so the refresh's stats splice and
restamps run against a live that is current rather than failing the job.

Rows are deduped by id keeping the highest s2:generation_time, then sorted
(_month, _hilbert): month-first keeps month pruning inside a year file,
Hilbert-within-month keeps row-group bboxes tight for spatial pruning. The
two helper columns are published and documented in the collection AGENTS.md.

DuckDB stages the deduped rows; `gpio sort column` does the ordered
GeoParquet 2.0 write. Do NOT sort in DuckDB and `gpio convert`: convert does
not preserve row order (firms-catalog; re-measured 2026-09-16 -- a sorted
50k-row fixture came back unsorted).

Why gpio and not a DuckDB COPY, which this file briefly used (b70f6bb):
DuckDB's own Parquet writer emits GeoParquet *1.0.0* -- a `geo` key over a
plain BYTE_ARRAY -- and never the native Parquet GEOMETRY logical type, at
any row count, with or without PARQUET_VERSION V2 (measured on DuckDB 1.5.3
+ spatial). `rashid check --data` rejects that at error severity, twice:
PTL-DAT-012 (version must be 1.1 or 2.x) and PTL-DAT-007 (no per-row-group
spatial statistics). gpio writes geo 2.0.0 plus the native GEOMETRY type with
its geo statistics, and rashid raises neither finding. So the writer stays
gpio, and the whole point of this file's memory and instrumentation work is
to make that write observable and bounded.

Both `gpio sort column` flags this tool depends on work from geoparquet-io
**1.4.0** onward (PR #663 wired `--write-memory` through to the write engine;
aeaea98b made `_build_copy_options` emit COMPRESSION_LEVEL). The catalog's
workflows pin `geoparquet-io==1.5.0` -- unpinned installs are how the
toolchain moved twice underneath this repo without anyone noticing, which is
what made a flag that "did nothing" quietly start doing something very
expensive. Do not unpin without re-reading
.superpowers/sdd/2026-09-15-sentinel-2-catalog/gpio-fix-report.md.

`--write-memory` is the caller's `--memory` verbatim. The two processes do
not hold RAM at the same time: this one's DuckDB limit is dropped to
GPIO_HANDOFF while gpio runs and restored afterwards, so the budget goes to
whichever process is actually working. Headroom is still the runner's
problem, not this file's -- 12GB (what the workflows pass) on a 16GB runner
is already close, because DuckDB's accounting undershot real RSS by ~1GB+ on
the staging phase in the y2017 diagnosis.

The part lands through a temporary name plus os.replace(): gpio writes
`.items.tmp.parquet` (a dotfile, so upload_data's dotfile rule and every
`*.parquet` glob ignore it if a killed build leaves it behind; still
`.parquet`, because `gpio check` sniffs the extension), `gpio check all`
gates it there, and only then does the rename put it on `items.parquet` in
one atomic step. So a killed build, or a part that failed its check, never
leaves anything at a final name that a resume's exists() check downstream
would trust -- the same protection copy_ndjson_to_parquet() gives chunk
files.

Every phase prints a timestamped line with rows, bytes and seconds. The 2017
job burned six hours with no output at all; a stall should be visible in the
log, not inferred from a timeout.
"""
from __future__ import annotations

import argparse
import http.client
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_fetch import UA as _FETCH_UA, with_retries
from s2_schema import COLUMNS

# The same client name every other tool here sends (Source Cooperative's CDN
# answers 403 to Python-urllib's default); s2_fetch's copy carries a POST
# Content-Type that a HEAD has no use for.
UA = {"User-Agent": _FETCH_UA["User-Agent"]}

# Row-group size is the unit of read amplification for a remote lookup: a
# tile-and-month query fetches every row group that might hold a match,
# whole. Measured on the published 2020/z36-46 part (477 MB, 4.6M rows):
# one 100k-row group is 26.5 MB with assets or 13.8 MB without; at 5k rows
# it is ~0.7 MB. This catalog's access pattern is "find the scenes over my
# field in this window", not a bulk scan, so small groups win -- the
# distribution best-practices doc's 50k-150k guidance assumes the opposite
# workload. Parts published before this changed (2015-2023) carry 100k-row
# groups and are rebuilt when a bigger machine allows; both sizes read
# identically to every client, only the bytes-per-hit differ. gpio rounds
# the request to a power of two: 5,000 writes 6,144-row groups.
ROW_GROUP = 5_000
# What the published part is written with; main() overrides it from
# --row-group-size. Staging COPYs keep ROW_GROUP: they are deleted after
# the sort, so their group size only affects the build, never a reader.
_row_group_size = ROW_GROUP
# The one knob. zstd decompression cost is flat across levels, so a reader
# pays nothing for a high one -- but the writer pays, and on this row shape
# (geometry + array + JSON-heavy columns) the ultra tiers fall off a cliff.
# Benchmarked single-threaded on the first 50k rows of the real staged 2017
# file, DuckDB COPY with threads=1 (levels 3/15/22 from gpio-fix-report.md;
# 18 re-measured 2026-09-16 the same way, with 15 re-run alongside it as the
# control -- it reproduced byte-for-byte, 19,638,531 B, in 6.4s):
#
#   level  3 =   1.4s / 22.3MB
#   level 15 =   5.0s / 19.6MB
#   level 18 = 164.8s / 18.7MB   <- published
#   level 22 = 662.1s / 15.7MB
#
# 18 buys 5% off level 15's size for 26x its time (against the 6.4s
# control run; 33x against the report's 5.0s), and extrapolates to
# roughly 70 minutes of single-threaded compression for a 1.3M-row year --
# hours less than 22's ~4.9 (the two 6-hour CI timeouts on 2017), but not
# minutes. The user chose 18 over both (2026-09-16): "worth some extra time
# for smaller files, as everyone who downloads them benefits". S2_ZSTD_LEVEL
# is a test hook (tests/test_build.py builds one fixture at two levels);
# nothing in CI sets it, and no year part should ever be built with it set.
ZSTD_LEVEL = int(os.environ.get("S2_ZSTD_LEVEL", "18"))
# What this process's DuckDB keeps while the gpio subprocess writes. Small
# enough to hand the runner's RAM over, big enough that the connection
# survives to stage the next year.
GPIO_HANDOFF = "512MB"
WORLD = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"
# The spatial parts of a zone-split year: (file stem, first zone, last zone),
# inclusive. Fixed catalog-wide (spec Amendment 3) from the 2018 row
# distribution so the four balance (27/26/22/25%); do not retune them per
# year, a client picks its part from the tile id alone. make_items.py,
# make_collection.py, the explorer app and the collection docs all name
# these, and tests/test_build.py pins the docs to this tuple.
ZONE_PARTS = (
    ("z01-20", 1, 20),
    ("z21-35", 21, 35),
    ("z36-46", 36, 46),
    ("z47-60", 47, 60),
)
# The first year published as zone parts. 2015-2018 were published whole
# before the split existed and stay that way; the workflows pass --split
# zones for every year from this one on.
ZONE_SPLIT_FROM = 2019
# The eight-part tier: every quartile boundary above is also a boundary here,
# so an octant is always inside exactly one quartile and a reader that knows
# a tile's zone picks one file in either tier. Balanced on the same 2018
# distribution (10-15% each). 2019 and 2020 are published as quartiles and
# stay that way; from ZONE_SPLIT_8_FROM a year is these eight.
ZONE_PARTS_8 = (
    ("z01-15", 1, 15),
    ("z16-20", 16, 20),
    ("z21-31", 21, 31),
    ("z32-35", 32, 35),
    ("z36-40", 36, 40),
    ("z41-46", 41, 46),
    ("z47-52", 47, 52),
    ("z53-60", 53, 60),
)
ZONE_SPLIT_8_FROM = 2021

# Sort key by vintage. Measured on published parts (2026-09-19): a tile-and-
# window lookup took 5.0 s against 2020's 100k-row groups and 6.8 s against
# 2024's ~6k-row groups -- small groups alone bought nothing, because Hilbert
# order within a month scatters one tile's scenes across ~13 neighbours'
# groups and each group is a request. Sorting by tile within the month pins a
# tile-month to one small group, which is what the catalog's primary query
# ("scenes over my tile in this window") wants; _hilbert stays as the
# tiebreak so bbox queries inside a tile keep their locality. Years published
# before this changed keep (_month, _hilbert) until a rebuild.
TILE_SORT_FROM = 2026


def sort_key(year: int) -> str:
    """The gpio --sort column list for a year's parts."""
    return ("_month,s2:mgrs_tile,_hilbert" if year >= TILE_SORT_FROM
            else "_month,_hilbert")


def zone_parts_for(year: int) -> tuple[tuple[str, int, int], ...]:
    """The zone parts a year is published as: () for a single items.parquet
    (before ZONE_SPLIT_FROM), ZONE_PARTS for 2019-2020, ZONE_PARTS_8 from
    ZONE_SPLIT_8_FROM. Every caller that needs a year's part list -- the
    build, the generators, the workflows -- asks here, so no caller can pick
    a tier by hand."""
    if year >= ZONE_SPLIT_8_FROM:
        return ZONE_PARTS_8
    if year >= ZONE_SPLIT_FROM:
        return ZONE_PARTS
    return ()


def archive_part_names() -> tuple[str, ...]:
    """Every file stem an archive part can have, across all tiers and in
    advertised order: items, then the quartiles, then the octants. A year
    holds exactly one tier of these (never live.parquet, which is the
    current year's tail and not an archive part). The probing workflows
    enumerate this list because a HEAD is cheap and a hand-typed list would
    drift."""
    return ("items",
            *(label for label, _, _ in ZONE_PARTS),
            *(label for label, _, _ in ZONE_PARTS_8))


def only_zone_parts(year: int, parts: tuple[tuple[str, int, int], ...],
                    only: tuple[str, ...]) -> tuple[tuple[str, int, int], ...]:
    """The subset of `parts` (a year's tier) named by `only`, in tier
    order. A label that is not in the tier stops the build and names the
    labels that are: a typo would otherwise build nothing and exit as if
    the year were empty."""
    valid = [label for label, _, _ in parts]
    unknown = [label for label in only if label not in valid]
    if unknown:
        raise SystemExit(
            f"--only-parts: {', '.join(unknown)} not among year={year}'s "
            f"zone parts; valid labels: {', '.join(valid)}")
    return tuple(part for part in parts if part[0] in only)


# The UTM zone of a scene, from the leading one or two digits of its MGRS
# tile id ('1VCJ', '31UFU'). NULL when the id does not start with a digit.
ZONE_SQL = """TRY_CAST(regexp_extract("s2:mgrs_tile", '^(\\d{1,2})', 1) AS INTEGER)"""


def say(msg: str) -> None:
    """One timestamped phase line. The 2017 timeout burned six hours with no
    output at all, so every phase reports what it did and how long it took."""
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{stamp}] {msg}", flush=True)


def connect(mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    """One connection for every staging phase. `mem` is the caller's --memory
    verbatim; build_year() borrows it for gpio's --write-memory too."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    # _month is month(datetime) in the session zone. The published parts
    # were built on UTC runners; pin it so a build anywhere else buckets the
    # same rows into the same months, which --exclude-ids-from relies on
    # when it matches staged months against the archive's _month.
    con.execute("SET TimeZone='UTC';")
    con.execute(f"SET memory_limit='{mem}'; SET temp_directory='{tmp}';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def gather(sources: list[str]) -> list[str]:
    files: list[str] = []
    for s in sources:
        p = Path(s)
        if p.is_dir():
            files += [str(f.resolve()) for f in sorted(p.rglob("*.parquet"))
                      if f.stat().st_size > 0]
        elif p.suffix == ".parquet" and p.stat().st_size > 0:
            files.append(str(p.resolve()))
    if not files:
        raise SystemExit(f"no non-empty parquet under {sources}")
    return files


def _select(con, lst: str) -> str:
    """Canonical select list. A column absent from EVERY source cannot be
    referenced even with union_by_name, so it becomes a typed NULL — this is
    what lets partial fixtures and differently-shaped chunks build."""
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{lst}], union_by_name=true)"
    ).fetchall()}
    parts = []
    for name, typ, _ in COLUMNS:
        if name in ("_month", "_hilbert", "geometry"):
            continue
        if name in have:
            parts.append(f'CAST("{name}" AS {typ}) AS "{name}"')
        else:
            parts.append(f'NULL::{typ} AS "{name}"')
    return ", ".join(parts)


def _sort_and_check(con, staged: Path, final: Path, year: int,
                    memory: str) -> None:
    """The ordered GeoParquet 2.0 write of one part, its gate, then its name.

    gpio writes `.<stem>.tmp.parquet`, `gpio check all` runs on that, and
    only a part that passed is os.replace()d onto `<name>` in one atomic
    rename -- so neither a killed build nor a part that failed its check
    leaves anything at the final name that a resume's exists() check
    downstream would trust. --write-memory needs gpio >= 1.4; on 1.3.0 it
    was silently dropped and the write self-picked 50% of available RAM.
    The DuckDB limit drops to GPIO_HANDOFF for the duration so the two
    processes are not bidding for the same RAM.
    """
    name = final.name
    tmp = final.with_name(f".{final.stem}.tmp.parquet")
    tmp.unlink(missing_ok=True)
    t0 = time.monotonic()
    try:
        con.execute(f"SET memory_limit='{GPIO_HANDOFF}';")
        r = subprocess.run(
            ["gpio", "sort", "column", str(staged), str(tmp),
             sort_key(year), "--geoparquet-version", "2.0",
             "--compression", "zstd",
             "--compression-level", str(ZSTD_LEVEL),
             "--row-group-size", str(_row_group_size),
             "--write-memory", memory],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-1500:], r.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio sort failed for {year}")
        say(f"year={year}/{name}: sorted ({sort_key(year)}) and written "
            f"zstd-{ZSTD_LEVEL}, {tmp.stat().st_size / 1e6:,.0f} MB, "
            f"{time.monotonic() - t0:,.1f}s")
        # Best-practices gate on the artifact itself: compression, row
        # groups, spatial order, bbox metadata. Fails the build only on
        # gpio's error-level violations (non-zero exit); WARNING-level
        # findings (e.g. omitted geo-metadata CRS, which the GeoParquet
        # spec defaults to OGC:CRS84) pass and are acceptable.
        t0 = time.monotonic()
        chk = subprocess.run(["gpio", "check", "all", str(tmp)],
                             capture_output=True, text=True)
        if chk.returncode != 0:
            print(chk.stdout[-1500:], chk.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio check failed for {year}")
        say(f"year={year}/{name}: gpio check all passed, "
            f"{time.monotonic() - t0:,.1f}s")
        os.replace(tmp, final)
    finally:
        con.execute(f"SET memory_limit='{memory}';")
        tmp.unlink(missing_ok=True)


def published_url(url: str, tries: int = 8, what: str = "") -> bool:
    """Is `url` published? One HEAD, with the s2_fetch retry/backoff on
    network errors and 5xx answers.

    True on 200, False on 404. Anything else stops the build: a 403, a
    persistent 5xx or an unreachable host is a question that went unanswered,
    and guessing either way is wrong -- "not published" would rebuild and
    re-upload an hour of work at best, and "published" would skip a part
    that is not there and leave the year short on the bucket. `what` names
    the part in the refusal (published_part passes year=YYYY/<name>).
    """
    label = what or url

    def head() -> int:
        req = urllib.request.Request(url, method="HEAD", headers=UA)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status
        except urllib.error.HTTPError as e:
            if e.code >= 500:
                raise  # transient on the server's side: back off and re-ask
            return e.code

    try:
        code = with_retries(head, tries)
    except (urllib.error.URLError, TimeoutError, http.client.HTTPException,
            ConnectionError) as e:
        raise SystemExit(
            f"{label}: cannot ask {url} whether it is already "
            f"published ({e}); refusing to guess")
    if code == 200:
        return True
    if code == 404:
        return False
    raise SystemExit(
        f"{label}: HEAD {url} answered {code}, not 200 or 404; "
        f"refusing to guess whether it is published")


def published_part(base: str, year: int, name: str, tries: int = 8) -> bool:
    """Is BASE/year=YYYY/<name> already published? published_url() on the
    part's URL, with the same 200/404/anything-else semantics."""
    return published_url(f"{base.rstrip('/')}/year={year}/{name}", tries,
                         what=f"year={year}/{name}")


def published_rows(con, url: str) -> int:
    """The row count of a published part, from its footer over HTTP (DuckDB
    httpfs: a HEAD and a range read of the footer, never the data). Used to
    check that a part being skipped as already published holds the same
    rows this build's slices would give it."""
    con.execute("INSTALL httpfs; LOAD httpfs;")
    try:
        return con.execute(
            "SELECT num_rows FROM parquet_file_metadata(?)", [url]).fetchone()[0]
    except duckdb.Error as e:
        raise SystemExit(
            f"cannot read the footer of {url} to check its row count ({e}); "
            f"refusing to guess whether the published part is current")


def live_row_count(url: str) -> int:
    """published_rows() on its own connection: the row count of a published
    live.parquet from its footer, for a plan that has no build connection."""
    return published_rows(duckdb.connect(), url)


def consolidation_plan(base: str, year: int, probe=published_part,
                       live_rows=live_row_count) -> dict:
    """What consolidate-month.yml's plan job needs to know about a year,
    from one round of HEADs: whether live.parquet is published, how many
    rows it holds, and for each archive part of the year's tier ("items"
    when it has none) whether it is published. Every HEAD goes through
    `probe` (published_part, with its retries), so a transient failure is
    retried here, once, instead of in each of eight part jobs -- one part
    job that took a passing 404 for "never published" would rebuild its
    part from live alone and drop the rest of the year's scenes on upload.
    A probe that cannot answer stops the plan (published_part raises), and
    nothing is built.

    The row count comes from the footer through `live_rows` (published_rows
    over HTTP) and decides `fold`: a year is folded only when its live is
    published AND holds rows. An emptied live (what the last consolidation
    left, zero rows) has nothing to fold, and folding it would rewrite every
    part of the year from the part alone -- hours of runner time for a
    byte-identical result. This is what lets the plan look at the previous
    year every month: in January it folds December's tail, and from
    February on the previous year's live is empty and is skipped.

    Returns {"live": bool, "rows": int | None, "fold": bool,
             "parts": [label, ...],
             "include": [{"year": year, "part": label, "exists": bool}, ...]}.
    """
    labels = [label for label, _, _ in zone_parts_for(year)] or ["items"]
    live = probe(base, year, "live.parquet")
    rows = live_rows(f"{base.rstrip('/')}/year={year}/live.parquet") if live else None
    include = [{"year": year, "part": label,
                "exists": probe(base, year, f"{label}.parquet")}
               for label in labels]
    return {"live": live, "rows": rows, "fold": bool(live and rows),
            "parts": labels, "include": include}


def consolidation_plans(base: str, years: list[int], probe=published_part,
                        live_rows=live_row_count) -> dict:
    """consolidation_plan() over several years, merged into the shape the
    plan job writes to its outputs: the years to fold (those whose live is
    published with rows), each folded year's part labels, and one matrix
    entry per (year, part). consolidate-month.yml asks for the current
    year and the previous one every month, so the year's last tail (the
    scenes of late December, fetched in the new year's first days) is
    folded on January 3rd instead of never.

    Returns {"years": [year, ...], "parts": {"YYYY": [label, ...]},
             "include": [{"year", "part", "exists"}, ...],
             "plans": {year: consolidation_plan(...)}}.
    """
    plans = {y: consolidation_plan(base, y, probe, live_rows) for y in years}
    folded = [y for y in years if plans[y]["fold"]]
    return {"years": folded,
            "parts": {str(y): plans[y]["parts"] for y in folded},
            "include": [e for y in folded for e in plans[y]["include"]],
            "plans": plans}


def run_part_hook(cmd: list[str], part: Path, year: int) -> None:
    """Run the --on-part-done command with the finished part's path appended
    (`shlex.split(CMD) + [path]`, stdio inherited so an upload's progress
    lands in the job log). A non-zero exit stops the build: the workflow
    uses this to upload each part as it is finished, and a part whose upload
    failed must not be counted as done -- the next resume would HEAD it,
    find it missing, and rebuild it, which is the right outcome, but only
    if this run stops here instead of spending hours on parts whose uploads
    will fail the same way."""
    t0 = time.monotonic()
    say(f"year={year}/{part.name}: running {shlex.join(cmd)} {part}")
    r = subprocess.run(cmd + [str(part)])
    if r.returncode != 0:
        raise SystemExit(
            f"year={year}/{part.name}: --on-part-done command exited "
            f"{r.returncode}; stopping the build")
    say(f"year={year}/{part.name}: on-part-done ok, "
        f"{time.monotonic() - t0:,.1f}s")


def _stage_zone_parts(con, staged: Path, year: int,
                      parts: tuple[tuple[str, int, int], ...],
                      skip: dict[str, str],
                      remote_rows=published_rows,
                      partial: bool = False,
                      ) -> tuple[list[tuple[str, Path | None, int]], int]:
    """Copy each range of `parts` out of the staged year into its own staged
    file. Returns ((label, path, rows) for the ranges that have rows, rows
    dropped) -- path None for a label in `skip` (label -> published URL),
    which is never copied but is counted, and its count compared with the
    published part's footer: a published part built from an older slice
    set does not match what this build would write, and keeping it
    silently would leave the year stale, so that stops the build naming
    both counts. Removes the whole-year staged file once the copies exist
    so the disk never holds the year twice on top of a sort spill. A row
    with no parseable zone would land in no part; rather than publish a
    year that is quietly short, the build stops on the first one.

    `partial` says `parts` is a --only-parts subset of the year's tier: the
    rows of every other range are dropped on purpose, and their count is
    logged and returned so the caller's row accounting still closes. Without
    it the tier covers zones 1-60 and nothing is dropped."""
    lost = con.execute(
        f"SELECT count(*) FROM read_parquet('{staged}') "
        f"WHERE {ZONE_SQL} IS NULL OR {ZONE_SQL} NOT BETWEEN 1 AND 60"
    ).fetchone()[0]
    if lost:
        raise SystemExit(
            f"year={year}: {lost:,} row(s) with no UTM zone in "
            f"s2:mgrs_tile fall outside every zone part; refusing to "
            f"drop them")
    dropped = 0
    if partial:
        kept = " OR ".join(f"{ZONE_SQL} BETWEEN {lo} AND {hi}"
                           for _, lo, hi in parts)
        dropped = con.execute(
            f"SELECT count(*) FROM read_parquet('{staged}') "
            f"WHERE NOT ({kept})").fetchone()[0]
        say(f"year={year}: --only-parts "
            f"{','.join(label for label, _, _ in parts)}: {dropped:,} row(s) "
            f"of other zone ranges dropped")
    out = []
    for label, lo, hi in parts:
        name = f"{label}.parquet"
        if label in skip:
            n = con.execute(
                f"SELECT count(*) FROM read_parquet('{staged}') "
                f"WHERE {ZONE_SQL} BETWEEN {lo} AND {hi}").fetchone()[0]
            have = remote_rows(con, skip[label])
            if have != n:
                raise SystemExit(
                    f"year={year}/{name}: already published with {have:,} "
                    f"rows, but this build's slices give zones {lo}-{hi} "
                    f"{n:,} rows; the published part is not this build's. "
                    f"Not skipping it silently -- rebuild the year without "
                    f"--skip-existing-url, or remove the stale part first.")
            say(f"year={year}/{name}: already published with the same "
                f"{n:,} rows (zones {lo}-{hi}), skipping")
            out.append((label, None, n))
            continue
        part_staged = staged.with_name(name)
        t0 = time.monotonic()
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{staged}')
                  WHERE {ZONE_SQL} BETWEEN {lo} AND {hi})
            TO '{part_staged}'
              (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {ROW_GROUP})
        """)
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{part_staged}')").fetchone()[0]
        if n == 0:
            part_staged.unlink()
            say(f"year={year}/{name}: no rows, skipped")
            continue
        say(f"year={year}/{name}: staged {n:,} rows (zones {lo}-{hi}), "
            f"{part_staged.stat().st_size / 1e6:,.0f} MB, "
            f"{time.monotonic() - t0:,.1f}s")
        out.append((label, part_staged, n))
    staged.unlink()
    return out, dropped


def exclude_published_ids(con, staged: Path, urls: list[str], year: int,
                          label: str, probe_url=published_url) -> int:
    """Drop from the staged year every row whose id one of `urls` (published
    archive parts of the same year) already holds at the same or a newer
    s2:generation_time. Returns the number dropped; the staged file is
    rewritten in place only when that is > 0.

    Each URL is HEADed first through `probe_url` (published_url: 200 reads,
    404 skips with a log line, anything else stops the build). The archive
    is read with DuckDB httpfs, projecting only `id` and
    `s2:generation_time` and filtering on `_month` to the months the staged
    rows span: the parts are sorted (_month, ...) so the filter prunes on
    row-group statistics and the read is a few range requests per part
    rather than the part. `_month` is the same session-UTC month(datetime)
    on both sides (see connect()).

    The generation comparison is the year build's own dedupe rule (highest
    s2:generation_time wins, NULLS LAST) applied across the archive
    boundary: a staged row is dropped unless its generation is strictly
    newer than the archive's for that id. A reprocessed product that
    arrives in the lookback after its predecessor was consolidated is
    therefore kept in live, where the next consolidation's dedupe replaces
    the archive copy; dropping it by id alone would lose it for good. The
    rewrite is an ANTI JOIN against the ids to drop, not NOT IN, so a NULL
    id cannot empty the result."""
    months = [r[0] for r in con.execute(
        f"SELECT DISTINCT _month FROM read_parquet('{staged}') ORDER BY 1"
    ).fetchall()]
    present = []
    for url in urls:
        if probe_url(url):
            present.append(url)
        else:
            say(f"year={year}/{label}: {url} is not published (404); "
                f"nothing to exclude from it")
    if not present:
        return 0
    con.execute("INSTALL httpfs; LOAD httpfs;")
    lst = ",".join(f"'{u}'" for u in present)
    month_list = ",".join(str(m) for m in months)
    t0 = time.monotonic()
    con.execute("DROP TABLE IF EXISTS archived; DROP TABLE IF EXISTS to_drop;")
    con.execute(f"""
        CREATE TEMP TABLE archived AS
        SELECT id, max("s2:generation_time") AS gen
        FROM read_parquet([{lst}])
        WHERE _month IN ({month_list})
        GROUP BY id
    """)
    held = con.execute("SELECT count(*) FROM archived").fetchone()[0]
    say(f"year={year}/{label}: {held:,} id(s) read from {len(present)} "
        f"published part(s) for month(s) {month_list}, "
        f"{time.monotonic() - t0:,.1f}s")
    # Newer means strictly greater, with the build's NULLS LAST reading: a
    # NULL staged generation is never newer, a NULL archive generation is
    # beaten by any non-NULL one, and two NULLs are equal (dropped).
    con.execute(f"""
        CREATE TEMP TABLE to_drop AS
        SELECT s.id FROM read_parquet('{staged}') s
        JOIN archived a USING (id)
        WHERE NOT coalesce(
            s."s2:generation_time" > a.gen
            OR (s."s2:generation_time" IS NOT NULL AND a.gen IS NULL), FALSE)
    """)
    dropped = con.execute("SELECT count(*) FROM to_drop").fetchone()[0]
    if dropped:
        kept = staged.with_name(".rows.kept.parquet")
        con.execute(f"""
            COPY (SELECT s.* FROM read_parquet('{staged}') s
                  ANTI JOIN to_drop d USING (id))
            TO '{kept}'
              (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {ROW_GROUP})
        """)
        os.replace(kept, staged)
    con.execute("DROP TABLE archived; DROP TABLE to_drop;")
    say(f"year={year}/{label}: {dropped:,} row(s) dropped that the published "
        f"archive already holds at the same or a newer generation")
    return dropped


def write_empty_part(con, staged: Path, final: Path, year: int) -> None:
    """A zero-row part with the schema of `staged` (a staged year, or a
    finished archive part): a plain DuckDB COPY, not the gpio
    sort/check pipeline, which has nothing to sort or check in an empty
    file. Written through the same dotfile temporary and os.replace() as
    a real part. Two callers: build_year() when --exclude-ids-from drops
    every staged row, and consolidate-month.yml's finalize job for the
    emptied live.parquet of each year it folded."""
    tmp = final.with_name(f".{final.stem}.tmp.parquet")
    con.execute(f"""
        COPY (SELECT * FROM read_parquet('{staged}') LIMIT 0)
        TO '{tmp}' (FORMAT PARQUET, COMPRESSION zstd)
    """)
    os.replace(tmp, final)
    say(f"year={year}/{final.name}: wrote it with zero rows "
        f"(schema of {staged.name})")


def build_year(con, files: list[str], year: int, outdir: Path,
               name: str = "items.parquet", memory: str = "8GB",
               split: str | None = None, skip_existing_url: str | None = None,
               on_part_done: list[str] | None = None,
               probe=published_part, remote_rows=published_rows,
               only_parts: tuple[str, ...] | None = None,
               exclude_ids_from: list[str] | None = None,
               probe_url=published_url) -> tuple[int, int, int]:
    """Build one year. Returns (rows written, parts skipped as already
    published, part files written -- which counts a zero-row live written
    because --exclude-ids-from dropped every staged row). With --split zones the parts are zone_parts_for(year); a
    year below ZONE_SPLIT_FROM has none and the split is refused rather
    than silently written whole. `only_parts` narrows those to the named
    labels (only_zone_parts) and the other ranges' rows are dropped.
    `exclude_ids_from` (--exclude-ids-from) drops staged rows whose id a
    listed published part holds, see exclude_published_ids(). `probe`
    (published_part), `remote_rows` (published_rows) and `probe_url`
    (published_url) are injectable for tests. A year whose every part is
    published is skipped before staging, on the HEADs alone: the point of
    the flag is that re-dispatching a finished year costs nothing."""
    lst = ",".join(f"'{f}'" for f in files)
    label = "zones" if split == "zones" else name
    # Every refusal comes before the year directory exists, so a refused
    # build leaves no empty year=YYYY/ behind.
    parts = ()
    if split == "zones":
        parts = zone_parts_for(year)
        if not parts:
            raise SystemExit(
                f"--split zones: {year} is before {ZONE_SPLIT_FROM} and has "
                f"no zone parts; it is published as one items.parquet")
        if only_parts:
            parts = only_zone_parts(year, parts, only_parts)
    elif only_parts:
        raise SystemExit("--only-parts applies to --split zones only")
    dest = outdir / f"year={year}"
    dest.mkdir(parents=True, exist_ok=True)
    final = dest / name
    # Ask the bucket before staging anything: a year whose every part is
    # already up costs one HEAD per part and no minutes, and a rerun after a
    # timeout builds only what the last run did not finish uploading.
    skip: dict[str, str] = {}
    if skip_existing_url:
        wanted = [f"{lb}.parquet" for lb, _, _ in parts] if parts else [name]
        skip = {n[:-len(".parquet")]:
                f"{skip_existing_url.rstrip('/')}/year={year}/{n}"
                for n in wanted if probe(skip_existing_url, year, n)}
        if len(skip) == len(wanted):
            say(f"year={year}/{label}: every part already published "
                f"({', '.join(wanted)}); nothing to build")
            return 0, len(skip), 0
    with tempfile.TemporaryDirectory() as td:
        staged = Path(td) / "rows.parquet"
        t0 = time.monotonic()
        con.execute(f"""
            COPY (
              SELECT {_select(con, lst)},
                     month(datetime)::TINYINT AS _month,
                     ST_Hilbert(geometry, {WORLD}) AS _hilbert,
                     geometry
              FROM read_parquet([{lst}], union_by_name=true)
              WHERE year(datetime) = {year}
              QUALIFY row_number() OVER (
                PARTITION BY id
                ORDER BY "s2:generation_time" DESC NULLS LAST) = 1
            ) TO '{staged}'
              (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {ROW_GROUP})
        """)
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{staged}')").fetchone()[0]
        say(f"year={year}/{label}: staged {n:,} rows, "
            f"{staged.stat().st_size / 1e6:,.0f} MB, "
            f"{time.monotonic() - t0:,.1f}s")
        staged_rows = n
        if n and exclude_ids_from:
            n -= exclude_published_ids(con, staged, exclude_ids_from, year,
                                       label, probe_url)
        if n == 0:
            if staged_rows and split != "zones":
                # Rows were staged and the archive already had every one:
                # the part is current and empty, which is a result to
                # publish (the stats splice and the restamps still run),
                # not a failed build.
                say(f"year={year}/{name}: every staged row is already in "
                    f"the published archive")
                write_empty_part(con, staged, final, year)
                if on_part_done:
                    run_part_hook(on_part_done, final, year)
                return 0, 0, 1
            if not any(dest.iterdir()):
                shutil.rmtree(dest, ignore_errors=True)
            return 0, 0, 0
        if split != "zones":
            _sort_and_check(con, staged, final, year, memory)
            print(f"  year={year}/{name}: {n:,} rows, "
                  f"{final.stat().st_size / 1e6:,.0f} MB", flush=True)
            if on_part_done:
                run_part_hook(on_part_done, final, year)
            return n, 0, 1
        # The parts one at a time: each staged range is sorted, written,
        # checked, handed to --on-part-done and deleted before the next
        # one's sort starts, so the peak is one range's spill plus the other
        # ranges waiting on disk, never a whole year's sort.
        written = skipped = parts_written = 0
        staged_parts, dropped = _stage_zone_parts(
            con, staged, year, parts, skip, remote_rows,
            partial=bool(only_parts))
        for part_label, part_staged, part_rows in staged_parts:
            if part_staged is None:
                skipped += part_rows
                continue
            part_final = dest / f"{part_label}.parquet"
            _sort_and_check(con, part_staged, part_final, year, memory)
            part_staged.unlink()
            print(f"  year={year}/{part_final.name}: {part_rows:,} rows, "
                  f"{part_final.stat().st_size / 1e6:,.0f} MB", flush=True)
            if on_part_done:
                run_part_hook(on_part_done, part_final, year)
            written += part_rows
            parts_written += 1
        if written + skipped + dropped != n:
            raise SystemExit(
                f"year={year}: staged {n:,} rows but the zone parts hold "
                f"{written + skipped:,} and {dropped:,} were dropped")
    return written, len(skip), parts_written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True,
                    help="chunk dirs and/or parquet files (a published "
                         "part or live.parquet counts)")
    ap.add_argument("--years", help="comma list; default = every year found")
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="items.parquet")
    ap.add_argument("--memory", default="8GB")
    ap.add_argument("--row-group-size", type=int, default=ROW_GROUP,
                    help=f"rows per Parquet row group (default {ROW_GROUP}; the "
                         "read-amplification knob for remote lookups)")
    ap.add_argument("--split", choices=["zones"],
                    help="write the year as zone parts by UTM zone instead "
                         f"of one --name file: {len(ZONE_PARTS)} parts from "
                         f"{ZONE_SPLIT_FROM}, {len(ZONE_PARTS_8)} from "
                         f"{ZONE_SPLIT_8_FROM} (zone_parts_for)")
    ap.add_argument("--only-parts", metavar="LABEL[,LABEL...]",
                    help="with --split zones: write only these parts of the "
                         "year's tier and drop the rows of every other zone "
                         "range (consolidate-month.yml builds one part per "
                         "job this way)")
    ap.add_argument("--skip-existing-url", metavar="BASE",
                    help="HEAD BASE/year=YYYY/<part>.parquet before each "
                         "part; 200 skips it, 404 builds it, anything else "
                         "stops the build")
    ap.add_argument("--on-part-done", metavar="CMD",
                    help="after a part passes gpio check, run shlex.split(CMD) "
                         "+ [part path]; a non-zero exit stops the build")
    ap.add_argument("--exclude-ids-from", metavar="URL", nargs="+",
                    help="published parts of the same year (URLs); staged "
                         "rows whose id one of them holds are dropped before "
                         "the sort. 404 skips a URL, anything else stops "
                         "the build")
    a = ap.parse_args()
    global _row_group_size
    _row_group_size = a.row_group_size
    if a.split and a.name != "items.parquet":
        ap.error("--split zones names its own parts; --name does not apply")
    only_parts = None
    if a.only_parts is not None:
        if a.split != "zones":
            ap.error("--only-parts applies to --split zones only")
        only_parts = tuple(dict.fromkeys(
            label.strip() for label in a.only_parts.split(",") if label.strip()))
        if not only_parts:
            ap.error("--only-parts needs at least one part label")
    hook = shlex.split(a.on_part_done) if a.on_part_done else None
    if a.on_part_done and not hook:
        ap.error("--on-part-done needs a command")

    outdir = Path(a.out).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    tmp = outdir.parent / ".duckdb-tmp"
    tmp.mkdir(exist_ok=True)
    con = connect(a.memory, tmp)
    files = gather(a.sources)

    if a.years:
        years = [int(y) for y in a.years.split(",")]
    else:
        lst = ",".join(f"'{f}'" for f in files)
        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year(datetime) y "
            f"FROM read_parquet([{lst}], union_by_name=true) ORDER BY y"
        ).fetchall()]

    total = skipped = written = 0
    for y in years:
        rows, skips, parts_written = build_year(
            con, files, y, outdir, a.name, a.memory, a.split,
            a.skip_existing_url, hook, only_parts=only_parts,
            exclude_ids_from=a.exclude_ids_from)
        total += rows
        skipped += skips
        written += parts_written
    print(f"TOTAL {total:,} rows across {len(years)} year(s)"
          + (f", {skipped} part(s) already published" if skipped else ""))
    # A zero-row part written because --exclude-ids-from dropped every
    # staged row counts as written: the part is current. No rows and no
    # part is the failure it always was.
    if total == 0 and not skipped and not written:
        print("no rows matched: nothing was written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
