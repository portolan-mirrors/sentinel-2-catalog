#!/usr/bin/env python3
"""Compact chunks (and/or the seed archive) into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)
    sentinel-2-l2a/year=<YYYY>/z01-20.parquet  (--split zones: four parts by
                          z21-35.parquet   UTM zone of s2:mgrs_tile, see
                          z36-46.parquet   ZONE_PARTS; spec Amendment 3)
                          z47-60.parquet

Without --split the year is one file, which is how 2015-2018 are published
and how `live.parquet` is always built. With `--split zones` the year is
staged ONCE (dedupe + helper columns, exactly as without), then each zone
range is copied out of that staged file into its own staged part and put
through the same gpio sort/write/check/rename pipeline on its own -- so the
sort, the spill and the output only ever hold a quarter of the year. That is
what lets a 7M-row year build on a free runner: 2019 as one file spilled
past the runner's 51.9 GB disk in gpio's sort. A range with no rows writes
no file. A row whose tile has no parseable zone belongs to no part, and
rather than drop it the build stops.

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

The part lands through a `.tmp` name plus os.replace(): gpio writes
`items.parquet.tmp` (hence --any-extension) and the rename puts it on
`items.parquet` in one atomic step. Without it a killed build leaves a
half-written part that every resume's exists() check downstream would trust
-- the same protection copy_ndjson_to_parquet() gives chunk files.

`gpio check all` gates the artifact, and every phase prints a timestamped
line with rows, bytes and seconds. The 2017 job burned six hours with no
output at all; a stall should be visible in the log, not inferred from a
timeout.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_schema import COLUMNS

ROW_GROUP = 100_000
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
    """The ordered GeoParquet 2.0 write of one part, then its gate.

    gpio writes `<name>.tmp` (hence --any-extension) and os.replace() puts it
    on `<name>` in one atomic rename, so a killed build leaves the previous
    part intact rather than a half-written one that every resume's exists()
    check downstream would trust. --write-memory needs gpio >= 1.4; on 1.3.0
    it was silently dropped and the write self-picked 50% of available RAM.
    The DuckDB limit drops to GPIO_HANDOFF for the duration so the two
    processes are not bidding for the same RAM.
    """
    name = final.name
    tmp = final.with_name(final.name + ".tmp")
    tmp.unlink(missing_ok=True)
    t0 = time.monotonic()
    try:
        con.execute(f"SET memory_limit='{GPIO_HANDOFF}';")
        r = subprocess.run(
            ["gpio", "sort", "column", str(staged), str(tmp),
             "_month,_hilbert", "--geoparquet-version", "2.0",
             "--compression", "zstd",
             "--compression-level", str(ZSTD_LEVEL),
             "--row-group-size", str(ROW_GROUP),
             "--write-memory", memory, "--any-extension"],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-1500:], r.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio sort failed for {year}")
        os.replace(tmp, final)
    finally:
        con.execute(f"SET memory_limit='{memory}';")
        tmp.unlink(missing_ok=True)
    say(f"year={year}/{name}: sorted (_month, _hilbert) and written "
        f"zstd-{ZSTD_LEVEL}, {final.stat().st_size / 1e6:,.0f} MB, "
        f"{time.monotonic() - t0:,.1f}s")
    # Best-practices gate on the artifact itself: compression, row
    # groups, spatial order, bbox metadata. Fails the build only on
    # gpio's error-level violations (non-zero exit); WARNING-level
    # findings (e.g. omitted geo-metadata CRS, which the GeoParquet
    # spec defaults to OGC:CRS84) pass and are acceptable.
    t0 = time.monotonic()
    chk = subprocess.run(["gpio", "check", "all", str(final)],
                         capture_output=True, text=True)
    if chk.returncode != 0:
        print(chk.stdout[-1500:], chk.stderr[-1500:], file=sys.stderr)
        raise SystemExit(f"gpio check failed for {year}")
    say(f"year={year}/{name}: gpio check all passed, "
        f"{time.monotonic() - t0:,.1f}s")


def _stage_zone_parts(con, staged: Path, year: int) -> list[tuple[str, Path, int]]:
    """Copy each ZONE_PARTS range out of the staged year into its own staged
    file. Returns (label, path, rows) for the ranges that have rows, and
    removes the whole-year staged file once they all exist so the disk never
    holds the year twice on top of a sort spill. A row with no parseable zone
    would land in no part; rather than publish a year that is quietly short,
    the build stops on the first one."""
    lost = con.execute(
        f"SELECT count(*) FROM read_parquet('{staged}') "
        f"WHERE {ZONE_SQL} IS NULL OR {ZONE_SQL} NOT BETWEEN 1 AND 60"
    ).fetchone()[0]
    if lost:
        raise SystemExit(
            f"year={year}: {lost:,} row(s) with no UTM zone in "
            f"s2:mgrs_tile fall outside every zone part; refusing to "
            f"drop them")
    parts = []
    for label, lo, hi in ZONE_PARTS:
        name = f"{label}.parquet"
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
        parts.append((label, part_staged, n))
    staged.unlink()
    return parts


def build_year(con, files: list[str], year: int, outdir: Path,
               name: str = "items.parquet", memory: str = "8GB",
               split: str | None = None) -> int:
    lst = ",".join(f"'{f}'" for f in files)
    dest = outdir / f"year={year}"
    dest.mkdir(parents=True, exist_ok=True)
    final = dest / name
    label = "zones" if split == "zones" else name
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
        if n == 0:
            if not any(dest.iterdir()):
                shutil.rmtree(dest, ignore_errors=True)
            return 0
        if split != "zones":
            _sort_and_check(con, staged, final, year, memory)
            print(f"  year={year}/{name}: {n:,} rows, "
                  f"{final.stat().st_size / 1e6:,.0f} MB", flush=True)
            return n
        # Four parts, one at a time: each staged range is sorted, written,
        # checked and deleted before the next one's sort starts, so the
        # peak is one range's spill plus the other ranges waiting on disk,
        # never a whole year's sort.
        written = 0
        for part_label, part_staged, part_rows in _stage_zone_parts(con, staged, year):
            part_final = dest / f"{part_label}.parquet"
            _sort_and_check(con, part_staged, part_final, year, memory)
            part_staged.unlink()
            print(f"  year={year}/{part_final.name}: {part_rows:,} rows, "
                  f"{part_final.stat().st_size / 1e6:,.0f} MB", flush=True)
            written += part_rows
        if written != n:
            raise SystemExit(
                f"year={year}: staged {n:,} rows but the zone parts hold "
                f"{written:,}")
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True,
                    help="chunk dirs and/or parquet files (seed included)")
    ap.add_argument("--years", help="comma list; default = every year found")
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="items.parquet")
    ap.add_argument("--memory", default="8GB")
    ap.add_argument("--split", choices=["zones"],
                    help="write the year as ZONE_PARTS files by UTM zone "
                         "instead of one --name file (years >= "
                         f"{ZONE_SPLIT_FROM})")
    a = ap.parse_args()
    if a.split and a.name != "items.parquet":
        ap.error("--split zones names its own parts; --name does not apply")

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

    total = 0
    for y in years:
        total += build_year(con, files, y, outdir, a.name, a.memory, a.split)
    print(f"TOTAL {total:,} rows across {len(years)} year(s)")
    if total == 0:
        print("no rows matched: nothing was written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
