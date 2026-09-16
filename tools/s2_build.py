#!/usr/bin/env python3
"""Compact chunks (and/or the seed archive) into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)

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
# Benchmarked single-threaded on 50k rows of the real staged 2017 file
# (gpio-fix-report.md): level 3 = 1.4s / 22.3MB, level 15 = 5.0s / 19.6MB,
# level 22 = 662.1s / 15.7MB. That is 132x level 15's time for 20% off its
# size, and extrapolates to ~4.9 hours of compression for one year part --
# the best explanation of the two 6-hour CI timeouts on 2017. 22 is what the
# user asked for on 2026-09-15, when every timing in front of them was
# secretly level 3; the 15-vs-22 call is being made separately, so this
# constant stays 22 until it is. S2_ZSTD_LEVEL is a test hook
# (tests/test_build.py builds one fixture at two levels); nothing in CI sets
# it, and no year part should ever be built with it set.
ZSTD_LEVEL = int(os.environ.get("S2_ZSTD_LEVEL", "22"))
# What this process's DuckDB keeps while the gpio subprocess writes. Small
# enough to hand the runner's RAM over, big enough that the connection
# survives to stage the next year.
GPIO_HANDOFF = "512MB"
WORLD = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"


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


def build_year(con, files: list[str], year: int, outdir: Path,
               name: str = "items.parquet", memory: str = "8GB") -> int:
    lst = ",".join(f"'{f}'" for f in files)
    dest = outdir / f"year={year}"
    dest.mkdir(parents=True, exist_ok=True)
    final = dest / name
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
        say(f"year={year}/{name}: staged {n:,} rows, "
            f"{staged.stat().st_size / 1e6:,.0f} MB, "
            f"{time.monotonic() - t0:,.1f}s")
        if n == 0:
            if not any(dest.iterdir()):
                shutil.rmtree(dest, ignore_errors=True)
            return 0
        # The ordered GeoParquet 2.0 write. gpio writes `<name>.tmp` (hence
        # --any-extension) and os.replace() puts it on `<name>` in one
        # atomic rename, so a killed build leaves the previous part intact
        # rather than a half-written one that every resume's exists() check
        # downstream would trust. --write-memory needs gpio >= 1.4; on 1.3.0
        # it was silently dropped and the write self-picked 50% of available
        # RAM. The DuckDB limit drops to GPIO_HANDOFF for the duration so
        # the two processes are not bidding for the same RAM.
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
    print(f"  year={year}/{name}: {n:,} rows, "
          f"{final.stat().st_size / 1e6:,.0f} MB", flush=True)
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True,
                    help="chunk dirs and/or parquet files (seed included)")
    ap.add_argument("--years", help="comma list; default = every year found")
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="items.parquet")
    ap.add_argument("--memory", default="8GB")
    a = ap.parse_args()

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
        total += build_year(con, files, y, outdir, a.name, a.memory)
    print(f"TOTAL {total:,} rows across {len(years)} part(s)")
    if total == 0:
        print("no rows matched: nothing was written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
