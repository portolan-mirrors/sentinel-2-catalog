#!/usr/bin/env python3
"""Compact chunks (and/or the seed archive) into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)

Rows are deduped by id keeping the highest s2:generation_time, then sorted
(_month, _hilbert): month-first keeps month pruning inside a year file,
Hilbert-within-month keeps row-group bboxes tight for spatial pruning. The
two helper columns are published and documented in the collection AGENTS.md.

DuckDB does both the staging write and the ordered final write. It used to
be `gpio sort column`, which silently drops BOTH `--write-memory` and
`--compression-level` (geoparquet-io 1.3.0; see
.superpowers/sdd/2026-09-15-sentinel-2-catalog/y2017-diagnosis.md): the sort
therefore ran against a self-picked 50%-of-available-RAM budget that starved
and spilled on a 16GB runner (two 6-hour CI timeouts on year 2017), and every
part built so far is DuckDB-default zstd, not the level this file asks for.
`gpio check all` is unaffected by those bugs and stays as the gate on the
artifact.

Sorting in DuckDB and writing in DuckDB is one tool, one pass: the ORDER BY
lives in the final COPY's subquery, so nothing downstream can reorder it. Do
NOT sort here and then run `gpio convert`: convert does not preserve row
order (firms-catalog; re-measured 2026-09-16 on a 50k-row fixture -- the
converted file came back unsorted).

preserve_insertion_order: the connection runs with it FALSE for the staging
COPY and TRUE for the final ordered COPY only. Measured on DuckDB 1.5.3 with
12 threads, ORDER BY inside a COPY subquery was honoured either way -- files
written with the setting off came back correctly ordered at 150k, 1M and 5M
rows, including a sort forced to spill under a 1GB memory_limit. (Beware the
trap that cost an hour here: reading a multi-row-group file back on a
connection that has preserve_insertion_order=false returns the rows
scrambled. That is a read-side artifact and says nothing about the file.
Check written order from a fresh connection, which is what
tests/test_build.py does.) TRUE is kept for the final COPY anyway, because
it makes the ordering a documented guarantee rather than an observed
behaviour, and published row order is the one thing this tool exists to get
right. At the scale the workflows run it is free: a 1.3M-row final COPY at
zstd-22 under a 12GB limit took 549.5s either way, peak RSS 3.66GB with the
setting off against 3.67GB with it on. It stops being free when the sort is
starved -- a 5M-row spilling sort that succeeded with the setting off raised
OutOfMemoryException with it on at a 1GB limit, and DuckDB's own OOM hint
suggests turning it off. So if a part ever OOMs in the final COPY, this
setting is the first thing to try turning off, and the sort survives it.

GeoParquet, as measured (2026-09-16, DuckDB 1.5.3 + spatial, gpio 1.3.0,
rashid 0.1.6) -- READ THIS BEFORE PUBLISHING A PART BUILT BY THIS CODE:
DuckDB's COPY writes GeoParquet *1.0.0* -- a `geo` file-metadata key
(primary_column, WKB encoding, geometry_types, bbox) over a plain BYTE_ARRAY
column. It does NOT write the native Parquet GEOMETRY logical type, at any
row count, with or without PARQUET_VERSION V2 and whatever
geometry_minimum_shredding_size suggests; `SET enable_geoparquet_conversion
=false` just drops the `geo` key entirely and leaves a BLOB that gpio cannot
check. `gpio sort column --geoparquet-version 2.0` did write the native type
(geo 2.0.0 + GeometryType(crs=...) in the schema, hence row-group geo
statistics), so this change trades GeoParquet 2.0 for 1.0.0.

What still holds: `gpio check all` passes (exit 0; warnings only, "version
1.0.0 is outdated" and "no bbox column"), DESCRIBE reads the column back as
GEOMETRY('OGC:CRS84'), and make_items.py still finds the bbox it needs on the
`geo` key. What does NOT hold: `rashid check --data` reports two
error-severity findings on a part written this way -- PTL-DAT-012 ("geo
metadata declares '1.0.0'; data must be GeoParquet 1.1 or 2.x") and
PTL-DAT-007 ("no per-row-group spatial statistics: no bbox covering column
with min/max stats, nor native GeospatialStatistics"). A gpio-written part
raises neither. tests/test_conformance.py runs rashid with --no-data, so CI
stays green either way; the published bytes would not be conformant, and
`rashid check catalog/` is the documented pre-publish command.

Measured fix, not yet taken because it changes the published schema: a
`_bbox` STRUCT(xmin, ymin, xmax, ymax) covering column computed in the
staging COPY, plus a hand-written `geo` key (version 1.1.0 + covering) passed
through DuckDB's KV_METADATA COPY option with enable_geoparquet_conversion
off so DuckDB does not also emit its own 1.0.0 key. Prototyped on a 20k-row
fixture: single `geo` key, geometry still reads back as GEOMETRY, order and
zstd-22 intact, `gpio check all` 26/26 spec checks, and both PTL-DAT errors
gone. It costs a published helper column and contradicts the plan's "no bbox
covering column", so it is a decision, not a detail. `gpio add bbox-metadata`
is the other route (it does preserve row order) but it rewrites the file at
its own compression level (+17% on the same fixture) and still leaves the
version at 1.0.0, so it clears neither finding on its own.

The "GeoParquet 2.0" strings in make_items.py (asset title) and
make_collection.py (description) describe the old writer and are wrong for
anything this code writes.
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
# 22 everywhere, live.parquet included: distribution best practices say go as
# high as you have time for, and the user said crank it (2026-09-15). zstd
# decompression cost is flat across levels, so clients pay nothing. This now
# truly reaches the writer: it is a COMPRESSION_LEVEL option on DuckDB's own
# COPY. Under `gpio sort column` it was a no-op (levels 15 and 22 produced
# byte-identical files), so everything built before this change is
# DuckDB-default zstd and gets the real level only when it is rebuilt.
# S2_ZSTD_LEVEL is a test hook (tests/test_build.py builds the same fixture
# at level 1 to prove the level is applied); nothing in CI sets it.
ZSTD_LEVEL = int(os.environ.get("S2_ZSTD_LEVEL", "22"))
WORLD = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"


def say(msg: str) -> None:
    """One timestamped phase line. The 2017 timeout burned six hours with no
    output at all, so every phase reports what it did and how long it took."""
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"  [{stamp}] {msg}", flush=True)


def connect(mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    """One connection for every phase. `mem` is the caller's --memory
    verbatim: the workflows pass 12GB and the GH-hosted runner has 16GB, and
    DuckDB's accounting is known to undershoot real RSS by ~1GB+ on this
    workload (y2017-diagnosis.md phase A measured 13.4GB peak RSS under a
    12GB limit), so a runner change is what moves this number, not a cap
    applied behind the caller's back."""
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
               name: str = "items.parquet") -> int:
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
        # Sort and write in one statement, so no second tool can reorder
        # what this one ordered. preserve_insertion_order goes TRUE for
        # this COPY so the subquery's ORDER BY is a guarantee and not an
        # observed behaviour of one DuckDB version (module docstring has
        # what was measured, and what it costs), and back to FALSE
        # afterwards so the next year's staging scan is not made to
        # preserve an order nobody asked for. COPY lands on
        # `.tmp` and os.replace()s onto `final` -- a same-directory rename,
        # atomic on POSIX and Windows, so a killed build leaves the
        # previous part intact instead of a half-written one that every
        # exists() check downstream would trust.
        tmp = final.with_name(final.name + ".tmp")
        t0 = time.monotonic()
        try:
            con.execute("SET preserve_insertion_order=true;")
            con.execute(f"""
                COPY (
                  SELECT * FROM read_parquet('{staged}')
                  ORDER BY _month, _hilbert
                ) TO '{tmp}'
                  (FORMAT PARQUET, COMPRESSION zstd,
                   COMPRESSION_LEVEL {ZSTD_LEVEL},
                   ROW_GROUP_SIZE {ROW_GROUP})
            """)
            os.replace(tmp, final)
        finally:
            con.execute("SET preserve_insertion_order=false;")
            tmp.unlink(missing_ok=True)
        say(f"year={year}/{name}: sorted (_month, _hilbert) and written "
            f"zstd-{ZSTD_LEVEL}, {final.stat().st_size / 1e6:,.0f} MB, "
            f"{time.monotonic() - t0:,.1f}s")
        # Best-practices gate on the artifact itself: compression, row
        # groups, spatial order, bbox metadata. Fails the build only on
        # gpio's error-level violations (non-zero exit); WARNING-level
        # findings (e.g. omitted geo-metadata CRS, which the GeoParquet
        # spec defaults to OGC:CRS84, and the GeoParquet 1.0.0 version this
        # writer produces) pass and are acceptable.
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
        total += build_year(con, files, y, outdir, a.name)
    print(f"TOTAL {total:,} rows across {len(years)} part(s)")
    if total == 0:
        print("no rows matched: nothing was written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
