#!/usr/bin/env python3
"""Compact chunks (and/or the seed archive) into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)

Rows are deduped by id keeping the highest s2:generation_time, then sorted
(_month, _hilbert): month-first keeps month pruning inside a year file,
Hilbert-within-month keeps row-group bboxes tight for spatial pruning. The
two helper columns are published and documented in the collection AGENTS.md.

gpio does the ordered GeoParquet 2.0 write. Do NOT sort in DuckDB and
`gpio convert`: convert does not preserve row order (see firms-catalog).
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_schema import COLUMNS

ROW_GROUP = 100_000
# 22 everywhere, live.parquet included: distribution best practices say go as
# high as you have time for, and the user said crank it (2026-09-15). zstd
# decompression cost is flat across levels, so clients pay nothing.
ZSTD_LEVEL = 22
WORLD = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"


def connect(mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
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
        if n == 0:
            if not any(dest.iterdir()):
                shutil.rmtree(dest, ignore_errors=True)
            return 0
        final.unlink(missing_ok=True)
        r = subprocess.run(
            ["gpio", "sort", "column", str(staged), str(final),
             "_month,_hilbert", "--geoparquet-version", "2.0",
             "--compression", "zstd", "--compression-level", str(ZSTD_LEVEL),
             "--row-group-size", str(ROW_GROUP)],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-1500:], r.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio sort failed for {year}")
        # Best-practices gate on the artifact itself: compression, row
        # groups, spatial order, bbox metadata. Fails the build only on
        # gpio's error-level violations (non-zero exit); WARNING-level
        # findings (e.g. omitted geo-metadata CRS, which the GeoParquet
        # spec defaults to OGC:CRS84) pass and are acceptable.
        chk = subprocess.run(["gpio", "check", "all", str(final)],
                             capture_output=True, text=True)
        if chk.returncode != 0:
            print(chk.stdout[-1500:], chk.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio check failed for {year}")
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
