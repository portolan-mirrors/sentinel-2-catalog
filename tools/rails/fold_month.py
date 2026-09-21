#!/usr/bin/env python3
"""Fold the day chunks of one month into one month slice.

    python3 tools/rails/fold_month.py $SLICES/2019-03 $SLICES/2019-03.parquet
    python3 tools/rails/fold_month.py $SLICES/2019-03 $SLICES/2019-03.parquet --subdirs api repair

s2_fetch.py writes its day chunks under <month dir>/api/ and s2_repair.py
under <month dir>/repair/. The slice is every non-empty chunk of the named
subdirectories, sorted by datetime, at zstd 3 in 100,000-row groups: a
staging file the year build reads once, not a published part. A zero-row
day is an empty sentinel file that DuckDB must not read, so it is skipped.
A month with no rows at all becomes an empty sentinel slice, which tells
build_year.sbatch the month was fetched and has nothing.

Repair rows are unioned with API rows here; s2_build.py dedupes by id
keeping the highest s2:generation_time, so a scene both lanes fetched is
published once.

The slice is written to <dest>.tmp and renamed, so a killed fold leaves
the previous slice in place.
"""
from __future__ import annotations

import argparse
import glob
import os


def chunk_files(month_dir: str, subdirs: list[str]) -> list[str]:
    files = []
    for sub in subdirs:
        files += sorted(glob.glob(os.path.join(month_dir, sub, "*.parquet")))
    return [f for f in files if os.path.getsize(f) > 0]


def fold(month_dir: str, dest: str, subdirs: list[str]) -> int:
    """Write the slice; return its row count (0 for the empty sentinel)."""
    files = chunk_files(month_dir, subdirs)
    if not files:
        open(dest, "wb").close()
        print(f"{month_dir}: no rows; wrote the empty sentinel {dest}")
        return 0
    import duckdb
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    tmp = dest + ".tmp"
    con.execute(f"""
        COPY (SELECT * FROM read_parquet({files!r}, union_by_name=true)
              ORDER BY datetime)
        TO '{tmp}' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 3,
                    ROW_GROUP_SIZE 100000)""")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    os.replace(tmp, dest)
    print(f"{dest}: {n:,} rows from {len(files)} day chunk(s) in "
          f"{', '.join(subdirs)}")
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("month_dir", help="the month directory holding api/ (and repair/)")
    ap.add_argument("dest", help="the slice to write, $SLICES/YYYY-MM.parquet")
    ap.add_argument("--subdirs", nargs="+", default=["api"],
                    help="which chunk directories to fold (default: api)")
    a = ap.parse_args(argv)
    fold(a.month_dir, a.dest, a.subdirs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
