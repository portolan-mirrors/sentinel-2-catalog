#!/usr/bin/env python3
"""Build the few V7 grid-zone parts a measurement needs, from the laptop.

    python3 tools/rails/experiments/v7_parts_local.py --year 2024 \
        --prefixes 31U,33U,23K --out /tmp/v7 --upload

`build_layout.py` on RAILS builds a whole year's 1,011 parts with `gpio`, which
is the artefact a repartition would actually publish. This builds only the three
parts the four query shapes of docs/c1-search-speed-brief.md read, straight off
the published year part over HTTPS: the grid-zone prefix is a *range* on the
sorted `_tile` column, so DuckDB prunes to a handful of row groups and each part
costs seconds instead of a cluster job. It is the fallback for a RAILS job that
did not land, and it is honest about what it is not: DuckDB writes the parquet,
not `gpio`, so the GeoParquet metadata is DuckDB's and no `gpio check` gated it.
Row count, schema, sort key, compression and row-group size are the same.
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path

BASE = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"
S3 = "s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog"
PREFIX = "_experiments/layout"
ZSTD_LEVEL = 18
ROW_GROUP = 2000


def footer_len(path: Path) -> int:
    with path.open("rb") as fh:
        fh.seek(-8, os.SEEK_END)
        return struct.unpack("<I", fh.read(8)[:4])[0] + 8


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--prefixes", default="31U,33U,23K")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--profile", default="source-coop-uploader")
    a = ap.parse_args()

    import duckdb

    src = f"{BASE}/sentinel-2-c1-l2a/year={a.year}/items.parquet"
    a.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": "8GB"})
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
    made = {}
    for p in a.prefixes.split(","):
        # The exclusive upper bound of the prefix range, so the predicate is a
        # plain comparison DuckDB can push onto the sorted _tile statistics.
        hi = p[:-1] + chr(ord(p[-1]) + 1)
        dst = a.out / f"t={p}.parquet"
        t0 = time.monotonic()
        con.execute(f"""
            COPY (SELECT * FROM read_parquet('{src}')
                  WHERE _tile >= '{p}' AND _tile < '{hi}'
                  ORDER BY _tile, datetime)
            TO '{dst}' (FORMAT PARQUET, COMPRESSION zstd,
                 COMPRESSION_LEVEL {ZSTD_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP});
        """)
        import pyarrow.parquet as pq
        md = pq.ParquetFile(dst).metadata
        made[f"t={p}"] = dict(rows=md.num_rows, bytes=dst.stat().st_size,
                              row_groups=md.num_row_groups,
                              columns=md.num_columns, footer=footer_len(dst),
                              seconds=round(time.monotonic() - t0, 1))
        print(f"  t={p}: {md.num_rows:,} rows, {dst.stat().st_size / 1e6:,.1f} MB, "
              f"{md.num_row_groups} groups, footer {footer_len(dst) / 1024:,.0f} KB, "
              f"{made[f't={p}']['seconds']}s", flush=True)
    (a.out / "layout-local.json").write_text(json.dumps(
        dict(variant="V7", built_by="v7_parts_local.py (DuckDB, not gpio)",
             year=a.year, row_group=ROW_GROUP, zstd_level=ZSTD_LEVEL,
             sort_key="_tile,datetime", parts=made), indent=1))
    if not a.upload:
        return
    for stem in made:
        key = f"{PREFIX}/{a.year}/V7/{stem}.parquet"
        cmd = ["aws", "s3", "cp", str(a.out / f"{stem}.parquet"), f"{S3}/{key}"]
        print("  " + " ".join(cmd), flush=True)
        r = subprocess.run(cmd, env={**os.environ, "AWS_PROFILE": a.profile},
                           capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f"upload failed: {r.stdout}\n{r.stderr}")
    print(f"uploaded {len(made)} part(s) to {S3}/{PREFIX}/{a.year}/V7/")


if __name__ == "__main__":
    main()
