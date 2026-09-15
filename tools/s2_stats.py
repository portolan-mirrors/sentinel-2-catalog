#!/usr/bin/env python3
"""MGRS tile x month aggregates, and (optionally) the tile-footprint PMTiles.

The stats table is the timeline/choropleth backend for the explorer app and
the cheap first stop for "which month has a cloud-free scene here". The
PMTiles carries geometry only (envelope of each tile's item bboxes); the app
joins stats to tiles by mgrs_tile, so daily stat refreshes never touch the
tileset. Scenes whose bbox spans >20 degrees of longitude (antimeridian
wraps) are excluded from footprint aggregation only.

--merge-years: the daily refresh recomputes only the current year and
splices it into the existing table instead of scanning ten years of parquet.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Project-wide pitfall: DuckDB's default session timezone is the host's
    # local zone, not UTC. Every query here buckets by year(datetime) and
    # month(datetime), so an unset timezone would shift rows near local
    # midnight into the wrong month for anyone not already running in UTC.
    con.execute("SET TimeZone='UTC';")
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def _sources_sql(sources: list[str]) -> str:
    files = []
    for s in sources:
        p = Path(s)
        if p.is_dir():
            files += [str(f) for f in sorted(p.rglob("*.parquet"))]
        elif "://" in s:
            files.append(s)          # URL
        else:
            files.append(str(p.resolve()))  # local file: resolve to absolute
    return ",".join(f"'{f}'" for f in files)


STATS_SQL = """
  SELECT "s2:mgrs_tile" AS mgrs_tile,
         year(datetime)::SMALLINT AS year,
         month(datetime)::TINYINT AS month,
         count(*)::INTEGER AS scene_count,
         min("eo:cloud_cover") AS min_cloud_cover,
         median("eo:cloud_cover") AS median_cloud_cover,
         arg_min(id, "eo:cloud_cover") AS best_item_id,
         arg_min(datetime, "eo:cloud_cover") AS best_item_datetime
  FROM read_parquet([{files}], union_by_name=true)
  {where}
  GROUP BY 1, 2, 3
"""


def build_stats(con, sources, out: Path, merge_years=None, existing=None):
    out.mkdir(parents=True, exist_ok=True)
    files = _sources_sql(sources)
    dest = out / "mgrs-monthly.parquet"
    if merge_years:
        yrs = ",".join(str(y) for y in merge_years)
        con.execute(f"""
          COPY (
            SELECT * FROM read_parquet('{existing}')
            WHERE year NOT IN ({yrs})
            UNION ALL BY NAME
            {STATS_SQL.format(files=files,
                              where=f'WHERE year(datetime) IN ({yrs})')}
            ORDER BY mgrs_tile, year, month
          ) TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    else:
        con.execute(f"""
          COPY ({STATS_SQL.format(files=files, where='')}
                ORDER BY mgrs_tile, year, month)
          TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    print(f"  mgrs-monthly.parquet: {n:,} tile-months")


def build_footprints(con, sources, out: Path):
    files = _sources_sql(sources)
    fp = (out / "mgrs-tiles.parquet").resolve()
    pmtiles = (out / "mgrs.pmtiles").resolve()
    con.execute(f"""
      COPY (
        SELECT "s2:mgrs_tile" AS mgrs_tile,
               ST_Envelope(ST_Extent_Agg(geometry)) AS geometry
        FROM read_parquet([{files}], union_by_name=true)
        WHERE bbox[3] - bbox[1] < 20
        GROUP BY 1
      ) TO '{fp}' (FORMAT PARQUET, COMPRESSION zstd)
    """)
    # gpio drives tippecanoe straight from the GeoParquet, so no GeoJSONSeq
    # detour. Layer name must stay "mgrs" — the app's source-layer. gpio
    # rejects any path containing "..", so both paths above are resolved to
    # absolute before this call (same fix as tools/s2_build.py). gpio's
    # `pmtiles create` has no --force flag; remove a stale output first so a
    # rerun does not fail on an existing file.
    pmtiles.unlink(missing_ok=True)
    subprocess.run(
        ["gpio", "pmtiles", "create", str(fp), str(pmtiles),
         "--layer", "mgrs"],
        check=True)
    fp.unlink()
    print(f"  mgrs.pmtiles written ({pmtiles.stat().st_size / 1e6:,.1f} MB)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--footprints", action="store_true")
    ap.add_argument("--merge-years", help="comma list recomputed from sources")
    ap.add_argument("--existing", help="current stats parquet (path or URL)")
    a = ap.parse_args()
    con = connect()
    merge = [int(y) for y in a.merge_years.split(",")] if a.merge_years else None
    if merge and not a.existing:
        raise SystemExit("--merge-years requires --existing")
    build_stats(con, a.sources, Path(a.out), merge, a.existing)
    if a.footprints:
        build_footprints(con, a.sources, Path(a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
