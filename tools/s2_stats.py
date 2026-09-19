#!/usr/bin/env python3
"""MGRS tile x month aggregates, and (optionally) the tile-footprint PMTiles.

The stats table is the timeline/choropleth backend for the explorer app and
the cheap first stop for "which month has a cloud-free scene here" -- and,
via mean_cover/max_cover (100 - s2:nodata_pixel_percentage), "how much of
the tile did those scenes actually fill". The
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
  SELECT mgrs_tile, year, month, scene_count, min_cloud_cover,
         median_cloud_cover, best.id AS best_item_id,
         best.dt AS best_item_datetime, mean_cover, max_cover
  FROM (
    SELECT "s2:mgrs_tile" AS mgrs_tile,
           year(datetime)::SMALLINT AS year,
           month(datetime)::TINYINT AS month,
           count(*)::INTEGER AS scene_count,
           min("eo:cloud_cover") AS min_cloud_cover,
           median("eo:cloud_cover") AS median_cloud_cover,
           -- Percent of the tile a scene fills: 100 - s2:nodata_pixel_percentage.
           -- avg/max skip NULLs, so a scene without the property neither
           -- drags the mean down nor caps the max; all-NULL gives NULL.
           {cover_sql}
           -- A single arg_min over a packed struct, not two independent
           -- arg_min calls. Two independent arg_min(id, cc) / arg_min(dt, cc)
           -- calls can each break a cloud-cover tie differently and return
           -- the id of one scene alongside the datetime of another. Packing
           -- them into one value makes the tiebreak a single decision, so
           -- best_item_id and best_item_datetime always describe one scene.
           arg_min(struct_pack(id := id, dt := datetime),
                   "eo:cloud_cover") AS best
    FROM read_parquet([{files}], union_by_name=true)
    {where}
    GROUP BY 1, 2, 3
  )
"""


COVER_COLUMNS = ("mean_cover", "max_cover")
NODATA_COLUMN = "s2:nodata_pixel_percentage"


def _columns(con, relation_sql: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {relation_sql}").fetchall()}


def cover_sql(con, files: str) -> str:
    """The mean_cover / max_cover aggregate expressions for these sources.

    Real item parts always carry s2:nodata_pixel_percentage, but a source
    without it (a minimal fixture, an older export) must still aggregate:
    both columns are then NULL rather than a failed read.
    """
    if NODATA_COLUMN in _columns(con, f"read_parquet([{files}], union_by_name=true)"):
        return (f'avg(100 - "{NODATA_COLUMN}")::DOUBLE AS mean_cover, '
                f'max(100 - "{NODATA_COLUMN}")::DOUBLE AS max_cover,')
    return "NULL::DOUBLE AS mean_cover, NULL::DOUBLE AS max_cover,"


def existing_cover_sql(con, existing: str) -> str:
    """The cover columns of the table being merged into.

    The published stats file predates the coverage columns until the next full
    rebuild; the daily merge must read it without them and leave those rows
    NULL, not fail.
    """
    have = _columns(con, f"read_parquet('{existing}')")
    return ", ".join(f"{c}::DOUBLE AS {c}" if c in have else f"NULL::DOUBLE AS {c}"
                     for c in COVER_COLUMNS)


def build_stats(con, sources, out: Path, merge_years=None, existing=None):
    out.mkdir(parents=True, exist_ok=True)
    files = _sources_sql(sources)
    dest = out / "mgrs-monthly.parquet"
    cover = cover_sql(con, files)
    if merge_years:
        yrs = ",".join(str(y) for y in merge_years)
        con.execute(f"""
          COPY (
            SELECT mgrs_tile::VARCHAR AS mgrs_tile,
                   year::SMALLINT AS year,
                   month::TINYINT AS month,
                   scene_count::INTEGER AS scene_count,
                   min_cloud_cover::DOUBLE AS min_cloud_cover,
                   median_cloud_cover::DOUBLE AS median_cloud_cover,
                   best_item_id::VARCHAR AS best_item_id,
                   best_item_datetime::TIMESTAMPTZ AS best_item_datetime,
                   {existing_cover_sql(con, existing)}
            FROM read_parquet('{existing}')
            WHERE year NOT IN ({yrs})
            UNION ALL BY NAME
            {STATS_SQL.format(files=files, cover_sql=cover,
                              where=f'WHERE year(datetime) IN ({yrs})')}
            ORDER BY mgrs_tile, year, month
          ) TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    else:
        con.execute(f"""
          COPY ({STATS_SQL.format(files=files, cover_sql=cover, where='')}
                ORDER BY mgrs_tile, year, month)
          TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    print(f"  mgrs-monthly.parquet: {n:,} tile-months")


def build_footprint_table(con, sources, dest: Path) -> None:
    """Write the tile-envelope GeoParquet that feeds gpio pmtiles create.

    Split out from build_footprints so the antimeridian exclusion can be
    tested against plain DuckDB output, with no tippecanoe/gpio involved.
    """
    files = _sources_sql(sources)
    con.execute(f"""
      COPY (
        WITH scenes AS (
          SELECT "s2:mgrs_tile" AS mgrs_tile,
                 -- Earth Search geometries can poke slightly past the
                 -- dateline (xmin of -180.6 observed); clamp here so no
                 -- output envelope ever leaks outside [-180, 180].
                 GREATEST(ST_XMin(geometry), -180.0) AS x0,
                 LEAST(ST_XMax(geometry), 180.0) AS x1,
                 ST_YMin(geometry) AS y0,
                 ST_YMax(geometry) AS y1
          FROM read_parquet([{files}], union_by_name=true)
          -- Earth Search reports some dateline scenes with west > east
          -- bboxes (e.g. [179.36, -41.61, -179.85, -40.61]), which makes
          -- bbox[3] - bbox[1] negative -- it slips under a `< 20` filter
          -- meant to catch WIDE antimeridian wraps, and the resulting
          -- geometry (built from that same bbox upstream) really does span
          -- -180..180, blowing the tile's envelope across the whole world.
          -- Test the geometry's actual width instead of trusting the bbox
          -- arithmetic, and belt-and-braces reject any west > east bbox
          -- outright regardless of what the geometry looks like.
          WHERE ST_XMax(geometry) - ST_XMin(geometry) < 20
            AND bbox[3] >= bbox[1]
        ),
        tiles AS (
          SELECT mgrs_tile,
                 MIN(x0) AS x0, MAX(x1) AS x1,
                 MIN(y0) AS y0, MAX(y1) AS y1,
                 -- The per-scene filter above leaves every scene narrow and
                 -- valid, but the ~33 MGRS tiles in UTM zones 1 and 60 that
                 -- sit on the dateline have scenes on BOTH sides of it
                 -- (some at +179.x, some at -179.x), so a plain envelope
                 -- of their union spans the whole world. Flag those tiles
                 -- and compute their extent in a longitude frame shifted so
                 -- the dateline is interior: lon' = lon + 360 for lon < 0,
                 -- giving [sx0, sx1] within (0, 360) with sx1 > sx0.
                 BOOL_OR(x1 > 170) AND BOOL_OR(x0 < -170) AS straddles,
                 MIN(CASE WHEN x0 < 0 THEN x0 + 360 ELSE x0 END) AS sx0,
                 MAX(CASE WHEN x0 < 0 THEN x1 + 360 ELSE x1 END) AS sx1
          FROM scenes
          GROUP BY 1
        )
        SELECT mgrs_tile,
               CASE
                 WHEN NOT straddles THEN ST_MakeEnvelope(x0, y0, x1, y1)
                 -- everything west of the dateline: one box back in [-180, 0)
                 WHEN sx0 >= 180 THEN ST_MakeEnvelope(sx0 - 360, y0, sx1 - 360, y1)
                 -- everything east of it: one box in (0, 180]
                 WHEN sx1 <= 180 THEN ST_MakeEnvelope(sx0, y0, sx1, y1)
                 -- genuinely across it: split at +-180 into two pieces
                 ELSE ST_Union(ST_MakeEnvelope(sx0, y0, 180, y1),
                               ST_MakeEnvelope(-180, y0, sx1 - 360, y1))
               END AS geometry
        FROM tiles
      ) TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
    """)


def pmtiles_command(fp: Path, pmtiles: Path) -> list[str]:
    """The gpio invocation that turns the footprint table into PMTiles.

    A separate function so a test can assert on the command (layer name,
    input/output paths) without requiring tippecanoe to be on PATH.
    """
    return ["gpio", "pmtiles", "create", str(fp), str(pmtiles),
            "--layer", "mgrs"]


def build_footprints(con, sources, out: Path, keep_footprint_table: bool = False):
    fp = (out / "mgrs-tiles.parquet").resolve()
    pmtiles = (out / "mgrs.pmtiles").resolve()
    build_footprint_table(con, sources, fp)
    # gpio drives tippecanoe straight from the GeoParquet, so no GeoJSONSeq
    # detour. Layer name must stay "mgrs" — the app's source-layer. gpio
    # rejects any path containing "..", so both paths above are resolved to
    # absolute before this call (same fix as tools/s2_build.py). gpio's
    # `pmtiles create` has no --force flag; remove a stale output first so a
    # rerun does not fail on an existing file.
    pmtiles.unlink(missing_ok=True)
    subprocess.run(pmtiles_command(fp, pmtiles), check=True)
    # mgrs-tiles.parquet was only ever a pmtiles-build intermediate, so the
    # default stays "delete it" -- current behavior, and nothing new lands
    # in the publish dir uninvited. --keep-footprint-table leaves it so the
    # tileset can be rebuilt/A-B-tested locally without rescanning every
    # published year part; the caller (the publish-stats workflow) is then
    # responsible for moving it out of the publish dir before upload_data.py
    # runs, since that script uploads every .parquet under --data-dir.
    if not keep_footprint_table:
        fp.unlink()
    print(f"  mgrs.pmtiles written ({pmtiles.stat().st_size / 1e6:,.1f} MB)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--footprints", action="store_true")
    ap.add_argument("--merge-years", help="comma list recomputed from sources")
    ap.add_argument("--existing", help="current stats parquet (path or URL)")
    ap.add_argument(
        "--keep-footprint-table", action="store_true",
        help=(
            "leave mgrs-tiles.parquet (the tile-envelope GeoParquet behind "
            "mgrs.pmtiles) in --out instead of deleting it. Default is off "
            "(current behavior: delete). Not published to the bucket -- the "
            "caller must move it out of the publish dir before uploading."
        ),
    )
    a = ap.parse_args()
    con = connect()
    merge = [int(y) for y in a.merge_years.split(",")] if a.merge_years else None
    if merge and not a.existing:
        raise SystemExit("--merge-years requires --existing")
    build_stats(con, a.sources, Path(a.out), merge, a.existing)
    if a.footprints:
        build_footprints(con, a.sources, Path(a.out), a.keep_footprint_table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
