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

Three outputs land under --out (Task 24):
  mgrs-monthly.parquet   the full table, sorted by (mgrs_tile, year, month)
                         in 50k-row groups so one tile's history is a range
                         read of one row group and a small footer
  months/YYYY-MM.parquet one slice per month with the five paint columns,
                         sorted by tile, one row group: what the explorer
                         fetches to paint the choropleth (~100-150 KB)
  timeline.parquet       one row per month with tile/scene counts and the
                         clearest tile: the global timeline and the month
                         span, a few KB
Percent columns are rounded to integers (UTINYINT 0-100) and the best item's
timestamp is kept as a DATE: the app never needed more, and it takes the full
table from ~97 MB to ~21 MB.

`--collection` picks the collection (s2_collections; default the first one,
so every existing call is unchanged). The only per-collection fact the
aggregates need is where the MGRS tile lives: `s2:mgrs_tile` for the first
collection, `_tile` for `sentinel-2-c1-l2a` (config.tile_column). The
output column is `mgrs_tile` either way, so the three products and the
PMTiles layer have one shape and the explorer joins them the same way for
both. `--out` stays explicit; by convention it is
`staging/publish/<config.stats_dir>` (`stats`, `stats-c1`), which is what
the workflows stage and make_stats_collection reads.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import s2_collections as cols  # noqa: E402
from s2_collections import CollectionConfig  # noqa: E402

DEFAULT_CONFIG = cols.get(cols.DEFAULT)


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


# The published column order. Every writer below selects exactly this list
# so the full table, the daily merge and the month slices never disagree.
STATS_COLUMNS = ("mgrs_tile", "year", "month", "scene_count",
                 "min_cloud_cover", "median_cloud_cover",
                 "mean_cover", "max_cover", "best_item_id", "best_item_date")
PERCENT_COLUMNS = ("min_cloud_cover", "median_cloud_cover",
                   "mean_cover", "max_cover")
MONTH_COLUMNS = ("mgrs_tile", "scene_count") + PERCENT_COLUMNS
COVER_COLUMNS = ("mean_cover", "max_cover")
NODATA_COLUMN = "s2:nodata_pixel_percentage"

# Measured on the live 3.1M-row table: zstd 12 gives 24.2 MB, zstd 18 gives
# 21.2 MB (DuckDB's own default is level 3), and the level-18 write is
# seconds. The app range-reads the full table for one tile's history, so the
# row group is the trade between footer size and column-chunk size: 50k rows
# (DuckDB writes 51,200) is 6 requests / ~150 KB per tile against
# 6 / ~310 KB at 10k, where a 266 KB footer dominated.
FULL_TABLE_OPTS = "FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 18, ROW_GROUP_SIZE 50000"
# A month slice is one row per tile (< 40k rows): one row group, so the app
# reads it in one request with no footer round trip worth optimising.
SLICE_OPTS = "FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 18, ROW_GROUP_SIZE 1000000"


def tile_sql(config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The collection's tile column as a quoted identifier: "s2:mgrs_tile"
    for the first collection, "_tile" for Collection 1. Both STATS_SQL and
    the footprint table select it AS mgrs_tile, the one published name."""
    return '"' + config.tile_column.replace('"', '""') + '"'


def _pct(expr: str) -> str:
    """A 0-100 percent as an integer; NULL stays NULL.

    Valid eo:cloud_cover and s2:nodata_pixel_percentage are 0-100, so the
    clamp changes nothing for real data; it keeps one bad value (-0.5 would
    fail the UTINYINT cast, 100.5 would store 101) from aborting the build.
    DuckDB's least()/greatest() skip NULLs (greatest(0, NULL) is 0), so the
    NULL case is handled first, or an all-NULL cover would read as 0 %.
    """
    return (f"CASE WHEN {expr} IS NULL THEN NULL "
            f"ELSE least(100, greatest(0, round({expr}))) END::UTINYINT")


STATS_SQL = """
  SELECT mgrs_tile, year, month, scene_count,
         {min_pct} AS min_cloud_cover,
         {median_pct} AS median_cloud_cover,
         {mean_pct} AS mean_cover,
         {max_pct} AS max_cover,
         best.id AS best_item_id,
         -- The best item's UTC calendar date. The session zone is UTC (see
         -- connect()), so the cast buckets the same way year()/month() do.
         (best.dt AT TIME ZONE 'UTC')::DATE AS best_item_date
  FROM (
    SELECT {tile} AS mgrs_tile,
           year(datetime)::SMALLINT AS year,
           month(datetime)::TINYINT AS month,
           count(*)::USMALLINT AS scene_count,
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
           -- best_item_id and best_item_date always describe one scene.
           arg_min(struct_pack(id := id, dt := datetime),
                   "eo:cloud_cover") AS best
    FROM read_parquet([{files}], union_by_name=true)
    {where}
    GROUP BY 1, 2, 3
  )
""".replace("{min_pct}", _pct("min_cloud_cover")) \
   .replace("{median_pct}", _pct("median_cloud_cover")) \
   .replace("{mean_pct}", _pct("mean_cover")) \
   .replace("{max_pct}", _pct("max_cover"))


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


def existing_sql(con, existing: str) -> str:
    """A SELECT that reads the table being merged into as the current schema.

    --existing can be the published file in the OLD schema (DOUBLE percents,
    INTEGER scene_count, best_item_datetime TIMESTAMPTZ, possibly without the
    cover columns) or a file this tool already wrote in the new one. The
    columns are detected, and each is cast to what STATS_COLUMNS expects; a
    cover column the old file lacks comes through as NULL, not a failed read.
    """
    have = _columns(con, f"read_parquet('{existing}')")
    cols = ["mgrs_tile::VARCHAR AS mgrs_tile",
            "year::SMALLINT AS year",
            "month::TINYINT AS month",
            "scene_count::USMALLINT AS scene_count"]
    for c in PERCENT_COLUMNS:
        cols.append(f"{_pct(c) if c in have else 'NULL::UTINYINT'} AS {c}")
    cols.append("best_item_id::VARCHAR AS best_item_id")
    if "best_item_date" in have:
        cols.append("best_item_date::DATE AS best_item_date")
    else:
        cols.append("(best_item_datetime AT TIME ZONE 'UTC')::DATE AS best_item_date")
    return f"SELECT {', '.join(cols)} FROM read_parquet('{existing}')"


def _month_name(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def build_stats(con, sources, out: Path, merge_years=None, existing=None,
                config: CollectionConfig = DEFAULT_CONFIG):
    out.mkdir(parents=True, exist_ok=True)
    files = _sources_sql(sources)
    cover = cover_sql(con, files)
    tile = tile_sql(config)
    columns = ", ".join(STATS_COLUMNS)
    if merge_years:
        yrs = ",".join(str(y) for y in merge_years)
        table_sql = f"""
          SELECT {columns} FROM ({existing_sql(con, existing)}) WHERE year NOT IN ({yrs})
          UNION ALL BY NAME
          SELECT {columns} FROM ({STATS_SQL.format(
              tile=tile, files=files, cover_sql=cover,
              where=f'WHERE year(datetime) IN ({yrs})')})
        """
    else:
        fresh = STATS_SQL.format(tile=tile, files=files, cover_sql=cover, where="")
        table_sql = f"SELECT {columns} FROM ({fresh})"
    # One in-memory copy feeds all three outputs, so the month slices and the
    # timeline are cut from exactly the table that was written, not from a
    # second aggregation that could round or bucket differently.
    con.execute(f"CREATE OR REPLACE TABLE stats AS SELECT {columns} FROM ({table_sql}) "
                "ORDER BY mgrs_tile, year, month")

    dest = out / "mgrs-monthly.parquet"
    con.execute(f"""
      COPY (SELECT {columns} FROM stats ORDER BY mgrs_tile, year, month)
      TO '{dest}' ({FULL_TABLE_OPTS})
    """)
    n = con.execute("SELECT count(*) FROM stats").fetchone()[0]
    print(f"  mgrs-monthly.parquet: {n:,} tile-months ({dest.stat().st_size / 1e6:,.1f} MB)")

    build_month_slices(con, out / "months")
    build_timeline(con, out / "timeline.parquet")


def build_month_slices(con, months_dir: Path) -> list[str]:
    """Write months/YYYY-MM.parquet for every month in the `stats` table.

    Every month is rewritten on every run (a slice is ~100 KB; the loop is
    seconds) and any months/*.parquet that no longer matches a month in the
    table is deleted, so the directory never advertises a month the table
    does not have. DuckDB's PARTITION_BY writes a hive layout
    (year=2024/month=5/data_0.parquet), not this flat naming, so this is a
    loop over the months.
    """
    months_dir.mkdir(parents=True, exist_ok=True)
    months = con.execute(
        "SELECT DISTINCT year, month FROM stats ORDER BY 1, 2").fetchall()
    cols = ", ".join(MONTH_COLUMNS)
    written = []
    for year, month in months:
        name = _month_name(year, month)
        con.execute(f"""
          COPY (SELECT {cols} FROM stats
                WHERE year = {year} AND month = {month}
                ORDER BY mgrs_tile)
          TO '{months_dir / name}.parquet' ({SLICE_OPTS})
        """)
        written.append(name)
    keep = {f"{name}.parquet" for name in written}
    stale = [p for p in months_dir.glob("*.parquet") if p.name not in keep]
    for p in stale:
        p.unlink()
    print(f"  months/: {len(written)} month slices written"
          + (f", {len(stale)} stale removed" if stale else ""))
    return written


def build_timeline(con, dest: Path) -> None:
    """One row per month over every tile: the explorer's global timeline and
    the source of its month span (the newest month is max(year, month))."""
    con.execute(f"""
      COPY (SELECT year, month,
                   count(*)::INTEGER AS tile_count,
                   sum(scene_count)::INTEGER AS scene_count,
                   min(min_cloud_cover)::UTINYINT AS min_cloud_cover
            FROM stats GROUP BY 1, 2 ORDER BY 1, 2)
      TO '{dest}' ({SLICE_OPTS})
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    print(f"  timeline.parquet: {n} months")


def build_footprint_table(con, sources, dest: Path,
                          config: CollectionConfig = DEFAULT_CONFIG) -> None:
    """Write the tile-envelope GeoParquet that feeds gpio pmtiles create.

    Split out from build_footprints so the antimeridian exclusion can be
    tested against plain DuckDB output, with no tippecanoe/gpio involved.
    """
    files = _sources_sql(sources)
    con.execute(f"""
      COPY (
        WITH scenes AS (
          SELECT {tile_sql(config)} AS mgrs_tile,
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


def build_footprints(con, sources, out: Path, keep_footprint_table: bool = False,
                     config: CollectionConfig = DEFAULT_CONFIG):
    fp = (out / "mgrs-tiles.parquet").resolve()
    pmtiles = (out / "mgrs.pmtiles").resolve()
    build_footprint_table(con, sources, fp, config)
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
    ap.add_argument("--out", required=True,
                    help="output directory; by convention "
                         "staging/publish/<stats_dir> of the collection "
                         f"({', '.join(cols.get(n).stats_dir for n in cols.NAMES)})")
    cols.add_collection_arg(ap)
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
    config = cols.get(a.collection)
    con = connect()
    merge = [int(y) for y in a.merge_years.split(",")] if a.merge_years else None
    if merge and not a.existing:
        raise SystemExit("--merge-years requires --existing")
    build_stats(con, a.sources, Path(a.out), merge, a.existing, config)
    if a.footprints:
        build_footprints(con, a.sources, Path(a.out), a.keep_footprint_table, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
