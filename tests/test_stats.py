"""Aggregate a synthetic two-tile fixture and verify counts, medians,
best-item selection (including cloud-cover ties), the percent-cover
columns (mean_cover/max_cover from s2:nodata_pixel_percentage, NULL-safe
and tolerant of sources and merge targets that lack them), the merge path
used by the daily refresh, the antimeridian exclusion from footprint
polygons (and that the same scene is still counted in the stats table), and
the real PMTiles archive gpio/tippecanoe produce.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

from s2_stats import build_footprint_table, build_footprints, connect  # noqa: E402


def _fixture(con, path):
    con.execute(f"""
      COPY (SELECT * FROM (VALUES
        ('A1', TIMESTAMPTZ '2024-05-01 10:00:00+00', '31UFU', 80.0, ST_Point(4, 52)),
        ('A2', TIMESTAMPTZ '2024-05-11 10:00:00+00', '31UFU', 10.0, ST_Point(4, 52)),
        ('A3', TIMESTAMPTZ '2024-05-21 10:00:00+00', '31UFU', 40.0, ST_Point(4, 52)),
        ('B1', TIMESTAMPTZ '2024-06-01 10:00:00+00', '32UMV', 5.0,  ST_Point(9, 51))
      ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover", geometry)
      ) TO '{path}' (FORMAT PARQUET)
    """)


def _run(sources, out):
    subprocess.run([sys.executable, "tools/s2_stats.py",
                    "--sources", *[str(s) for s in sources], "--out", str(out)],
                   check=True, cwd=ROOT)


def test_monthly_stats():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        _fixture(con, src)
        _run([src], out)
        r = con.execute(f"""
            SELECT mgrs_tile, year, month, scene_count, min_cloud_cover,
                   median_cloud_cover, best_item_id
            FROM read_parquet('{out}/mgrs-monthly.parquet')
            ORDER BY mgrs_tile""").fetchall()
        assert r[0] == ("31UFU", 2024, 5, 3, 10.0, 40.0, "A2")
        assert r[1] == ("32UMV", 2024, 6, 1, 5.0, 5.0, "B1")


def test_cloud_cover_tie_is_one_scene():
    """A tied minimum must not let best_item_id and best_item_datetime name
    two different scenes: two independent arg_min() calls could each break
    the tie differently."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('T1', TIMESTAMPTZ '2024-07-01 09:00:00+00', '33ABC', 50.0, ST_Point(10, 20)),
            ('T2', TIMESTAMPTZ '2024-07-15 09:00:00+00', '33ABC', 50.0, ST_Point(10, 20))
          ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover", geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)
        _run([src], out)
        best_id, best_dt = con.execute(f"""
            SELECT best_item_id, best_item_datetime
            FROM read_parquet('{out}/mgrs-monthly.parquet')""").fetchone()
        lookup = dict(con.execute(
            f"SELECT id, datetime FROM read_parquet('{src}')").fetchall())
        assert best_id in lookup
        assert best_dt == lookup[best_id], (
            "best_item_id and best_item_datetime must describe the same "
            "tied scene")


def test_percent_cover_mean_and_max():
    """mean_cover is the average of 100 - s2:nodata_pixel_percentage over
    the tile-month's scenes and max_cover its maximum: nodata 0 / 40 / 100
    gives cover 100 / 60 / 0, so mean 53.33 and max 100. A NULL nodata is
    skipped by both (the second tile: NULL and 25 -> mean 75, max 75), and a
    tile whose scenes all lack it gets NULL, not 0 or 100."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('A1', TIMESTAMPTZ '2024-05-01 10:00:00+00', '31UFU', 80.0, 0.0,  ST_Point(4, 52)),
            ('A2', TIMESTAMPTZ '2024-05-11 10:00:00+00', '31UFU', 10.0, 40.0, ST_Point(4, 52)),
            ('A3', TIMESTAMPTZ '2024-05-21 10:00:00+00', '31UFU', 40.0, 100.0, ST_Point(4, 52)),
            ('B1', TIMESTAMPTZ '2024-06-01 10:00:00+00', '32UMV', 5.0,  NULL, ST_Point(9, 51)),
            ('B2', TIMESTAMPTZ '2024-06-02 10:00:00+00', '32UMV', 7.0,  25.0, ST_Point(9, 51)),
            ('C1', TIMESTAMPTZ '2024-06-03 10:00:00+00', '33UVP', 9.0,  NULL, ST_Point(14, 50))
          ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover",
              "s2:nodata_pixel_percentage", geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)
        _run([src], out)
        rows = {r[0]: r[1:] for r in con.execute(f"""
            SELECT mgrs_tile, scene_count, mean_cover, max_cover
            FROM read_parquet('{out}/mgrs-monthly.parquet')""").fetchall()}
        n, mean, mx = rows["31UFU"]
        assert n == 3
        assert abs(mean - (100 + 60 + 0) / 3) < 1e-9, mean
        assert mx == 100.0
        n, mean, mx = rows["32UMV"]
        assert n == 2, "the NULL-nodata scene is still counted"
        assert mean == 75.0 and mx == 75.0, "NULL nodata is skipped, not zeroed"
        assert rows["33UVP"] == (1, None, None), "all-NULL nodata -> NULL cover"
        types = dict((r[0], r[1]) for r in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{out}/mgrs-monthly.parquet')").fetchall())
        assert types["mean_cover"] == "DOUBLE" and types["max_cover"] == "DOUBLE"


def test_percent_cover_absent_from_source_is_null():
    """A source with no s2:nodata_pixel_percentage column at all (the older
    fixtures in this file) still aggregates, with both cover columns present
    and NULL -- the schema is fixed, the values are not."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        _fixture(con, src)
        _run([src], out)
        rows = con.execute(f"""
            SELECT mgrs_tile, mean_cover, max_cover
            FROM read_parquet('{out}/mgrs-monthly.parquet') ORDER BY 1""").fetchall()
        assert rows == [("31UFU", None, None), ("32UMV", None, None)]


def test_merge_into_existing_without_cover_columns():
    """The daily merge reads the published stats file, which will not have
    the cover columns until the next full rebuild: untouched years come
    through with NULL cover, the recomputed year with real values."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        existing = Path(td) / "old-stats.parquet"
        con.execute(f"""
          COPY (SELECT 'P1' AS best_item_id, '10ABC' AS mgrs_tile,
                       2023::SMALLINT AS year, 3::TINYINT AS month,
                       1::INTEGER AS scene_count, 20.0 AS min_cloud_cover,
                       20.0 AS median_cloud_cover,
                       TIMESTAMPTZ '2023-03-01 00:00:00+00' AS best_item_datetime
          ) TO '{existing}' (FORMAT PARQUET)
        """)
        new_src = Path(td) / "new2024.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('Q2', TIMESTAMPTZ '2024-03-10 00:00:00+00', '20XYZ', 1.0, 30.0, ST_Point(2, 2))
          ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover",
              "s2:nodata_pixel_percentage", geometry)
          ) TO '{new_src}' (FORMAT PARQUET)
        """)
        merged_out = Path(td) / "merged_stats"
        subprocess.run([sys.executable, "tools/s2_stats.py",
                        "--sources", str(new_src), "--out", str(merged_out),
                        "--merge-years", "2024", "--existing", str(existing)],
                       check=True, cwd=ROOT)
        rows = con.execute(f"""
            SELECT mgrs_tile, year, scene_count, mean_cover, max_cover
            FROM read_parquet('{merged_out}/mgrs-monthly.parquet') ORDER BY 1""").fetchall()
        assert rows == [("10ABC", 2023, 1, None, None), ("20XYZ", 2024, 1, 70.0, 70.0)]


def test_merge_replaces_only_named_years():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        base_src = Path(td) / "base.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('P1', TIMESTAMPTZ '2023-03-01 00:00:00+00', '10ABC', 20.0, ST_Point(1, 1)),
            ('Q1', TIMESTAMPTZ '2024-03-01 00:00:00+00', '20XYZ', 30.0, ST_Point(2, 2))
          ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover", geometry)
          ) TO '{base_src}' (FORMAT PARQUET)
        """)
        base_out = Path(td) / "base_stats"
        _run([base_src], base_out)
        existing = base_out / "mgrs-monthly.parquet"
        before = con.execute(
            f"SELECT * FROM read_parquet('{existing}') ORDER BY mgrs_tile"
        ).fetchall()

        # A source holding only new 2024 data -- the daily refresh's shape:
        # recompute the current year from a fresh read, not from the old rows.
        new_src = Path(td) / "new2024.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('Q2', TIMESTAMPTZ '2024-03-10 00:00:00+00', '20XYZ', 1.0, ST_Point(2, 2))
          ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover", geometry)
          ) TO '{new_src}' (FORMAT PARQUET)
        """)
        merged_out = Path(td) / "merged_stats"
        subprocess.run([sys.executable, "tools/s2_stats.py",
                        "--sources", str(new_src), "--out", str(merged_out),
                        "--merge-years", "2024", "--existing", str(existing)],
                       check=True, cwd=ROOT)
        after = con.execute(
            f"SELECT * FROM read_parquet('{merged_out}/mgrs-monthly.parquet') "
            "ORDER BY mgrs_tile"
        ).fetchall()

        assert after[0] == before[0], "the untouched year must survive verbatim"
        assert after[1][:4] == ("20XYZ", 2024, 3, 1), "the named year is recomputed"
        assert after[1][6] == "Q2", "recomputed from the new source, not the old rows"
        assert after[1] != before[1]


def test_antimeridian_excluded_from_footprints_but_counted_in_stats():
    """A scene bbox wider than 20 degrees of longitude (an antimeridian
    wrap, reported as [-180, ..., 180, ...]) must not contribute a polygon
    to the footprint table -- tested directly against DuckDB output, no
    tippecanoe or gpio involved -- but the exclusion is a geometry-rendering
    fix, not a data-quality filter: the same scene must still be counted in
    mgrs-monthly.parquet's scene_count.

    Also covers the real production shape: Earth Search reports some
    dateline scenes with west > east in the bbox itself (e.g.
    [179.36, -41.61, -179.85, -40.61] for tile 60GYV), which makes
    bbox[3] - bbox[1] negative and used to slip under the `< 20` filter
    while the geometry it produced really did span -180..180."""
    con = connect()
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('N1', '31ABC', TIMESTAMPTZ '2024-08-01 00:00:00+00', 10.0,
             [3.9, 51.9, 4.1, 52.1]::DOUBLE[],
             ST_GeomFromText('POLYGON((3.9 51.9,4.1 51.9,4.1 52.1,3.9 52.1,3.9 51.9))')),
            ('W1', '60ZZZ', TIMESTAMPTZ '2024-08-02 00:00:00+00', 20.0,
             [-179.5, 10, 179.5, 11]::DOUBLE[],
             ST_GeomFromText('POLYGON((-179.5 10,179.5 10,179.5 11,-179.5 11,-179.5 10))')),
            ('W2', '60GYV', TIMESTAMPTZ '2024-08-03 00:00:00+00', 5.0,
             [179.36, -41.61, -179.85, -40.61]::DOUBLE[],
             ST_MakeEnvelope(-179.85, -41.61, 179.36, -40.61))
          ) t(id, "s2:mgrs_tile", datetime, "eo:cloud_cover", bbox, geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)

        # Footprints: both antimeridian-wrapping tiles are absent.
        footprints = Path(td) / "mgrs-tiles.parquet"
        build_footprint_table(con, [str(src)], footprints)
        tiles = {r[0] for r in con.execute(
            f"SELECT mgrs_tile FROM read_parquet('{footprints}')").fetchall()}
        assert tiles == {"31ABC"}, (
            "the antimeridian-wrapping tiles must be excluded from "
            "footprints, including the west > east bbox shape")

        # Stats: the same scenes are still counted.
        out = Path(td) / "stats"
        _run([src], out)
        counts = dict(con.execute(
            f"SELECT mgrs_tile, scene_count "
            f"FROM read_parquet('{out}/mgrs-monthly.parquet')").fetchall())
        assert counts.get("60ZZZ") == 1, (
            "the antimeridian scene must still be counted in scene_count "
            "even though it has no footprint polygon")
        assert counts.get("60GYV") == 1, (
            "the west > east antimeridian scene must still be counted too")
        assert counts.get("31ABC") == 1


def test_dateline_straddling_tile_gets_split_envelope():
    """MGRS tiles in UTM zones 1 and 60 have scenes that are each narrow and
    valid (they pass the per-scene width filter) but sit on BOTH sides of
    the dateline -- e.g. tile 1CDK has scenes at lon +179.4 and at -179.9.
    A plain envelope of their union spanned -179.9..+179.9, painting a
    world-wide band across the tileset. Such a tile must instead get a
    two-piece MULTIPOLYGON split at +-180, each piece narrow; a tile whose
    scenes all sit just west of the dateline keeps a single narrow box; a
    scene poking past -180 (Earth Search emits -180.6) is clamped; and
    nothing in the output ever leaves [-180, 180]."""
    con = connect()
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            -- straddler: one scene each side of the dateline
            ('S1', '01CDK', [179.4, -80, 180, -79]::DOUBLE[],
             ST_MakeEnvelope(179.4, -80, 180, -79)),
            ('S2', '01CDK', [-180, -80, -179.6, -79]::DOUBLE[],
             ST_MakeEnvelope(-180, -80, -179.6, -79)),
            -- all-west tile: two narrow scenes at -179.x
            ('W1', '01CDL', [-179.9, -78, -179.5, -77]::DOUBLE[],
             ST_MakeEnvelope(-179.9, -78, -179.5, -77)),
            ('W2', '01CDL', [-179.8, -78, -179.3, -77]::DOUBLE[],
             ST_MakeEnvelope(-179.8, -78, -179.3, -77)),
            -- clamp: a scene reported past -180 on the west side
            ('C1', '01CDM', [-180.6, -76, -179.7, -75]::DOUBLE[],
             ST_MakeEnvelope(-180.6, -76, -179.7, -75)),
            ('C2', '01CDM', [179.5, -76, 180.3, -75]::DOUBLE[],
             ST_MakeEnvelope(179.5, -76, 180.3, -75)),
            -- ordinary narrow tile, far from the dateline
            ('N1', '31ABC', [3.9, 51.9, 4.1, 52.1]::DOUBLE[],
             ST_MakeEnvelope(3.9, 51.9, 4.1, 52.1)),
            -- per-scene wrap (west > east bbox): still excluded outright
            ('X1', '60GYV', [179.36, -41.61, -179.85, -40.61]::DOUBLE[],
             ST_MakeEnvelope(-179.85, -41.61, 179.36, -40.61))
          ) t(id, "s2:mgrs_tile", bbox, geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)
        footprints = Path(td) / "mgrs-tiles.parquet"
        build_footprint_table(con, [str(src)], footprints)

        rows = {r[0]: r[1:] for r in con.execute(f"""
          SELECT mgrs_tile, ST_GeometryType(geometry)::VARCHAR,
                 ST_XMin(geometry), ST_XMax(geometry),
                 ST_YMin(geometry), ST_YMax(geometry)
          FROM read_parquet('{footprints}')
        """).fetchall()}
        assert set(rows) == {"01CDK", "01CDL", "01CDM", "31ABC"}, (
            "the per-scene antimeridian wrap must still be excluded")

        # Straddler: two pieces, overall extent touches both +-180, but
        # neither piece is anywhere near a world-wide band.
        kind, xmin, xmax, ymin, ymax = rows["01CDK"]
        assert kind == "MULTIPOLYGON", kind
        assert xmin == -180 and xmax == 180
        assert (ymin, ymax) == (-80, -79)
        pieces = con.execute(f"""
          SELECT ST_XMin(UNNEST(ST_Dump(geometry)).geom),
                 ST_XMax(UNNEST(ST_Dump(geometry)).geom)
          FROM read_parquet('{footprints}')
          WHERE mgrs_tile = '01CDK'
          ORDER BY 1
        """).fetchall()
        assert len(pieces) == 2, pieces
        for px0, px1 in pieces:
            assert px1 - px0 < 1, f"piece {px0}..{px1} is not narrow"
        assert pieces[0] == (-180, -179.6)
        assert pieces[1] == (179.4, 180)

        # All-west tile: one narrow box within [-180, -179].
        kind, xmin, xmax, _, _ = rows["01CDL"]
        assert kind == "POLYGON", kind
        assert -180 <= xmin < xmax <= -179, (xmin, xmax)
        assert xmax - xmin < 1

        # Clamped straddler: pieces reach exactly +-180 and no further.
        kind, xmin, xmax, _, _ = rows["01CDM"]
        assert kind == "MULTIPOLYGON", kind
        pieces = con.execute(f"""
          SELECT ST_XMin(UNNEST(ST_Dump(geometry)).geom),
                 ST_XMax(UNNEST(ST_Dump(geometry)).geom)
          FROM read_parquet('{footprints}')
          WHERE mgrs_tile = '01CDM'
          ORDER BY 1
        """).fetchall()
        assert pieces == [(-180, -179.7), (179.5, 180)], pieces

        # Ordinary tile: unchanged plain envelope.
        assert rows["31ABC"] == ("POLYGON", 3.9, 4.1, 51.9, 52.1)

        # Nothing in the output leaks past the dateline.
        lo, hi = con.execute(f"""
          SELECT MIN(ST_XMin(geometry)), MAX(ST_XMax(geometry))
          FROM read_parquet('{footprints}')
        """).fetchone()
        assert lo >= -180 and hi <= 180, (lo, hi)


def test_footprints_pmtiles_layer_is_mgrs():
    """End to end: build a tiny fixture through build_footprints() (real
    tippecanoe, via gpio pmtiles create -- no mocking) and confirm the
    resulting archive's vector layer is really named "mgrs" and carries an
    "mgrs_tile" field, the way the app's source-layer/promoteId expect. This
    is the same check the original self-review did ad hoc with the pmtiles
    package, now committed."""
    con = connect()
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('N1', '31ABC',
             [3.9, 51.9, 4.1, 52.1]::DOUBLE[],
             ST_GeomFromText('POLYGON((3.9 51.9,4.1 51.9,4.1 52.1,3.9 52.1,3.9 51.9))')),
            ('N2', '32DEF',
             [8.9, 50.9, 9.1, 51.1]::DOUBLE[],
             ST_GeomFromText('POLYGON((8.9 50.9,9.1 50.9,9.1 51.1,8.9 51.1,8.9 50.9))'))
          ) t(id, "s2:mgrs_tile", bbox, geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)
        out = Path(td) / "stats"
        out.mkdir()
        build_footprints(con, [str(src)], out)
        archive = out / "mgrs.pmtiles"
        assert archive.exists(), "build_footprints must write mgrs.pmtiles"
        assert not (out / "mgrs-tiles.parquet").exists(), (
            "the footprint table is an intermediate: default behavior "
            "(keep_footprint_table=False) must delete it")

        try:
            from pmtiles.reader import MmapSource, Reader
            with open(archive, "rb") as f:
                metadata = Reader(MmapSource(f)).metadata()
        except ImportError:
            result = subprocess.run(
                ["pmtiles", "show", str(archive)],
                capture_output=True, text=True, check=True)
            metadata = json.loads(result.stdout)

        layers = metadata["vector_layers"]
        assert layers[0]["id"] == "mgrs"
        assert "mgrs_tile" in layers[0]["fields"]


def test_keep_footprint_table_flag():
    """--keep-footprint-table (build_footprints(..., keep_footprint_table=True))
    leaves mgrs-tiles.parquet next to mgrs.pmtiles instead of deleting it, so
    the tileset can be rebuilt and A/B-tested locally without rescanning the
    full published archive. Default (False, current behavior) still deletes
    it -- covered by test_footprints_pmtiles_layer_is_mgrs above."""
    con = connect()
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"
        con.execute(f"""
          COPY (SELECT * FROM (VALUES
            ('N1', '31ABC',
             [3.9, 51.9, 4.1, 52.1]::DOUBLE[],
             ST_GeomFromText('POLYGON((3.9 51.9,4.1 51.9,4.1 52.1,3.9 52.1,3.9 51.9))'))
          ) t(id, "s2:mgrs_tile", bbox, geometry)
          ) TO '{src}' (FORMAT PARQUET)
        """)
        out = Path(td) / "stats"
        out.mkdir()
        build_footprints(con, [str(src)], out, keep_footprint_table=True)
        assert (out / "mgrs.pmtiles").exists()
        fp = out / "mgrs-tiles.parquet"
        assert fp.exists(), (
            "--keep-footprint-table must leave mgrs-tiles.parquet in --out")
        tiles = {r[0] for r in con.execute(
            f"SELECT mgrs_tile FROM read_parquet('{fp}')").fetchall()}
        assert tiles == {"31ABC"}
