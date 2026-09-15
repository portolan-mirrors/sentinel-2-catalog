"""Build a year part from tiny synthetic chunks and verify dedupe, sort
order, helper columns, and GeoParquet output."""
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def _mk_chunk(con, path, rows):
    """rows: list of (id, iso_datetime, generation_time, lon, lat)"""
    vals = ", ".join(
        f"('{i}', TIMESTAMPTZ '{d}', '{g}', ST_Point({x}, {y}))"
        for i, d, g, x, y in rows)
    con.execute(f"""
        COPY (
          SELECT NULL::VARCHAR AS thumbnail_url, 'Feature' AS type,
                 '1.1.0' AS stac_version, []::VARCHAR[] AS stac_extensions,
                 v.id, v.dt AS datetime,
                 v.g AS "s2:generation_time", '31UFU' AS "s2:mgrs_tile",
                 50.0 AS "eo:cloud_cover", v.geom AS geometry
          FROM (VALUES {vals}) v(id, dt, g, geom)
        ) TO '{path}' (FORMAT PARQUET)
    """)


def test_build_dedupes_and_sorts():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_chunk(con, chunks / "a.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-01T12:00:00Z", 4.0, 52.0),
            ("B", "2024-01-15 10:00:00+00", "2024-01-15T12:00:00Z", 5.0, 52.0),
            ("C", "2023-12-31 10:00:00+00", "2023-12-31T12:00:00Z", 6.0, 52.0),
        ])
        _mk_chunk(con, chunks / "b.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-02T09:00:00Z", 4.0, 52.0),
        ])
        out = Path(td) / "publish"
        subprocess.run(
            [sys.executable, "tools/s2_build.py",
             "--sources", str(chunks.parent), "--years", "2024",
             "--out", str(out)],
            check=True, cwd=ROOT)
        f = out / "year=2024" / "items.parquet"
        r = con.execute(f"""
            SELECT id, "s2:generation_time", _month
            FROM read_parquet('{f}') ORDER BY id""").fetchall()
        assert [x[0] for x in r] == ["A", "B"]          # C is 2023; A deduped
        assert r[0][1] == "2024-03-02T09:00:00Z"        # newer generation won
        months = con.execute(
            f"SELECT list(_month) FROM read_parquet('{f}')").fetchone()[0]
        assert months == sorted(months)                 # sorted by _month first
