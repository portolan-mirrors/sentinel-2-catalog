"""Aggregate a synthetic two-tile fixture and verify counts, medians,
best-item selection, and the merge path used by the daily refresh."""
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


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


def test_monthly_stats():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        _fixture(con, src)
        subprocess.run([sys.executable, "tools/s2_stats.py",
                        "--sources", str(src), "--out", str(out)],
                       check=True, cwd=ROOT)
        r = con.execute(f"""
            SELECT mgrs_tile, year, month, scene_count, min_cloud_cover,
                   median_cloud_cover, best_item_id
            FROM read_parquet('{out}/mgrs-monthly.parquet')
            ORDER BY mgrs_tile""").fetchall()
        assert r[0] == ("31UFU", 2024, 5, 3, 10.0, 40.0, "A2")
        assert r[1] == ("32UMV", 2024, 6, 1, 5.0, 5.0, "B1")
