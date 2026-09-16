"""Build a year part from tiny synthetic chunks and verify dedupe, sort
order, helper columns, and GeoParquet output."""
import os
import shutil
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


def test_build_handles_dotdot_in_paths():
    """gpio 1.3.0 rejects a path whose normalized form still starts with
    '..' ("directory traversal detected") -- exactly the shape of the
    project's `../s2-staging/...` staging convention when --out/--sources
    are passed as relative paths. s2_build.py must resolve --sources and
    --out to absolute paths before any gpio subprocess call so a
    leading-'..' relative path (relative to the invocation cwd) still
    builds successfully."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_chunk(con, chunks / "a.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-01T12:00:00Z", 4.0, 52.0),
        ])
        out = Path(td) / "publish"
        # Relative to ROOT (the subprocess cwd below), so os.path.normpath
        # leaves a leading ".." that gpio's own traversal check inspects --
        # unlike an absolute path with an embedded ".." segment, which
        # normpath collapses away before gpio ever sees it.
        rel_sources = os.path.relpath(chunks.parent, ROOT)
        rel_out = os.path.relpath(out, ROOT)
        assert rel_sources.startswith("..") and rel_out.startswith("..")
        subprocess.run(
            [sys.executable, "tools/s2_build.py",
             "--sources", rel_sources, "--years", "2024",
             "--out", rel_out],
            check=True, cwd=ROOT)
        f = out / "year=2024" / "items.parquet"
        r = con.execute(
            f"SELECT id FROM read_parquet('{f}')").fetchall()
        assert [x[0] for x in r] == ["A"]


# The two fixtures above are three rows each: one row group, one batch. The
# year parts this tool publishes are 1.3M rows, so the ordering gate below
# uses a fixture past the 100k row-group line and checks the file as written.
# Read such a file back on a FRESH connection: a connection with
# preserve_insertion_order=false (which s2_build's own connection has) hands
# back the rows of a multi-row-group file scrambled, and that read-side
# artifact looks exactly like an unsorted write.
BIG_ROWS = 150_000
# Levels for the compression gate. Never the published level (18) here:
# benchmarked single-threaded on real staged rows, 18 costs 164.8s per 50k
# rows and 22 costs 662s, against 5.0s at 15 (s2_build.py's ZSTD_LEVEL
# comment). A CI gate that took minutes to prove a flag is wired would not
# survive. 1-vs-15 proves the same thing in under a second.
LEVEL_LOW, LEVEL_HIGH = 1, 15
SMALL_ROWS = 50_000


def _mk_big_chunk(con, path, rows=BIG_ROWS):
    """One chunk of `rows` synthetic scenes: dates spread over a year (so
    _month spans 1-12), footprints scattered over the globe by a pair of
    coprime strides (so _hilbert is well mixed and an unsorted write is
    obvious), and a realistic repetitive `assets` JSON string, which is what
    gives zstd something to compress differently at one level than another."""
    con.execute(f"""
        COPY (
          SELECT NULL::VARCHAR AS thumbnail_url, 'Feature' AS type,
                 '1.1.0' AS stac_version, []::VARCHAR[] AS stac_extensions,
                 'S2A_' || i AS id,
                 TIMESTAMPTZ '2024-01-01 00:00:00+00'
                   + INTERVAL (i % 365) DAY AS datetime,
                 '2024-01-01T12:00:00Z' AS "s2:generation_time",
                 '31UFU' AS "s2:mgrs_tile",
                 (i % 100)::DOUBLE AS "eo:cloud_cover",
                 '{{"visual":{{"href":"https://sentinel-cogs.s3.us-west-2.'
                 || 'amazonaws.com/sentinel-s2-l2a-cogs/31/U/FU/2024/1/S2A_'
                 || i || '/TCI.tif","type":"image/tiff; application=geotiff;'
                 || ' profile=cloud-optimized"}}}}' AS assets,
                 ST_Point(((i * 7919) % 36000) / 100.0 - 180,
                          ((i * 104729) % 17000) / 100.0 - 85) AS geometry
          FROM range({rows}) t(i)
        ) TO '{path}' (FORMAT PARQUET, COMPRESSION zstd)
    """)


def _build(out, chunks, level, env=None):
    """Run the CLI the way the workflows do, with zstd pinned through the
    S2_ZSTD_LEVEL test hook. Every test passes a level: the published default
    is 18, which no test can afford to wait for."""
    env = dict(env or os.environ, S2_ZSTD_LEVEL=str(level))
    return subprocess.run(
        [sys.executable, "tools/s2_build.py", "--sources", str(chunks.parent),
         "--years", "2024", "--out", str(out)],
        cwd=ROOT, env=env, capture_output=True, text=True)


def test_build_keeps_the_sort_past_one_row_group():
    """The gate on the year-part write path: 150k rows, more than one 100k
    row group, must come back globally ordered by (_month, _hilbert), and the
    part must be the only file in the year directory -- gpio writes
    `items.parquet.tmp` and os.replace() renames it, so a leftover .tmp means
    the atomic write broke."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_big_chunk(con, chunks / "a.parquet")
        out = Path(td) / "publish"
        proc = _build(out, chunks, LEVEL_LOW)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        f = out / "year=2024" / "items.parquet"
        keys = con.execute(
            f"SELECT _month, _hilbert FROM read_parquet('{f}')").fetchall()
        assert len(keys) == BIG_ROWS
        assert keys == sorted(keys)
        assert len({k[0] for k in keys}) == 12       # months really do vary
        assert [p.name for p in (out / "year=2024").iterdir()] == \
            ["items.parquet"]


def test_zstd_level_reaches_the_written_file():
    """geoparquet-io 1.3.0 dropped --compression-level on this write path
    (levels 15 and 22 wrote byte-identical files); 1.4.0 fixed it and the
    workflows pin 1.5.0. This asserts the flag is really wired in whatever
    gpio is installed here: the same fixture at level 15 must be smaller than
    at level 1. Sizes, not metadata -- Parquet records the codec, never the
    level."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_big_chunk(con, chunks / "a.parquet", rows=SMALL_ROWS)
        sizes = {}
        for level in (LEVEL_LOW, LEVEL_HIGH):
            out = Path(td) / f"publish{level}"
            proc = _build(out, chunks, level)
            assert proc.returncode == 0, proc.stdout + proc.stderr
            part = out / "year=2024" / "items.parquet"
            sizes[level] = part.stat().st_size
            assert con.execute(
                "SELECT DISTINCT compression FROM parquet_metadata(?) "
                "WHERE path_in_schema = 'geometry'", [str(part)],
            ).fetchall() == [("ZSTD",)]
        assert sizes[LEVEL_HIGH] < sizes[LEVEL_LOW], sizes


def test_gpio_check_still_gates_the_build():
    """`gpio check all` is the gate on the artifact, and a failing check has
    to fail the build. The shim passes `gpio sort` through to the real
    binary and fails only on `gpio check`, standing in for a check that finds
    an error-level violation."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    real_gpio = shutil.which("gpio")
    assert real_gpio, "gpio is not installed, so this gate checks nothing"
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_chunk(con, chunks / "a.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-01T12:00:00Z", 4.0, 52.0),
        ])
        shim_dir = Path(td) / "bin"
        shim_dir.mkdir()
        shim = shim_dir / "gpio"
        shim.write_text(
            "#!/bin/sh\n"
            'if [ "$1" = "check" ]; then\n'
            "  echo 'ERROR: invented violation' >&2\n"
            "  exit 1\n"
            "fi\n"
            f'exec "{real_gpio}" "$@"\n')
        shim.chmod(0o755)
        out = Path(td) / "publish"
        env = dict(os.environ,
                   PATH=f"{shim_dir}{os.pathsep}{os.environ['PATH']}")
        proc = _build(out, chunks, LEVEL_LOW, env=env)
        assert proc.returncode != 0
        assert "gpio check failed" in proc.stderr
