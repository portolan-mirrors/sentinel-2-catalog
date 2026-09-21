"""Build a year part from tiny synthetic chunks and verify dedupe, sort
order, helper columns, and GeoParquet output."""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pytest

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


def _mk_big_chunk(con, path, rows=BIG_ROWS, year=2024, tiles=("31UFU",)):
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
                 TIMESTAMPTZ '{year}-01-01 00:00:00+00'
                   + INTERVAL (i % 365) DAY AS datetime,
                 '{year}-01-01T12:00:00Z' AS "s2:generation_time",
                 list_value({", ".join(repr(t) for t in tiles)})[1 + i % {len(tiles)}] AS "s2:mgrs_tile",
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


def _build(out, chunks, level, env=None, years="2024"):
    """Run the CLI the way the workflows do, with zstd pinned through the
    S2_ZSTD_LEVEL test hook. Every test passes a level: the published default
    is 18, which no test can afford to wait for."""
    env = dict(env or os.environ, S2_ZSTD_LEVEL=str(level))
    return subprocess.run(
        [sys.executable, "tools/s2_build.py", "--sources", str(chunks.parent),
         "--years", years, "--out", str(out)],
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
        # The part that failed its check never reached its final name, and
        # the temporary it was checked under is gone too.
        assert not (out / "year=2024" / "items.parquet").exists()
        assert list((out / "year=2024").iterdir()) == []


# ---------------------------------------------------------------------------
# --split zones (spec Amendment 3): a year lands as UTM-zone parts.
# ---------------------------------------------------------------------------
# The fixture years are chosen by tier: 2020 is a quartile year (ZONE_PARTS),
# 2021 the first octant year (ZONE_PARTS_8), and a year before ZONE_SPLIT_FROM
# has no parts at all.
sys.path.insert(0, str(ROOT / "tools"))
from s2_build import (  # noqa: E402
    ZONE_PARTS, ZONE_PARTS_8, ZONE_SPLIT_8_FROM, ZONE_SPLIT_FROM,
    archive_part_names, zone_parts_for,
)

ZONE_LABELS = [label for label, _, _ in ZONE_PARTS]
OCTANT_LABELS = [label for label, _, _ in ZONE_PARTS_8]


def _mk_zone_chunk(con, path, zones, per_zone=3, year=2020, prefix=""):
    """`per_zone` scenes in each UTM zone of `zones`, spread over months and
    longitudes so a part's (_month, _hilbert) order is checkable. Tile ids
    take the upstream shape: no zero padding ('1VCJ', '31UFU')."""
    rows = []
    for z in zones:
        for i in range(per_zone):
            rows.append(
                f"('{prefix}S2A_{z}_{i}', "
                f"TIMESTAMPTZ '{year}-{(z + i) % 12 + 1:02d}-10 10:00:00+00', "
                f"'{year}-01-01T12:00:00Z', '{z}UFU', "
                f"ST_Point({(z * 6 - 183 + i) % 180}, {(z * 3 + i) % 80 - 40}))")
    con.execute(f"""
        COPY (
          SELECT NULL::VARCHAR AS thumbnail_url, 'Feature' AS type,
                 '1.1.0' AS stac_version, []::VARCHAR[] AS stac_extensions,
                 v.id, v.dt AS datetime, v.g AS "s2:generation_time",
                 v.tile AS "s2:mgrs_tile", 50.0 AS "eo:cloud_cover",
                 v.geom AS geometry
          FROM (VALUES {", ".join(rows)}) v(id, dt, g, tile, geom)
        ) TO '{path}' (FORMAT PARQUET)
    """)


def _build_split(out, chunks, extra=(), year=2020):
    env = dict(os.environ, S2_ZSTD_LEVEL=str(LEVEL_LOW))
    return subprocess.run(
        [sys.executable, "tools/s2_build.py", "--sources", str(chunks.parent),
         "--years", str(year), "--out", str(out), *extra],
        cwd=ROOT, env=env, capture_output=True, text=True)


def _zone(tile: str) -> int:
    return int(tile[:2] if tile[:2].isdigit() else tile[:1])


def test_split_zones_writes_one_sorted_part_per_range():
    """Every ZONE_PARTS range gets exactly one file holding only its zones,
    sorted (_month, _hilbert), passing `gpio check all`; the parts together
    hold every deduped input row and nothing else is left in the year dir."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    zones = [1, 5, 20, 21, 30, 35, 36, 40, 46, 47, 55, 60]
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", zones)
        # A second copy of every zone-1 scene with a newer generation time:
        # deduped across the split, so the part total is the distinct count.
        _mk_zone_chunk(con, chunks / "b.parquet", [1])
        con.execute(f"""
            COPY (SELECT * REPLACE ('2020-06-01T00:00:00Z' AS "s2:generation_time")
                  FROM read_parquet('{chunks / "b.parquet"}'))
            TO '{chunks / "b.parquet"}' (FORMAT PARQUET)""")
        distinct = con.execute(
            f"SELECT count(DISTINCT id) FROM read_parquet('{chunks}/*.parquet')"
        ).fetchone()[0]
        assert distinct == len(zones) * 3

        out = Path(td) / "publish"
        proc = _build_split(out, chunks, ["--split", "zones"])
        assert proc.returncode == 0, proc.stdout + proc.stderr
        year_dir = out / "year=2020"
        assert sorted(p.name for p in year_dir.iterdir()) == \
            [f"{label}.parquet" for label in ZONE_LABELS]

        total = 0
        for label, lo, hi in ZONE_PARTS:
            part = year_dir / f"{label}.parquet"
            rows = con.execute(
                f'SELECT "s2:mgrs_tile", _month, _hilbert, "s2:generation_time" '
                f"FROM read_parquet('{part}')").fetchall()
            assert rows, label
            assert all(lo <= _zone(r[0]) <= hi for r in rows), label
            keys = [(r[1], r[2]) for r in rows]
            assert keys == sorted(keys), label
            chk = subprocess.run(["gpio", "check", "all", str(part)],
                                 capture_output=True, text=True)
            assert chk.returncode == 0, chk.stdout + chk.stderr
            total += len(rows)
            for line in (f"year=2020/{label}.parquet: staged",
                         f"year=2020/{label}.parquet: sorted",
                         f"year=2020/{label}.parquet: gpio check all passed"):
                assert line in proc.stdout, line
        assert total == distinct
        # The dedupe kept the newer generation for zone 1.
        gens = con.execute(
            f"SELECT DISTINCT \"s2:generation_time\" FROM "
            f"read_parquet('{year_dir / 'z01-20.parquet'}') "
            f"WHERE \"s2:mgrs_tile\" = '1UFU'").fetchall()
        assert gens == [("2020-06-01T00:00:00Z",)]


def test_split_zones_writes_nothing_for_an_empty_range():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", [3, 33, 58])
        out = Path(td) / "publish"
        proc = _build_split(out, chunks, ["--split", "zones"])
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert sorted(p.name for p in (out / "year=2020").iterdir()) == \
            ["z01-20.parquet", "z21-35.parquet", "z47-60.parquet"]
        assert "year=2020/z36-46.parquet: no rows, skipped" in proc.stdout


def test_split_zones_refuses_a_tile_it_cannot_place():
    """A row whose zone cannot be parsed belongs to no part. Dropping it on
    the floor would publish a year that is quietly short, so the build
    stops instead."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", [3, 33])
        con.execute(f"""
            COPY (SELECT * REPLACE (
                    CASE WHEN id = 'S2A_3_0' THEN 'XXABC' ELSE "s2:mgrs_tile" END
                    AS "s2:mgrs_tile")
                  FROM read_parquet('{chunks / "a.parquet"}'))
            TO '{chunks / "a.parquet"}' (FORMAT PARQUET)""")
        out = Path(td) / "publish"
        proc = _build_split(out, chunks, ["--split", "zones"])
        assert proc.returncode != 0
        assert "1 row(s) with no UTM zone" in proc.stderr
        assert not list((out / "year=2020").glob("*.parquet"))


def test_without_split_the_year_is_one_file_as_before():
    """The legacy path: no --split, one items.parquet, every row, sorted."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    zones = [1, 20, 21, 35, 36, 46, 47, 60]
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", zones)
        out = Path(td) / "publish"
        proc = _build_split(out, chunks)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert [p.name for p in (out / "year=2020").iterdir()] == \
            ["items.parquet"]
        keys = con.execute(
            f"SELECT _month, _hilbert FROM "
            f"read_parquet('{out / 'year=2020' / 'items.parquet'}')").fetchall()
        assert len(keys) == len(zones) * 3
        assert keys == sorted(keys)


def test_split_and_name_do_not_mix():
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        proc = _build_split(Path(td) / "publish", chunks,
                            ["--split", "zones", "--name", "live.parquet"])
        assert proc.returncode != 0
        assert "--name" in proc.stderr



def test_docs_name_every_zone_part():
    """The zone ranges are documented by hand in the collection docs, so this
    pins the prose to the constants: every part file name of both tiers, its
    zone range, and the year each tier starts must appear where a reader
    will look for them."""
    for doc in ("catalog/sentinel-2-l2a/AGENTS.md",
                "catalog/sentinel-2-l2a/README.md"):
        text = (ROOT / doc).read_text()
        for label, lo, hi in (*ZONE_PARTS, *ZONE_PARTS_8):
            assert f"{label}.parquet" in text, (doc, label)
            assert f"{lo}–{hi}" in text or f"{lo}-{hi}" in text, (doc, label)
        assert str(ZONE_SPLIT_FROM) in text and str(ZONE_SPLIT_8_FROM) in text
    root = (ROOT / "README.md").read_text()
    for label, _, _ in (*ZONE_PARTS, *ZONE_PARTS_8):
        assert label in root, label


def test_app_mirrors_both_zone_tiers():
    """The explorer cannot import s2_build, so it carries a copy of both
    tuples and both thresholds. Pin every octant and quartile name, and the
    two years, to the file so a retune here cannot leave the app reading
    the wrong part."""
    js = (ROOT / "apps/explorer/app.js").read_text()
    for label, lo, hi in ZONE_PARTS:
        assert f'["{label}", {lo}, {hi}]' in js, label
    for label, lo, hi in ZONE_PARTS_8:
        assert f'["{label}", {lo}, {hi}]' in js, label
    assert f"const ZONE_SPLIT_FROM = {ZONE_SPLIT_FROM};" in js
    assert f"const ZONE_SPLIT_8_FROM = {ZONE_SPLIT_8_FROM};" in js


# ---------------------------------------------------------------------------
# The eight-part tier (Task 19), and the two flags that make a build resume.
# ---------------------------------------------------------------------------

def test_zone_parts_for_picks_the_tier_by_year():
    assert zone_parts_for(ZONE_SPLIT_FROM - 1) == ()
    assert zone_parts_for(2015) == ()
    assert zone_parts_for(ZONE_SPLIT_FROM) == ZONE_PARTS
    assert zone_parts_for(ZONE_SPLIT_8_FROM - 1) == ZONE_PARTS
    assert zone_parts_for(ZONE_SPLIT_8_FROM) == ZONE_PARTS_8
    assert zone_parts_for(2026) == ZONE_PARTS_8
    assert ZONE_SPLIT_FROM == 2019 and ZONE_SPLIT_8_FROM == 2021
    # The octants tile 1..60 exactly and nest inside the quartiles: every
    # quartile boundary is an octant boundary, so a tile's octant is always
    # inside its quartile and a reader picks one file in either tier.
    for parts in (ZONE_PARTS, ZONE_PARTS_8):
        edges = [(lo, hi) for _, lo, hi in parts]
        assert edges[0][0] == 1 and edges[-1][1] == 60
        assert all(a[1] + 1 == b[0] for a, b in zip(edges, edges[1:]))
    octant_starts = {lo for _, lo, _ in ZONE_PARTS_8}
    assert all(lo in octant_starts for _, lo, _ in ZONE_PARTS)
    assert archive_part_names() == ("items", *ZONE_LABELS, *OCTANT_LABELS)
    assert len(set(archive_part_names())) == 13


def test_split_zones_writes_eight_parts_from_2021():
    """The pilot-shaped fixture, one scene per zone 1..60 plus a duplicate
    to dedupe, lands as exactly the ZONE_PARTS_8 files: sum == distinct
    input, every part holds only its zones, every part sorted and passing
    `gpio check all`, and no quartile file anywhere."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    zones = list(range(1, 61))
    year = ZONE_SPLIT_8_FROM
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", zones, per_zone=2, year=year)
        _mk_zone_chunk(con, chunks / "b.parquet", [7], per_zone=2, year=year)
        distinct = con.execute(
            f"SELECT count(DISTINCT id) FROM read_parquet('{chunks}/*.parquet')"
        ).fetchone()[0]
        assert distinct == len(zones) * 2

        out = Path(td) / "publish"
        proc = _build_split(out, chunks, ["--split", "zones"], year=year)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        year_dir = out / f"year={year}"
        assert sorted(p.name for p in year_dir.iterdir()) == \
            sorted(f"{label}.parquet" for label in OCTANT_LABELS)

        total = 0
        for label, lo, hi in ZONE_PARTS_8:
            part = year_dir / f"{label}.parquet"
            rows = con.execute(
                f'SELECT "s2:mgrs_tile", _month, _hilbert '
                f"FROM read_parquet('{part}')").fetchall()
            assert rows, label
            assert {_zone(r[0]) for r in rows} == set(range(lo, hi + 1)), label
            keys = [(r[1], r[2]) for r in rows]
            assert keys == sorted(keys), label
            chk = subprocess.run(["gpio", "check", "all", str(part)],
                                 capture_output=True, text=True)
            assert chk.returncode == 0, chk.stdout + chk.stderr
            total += len(rows)
        assert total == distinct
        assert f"TOTAL {distinct:,} rows" in proc.stdout


def test_split_zones_refuses_a_year_with_no_parts():
    """--split zones on a year before ZONE_SPLIT_FROM has no tier to write;
    writing it whole under a flag that says "split" would be a surprise."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", [3], year=2017)
        proc = _build_split(Path(td) / "publish", chunks,
                            ["--split", "zones"], year=2017)
        assert proc.returncode != 0
        assert "no zone parts" in proc.stderr
        assert not (Path(td) / "publish" / "year=2017").exists() or \
            not list((Path(td) / "publish" / "year=2017").glob("*.parquet"))


def _mk_octant_year(con, td: Path, per_zone=2):
    """One chunk dir holding `per_zone` scenes in every zone 1..60 of the
    first octant year. Returns (chunks, year, distinct ids)."""
    year = ZONE_SPLIT_8_FROM
    chunks = td / "chunks" / "api"
    chunks.mkdir(parents=True)
    _mk_zone_chunk(con, chunks / "a.parquet", list(range(1, 61)),
                   per_zone=per_zone, year=year)
    return chunks, year, 60 * per_zone


def test_only_parts_writes_just_the_named_part():
    """--only-parts z21-31 on a full octant year writes exactly that file:
    its rows are exactly zones 21-31, sorted and passing `gpio check all`;
    the other seven ranges' rows are dropped and the log names how many.
    This is the per-part job of consolidate-month.yml."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, year, distinct = _mk_octant_year(con, Path(td))
        out = Path(td) / "publish"
        proc = _build_split(out, chunks,
                            ["--split", "zones", "--only-parts", "z21-31"],
                            year=year)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        year_dir = out / f"year={year}"
        assert [p.name for p in year_dir.iterdir()] == ["z21-31.parquet"]
        part = year_dir / "z21-31.parquet"
        rows = con.execute(
            f'SELECT "s2:mgrs_tile", _month, _hilbert '
            f"FROM read_parquet('{part}')").fetchall()
        assert {_zone(r[0]) for r in rows} == set(range(21, 32))
        assert len(rows) == 11 * 2
        keys = [(r[1], r[2]) for r in rows]
        assert keys == sorted(keys)
        chk = subprocess.run(["gpio", "check", "all", str(part)],
                             capture_output=True, text=True)
        assert chk.returncode == 0, chk.stdout + chk.stderr
        dropped = distinct - len(rows)
        assert (f"--only-parts z21-31: {dropped:,} row(s) of other zone "
                f"ranges dropped") in proc.stdout
        assert f"TOTAL {len(rows):,} rows" in proc.stdout


def test_only_parts_accepts_several_labels():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, year, distinct = _mk_octant_year(con, Path(td))
        out = Path(td) / "publish"
        proc = _build_split(out, chunks,
                            ["--split", "zones", "--only-parts",
                             "z53-60,z01-15"], year=year)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert sorted(p.name for p in (out / f"year={year}").iterdir()) == \
            ["z01-15.parquet", "z53-60.parquet"]
        assert f"{distinct - (15 + 8) * 2:,} row(s) of other zone ranges" \
            in proc.stdout


def test_only_parts_refuses_a_label_outside_the_tier():
    """A quartile label on an octant year is not a part of that year. The
    refusal names every label that is, so the caller can fix the call."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, year, _ = _mk_octant_year(con, Path(td), per_zone=1)
        out = Path(td) / "publish"
        proc = _build_split(out, chunks,
                            ["--split", "zones", "--only-parts", "z01-20"],
                            year=year)
        assert proc.returncode != 0
        assert "z01-20" in proc.stderr
        for label in OCTANT_LABELS:
            assert label in proc.stderr, label
        # Refused before the year directory exists: nothing left behind.
        assert not (out / f"year={year}").exists()


def test_only_parts_needs_split_zones():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, year, _ = _mk_octant_year(con, Path(td), per_zone=1)
        out = Path(td) / "publish"
        proc = _build_split(out, chunks, ["--only-parts", "z01-15"],
                            year=year)
        assert proc.returncode != 0
        assert "--only-parts" in proc.stderr and "--split zones" in proc.stderr
        assert not (out / f"year={year}").exists()


def test_consolidation_plan_probes_live_and_every_part_once():
    """The plan job's one round of HEADs: live and each part of the year's
    tier, through the injected probe, in a shape the workflow turns into
    outputs and a matrix. A probe that raises (published_part on a code
    that is not 200 or 404) propagates and plans nothing."""
    from s2_build import consolidation_plan
    up = {"live.parquet", "z01-15.parquet", "z53-60.parquet"}
    asked = []

    def probe(base, year, name):
        asked.append((base, year, name))
        return name in up

    plan = consolidation_plan("https://x/sentinel-2-l2a", 2021, probe)
    assert plan["live"] is True
    assert plan["parts"] == OCTANT_LABELS
    assert plan["include"] == [
        {"part": label, "exists": label in ("z01-15", "z53-60")}
        for label in OCTANT_LABELS]
    assert [name for _, _, name in asked] == \
        ["live.parquet", *(f"{label}.parquet" for label in OCTANT_LABELS)]
    assert {base for base, _, _ in asked} == {"https://x/sentinel-2-l2a"}
    assert {year for _, year, _ in asked} == {2021}

    # A single-file year plans one "items" part, and no live means no work.
    plan = consolidation_plan("https://x/sentinel-2-l2a", 2017,
                              lambda base, year, name: name == "items.parquet")
    assert plan == {"live": False, "parts": ["items"],
                    "include": [{"part": "items", "exists": True}]}

    def broken(base, year, name):
        raise SystemExit(f"{name}: HEAD answered 403")
    with pytest.raises(SystemExit, match="403"):
        consolidation_plan("https://x/sentinel-2-l2a", 2021, broken)


class _Bucket:
    """A stand-in for the bucket's public base, serving `root` (a directory
    holding year=YYYY/<part>.parquet, or None for an empty bucket) so
    DuckDB can read a published part's footer over HTTP as s2_build does
    for real. `codes` forces a HEAD status per file name; otherwise a
    file that exists answers 200 and anything else 404. Every HEAD is
    logged with its User-Agent."""

    def __init__(self, codes: dict[str, int] | None = None, root=None):
        import threading
        from functools import partial
        from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

        self.codes, self.seen = codes or {}, []
        bucket = self
        directory = str(root) if root else tempfile.mkdtemp()

        class H(SimpleHTTPRequestHandler):
            def do_HEAD(self):
                name = self.path.rsplit("/", 1)[-1]
                bucket.seen.append((self.headers.get("User-Agent"), self.path))
                forced = bucket.codes.get(name)
                if forced is None:
                    return super().do_HEAD()
                self.send_response(forced)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def translate_path(self, path):
                # The bucket's public base has one more segment than the
                # directory: strip /sentinel-2-l2a.
                return super().translate_path(
                    path.replace("/sentinel-2-l2a/", "/", 1))

            def log_message(self, *a):
                pass

        self.srv = ThreadingHTTPServer(
            ("127.0.0.1", 0), partial(H, directory=directory))
        self.url = f"http://127.0.0.1:{self.srv.server_port}/sentinel-2-l2a"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()


def _publish_fixture(con, td: Path, year: int, zones, per_zone=1):
    """Build the fixture once, plainly, into <td>/bucket -- the state of the
    bucket after a run that finished those parts -- and return
    (chunks, bucket dir)."""
    chunks = td / "chunks" / "api"
    chunks.mkdir(parents=True)
    _mk_zone_chunk(con, chunks / "a.parquet", zones, per_zone=per_zone,
                   year=year)
    proc = _build_split(td / "bucket", chunks, ["--split", "zones"], year=year)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return chunks, td / "bucket"


def test_skip_existing_builds_only_the_unpublished_parts():
    """Resume after a timeout: the parts the bucket already has (HEAD 200,
    and a footer row count equal to what this build would write) are
    skipped with a log line and never written; the 404 ones are built; the
    part total still has to add up to the staged rows, skipped parts
    included; --on-part-done runs once per BUILT part, with its path."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    year = ZONE_SPLIT_8_FROM
    published = {"z01-15.parquet": 200, "z36-40.parquet": 200}
    with tempfile.TemporaryDirectory() as td:
        chunks, bucket_dir = _publish_fixture(con, Path(td), year,
                                              list(range(1, 61)))
        for part in (bucket_dir / f"year={year}").iterdir():
            if part.name not in published:
                part.unlink()  # the run "timed out" after these two
        bucket = _Bucket(root=bucket_dir)
        try:
            out = Path(td) / "publish"
            log = Path(td) / "hook.log"
            hook = (f"{sys.executable} -c \"import sys; open(sys.argv[1], 'a')"
                    f".write(sys.argv[2] + chr(10))\" {log}")
            proc = _build_split(out, chunks, [
                "--split", "zones", "--skip-existing-url", bucket.url,
                "--on-part-done", hook], year=year)
            assert proc.returncode == 0, proc.stdout + proc.stderr
            year_dir = out / f"year={year}"
            built = sorted(p.name for p in year_dir.iterdir())
            assert built == sorted(f"{lb}.parquet" for lb in OCTANT_LABELS
                                   if f"{lb}.parquet" not in published)
            for name, rows in (("z01-15.parquet", 15), ("z36-40.parquet", 5)):
                assert (f"year={year}/{name}: already published with the "
                        f"same {rows} rows") in proc.stdout
            # 60 rows staged: 15 + 5 skipped, 40 written, and the sum checked.
            assert "TOTAL 40 rows across 1 year(s), 2 part(s) already published" \
                in proc.stdout
            # One probe HEAD per part with the catalog's client name (DuckDB
            # adds its own HEAD when it reads a skipped part's footer).
            probes = [path for ua, path in bucket.seen
                      if ua.startswith("sentinel-2-catalog-tools/")]
            assert sorted(probes) == sorted(
                f"/sentinel-2-l2a/year={year}/{lb}.parquet" for lb in OCTANT_LABELS)
            # The hook ran once per built part, in order, with the final
            # (resolved: --out is) path.
            assert log.read_text().splitlines() == \
                [str(year_dir.resolve() / name) for name in
                 (f"{lb}.parquet" for lb in OCTANT_LABELS)
                 if name not in published]
        finally:
            bucket.close()


def test_skip_existing_refuses_a_published_part_with_other_rows():
    """A published part built from an older slice set does not hold what
    this build's slices give its range. Skipping it would leave the year
    stale, so the build stops and names the part and both counts -- read
    from the published part's footer over HTTP, as in production."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    year = ZONE_SPLIT_8_FROM
    with tempfile.TemporaryDirectory() as td:
        # Published from a slice set with zones 3 and 7 only (2 rows in
        # z01-15); the new slices have zones 3, 7 and 9 (3 rows).
        chunks, bucket_dir = _publish_fixture(con, Path(td), year, [3, 7])
        _mk_zone_chunk(con, chunks / "b.parquet", [9], per_zone=1, year=year)
        bucket = _Bucket(root=bucket_dir)
        try:
            out = Path(td) / "publish"
            proc = _build_split(out, chunks, [
                "--split", "zones", "--skip-existing-url", bucket.url], year=year)
            assert proc.returncode != 0
            assert ("year=2021/z01-15.parquet: already published with 2 rows, "
                    "but this build's slices give zones 1-15 3 rows") in proc.stderr
            assert not list((out / f"year={year}").glob("*.parquet"))
        finally:
            bucket.close()


def test_skip_existing_row_check_with_an_injected_count():
    """The same rule through build_year() with the probe and the footer
    read injected: an equal count skips, a different one stops."""
    from s2_build import build_year, connect
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    year = ZONE_SPLIT_8_FROM
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks"
        chunks.mkdir()
        _mk_zone_chunk(con, chunks / "a.parquet", [3, 40], per_zone=2, year=year)
        bcon = connect("1GB", Path(td))
        files = [str(chunks / "a.parquet")]
        seen = []

        def probe(base, y, name):
            return name == "z01-15.parquet"

        def same(con, url):
            seen.append(url)
            return 2

        # In-process, so ZSTD_LEVEL is the import-time default; four rows
        # at level 18 cost nothing.
        written, skipped, parts_written = build_year(
            bcon, files, year, Path(td) / "ok", split="zones",
            skip_existing_url="http://bucket/sentinel-2-l2a",
            probe=probe, remote_rows=same)
        assert (written, skipped, parts_written) == (2, 1, 1)
        assert seen == [f"http://bucket/sentinel-2-l2a/year={year}/z01-15.parquet"]
        assert sorted(p.name for p in (Path(td) / "ok" / f"year={year}").iterdir()) \
            == ["z36-40.parquet"]

        with pytest.raises(SystemExit) as caught:
            build_year(bcon, files, year, Path(td) / "stale", split="zones",
                       skip_existing_url="http://bucket/sentinel-2-l2a",
                       probe=probe, remote_rows=lambda con, url: 7)
        assert "z01-15.parquet: already published with 7 rows" in str(caught.value)
        assert "zones 1-15 2 rows" in str(caught.value)
        assert not list((Path(td) / "stale" / f"year={year}").glob("*.parquet"))


def test_skip_existing_skips_a_fully_published_year_without_staging():
    """Every part HEADs 200: nothing is staged, so nothing is compared --
    the point of the flag is that re-dispatching a finished year costs
    eight HEADs and no minutes."""
    year = ZONE_SPLIT_8_FROM
    bucket = _Bucket({f"{lb}.parquet": 200 for lb in OCTANT_LABELS})
    try:
        with tempfile.TemporaryDirectory() as td:
            chunks = Path(td) / "chunks" / "api"
            chunks.mkdir(parents=True)
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            _mk_zone_chunk(con, chunks / "a.parquet", [3], year=year)
            out = Path(td) / "publish"
            proc = _build_split(out, chunks, [
                "--split", "zones", "--skip-existing-url", bucket.url], year=year)
            assert proc.returncode == 0, proc.stdout + proc.stderr
            assert "every part already published" in proc.stdout
            assert "staged" not in proc.stdout
            assert not list((out / f"year={year}").glob("*.parquet"))
            assert "8 part(s) already published" in proc.stdout
    finally:
        bucket.close()


def test_skip_existing_applies_to_a_single_file_year_too():
    bucket = _Bucket({"items.parquet": 200})
    try:
        with tempfile.TemporaryDirectory() as td:
            chunks = Path(td) / "chunks" / "api"
            chunks.mkdir(parents=True)
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")
            _mk_zone_chunk(con, chunks / "a.parquet", [3], year=2017)
            proc = _build_split(Path(td) / "publish", chunks,
                                ["--skip-existing-url", bucket.url], year=2017)
            assert proc.returncode == 0, proc.stdout + proc.stderr
            assert "every part already published (items.parquet)" in proc.stdout
            assert [p for _, p in bucket.seen] == \
                ["/sentinel-2-l2a/year=2017/items.parquet"]
    finally:
        bucket.close()


def test_skip_existing_stops_on_an_answer_that_is_not_200_or_404():
    """A 5xx is retried (s2_fetch's backoff) and then fatal; a 403 is fatal
    at once. Neither may be read as "not published" -- that would rebuild
    and re-upload a finished part -- nor as "published", which would leave
    the year short on the bucket."""
    from s2_build import published_part
    bucket = _Bucket({"z01-15.parquet": 500, "z16-20.parquet": 403})
    try:
        with pytest.raises(SystemExit) as caught:
            published_part(bucket.url, 2021, "z01-15.parquet", tries=2)
        assert "500" in str(caught.value) and "refusing" in str(caught.value)
        assert len(bucket.seen) == 2  # tried, backed off, tried again
        with pytest.raises(SystemExit) as caught:
            published_part(bucket.url, 2021, "z16-20.parquet", tries=2)
        assert "403" in str(caught.value)
        assert len(bucket.seen) == 3  # a 403 is not retried
        assert published_part(bucket.url, 2021, "z21-31.parquet") is False
        # An unreachable host is not an answer either.
        with pytest.raises(SystemExit) as caught:
            published_part("http://127.0.0.1:9/x", 2021, "z21-31.parquet", tries=1)
        assert "cannot ask" in str(caught.value)
    finally:
        bucket.close()


def test_on_part_done_failure_halts_the_year():
    """A hook that fails (the upload did not happen) must stop the build
    after that part, not carry on to the next one: exit non-zero, the
    failed part is on disk (it passed its check), no later part exists."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    year = ZONE_SPLIT_8_FROM
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", [3, 18, 25, 58], year=year)
        out = Path(td) / "publish"
        log = Path(td) / "hook.log"
        # Succeeds for the first part, fails on the second.
        hook = (f"{sys.executable} -c \"import sys; f=open(sys.argv[1], 'a'); "
                f"f.write(sys.argv[2] + chr(10)); f.close(); "
                f"sys.exit(0 if 'z01-15' in sys.argv[2] else 3)\" {log}")
        proc = _build_split(out, chunks, [
            "--split", "zones", "--on-part-done", hook], year=year)
        assert proc.returncode != 0
        assert "--on-part-done command exited 3" in proc.stderr
        year_dir = out / f"year={year}"
        assert sorted(p.name for p in year_dir.iterdir()) == \
            ["z01-15.parquet", "z16-20.parquet"]
        assert log.read_text().splitlines() == [
            str(year_dir.resolve() / "z01-15.parquet"),
            str(year_dir.resolve() / "z16-20.parquet")]
        assert "z21-31.parquet: staged" not in proc.stdout or \
            not (year_dir / "z21-31.parquet").exists()


def test_on_part_done_runs_for_a_single_file_year():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_zone_chunk(con, chunks / "a.parquet", [3], year=2017)
        out = Path(td) / "publish"
        log = Path(td) / "hook.log"
        hook = (f"{sys.executable} -c \"import sys; open(sys.argv[1], 'a')"
                f".write(sys.argv[2] + chr(10))\" {log}")
        proc = _build_split(out, chunks, ["--on-part-done", hook], year=2017)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert log.read_text().splitlines() == \
            [str(out.resolve() / "year=2017" / "items.parquet")]


def test_tile_sort_from_2026_pins_a_tile_month_to_few_row_groups():
    """From TILE_SORT_FROM on, parts sort (_month, s2:mgrs_tile, _hilbert):
    within a month every row of one tile is contiguous, so a tile-month
    lookup touches one (rarely two) small row groups instead of every
    group the month spans. The 2024 fixture above keeps the old key."""
    from s2_build import sort_key, TILE_SORT_FROM
    assert sort_key(TILE_SORT_FROM - 1) == "_month,_hilbert"
    assert sort_key(TILE_SORT_FROM) == "_month,s2:mgrs_tile,_hilbert"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_big_chunk(con, chunks / "a.parquet", rows=60_000, year=TILE_SORT_FROM,
                      tiles=("31UFU", "32UMV", "33UUP", "34UDA"))
        out = Path(td) / "publish"
        proc = _build(out, chunks, LEVEL_LOW, years=str(TILE_SORT_FROM))
        assert proc.returncode == 0, proc.stdout + proc.stderr
        f = out / f"year={TILE_SORT_FROM}" / "items.parquet"
        keys = con.execute(f"""SELECT _month, "s2:mgrs_tile", _hilbert
                               FROM read_parquet('{f}')""").fetchall()
        assert keys == sorted(keys), "rows must be ordered by (_month, tile, _hilbert)"
        # A tile-month is contiguous, so it spans at most ceil(rows_in_tile_month / 6144) + 1 groups.
        spans = con.execute(f"""
            SELECT max(g) FROM (
              SELECT _month, "s2:mgrs_tile", count(DISTINCT rg) AS g FROM (
                SELECT _month, "s2:mgrs_tile",
                       (row_number() OVER (ORDER BY _month, "s2:mgrs_tile", _hilbert) - 1) // 6144 AS rg
                FROM read_parquet('{f}')
              ) GROUP BY 1, 2)""").fetchone()[0]
        assert spans <= 2, f"a tile-month spans {spans} row groups; expected contiguous"


# ---------------------------------------------------------------------------
# --exclude-ids-from: live.parquet stays disjoint from the year's archive.
# ---------------------------------------------------------------------------

def _archive_fixture(con, td: Path):
    """A published archive part for 2024 holding A and D in March and B in
    May, built by the tool itself so it carries the real `_month` column,
    served from <td>/bucket. Returns (chunks dir for the live build, bucket
    dir). The live chunks hold A, B and C, all in March: A is in the
    archive's March (dropped); B is only in the archive's May, which the
    exclusion must not consult for a March-only live (kept); C is new
    (kept)."""
    archive = td / "archive" / "api"
    archive.mkdir(parents=True)
    _mk_chunk(con, archive / "a.parquet", [
        ("A", "2024-03-05 10:00:00+00", "2024-03-05T12:00:00Z", 4.0, 52.0),
        ("D", "2024-03-20 10:00:00+00", "2024-03-20T12:00:00Z", 5.0, 52.0),
        ("B", "2024-05-05 10:00:00+00", "2024-05-05T12:00:00Z", 6.0, 52.0),
    ])
    proc = _build(td / "bucket", archive, LEVEL_LOW)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    chunks = td / "chunks" / "api"
    chunks.mkdir(parents=True)
    _mk_chunk(con, chunks / "a.parquet", [
        ("A", "2024-03-05 10:00:00+00", "2024-03-05T12:00:00Z", 4.0, 52.0),
        ("B", "2024-03-15 10:00:00+00", "2024-03-15T12:00:00Z", 6.0, 52.0),
        ("C", "2024-03-25 10:00:00+00", "2024-03-25T12:00:00Z", 7.0, 52.0),
    ])
    return chunks, td / "bucket"


def _build_live(out, chunks, urls):
    env = dict(os.environ, S2_ZSTD_LEVEL=str(LEVEL_LOW))
    return subprocess.run(
        [sys.executable, "tools/s2_build.py", "--sources", str(chunks.parent),
         "--years", "2024", "--out", str(out), "--name", "live.parquet",
         "--exclude-ids-from", *urls],
        cwd=ROOT, env=env, capture_output=True, text=True)


def test_exclude_ids_drops_archived_rows_and_consults_only_staged_months():
    """Rows whose id the published archive holds are dropped, rows it does
    not hold are kept, only the staged rows' months are read from the
    archive (B sits in the archive's May and survives a March-only live),
    a 404 URL is skipped with a log line, and the dropped count is logged.
    The ids are read over HTTP with DuckDB, as in production."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, bucket_dir = _archive_fixture(con, Path(td))
        bucket = _Bucket(root=bucket_dir)
        try:
            out = Path(td) / "publish"
            archive_url = f"{bucket.url}/year=2024/items.parquet"
            missing_url = f"{bucket.url}/year=2024/z01-15.parquet"
            proc = _build_live(out, chunks, [archive_url, missing_url])
            assert proc.returncode == 0, proc.stdout + proc.stderr
            live = out / "year=2024" / "live.parquet"
            ids = [r[0] for r in con.execute(
                f"SELECT id FROM read_parquet('{live}') ORDER BY id").fetchall()]
            assert ids == ["B", "C"]
            assert (f"year=2024/live.parquet: {missing_url} is not published "
                    f"(404); nothing to exclude from it") in proc.stdout
            # Only March was consulted: A and D, not May's B.
            assert ("2 id(s) read from 1 published part(s) for month(s) 3"
                    in proc.stdout), proc.stdout
            assert ("year=2024/live.parquet: 1 row(s) dropped that the "
                    "published archive already holds at the same or a newer "
                    "generation") in proc.stdout
            assert "TOTAL 2 rows" in proc.stdout
            # One probe HEAD per URL with the catalog's client name; DuckDB's
            # own requests carry its agent.
            probes = [path for ua, path in bucket.seen
                      if ua.startswith("sentinel-2-catalog-tools/")]
            assert sorted(probes) == ["/sentinel-2-l2a/year=2024/items.parquet",
                                      "/sentinel-2-l2a/year=2024/z01-15.parquet"]
        finally:
            bucket.close()


def test_exclude_ids_keeps_everything_when_no_url_is_published():
    """Every URL 404s (a year whose archive is not up yet): nothing is read,
    nothing is dropped, and the build is the plain one."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, _ = _archive_fixture(con, Path(td))
        bucket = _Bucket()
        try:
            out = Path(td) / "publish"
            proc = _build_live(out, chunks, [f"{bucket.url}/year=2024/items.parquet"])
            assert proc.returncode == 0, proc.stdout + proc.stderr
            ids = [r[0] for r in con.execute(
                f"SELECT id FROM read_parquet('{out / 'year=2024' / 'live.parquet'}') "
                f"ORDER BY id").fetchall()]
            assert ids == ["A", "B", "C"]
            assert "is not published (404)" in proc.stdout
            assert "id(s) read" not in proc.stdout
        finally:
            bucket.close()


def test_exclude_ids_stops_on_an_answer_that_is_not_200_or_404():
    """A 403 on an exclusion URL is fatal: an archive part skipped silently
    would republish its scenes in live, the overlap this flag removes."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, bucket_dir = _archive_fixture(con, Path(td))
        bucket = _Bucket({"items.parquet": 403}, root=bucket_dir)
        try:
            out = Path(td) / "publish"
            proc = _build_live(out, chunks, [f"{bucket.url}/year=2024/items.parquet"])
            assert proc.returncode != 0
            assert "403" in proc.stderr and "refusing" in proc.stderr
            assert not (out / "year=2024" / "live.parquet").exists()
        finally:
            bucket.close()


def test_exclude_ids_keeps_a_reprocessed_product():
    """The exclusion compares s2:generation_time, not ids alone: a staged
    row that is a newer generation of an archived id is a reprocessed
    product that never reached the archive and must stay in live; an equal
    or older generation is the archived scene fetched again and is dropped.
    Archive: R, S, T in March at generations 10, 12, 14 (day of month);
    live: R at 11 (newer, kept), S at 12 (equal, dropped), T at 13 (older,
    dropped)."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        archive = Path(td) / "archive" / "api"
        archive.mkdir(parents=True)
        _mk_chunk(con, archive / "a.parquet", [
            ("R", "2024-03-05 10:00:00+00", "2024-03-10T12:00:00Z", 4.0, 52.0),
            ("S", "2024-03-06 10:00:00+00", "2024-03-12T12:00:00Z", 5.0, 52.0),
            ("T", "2024-03-07 10:00:00+00", "2024-03-14T12:00:00Z", 6.0, 52.0),
        ])
        proc = _build(Path(td) / "bucket", archive, LEVEL_LOW)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_chunk(con, chunks / "a.parquet", [
            ("R", "2024-03-05 10:00:00+00", "2024-03-11T00:00:00Z", 4.0, 52.0),
            ("S", "2024-03-06 10:00:00+00", "2024-03-12T12:00:00Z", 5.0, 52.0),
            ("T", "2024-03-07 10:00:00+00", "2024-03-13T00:00:00Z", 6.0, 52.0),
        ])
        bucket = _Bucket(root=Path(td) / "bucket")
        try:
            out = Path(td) / "publish"
            proc = _build_live(out, chunks, [f"{bucket.url}/year=2024/items.parquet"])
            assert proc.returncode == 0, proc.stdout + proc.stderr
            rows = con.execute(
                f"SELECT id, \"s2:generation_time\" FROM "
                f"read_parquet('{out / 'year=2024' / 'live.parquet'}')").fetchall()
            assert rows == [("R", "2024-03-11T00:00:00Z")]
            assert "2 row(s) dropped that the published archive already holds" \
                in proc.stdout
        finally:
            bucket.close()


def test_exclude_ids_writes_a_zero_row_live_when_everything_is_archived():
    """The archive already holds every staged row: the build succeeds and
    writes live.parquet with zero rows and the staged schema (what
    consolidate-month publishes for an emptied live), so the refresh's
    stats splice and restamps still run. Without --exclude-ids-from, no
    rows is still the failure it always was."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks, bucket_dir = _archive_fixture(con, Path(td))
        # Stage exactly the archive's March rows (A and D), nothing new.
        for f in chunks.iterdir():
            f.unlink()
        _mk_chunk(con, chunks / "a.parquet", [
            ("A", "2024-03-05 10:00:00+00", "2024-03-05T12:00:00Z", 4.0, 52.0),
            ("D", "2024-03-20 10:00:00+00", "2024-03-20T12:00:00Z", 5.0, 52.0),
        ])
        bucket = _Bucket(root=bucket_dir)
        try:
            out = Path(td) / "publish"
            proc = _build_live(out, chunks, [f"{bucket.url}/year=2024/items.parquet"])
            assert proc.returncode == 0, proc.stdout + proc.stderr
            live = out / "year=2024" / "live.parquet"
            assert [p.name for p in live.parent.iterdir()] == ["live.parquet"]
            assert con.execute(
                f"SELECT count(*) FROM read_parquet('{live}')").fetchone()[0] == 0
            cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{live}')").fetchall()]
            assert {"id", "datetime", "_month", "_hilbert", "geometry"} <= set(cols)
            assert "2 row(s) dropped" in proc.stdout
            assert "wrote it with zero rows" in proc.stdout
            assert "TOTAL 0 rows" in proc.stdout
            assert "no rows matched" not in proc.stderr
        finally:
            bucket.close()
        # No exclusion, no rows in the year: exit 1 as before.
        proc = subprocess.run(
            [sys.executable, "tools/s2_build.py", "--sources", str(chunks.parent),
             "--years", "2019", "--out", str(Path(td) / "none"),
             "--name", "live.parquet"],
            cwd=ROOT, env=dict(os.environ, S2_ZSTD_LEVEL=str(LEVEL_LOW)),
            capture_output=True, text=True)
        assert proc.returncode == 1
        assert "no rows matched" in proc.stderr
