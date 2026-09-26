"""A failed probe must never shrink a year.

make_items.py decides what a year holds from three sources: the staged
directory, the published copy, and -- when the published copy cannot be
reached -- the item it wrote last time. The distinction these tests defend is
between "that part is not published" and "I could not ask", because collapsing
the two lets one timeout rewrite a year to whatever answered and exit zero.

No network: the prober is injected, and the one real Parquet is built here.
"""
import json
import sys
import tempfile
import urllib.error
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

from make_items import (  # noqa: E402
    ABSENT, PRESENT, UNKNOWN, build_item, connect, discover, remote_probe,
)
from s2_build import ZONE_PARTS, ZONE_PARTS_8  # noqa: E402

ZONE_NAMES = [f"{label}.parquet" for label, _, _ in ZONE_PARTS]
ZONE_KEYS = [f"data-{label}" for label, _, _ in ZONE_PARTS]
OCTANT_NAMES = [f"{label}.parquet" for label, _, _ in ZONE_PARTS_8]
OCTANT_KEYS = [f"data-{label}" for label, _, _ in ZONE_PARTS_8]


def prober(state, size=None, **by_name):
    """A probe that gives `state` for every part, or a per-file answer given
    by name: prober(ABSENT, **{"items.parquet": (PRESENT, 4242)})."""
    def probe(url):
        return by_name.get(url.rsplit("/", 1)[1], (state, size))
    return probe


def committed_item(rows=9_000_000, size=1234):
    """An item as a previous run would have written it."""
    return {
        "type": "Feature",
        "id": "2026",
        "bbox": [-180.0, -60.0, 180.0, 80.0],
        "properties": {
            "start_datetime": "2026-01-01T00:00:00Z",
            "end_datetime": "2026-09-01T00:00:00Z",
            "table:row_count": rows,
            "s2:platforms": ["sentinel-2a", "sentinel-2b"],
        },
        "assets": {
            "data": {
                "href": "./items.parquet",
                "start_datetime": "2026-01-01T00:00:00Z",
                "end_datetime": "2026-09-01T00:00:00Z",
                "table:row_count": rows,
                "file:size": size,
            }
        },
    }


def staged_live(directory: Path) -> Path:
    """A tiny but real year part, so the readable side is really read."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL spatial; LOAD spatial;")
    year_dir = directory / "year=2026"
    year_dir.mkdir(parents=True)
    path = year_dir / "live.parquet"
    con.execute(f"""
        COPY (
          SELECT 'S2C_1GCH_20260912_0_L2A' AS id,
                 TIMESTAMPTZ '2026-09-12 10:00:00+00' AS datetime,
                 'sentinel-2c' AS platform,
                 ST_Point(5.0, 52.0) AS geometry
        ) TO '{path}' (FORMAT PARQUET)
    """)
    return year_dir


def test_published_part_is_used_when_present():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        parts = discover(year_dir, 2026, True, None,
                         prober(ABSENT, **{"items.parquet": (PRESENT, 4242)}))
    assert [(p["key"], p["source"]) for p in parts] == [
        ("data", "remote"), ("live", "local")]
    assert next(p for p in parts if p["key"] == "data")["size"] == 4242


def test_every_published_part_is_a_candidate():
    """A year may hold the legacy file, the four zone quartiles, the eight
    zone octants and the tail -- fourteen candidates; whatever answers
    PRESENT is read, in the advertised order."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        parts = discover(year_dir, 2026, True, None, prober(PRESENT, 1))
    assert [p["name"] for p in parts] == \
        ["items.parquet", *ZONE_NAMES, *OCTANT_NAMES, "live.parquet"]
    assert [p["key"] for p in parts] == \
        ["data", *ZONE_KEYS, *OCTANT_KEYS, "live"]
    assert len(parts) == 14


def test_absent_part_is_skipped_when_nothing_recorded_it():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        parts = discover(year_dir, 2026, True, None, prober(ABSENT))
    assert [p["key"] for p in parts] == ["live"]


def test_absent_part_the_committed_item_describes_is_fatal():
    """A published file that has gone missing is not a smaller year."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        with pytest.raises(SystemExit) as caught:
            discover(year_dir, 2026, True, committed_item(), prober(ABSENT))
    assert "items.parquet" in str(caught.value)


def test_unreachable_part_falls_back_to_the_committed_record():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        committed = committed_item()
        parts = discover(year_dir, 2026, True, committed, prober(UNKNOWN))
        assert [(p["key"], p["source"]) for p in parts] == [
            ("data", "committed"), ("live", "local")]
        item = build_item(connect(), 2026, parts, committed)

    # The zone parts the committed item never recorded are not the year
    # shrinking: they were not there last time either, and a probe that could
    # not answer does not make them appear. Only items.parquet falls back.
    # The year still carries the archive's rows, and grows by the staged tail.
    assert item["properties"]["table:row_count"] == 9_000_001
    assert item["properties"]["start_datetime"] == "2026-01-01T00:00:00Z"
    assert item["properties"]["end_datetime"] == "2026-09-12T10:00:00Z"
    # The unreadable part keeps what it last recorded; the staged one is measured.
    assert item["assets"]["data"]["table:row_count"] == 9_000_000
    assert item["assets"]["data"]["file:size"] == 1234
    assert item["assets"]["live"]["table:row_count"] == 1
    # Platforms survive a part nobody could scan.
    assert item["properties"]["s2:platforms"] == [
        "sentinel-2a", "sentinel-2b", "sentinel-2c"]


def test_unreachable_part_with_nothing_recorded_stops_the_run():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        with pytest.raises(SystemExit) as caught:
            discover(year_dir, 2026, True, None, prober(UNKNOWN))
    assert "cannot reach" in str(caught.value)


def test_probe_reads_the_status_code(monkeypatch):
    """404 means absent; every other failure means unanswered."""
    def raise_http(code):
        def opener(request, timeout=None):
            raise urllib.error.HTTPError(
                request.full_url, code, "", {}, None)
        return opener

    for code, expected in ((404, ABSENT), (410, ABSENT), (403, UNKNOWN),
                           (500, UNKNOWN), (503, UNKNOWN)):
        monkeypatch.setattr("urllib.request.urlopen", raise_http(code))
        assert remote_probe("https://example.invalid/x.parquet")[0] == expected

    def raise_timeout(request, timeout=None):
        raise urllib.error.URLError(TimeoutError("timed out"))

    monkeypatch.setattr("urllib.request.urlopen", raise_timeout)
    assert remote_probe("https://example.invalid/x.parquet")[0] == UNKNOWN


def test_committed_json_round_trips_through_the_fallback():
    """The fallback reads an item exactly as make_items.py writes one.

    Run one: both parts staged, so the item is measured and written to disk.
    Run two: the archive part is gone from --data-dir and unreachable, which is
    the daily refresh on a bad day. The year must come back whole.
    """
    with tempfile.TemporaryDirectory() as td:
        staged = Path(td) / "staged"
        year_dir = staged_live(staged)
        con = connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            COPY (SELECT * FROM (VALUES
                    ('a', TIMESTAMPTZ '2026-01-02 01:00:00+00', 'sentinel-2a',
                     ST_Point(1.0, 2.0)),
                    ('b', TIMESTAMPTZ '2026-02-03 02:00:00+00', 'sentinel-2b',
                     ST_Point(3.0, 4.0)))
                  v(id, datetime, platform, geometry))
            TO '{year_dir / "items.parquet"}' (FORMAT PARQUET)
        """)
        out = Path(td) / "catalog" / "year=2026"
        out.mkdir(parents=True)
        first = build_item(con, 2026,
                           discover(year_dir, 2026, False, None), None)
        (out / "2026.json").write_text(json.dumps(first, indent=2) + "\n")
        assert first["properties"]["table:row_count"] == 3

        # Only the tail is staged now, and the published archive will not answer.
        (year_dir / "items.parquet").unlink()
        written = json.loads((out / "2026.json").read_text())
        again = build_item(
            con, 2026,
            discover(year_dir, 2026, True, written, prober(UNKNOWN)), written)

    assert again["properties"]["table:row_count"] == 3
    assert again["assets"]["data"]["table:row_count"] == 2
    assert again["properties"]["start_datetime"] == "2026-01-02T01:00:00Z"
    assert again["properties"]["s2:platforms"] == [
        "sentinel-2a", "sentinel-2b", "sentinel-2c"]


def staged_zone_parts(directory: Path, year: int = 2026,
                      tier=ZONE_PARTS) -> Path:
    """Tiny zone parts of one tier and nothing else: the shape of a year
    from 2019 (four quartiles) or from 2021 (eight octants)."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL spatial; LOAD spatial;")
    year_dir = directory / f"year={year}"
    year_dir.mkdir(parents=True)
    for i, (label, lo, hi) in enumerate(tier):
        rows = ", ".join(
            f"('{label}_{z}', TIMESTAMPTZ '{year}-0{i + 1}-{z % 28 + 1:02d} "
            f"10:00:00+00', 'sentinel-2{'ab'[z % 2]}', '{z}UFU', "
            f"ST_Point({z * 6 - 183}, {i * 10}))"
            for z in range(lo, hi + 1))
        con.execute(f"""
            COPY (SELECT * FROM (VALUES {rows})
                  v(id, datetime, platform, "s2:mgrs_tile", geometry))
            TO '{year_dir / f"{label}.parquet"}' (FORMAT PARQUET)
        """)
    return year_dir


def test_zone_split_year_gets_one_asset_per_part():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_zone_parts(Path(td))
        parts = discover(year_dir, 2026, False, None)
        assert [(p["key"], p["source"]) for p in parts] == \
            [(key, "local") for key in ZONE_KEYS]
        item = build_item(connect(), 2026, parts, None)

    assets = item["assets"]
    assert list(assets) == ZONE_KEYS
    for label, lo, hi in ZONE_PARTS:
        asset = assets[f"data-{label}"]
        assert asset["href"] == f"./{label}.parquet"
        assert asset["title"] == f"2026 scenes, UTM zones {lo}\u2013{hi}"
        assert asset["roles"] == ["data"]
        assert asset["table:row_count"] == hi - lo + 1
        assert asset["file:size"] > 0
    # 60 zones, one row each, summed across the four parts.
    assert item["properties"]["table:row_count"] == 60
    assert item["properties"]["start_datetime"] == "2026-01-02T10:00:00Z"
    assert item["properties"]["end_datetime"].startswith("2026-04-")
    assert item["properties"]["s2:platforms"] == ["sentinel-2a", "sentinel-2b"]
    # The bbox spans every part (zone 1 sits at -177, zone 60 at 177).
    assert item["bbox"][0] <= -177 and item["bbox"][2] >= 177


def test_octant_year_gets_one_asset_per_part():
    """An eight-part year (2021 onward): eight assets, no quartile asset,
    no items asset, totals summed over the octants."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_zone_parts(Path(td), 2021, ZONE_PARTS_8)
        parts = discover(year_dir, 2021, False, None)
        assert [(p["key"], p["source"]) for p in parts] == \
            [(key, "local") for key in OCTANT_KEYS]
        item = build_item(connect(), 2021, parts, None)

    assets = item["assets"]
    assert list(assets) == OCTANT_KEYS
    assert not set(assets) & {"data", *ZONE_KEYS}
    for label, lo, hi in ZONE_PARTS_8:
        asset = assets[f"data-{label}"]
        assert asset["href"] == f"./{label}.parquet"
        assert asset["title"] == f"2021 scenes, UTM zones {lo}\u2013{hi}"
        assert asset["table:row_count"] == hi - lo + 1
    assert item["properties"]["table:row_count"] == 60
    assert item["properties"]["start_datetime"].startswith("2021-01-")
    assert item["properties"]["end_datetime"].startswith("2021-08-")
    assert item["bbox"][0] <= -177 and item["bbox"][2] >= 177


def test_octant_year_is_discovered_remotely_among_fourteen_candidates():
    """--remote-baseline on an octant year with only live staged: the
    eight octants answer PRESENT, the other five candidates 404, and the
    year is the eight remote parts plus the local tail."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        parts = discover(year_dir, 2026, True, None, prober(
            ABSENT, **{name: (PRESENT, 100 + i)
                       for i, name in enumerate(OCTANT_NAMES)}))
    assert [(p["key"], p["source"]) for p in parts] == \
        [*((key, "remote") for key in OCTANT_KEYS), ("live", "local")]
    assert [p["size"] for p in parts][:8] == list(range(100, 108))


def test_zone_part_round_trips_through_the_fallback():
    """Run one measures the four parts and writes the item. Run two: only
    the tail is staged and the network gives no answers -- each zone part
    comes back from its own asset record, and the legacy items.parquet,
    which the item never recorded, is not invented."""
    with tempfile.TemporaryDirectory() as td:
        staged = Path(td) / "staged"
        year_dir = staged_zone_parts(staged)
        con = connect()
        first = build_item(con, 2026, discover(year_dir, 2026, False, None), None)
        written = json.loads(json.dumps(first))
        for name in ZONE_NAMES:
            (year_dir / name).unlink()
        staged_live(Path(td) / "later")
        (Path(td) / "later" / "year=2026" / "live.parquet").rename(
            year_dir / "live.parquet")
        parts = discover(year_dir, 2026, True, written, prober(UNKNOWN))
        assert [(p["key"], p["source"]) for p in parts] == \
            [*((key, "committed") for key in ZONE_KEYS), ("live", "local")]
        again = build_item(con, 2026, parts, written)

    assert again["properties"]["table:row_count"] == 61
    for key in ZONE_KEYS:
        assert again["assets"][key]["table:row_count"] == \
            first["assets"][key]["table:row_count"]
        assert again["assets"][key]["file:size"] == \
            first["assets"][key]["file:size"]
    assert "data" not in again["assets"]
    assert again["properties"]["s2:platforms"] == [
        "sentinel-2a", "sentinel-2b", "sentinel-2c"]


def test_a_recorded_zone_part_that_vanished_is_fatal():
    with tempfile.TemporaryDirectory() as td:
        staged = Path(td) / "staged"
        year_dir = staged_zone_parts(staged)
        written = build_item(connect(), 2026,
                             discover(year_dir, 2026, False, None), None)
        (year_dir / "z36-46.parquet").unlink()
        with pytest.raises(SystemExit) as caught:
            discover(year_dir, 2026, True, written,
                     prober(UNKNOWN, **{"z36-46.parquet": (ABSENT, None)}))
    assert "z36-46.parquet" in str(caught.value)


def test_unrecorded_unknown_part_is_left_out_when_the_year_has_a_record(capsys):
    """The ruling (Task 18): a candidate the committed item never recorded,
    whose probe cannot answer, is left out -- the year is built from what
    the record holds, which it can never be smaller than -- and the run
    says so on stderr. A RECORDED part whose probe cannot answer still falls
    back to its record (test_unreachable_part_falls_back_to_the_committed_record),
    and a recorded part that answers 404 still halts
    (test_absent_part_the_committed_item_describes_is_fatal)."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        committed = committed_item()
        parts = discover(year_dir, 2026, True, committed,
                         prober(ABSENT, **{"items.parquet": (UNKNOWN, None),
                                           "z21-35.parquet": (UNKNOWN, None)}))
        assert [(p["key"], p["source"]) for p in parts] == [
            ("data", "committed"), ("live", "local")]
        item = build_item(connect(), 2026, parts, committed)
    assert item["properties"]["table:row_count"] == 9_000_001
    assert "data-z21-35" not in item["assets"]
    err = capsys.readouterr().err
    assert "z21-35.parquet" in err and "left out" in err
    assert "items.parquet" not in err


def test_unrecorded_unknown_part_halts_only_without_a_record():
    """Nothing committed at all: a probe that cannot answer stops the run,
    because there is no record to keep the year from shrinking."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        with pytest.raises(SystemExit):
            discover(year_dir, 2026, True, None,
                     prober(ABSENT, **{"z21-35.parquet": (UNKNOWN, None)}))


# ---------------------------------------------------------------------------
# --collection sentinel-2-c1-l2a: two part candidates, its own public base,
# and a collection.json generated from the config.
# ---------------------------------------------------------------------------
import os  # noqa: E402
import subprocess  # noqa: E402

import s2_collections as cols  # noqa: E402
from make_items import PARTS, parts_for  # noqa: E402

C1 = cols.get("sentinel-2-c1-l2a")
FIRST = cols.get(cols.DEFAULT)


def staged_c1_year(directory: Path, year: int = 2026) -> Path:
    """One built Collection 1 year (the full 57-column schema, native
    GEOMETRY, uniform row groups, tile-major) plus a tail, through the
    real build, so the generators read what s2_build --collection writes."""
    sys.path.insert(0, str(ROOT / "tests"))
    from test_build import _build_c1, _mk_c1_full_chunk
    chunks = directory / "chunks" / "api"
    chunks.mkdir(parents=True)
    _mk_c1_full_chunk(chunks / "a.parquet", rows=300, months=3)
    out = directory / "publish" / "sentinel-2-c1-l2a"
    proc = _build_c1(out, chunks, ["--row-group-size", "100"], years=str(year))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    year_dir = out / f"year={year}"
    assert (year_dir / "items.parquet").is_file()
    # The tail: the same rows built again as the live part of one month.
    tail = directory / "tail"
    proc = _build_c1(tail, chunks, ["--months", "3", "--name", "live-03.parquet"],
                     years=str(year))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    (tail / f"year={year}" / "live-03.parquet").rename(year_dir / "live-03.parquet")
    return year_dir


def test_c1_parts_are_the_year_file_and_the_monthly_tail():
    """No zone split, so a Collection 1 year has the year file, the twelve
    monthly live parts and the single live.parquet it published before
    those existed -- and the first collection keeps its fourteen, PARTS
    being that list."""
    assert [(key, name) for key, name, _, _ in parts_for(C1)] == [
        ("data", "items.parquet"), ("live", "live.parquet"),
        *((f"live-{m:02d}", f"live-{m:02d}.parquet") for m in range(1, 13))]
    assert parts_for(FIRST) == PARTS and len(PARTS) == 14
    assert parts_for() == PARTS


def test_c1_discover_probes_the_c1_base_and_only_two_names():
    """--remote-baseline for Collection 1 asks about two files under the
    Collection 1 public base, never a zone part."""
    asked = []

    def probe(url):
        asked.append(url)
        return PRESENT, 7

    with tempfile.TemporaryDirectory() as td:
        year_dir = Path(td) / "year=2026"
        year_dir.mkdir()
        parts = discover(year_dir, 2026, True, None, probe, config=C1)
    assert asked == [f"{C1.public_base}/year=2026/items.parquet",
                     f"{C1.public_base}/year=2026/live.parquet",
                     *(f"{C1.public_base}/year=2026/live-{m:02d}.parquet"
                       for m in range(1, 13))]
    assert not any("z01-20" in url for url in asked)
    assert C1.public_base.endswith("/sentinel-2-c1-l2a")
    assert [(p["key"], p["source"]) for p in parts] == [
        ("data", "remote"), ("live", "remote"),
        *((f"live-{m:02d}", "remote") for m in range(1, 13))]


def test_c1_year_item_links_items_and_the_months_that_exist():
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_c1_year(Path(td))
        parts = discover(year_dir, 2026, False, None, config=C1)
        # Only the month that is staged: the other eleven and the
        # pre-monthly live.parquet are not there, and nothing probes for
        # them without --remote-baseline.
        assert [(p["key"], p["source"]) for p in parts] == [
            ("data", "local"), ("live-03", "local")]
        item = build_item(connect(), 2026, parts, None, config=C1)

    assert item["collection"] == "sentinel-2-c1-l2a"
    assert item["id"] == "2026"
    assert item["properties"]["title"] == "Sentinel-2 Collection 1 L2A scenes, 2026"
    assert [a["href"] for a in item["assets"].values()] == [
        "./items.parquet", "./live-03.parquet"]
    assert item["assets"]["data"]["title"] == "2026 scenes, GeoParquet 2.0"
    # Collection 1's tail is one file per month, merged by the fold on
    # RAILS, not consolidated.
    assert item["assets"]["live-03"]["title"] == (
        "March 2026 tail, refreshed daily since the last fold")
    assert PARTS[-1][2] == "Rolling tail since the last consolidation, refreshed daily"
    assert item["assets"]["data"]["table:row_count"] == 300
    # The year file holds every month; the tail holds March alone, so the
    # year's total is the sum of the parts that exist.
    assert item["assets"]["live-03"]["table:row_count"] == 100
    assert item["properties"]["table:row_count"] == 400
    assert item["properties"]["start_datetime"].startswith("2026-01-01T")
    assert item["properties"]["end_datetime"].startswith("2026-03-")
    assert item["properties"]["s2:platforms"] == ["sentinel-2b"]
    assert all(link["rel"] != "self" for link in item["links"])
    parent = next(l for l in item["links"] if l["rel"] == "parent")
    assert parent["title"] == "Sentinel-2 Collection 1 L2A scenes (item index)"


def test_c1_year_item_sums_over_whatever_months_exist():
    """The year's row count and extent sum over the monthly live parts that
    are there. Two staged months plus the year file, with the other ten
    months and the pre-monthly live.parquet answering 404: every month
    present is its own asset, the absent ones are left out, and the totals
    are the sum."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_c1_year(Path(td))
        sys.path.insert(0, str(ROOT / "tests"))
        from test_build import _build_c1
        second = Path(td) / "tail2"
        proc = _build_c1(second, Path(td) / "chunks" / "api",
                         ["--months", "2", "--name", "live-02.parquet"],
                         years="2026")
        assert proc.returncode == 0, proc.stdout + proc.stderr
        (second / "year=2026" / "live-02.parquet").rename(
            year_dir / "live-02.parquet")
        parts = discover(year_dir, 2026, True, None, prober(ABSENT),
                         config=C1)
        assert [(p["key"], p["source"]) for p in parts] == [
            ("data", "local"), ("live-02", "local"), ("live-03", "local")]
        item = build_item(connect(), 2026, parts, None, config=C1)
    assert item["properties"]["table:row_count"] == 300 + 100 + 100
    assert [a["table:row_count"] for a in item["assets"].values()] == [300, 100, 100]
    assert item["assets"]["live-02"]["start_datetime"].startswith("2026-02-")
    assert item["assets"]["live-03"]["start_datetime"].startswith("2026-03-")
    assert item["properties"]["start_datetime"].startswith("2026-01-01T")


def test_c1_collection_json_comes_from_the_config():
    """make_collection --collection sentinel-2-c1-l2a on a staged year with
    no committed items: the collection's id, glob, canonical link,
    item_assets and columns are Collection 1's, and nothing about it is
    the first collection's."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_c1_year(Path(td))
        out_dir = Path(td) / "catalog" / "sentinel-2-c1-l2a"
        out_dir.mkdir(parents=True)
        proc = subprocess.run(
            [sys.executable, "tools/make_collection.py",
             "--collection", "sentinel-2-c1-l2a",
             "--data-dir", str(year_dir.parent),
             "--out", str(out_dir / "collection.json")],
            cwd=ROOT, env=dict(os.environ), capture_output=True, text=True)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        coll = json.loads((out_dir / "collection.json").read_text())

    assert coll["id"] == "sentinel-2-c1-l2a"
    assert coll["title"] == "Sentinel-2 Collection 1 L2A scenes (item index)"
    assert coll["partition:glob"].endswith("/sentinel-2-c1-l2a/year=*/*.parquet")
    assert "sentinel-2-l2a/" not in coll["partition:glob"]
    assert coll["partition:file_count"] == 2
    assert coll["table:row_count"] == 400
    assert coll["extent"]["temporal"]["interval"][0][0].startswith("2026-01-01T")
    assert all(link["rel"] != "self" for link in coll["links"])
    canonical = next(l for l in coll["links"] if l["rel"] == "canonical")
    assert canonical["href"] == (
        "https://earth-search.aws.element84.com/v1/collections/sentinel-2-c1-l2a")
    # No item link: the year has no committed item yet.
    assert [l for l in coll["links"] if l["rel"] == "item"] == []

    names = [c["name"] for c in coll["table:columns"]]
    assert names == [name for name, _, _ in C1.schema.COLUMNS]
    assert "_tile" in names and "s2:mgrs_tile" not in names
    types = {c["name"]: c["type"] for c in coll["table:columns"]}
    assert types["storage:requester_pays"] == "bool"
    assert types["created"] == "timestamp[us, tz=UTC]"
    assert types["_tile"] == "string"

    # The description names the tile column and the Collection 1 layout,
    # and says nothing about the first collection's zone parts.
    desc = coll["description"]
    assert "`_tile`" in desc
    # Spec Amendment 1: tile-major sort, uniform groups near 6,000 rows.
    assert "ordered by MGRS tile, then acquisition time" in desc
    assert "one contiguous run" in desc
    assert "uniform row groups of about 6,000 rows" in desc
    assert "month-aligned" not in desc and "Hilbert" not in desc
    assert "live-01.parquet to live-12.parquet" in desc and "z01-20" not in desc
    assert "the live part of the month each scene was acquired in" in desc
    # The one mention of the first collection's tile column is the negation.
    assert desc.count("s2:mgrs_tile") == 1 and "no `s2:mgrs_tile`" in desc
    key_text = coll["partition:keys"][0]["description"]
    assert "s2:mgrs_tile" not in key_text and "z01-20" not in key_text
    assert "uniform row groups of about 6,000 rows" in key_text

    # item_assets from the committed Collection 1 cache: the keys the first
    # collection's template lacks are here, the per-scene proj fields are not.
    assets = coll["item_assets"]
    assert {"cloud", "snow", "preview", "thumbnail"} <= set(assets)
    assert "visual-jp2" not in assets
    assert assets["thumbnail"]["type"] == "image/jpeg"
    assert not any(k.startswith("proj:") for a in assets.values() for k in a)
