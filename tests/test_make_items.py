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
from s2_build import ZONE_PARTS  # noqa: E402

ZONE_NAMES = [f"{label}.parquet" for label, _, _ in ZONE_PARTS]
ZONE_KEYS = [f"data-{label}" for label, _, _ in ZONE_PARTS]


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
    """A year may hold the legacy file, the four zone parts and the tail;
    whatever answers PRESENT is read, in the advertised order."""
    with tempfile.TemporaryDirectory() as td:
        year_dir = staged_live(Path(td))
        parts = discover(year_dir, 2026, True, None, prober(PRESENT, 1))
    assert [p["name"] for p in parts] == \
        ["items.parquet", *ZONE_NAMES, "live.parquet"]
    assert [p["key"] for p in parts] == ["data", *ZONE_KEYS, "live"]


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


def staged_zone_parts(directory: Path, year: int = 2026) -> Path:
    """Four tiny zone parts and nothing else: the shape of a year from 2019."""
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL spatial; LOAD spatial;")
    year_dir = directory / f"year={year}"
    year_dir.mkdir(parents=True)
    for i, (label, lo, hi) in enumerate(ZONE_PARTS):
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
