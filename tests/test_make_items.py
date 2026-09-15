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


def prober(state, size=None):
    """A probe that always gives the same answer."""
    return lambda url: (state, size)


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
        parts = discover(year_dir, 2026, True, None, prober(PRESENT, 4242))
    assert [(p["key"], p["source"]) for p in parts] == [
        ("data", "remote"), ("live", "local")]
    assert next(p for p in parts if p["key"] == "data")["size"] == 4242


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
