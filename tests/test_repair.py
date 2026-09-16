"""Bucket repair (s2_repair.py) and inventory audit (s2_audit.py).

Only one real network call in this whole file: fetching the verified
S2C_53HNV_20260910_0_L2A static item JSON, to prove the bucket's item shape
still normalizes onto the canonical schema (same style as
test_fetch.py's live-normalize test). Everything else -- discovery, fetch,
month-file naming, and the audit's aggregation -- runs against injected
listers/getters or local fixtures, no S3.
"""
import csv
import json
import sys
import tempfile
import threading
import urllib.request
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

from s2_schema import COLUMNS  # noqa: E402
from s2_fetch import normalize  # noqa: E402
from s2_repair import (  # noqa: E402
    _is_valid_zone, discover_scenes, fetch_and_write, repair_month)
from s2_audit import audit, expected_month_counts, have_month_counts  # noqa: E402

STATIC_ITEM_URL = ("https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
                   "sentinel-s2-l2a-cogs/53/H/NV/2026/9/"
                   "S2C_53HNV_20260910_0_L2A/S2C_53HNV_20260910_0_L2A.json")
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]


# --------------------------------------------------------------------------
# normalize-compatibility: the ONE live fetch in this file
# --------------------------------------------------------------------------

def test_static_item_normalizes_to_canonical_schema():
    req = urllib.request.Request(STATIC_ITEM_URL,
                                 headers={"User-Agent": "sentinel-2-catalog-tools/1.0"})
    item = json.load(urllib.request.urlopen(req, timeout=60))
    row = normalize(item)
    want = {c[0] for c in DATA_COLUMNS if c[0] != "geometry"} | {"_geometry_json"}
    assert set(row) == want
    assert row["id"] == "S2C_53HNV_20260910_0_L2A"
    assert row["s2:mgrs_tile"] == "53HNV"
    a = json.loads(row["assets"])
    assert len(a) == 38
    assert a["red"]["href"].startswith("https://")
    assert len(item["links"]) == 4
    assert {l["rel"] for l in item["links"]} == {
        "self", "canonical", "license", "derived_from"}


def test_is_valid_zone_excludes_the_stray_root_prefix():
    """Live discovery (2026-09-16): the bucket root holds one stray,
    non-tile-structured prefix -- "sentinel-s2-l2a-cogs/2019/" -- alongside
    the 60 real zones. It must be excluded or the crawl balloons by ~65k
    needless LISTs. Real UTM zones are numeric strings 1-60."""
    assert _is_valid_zone("sentinel-s2-l2a-cogs/1/")
    assert _is_valid_zone("sentinel-s2-l2a-cogs/60/")
    assert not _is_valid_zone("sentinel-s2-l2a-cogs/2019/")
    assert not _is_valid_zone("sentinel-s2-l2a-cogs/0/")
    assert not _is_valid_zone("sentinel-s2-l2a-cogs/61/")


# --------------------------------------------------------------------------
# discovery/fetch logic against injected lister/getter
# --------------------------------------------------------------------------

def test_discover_scenes_builds_https_urls_and_uses_unpadded_month():
    """discover_scenes threads its LISTs across `workers` (finding #4 fix
    round), so call order is not guaranteed -- assert on the SET of calls
    and results, not list order."""
    calls = []
    lock = threading.Lock()

    def fake_list(prefix):
        with lock:
            calls.append(prefix)
        if prefix == "sentinel-s2-l2a-cogs/31/U/FU/2018/9/":
            return ["sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/"]
        return []

    scenes = discover_scenes(["sentinel-s2-l2a-cogs/31/U/FU/",
                             "sentinel-s2-l2a-cogs/32/V/MJ/"],
                             "2018", "9", fake_list, workers=4)
    # single-digit month in every prefix passed to list_fn, never "09"
    assert set(calls) == {"sentinel-s2-l2a-cogs/31/U/FU/2018/9/",
                          "sentinel-s2-l2a-cogs/32/V/MJ/2018/9/"}
    assert scenes == [
        ("S2A_31UFU_20180905_0_L2A",
         "https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
         "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/"
         "S2A_31UFU_20180905_0_L2A.json")]


def test_discover_scenes_uses_default_workers_when_not_given():
    """`workers` has a default, so existing call sites with a positional
    list_fn keep working without threading it through explicitly."""
    scenes = discover_scenes(["sentinel-s2-l2a-cogs/31/U/FU/"], "2018", "9",
                             lambda p: ["sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_X_0_L2A/"])
    assert scenes == [("S2A_X_0_L2A",
                       "https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
                       "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_X_0_L2A/S2A_X_0_L2A.json")]


def _fake_item(item_id: str, month: str = "2018-09-05T10:00:00Z") -> dict:
    return {
        "type": "Feature", "stac_version": "1.0.0", "stac_extensions": [],
        "id": item_id,
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        "bbox": [0, 0, 1, 1],
        "collection": "sentinel-2-l2a",
        "links": [{"href": "https://x/self.json", "rel": "self"}],
        "assets": {"red": {"href": "https://x/B04.tif"}},
        "properties": {
            "datetime": month, "platform": "sentinel-2a",
            "constellation": "sentinel-2", "instruments": ["msi"],
            "proj:epsg": 32631, "mgrs:utm_zone": 31,
            "mgrs:latitude_band": "U", "mgrs:grid_square": "FU",
            "eo:cloud_cover": 12.5,
            "s2:product_uri": "S2A_MSIL2A_20180905T000000_N0000_R000_T31UFU_20180905T000000.SAFE",
        },
    }


def test_fetch_and_write_streams_normalized_rows_to_ndjson():
    """fetch_and_write must not accumulate rows in memory (finding #3 fix
    round): it writes each normalize()d row straight to the NDJSON file at
    nd_path as its future completes. Verified here by reading the file
    back, not by inspecting a returned list."""
    scenes = [("A1", "https://x/A1.json"), ("A2", "https://x/A2.json")]
    items = {"https://x/A1.json": _fake_item("A1"),
            "https://x/A2.json": _fake_item("A2")}
    with tempfile.TemporaryDirectory() as td:
        nd = str(Path(td) / "rows.ndjson")
        n = fetch_and_write(scenes, lambda url: items[url], nd, workers=2)
        assert n == 2
        rows = [json.loads(line) for line in Path(nd).read_text().splitlines()]
        assert {r["id"] for r in rows} == {"A1", "A2"}
        assert all(r["s2:mgrs_tile"] == "31UFU" for r in rows)


# --------------------------------------------------------------------------
# repair_month: end-to-end against injected lister/getter, real parquet out
# --------------------------------------------------------------------------

def test_repair_month_writes_canonical_chunk():
    def fake_list(prefix):
        if prefix.endswith("2018/9/"):
            sid = "S2A_31UFU_2018090" + prefix[-4]
            return [f"sentinel-s2-l2a-cogs/31/U/FU/2018/9/{sid}_0_L2A/"]
        return []

    def fake_get(url):
        item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
        return _fake_item(item_id)

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         workers=2, list_fn=fake_list, get_fn=fake_get)
        assert n == 1
        dest = out / "repair" / "2018-09-01_2018-09-30.parquet"
        assert dest.exists()
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{dest}')").fetchall()
        got = [d[0] for d in desc]
        want = [n for n, _, _ in DATA_COLUMNS]
        assert got == want, "repair chunk must match s2_fetch's canonical schema"


def test_repair_month_skips_when_chunk_exists():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        dest_dir = out / "repair"
        dest_dir.mkdir()
        existing = dest_dir / "2019-01-01_2019-01-31.parquet"
        existing.write_bytes(b"not-empty")

        def boom(*a, **k):
            raise AssertionError("must not be called when chunk already exists")

        n = repair_month("2019-01", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         list_fn=boom, get_fn=boom)
        assert n == 0
        assert existing.read_bytes() == b"not-empty"


def test_repair_month_zero_scenes_writes_sentinel():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2019-02", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         list_fn=lambda p: [], get_fn=lambda u: {})
        assert n == 0
        dest = out / "repair" / "2019-02-01_2019-02-28.parquet"
        assert dest.exists() and dest.stat().st_size == 0


# --------------------------------------------------------------------------
# audit aggregation against fixtures (no S3)
# --------------------------------------------------------------------------

def _write_inventory_csv(path: Path, rows: list[tuple[str, str, str, str]]) -> None:
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)


def _write_staged_parquet(con, path: Path, rows: list[tuple[str, str, int]]) -> None:
    """rows: (id, iso_datetime, month)"""
    vals = ", ".join(f"('{i}', TIMESTAMPTZ '{d}', {m})" for i, d, m in rows)
    con.execute(f"""
        COPY (SELECT * FROM (VALUES {vals}) t(id, datetime, _month))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_expected_counts_exclude_tileinfo_and_handle_1_and_2_digit_months():
    with tempfile.TemporaryDirectory() as td:
        inv = Path(td) / "inventory.csv"
        _write_inventory_csv(inv, [
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/"
             "S2A_31UFU_20180905_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/"
             "tileinfo_metadata.json", "50", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/32/V/MJ/2018/12/S2A_32VMJ_20181215_0_L2A/"
             "S2A_32VMJ_20181215_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
        ])
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        counts = expected_month_counts(con, [str(inv)])
        assert counts == {(2018, 9): 1, (2018, 12): 1}, (
            "tileinfo_metadata.json must be excluded and both 1- and "
            "2-digit months must parse")


def test_expected_counts_exclude_stray_root_keys_without_crashing():
    """Regression net for finding #1 (fix round 1): a real inventory
    contains ~64k keys like this one -- self-named, ends in .json, but with
    NO {yyyy}/{m}/ pair at all because they sit directly under the bogus
    "sentinel-s2-l2a-cogs/2019/" root prefix (see s2_repair.py's
    _is_valid_zone() note). Before the fix, regexp_extract() found no match,
    returned '', and CAST('' AS INTEGER) raised -- crashing the whole query
    instead of excluding the row. It must now be silently excluded."""
    with tempfile.TemporaryDirectory() as td:
        inv = Path(td) / "inventory.csv"
        _write_inventory_csv(inv, [
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/2019/S2B_36KZC_20190806_0_L2A/"
             "S2B_36KZC_20190806_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/"
             "S2A_31UFU_20180905_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
        ])
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        counts = expected_month_counts(con, [str(inv)])  # must not raise
        assert counts == {(2018, 9): 1}


def test_expected_counts_exclude_non_self_named_json():
    """Regression net for finding #2 (fix round 1): the brief's rule is
    specifically {id}/{id}.json (self-named), not just "any .json that
    isn't tileinfo_metadata.json". A differently-named JSON dropped in a
    real scene directory must also be excluded."""
    with tempfile.TemporaryDirectory() as td:
        inv = Path(td) / "inventory.csv"
        _write_inventory_csv(inv, [
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/32/V/MJ/2018/12/S2A_32VMJ_20181215_0_L2A/"
             "other_metadata.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/32/V/MJ/2018/12/S2A_32VMJ_20181215_0_L2A/"
             "S2A_32VMJ_20181215_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
        ])
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        counts = expected_month_counts(con, [str(inv)])
        assert counts == {(2018, 12): 1}, (
            "only the self-named {id}/{id}.json row may count")


def test_audit_delta_table_and_exit_codes():
    with tempfile.TemporaryDirectory() as td:
        inv = Path(td) / "inventory.csv"
        _write_inventory_csv(inv, [
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180901_0_L2A/"
             "S2A_31UFU_20180901_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180902_0_L2A/"
             "S2A_31UFU_20180902_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/10/S2A_31UFU_20181001_0_L2A/"
             "S2A_31UFU_20181001_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
        ])
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
        published = Path(td) / "staging" / "sentinel-2-l2a" / "year=2018"
        published.mkdir(parents=True)
        # September: only 1 of 2 expected published (repair candidate).
        # October: exactly matches.
        _write_staged_parquet(con, published / "items.parquet", [
            ("A1", "2018-09-01 10:00:00+00", 9),
            ("B1", "2018-10-01 10:00:00+00", 10),
        ])

        rows, bad = audit(str(Path(td) / "staging"), [str(inv)], months=None,
                          tolerance=0)
        by_month = {(y, m): (e, h, d) for y, m, e, h, d in rows}
        assert by_month[(2018, 9)] == (2, 1, -1)
        assert by_month[(2018, 10)] == (1, 1, 0)
        assert bad is True, "a -1 delta must fail at tolerance 0"

        rows, bad = audit(str(Path(td) / "staging"), [str(inv)], months=None,
                          tolerance=1)
        assert bad is False, "a delta of 1 must pass at tolerance 1"


def test_audit_months_filter_and_out_csv():
    with tempfile.TemporaryDirectory() as td:
        inv = Path(td) / "inventory.csv"
        _write_inventory_csv(inv, [
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180901_0_L2A/"
             "S2A_31UFU_20180901_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
            ("sentinel-cogs",
             "sentinel-s2-l2a-cogs/31/U/FU/2018/10/S2A_31UFU_20181001_0_L2A/"
             "S2A_31UFU_20181001_0_L2A.json", "100", "2024-01-01T00:00:00Z"),
        ])
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'; INSTALL spatial; LOAD spatial;")
        published = Path(td) / "staging" / "sentinel-2-l2a" / "year=2018"
        published.mkdir(parents=True)
        _write_staged_parquet(con, published / "items.parquet", [
            ("A1", "2018-09-01 10:00:00+00", 9),
        ])
        out_csv = Path(td) / "audit.csv"
        rows, bad = audit(str(Path(td) / "staging"), [str(inv)],
                          months=["2018-09"], tolerance=0, out_csv=str(out_csv))
        assert [(y, m) for y, m, *_ in rows] == [(2018, 9)], (
            "--months must prune the October row entirely")
        assert bad is False
        assert out_csv.exists()
        with open(out_csv) as f:
            r = list(csv.reader(f))
        assert r[0] == ["year", "month", "expected", "have", "delta"]
        assert r[1] == ["2018", "9", "1", "1", "0"]


def test_have_month_counts_reads_local_staging_dir():
    with tempfile.TemporaryDirectory() as td:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        published = Path(td) / "sentinel-2-l2a" / "year=2020"
        published.mkdir(parents=True)
        _write_staged_parquet(con, published / "items.parquet", [
            ("Z1", "2020-06-01 10:00:00+00", 6),
            ("Z2", "2020-06-15 10:00:00+00", 6),
        ])
        counts = have_month_counts(con, str(td))
        assert counts == {(2020, 6): 2}
