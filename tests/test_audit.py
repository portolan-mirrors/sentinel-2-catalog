"""Inventory audit (s2_audit.py) for both collections.

One real network call in this file: listing the C1 inventory bucket's
manifest partitions and reading its newest symlink.txt, to prove the pinned
inventory_prefix and the Parquet data-file format still hold. Everything
else -- key parsing, month aggregation, the delta table -- runs against
local fixtures, no S3.
"""
import csv
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import s2_collections as cols  # noqa: E402
from s2_audit import (  # noqa: E402
    KEY_RE, audit, expected_month_counts, have_month_counts, inventory_source,
    manifest_data_urls, resolve_manifest_key)

C1 = cols.get("sentinel-2-c1-l2a")


# --------------------------------------------------------------------------
# audit aggregation against fixtures (no S3) -- first collection, CSV
# inventory. `published_base` is the COLLECTION's base (the directory that
# holds year=*/), so a staging dir mirrors <PUBLIC>/<catalog_dir>.
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

        rows, bad = audit(str(Path(td) / "staging" / "sentinel-2-l2a"), [str(inv)], months=None,
                          tolerance=0)
        by_month = {(y, m): (e, h, d) for y, m, e, h, d in rows}
        assert by_month[(2018, 9)] == (2, 1, -1)
        assert by_month[(2018, 10)] == (1, 1, 0)
        assert bad is True, "a -1 delta must fail at tolerance 0"

        rows, bad = audit(str(Path(td) / "staging" / "sentinel-2-l2a"), [str(inv)], months=None,
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
        rows, bad = audit(str(Path(td) / "staging" / "sentinel-2-l2a"), [str(inv)],
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
        counts = have_month_counts(con, str(Path(td) / "sentinel-2-l2a"))
        assert counts == {(2020, 6): 2}


# --------------------------------------------------------------------------
# --collection sentinel-2-c1-l2a: Parquet inventory, C1 key root, and the
# pinned inventory_prefix.
# --------------------------------------------------------------------------

C1_ITEM_KEY = ("sentinel-2-c1-l2a/31/U/ET/2026/9/S2B_T31UET_20260921T105030_L2A/"
               "S2B_T31UET_20260921T105030_L2A.json")


def test_key_re_matches_a_c1_item_key():
    import re
    m = re.search(KEY_RE, C1_ITEM_KEY)
    assert m is not None
    assert m.groups() == ("2026", "9", "S2B_T31UET_20260921T105030_L2A",
                          "S2B_T31UET_20260921T105030_L2A")
    # The C1 sidecar is tileInfo.json (camel case, unlike the first
    # collection's tileinfo_metadata.json): it matches the tail regex but
    # not the self-named rule, which is what excludes it.
    m = re.search(KEY_RE, C1_ITEM_KEY.rsplit("/", 1)[0] + "/tileInfo.json")
    assert m is not None and m.group(3) != m.group(4)


def _write_inventory_parquet(con, path: Path, keys: list[str]) -> None:
    """The C1 inventory's data-file shape (checked live 2026-09-21):
    bucket, key, size, last_modified_date -- Parquet, with a header."""
    vals = ", ".join(f"('e84-earth-search-sentinel-data', '{k}', 100, "
                     f"TIMESTAMPTZ '2026-09-21 01:00:00+00')" for k in keys)
    con.execute(f"""
        COPY (SELECT * FROM (VALUES {vals}) t(bucket, key, size, last_modified_date))
        TO '{path}' (FORMAT PARQUET)
    """)


def test_inventory_source_picks_the_reader_from_the_file_suffix():
    assert inventory_source(["a.csv.gz", "b.csv.gz"]).startswith("read_csv(")
    assert inventory_source(["inv.csv"]).startswith("read_csv(")
    assert inventory_source(["a.parquet", "b.parquet"]).startswith("read_parquet(")
    with pytest.raises(SystemExit):
        inventory_source(["a.parquet", "b.csv.gz"])
    with pytest.raises(SystemExit):
        inventory_source(["a.orc"])
    with pytest.raises(SystemExit):
        inventory_source([])


def test_expected_counts_for_c1_read_parquet_under_the_c1_key_root():
    with tempfile.TemporaryDirectory() as td:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        inv = Path(td) / "inventory.parquet"
        _write_inventory_parquet(con, inv, [
            C1_ITEM_KEY,
            C1_ITEM_KEY.rsplit("/", 1)[0] + "/tileInfo.json",
            C1_ITEM_KEY.rsplit("/", 1)[0] + "/L2A_PVI.jpg",
            "sentinel-2-c1-l2a/31/U/ET/2025/12/S2A_T31UET_20251203T105401_L2A/"
            "S2A_T31UET_20251203T105401_L2A.json",
            # A first-collection key must not count toward C1.
            "sentinel-s2-l2a-cogs/31/U/FU/2026/9/S2A_31UFU_20260921_0_L2A/"
            "S2A_31UFU_20260921_0_L2A.json",
        ])
        counts = expected_month_counts(con, [str(inv)], config=C1)
        assert counts == {(2026, 9): 1, (2025, 12): 1}
        counts = expected_month_counts(con, [str(inv)], months=["2025-12"], config=C1)
        assert counts == {(2025, 12): 1}
        # The default config still filters on the first collection's root.
        counts = expected_month_counts(con, [str(inv)])
        assert counts == {(2026, 9): 1}


def test_have_month_counts_reads_items_and_live_parts_for_c1():
    with tempfile.TemporaryDirectory() as td:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        published = Path(td) / "sentinel-2-c1-l2a" / "year=2026"
        published.mkdir(parents=True)
        _write_staged_parquet(con, published / "items.parquet", [
            ("A", "2026-08-01 10:00:00+00", 8),
        ])
        _write_staged_parquet(con, published / "live.parquet", [
            ("B", "2026-09-21 10:00:00+00", 9),
            ("C", "2026-09-22 10:00:00+00", 9),
        ])
        counts = have_month_counts(con, str(Path(td) / "sentinel-2-c1-l2a"))
        assert counts == {(2026, 8): 1, (2026, 9): 2}


def test_audit_end_to_end_for_c1():
    with tempfile.TemporaryDirectory() as td:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC';")
        inv = Path(td) / "inventory.parquet"
        _write_inventory_parquet(con, inv, [
            C1_ITEM_KEY,
            "sentinel-2-c1-l2a/31/U/ET/2026/9/S2A_T31UET_20260923T105031_L2A/"
            "S2A_T31UET_20260923T105031_L2A.json",
        ])
        published = Path(td) / "staging" / "sentinel-2-c1-l2a" / "year=2026"
        published.mkdir(parents=True)
        _write_staged_parquet(con, published / "items.parquet", [
            ("S2B_T31UET_20260921T105030_L2A", "2026-09-21 10:50:30+00", 9),
        ])
        rows, bad = audit(str(Path(td) / "staging" / "sentinel-2-c1-l2a"), [str(inv)],
                          months=None, tolerance=0, config=C1)
        assert rows == [(2026, 9, 2, 1, -1)]
        assert bad is True


def test_c1_inventory_prefix_is_pinned_and_live():
    """The one live call: the pinned prefix resolves to a real newest
    partition whose symlink.txt lists Parquet data files (not CSV) under
    the same primary/ tree, over anonymous HTTPS."""
    assert C1.inventory_prefix == "e84-earth-search-sentinel-data/primary/hive/"
    assert cols.get(cols.DEFAULT).inventory_prefix == "sentinel-cogs/sentinel-cogs/hive/"
    import s2_audit
    s3 = s2_audit._s3_client()
    key = resolve_manifest_key(s3, config=C1)
    assert key.startswith(C1.inventory_prefix + "dt=") and key.endswith("/symlink.txt")
    urls = manifest_data_urls(s3, key, config=C1)
    assert len(urls) > 100
    assert all(u.startswith("https://e84-earth-search-sentinel-data-inventory"
                            ".s3.us-west-2.amazonaws.com/e84-earth-search-sentinel-data/"
                            "primary/data/") for u in urls)
    assert all(u.endswith(".parquet") for u in urls)
    assert inventory_source(urls).startswith("read_parquet(")


def test_resolve_manifest_key_refuses_a_collection_without_inventory():
    import dataclasses
    no_inv = dataclasses.replace(C1, inventory_prefix="")
    with pytest.raises(SystemExit, match="no inventory"):
        resolve_manifest_key(object(), config=no_inv)


def test_audit_cli_takes_collection():
    r = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "s2_audit.py"), "--help"],
        capture_output=True, text=True)
    assert r.returncode == 0
    assert "--collection" in r.stdout
    assert "sentinel-2-c1-l2a" in r.stdout
