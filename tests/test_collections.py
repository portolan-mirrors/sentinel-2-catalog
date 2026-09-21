"""The per-collection config registry and the frozen Collection 1 schema.
The module is `s2_collections`, not `collections`: the stdlib module of
that name is already in sys.modules when any test runs, so a tools/
`collections.py` could never be imported by that name."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import s2_collections as cols         # noqa: E402  (tools/s2_collections.py)
import s2c1_schema, s2_schema         # noqa: E402

_FIXTURES = Path(__file__).parent / "fixtures"
# Baseline 05.13 (2026) and 05.00 (2019): the two upstream property shapes.
FIX = json.loads((_FIXTURES / "c1_item.json").read_text())
FIX_2019 = json.loads((_FIXTURES / "c1_item_2019.json").read_text())
BOTH = pytest.mark.parametrize("fix", [FIX, FIX_2019], ids=["2026-b05.13", "2019-b05.00"])


def test_default_is_the_first_collection():
    c = cols.get(cols.DEFAULT)
    assert c.id == "sentinel-2-l2a" and c.tile_column == "s2:mgrs_tile"
    assert c.schema is s2_schema and c.zone_split is True
    assert c.row_group_mode == "uniform" and c.lookback_field == "datetime"
    # None: s2_build falls back to its own ROW_GROUP, the single source.
    assert c.row_group_size is None and c.live_zstd_level == 18


def test_names_and_public_base():
    assert cols.NAMES == ("sentinel-2-l2a", "sentinel-2-c1-l2a")
    assert cols.get("sentinel-2-c1-l2a").public_base == (
        "https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a")


def test_c1_config():
    c = cols.get("sentinel-2-c1-l2a")
    assert c.tile_column == "_tile" and c.schema is s2c1_schema
    assert c.zone_split is False and c.row_group_mode == "month_aligned"
    assert c.row_group_size == 20_000 and c.live_zstd_level == 3
    assert c.lookback_field == "created"
    assert c.bucket == "e84-earth-search-sentinel-data"
    assert c.key_root == "sentinel-2-c1-l2a/"
    assert c.catalog_dir == "sentinel-2-c1-l2a" and c.stats_dir == "stats-c1"


def test_c1_id_regex_and_scene_day():
    c = cols.get("sentinel-2-c1-l2a")
    m = c.id_re.match("S2B_T31UET_20260921T105030_L2A")
    assert m and m.group("tile") == "31UET" and m.group("day") == "20260921"
    assert cols.get(cols.DEFAULT).id_re.match("S2A_31UFU_20220127_0_L2A").group("tile") == "31UFU"


def test_unknown_collection_names_the_valid_ones():
    with pytest.raises(SystemExit, match="sentinel-2-c1-l2a"):
        cols.get("nope")


def test_add_collection_arg_defaults_to_the_first_collection():
    import argparse
    ap = argparse.ArgumentParser()
    cols.add_collection_arg(ap)
    assert ap.parse_args([]).collection == cols.DEFAULT
    assert ap.parse_args(["--collection", "sentinel-2-c1-l2a"]).collection == "sentinel-2-c1-l2a"
    with pytest.raises(SystemExit):
        ap.parse_args(["--collection", "nope"])


def test_c1_schema_freezes_helper_columns_last():
    names = [c[0] for c in s2c1_schema.COLUMNS]
    assert names[0] == "thumbnail_url"
    assert names[-4:] == ["_month", "_hilbert", "_tile", "geometry"]
    assert "assets" in names and "s2:mgrs_tile" not in names
    assert "grid:code" in names and "created" in names and "updated" in names
    assert len(names) == len(set(names))
    assert s2c1_schema.USER_AGENT is s2_schema.USER_AGENT


@BOTH
def test_c1_schema_covers_every_fixture_property(fix):
    names = {c[0] for c in s2c1_schema.COLUMNS}
    assert set(fix["properties"]) <= names
    assert set(fix) - {"properties", "geometry"} <= names


def test_c1_schema_covers_the_union_of_both_baselines():
    names = {c[0] for c in s2c1_schema.COLUMNS}
    union = set(FIX["properties"]) | set(FIX_2019["properties"])
    assert union <= names
    # The one property that separates the baselines is in the union, so
    # this test would catch dropping it again.
    assert "s2:dark_features_percentage" in union - set(FIX["properties"])


@BOTH
def test_c1_normalize_derives_tile_and_keeps_assets_verbatim(fix):
    row = s2c1_schema.normalize(fix)
    assert row["_tile"] == fix["properties"]["grid:code"].removeprefix("MGRS-")
    assert json.loads(row["assets"]) == fix["assets"]
    assert row["thumbnail_url"] == fix["assets"]["thumbnail"]["href"]
    assert row["collection"] == "sentinel-2-c1-l2a"
    assert set(row) == {c[0] for c in s2c1_schema.COLUMNS if c[0] not in ("_month", "_hilbert", "geometry")} | {"_geometry_json"}
    assert json.loads(row["processing:software"]) == fix["properties"]["processing:software"]
    assert row["proj:centroid"] == fix["properties"]["proj:centroid"]
    assert row["created"] == fix["properties"]["created"]
    assert not any(l["rel"] in ("next", "prev", "root", "parent") for l in row["links"])
    # Every upstream property lands in the row under its own name.
    for k, v in fix["properties"].items():
        if k not in ("proj:centroid", "processing:software"):
            assert row[k] == v, k


def test_c1_normalize_keeps_dark_features_on_old_baselines():
    assert "s2:dark_features_percentage" in FIX_2019["properties"]
    assert (s2c1_schema.normalize(FIX_2019)["s2:dark_features_percentage"]
            == FIX_2019["properties"]["s2:dark_features_percentage"])
    assert s2c1_schema.normalize(FIX)["s2:dark_features_percentage"] is None


def test_c1_normalize_counts_unknown_properties_without_raising():
    s2c1_schema.UNKNOWN_PROPERTIES.clear()
    s2c1_schema.normalize(FIX)
    assert not s2c1_schema.UNKNOWN_PROPERTIES
    f = json.loads(json.dumps(FIX))
    f["properties"]["s2:brand_new"] = 1
    row = s2c1_schema.normalize(f)
    s2c1_schema.normalize(f)
    assert "s2:brand_new" not in row
    assert s2c1_schema.UNKNOWN_PROPERTIES == {"s2:brand_new": 2}
    s2c1_schema.UNKNOWN_PROPERTIES.clear()


def test_c1_normalize_tile_fallback_and_error():
    f = json.loads(json.dumps(FIX))
    del f["properties"]["grid:code"]
    assert s2c1_schema.normalize(f)["_tile"] == "31UET"
    del f["properties"]["mgrs:grid_square"]
    with pytest.raises(ValueError, match="grid:code"):
        s2c1_schema.normalize(f)
