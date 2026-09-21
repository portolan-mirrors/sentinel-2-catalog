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

FIX = json.loads((Path(__file__).parent / "fixtures" / "c1_item.json").read_text())


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


def test_c1_schema_covers_every_fixture_property():
    names = {c[0] for c in s2c1_schema.COLUMNS}
    assert set(FIX["properties"]) <= names
    assert set(FIX) - {"properties", "geometry"} <= names


def test_c1_normalize_derives_tile_and_keeps_assets_verbatim():
    row = s2c1_schema.normalize(FIX)
    assert row["_tile"] == FIX["properties"]["grid:code"].removeprefix("MGRS-")
    assert json.loads(row["assets"]) == FIX["assets"]
    assert row["thumbnail_url"] == FIX["assets"]["thumbnail"]["href"]
    assert row["collection"] == "sentinel-2-c1-l2a"
    assert set(row) == {c[0] for c in s2c1_schema.COLUMNS if c[0] not in ("_month", "_hilbert", "geometry")} | {"_geometry_json"}
    assert json.loads(row["processing:software"]) == FIX["properties"]["processing:software"]
    assert row["proj:centroid"] == FIX["properties"]["proj:centroid"]
    assert row["created"] == FIX["properties"]["created"]
    assert not any(l["rel"] in ("next", "prev", "root", "parent") for l in row["links"])


def test_c1_normalize_tile_fallback_and_error():
    f = json.loads(json.dumps(FIX))
    del f["properties"]["grid:code"]
    assert s2c1_schema.normalize(f)["_tile"] == "31UET"
    del f["properties"]["mgrs:grid_square"]
    with pytest.raises(ValueError, match="grid:code"):
        s2c1_schema.normalize(f)
