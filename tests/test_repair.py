"""Bucket repair (s2_repair.py). The inventory audit is tests/test_audit.py.

Two real network calls in this whole file: fetching the verified
S2C_53HNV_20260910_0_L2A static item JSON (first collection) and the
S2B_T31UET_20260921T105030_L2A one (Collection 1), to prove each bucket's
item shape still normalizes onto its collection's schema (same style as
test_fetch.py's live-normalize test). Everything else -- discovery, fetch,
month-file naming -- runs against injected listers/getters, no S3.
"""
import io
import json
import re
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import s2_collections as cols  # noqa: E402
import s2_fetch  # noqa: E402  (module import: needed to monkeypatch s2_fetch.time.sleep)
import s2_repair  # noqa: E402
import s2c1_schema  # noqa: E402
from s2_fetch import copy_ndjson_to_parquet  # noqa: E402
from s2_schema import COLUMNS, normalize  # noqa: E402
from s2_repair import (  # noqa: E402
    MissingItem, _get_json, _is_valid_zone, _list_common_prefixes, build_prefix_cache,
    discover_scenes, fetch_and_write, group_scenes_by_day, prefix_cache_path,
    repair_month, scene_day)

STATIC_ITEM_URL = ("https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
                   "sentinel-s2-l2a-cogs/53/H/NV/2026/9/"
                   "S2C_53HNV_20260910_0_L2A/S2C_53HNV_20260910_0_L2A.json")
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]

C1 = cols.get("sentinel-2-c1-l2a")
C1_STATIC_ITEM_URL = ("https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/"
                      "sentinel-2-c1-l2a/31/U/ET/2026/9/"
                      "S2B_T31UET_20260921T105030_L2A/S2B_T31UET_20260921T105030_L2A.json")


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


def _fake_item_missing_mgrs(item_id: str, month: str = "2018-09-01T10:00:00Z") -> dict:
    """A valid-JSON item that lacks both s2:mgrs_tile and every mgrs:*
    fallback field -- normalize()'s ValueError guard fires on this exact
    shape (production evidence, 2026-09-16, run 35092766147:
    S2B_35NKA_20180901_0_L2A)."""
    item = _fake_item(item_id, month)
    for key in ("mgrs:utm_zone", "mgrs:latitude_band", "mgrs:grid_square"):
        del item["properties"][key]
    return item


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
        n, missing = fetch_and_write(scenes, lambda url: items[url], nd, workers=2)
        assert n == 2
        assert missing == 0
        rows = [json.loads(line) for line in Path(nd).read_text().splitlines()]
        assert {r["id"] for r in rows} == {"A1", "A2"}
        assert all(r["s2:mgrs_tile"] == "31UFU" for r in rows)


# --------------------------------------------------------------------------
# _get_json: 404/410 is a permanent skip (no retry); 5xx still retries.
# Fix round, production run 35090066508: a genuinely-missing item JSON was
# retried through the full 8-attempt ladder (~11 minutes) before crashing
# the whole month.
# --------------------------------------------------------------------------

def _http_error(url: str, code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(url, code, f"HTTP {code}", None, None)


def test_get_json_raises_missing_item_on_404_without_retrying(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise _http_error(req.full_url, 404)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(MissingItem):
        _get_json("https://x/missing.json")
    assert calls["n"] == 1, "a 404 must not be retried"


def test_get_json_raises_missing_item_on_410_without_retrying(monkeypatch):
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        raise _http_error(req.full_url, 410)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(MissingItem):
        _get_json("https://x/gone.json")
    assert calls["n"] == 1, "a 410 must not be retried"


def test_get_json_still_retries_5xx(monkeypatch):
    """Existing retry behavior must survive the 404/410 special-case: a
    transient 503 is retried by with_retries's ladder, not skipped."""
    calls = {"n": 0}

    def fake_urlopen(req, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            raise _http_error(req.full_url, 503)
        return io.BytesIO(b'{"ok": true}')

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(s2_fetch.time, "sleep", lambda s: None)  # skip real backoff
    result = _get_json("https://x/flaky.json", tries=5)
    assert result == {"ok": True}
    assert calls["n"] == 3, "must retry a 5xx until it succeeds"


def test_fetch_and_write_skips_missing_scene_and_keeps_valid_ones():
    """Unit-level proof that fetch_and_write() treats MissingItem as a
    skip, not a crash: a get_fn that raises MissingItem for one scene among
    valid ones must still write the valid rows, count the miss, and warn."""
    def get_fn(url):
        if "missing" in url:
            raise MissingItem(f"{url}: HTTP 404")
        return _fake_item(url.rsplit("/", 1)[-1].removesuffix(".json"))

    scenes = [("A1", "https://x/A1.json"),
             ("MISSING", "https://x/missing.json"),
             ("A2", "https://x/A2.json")]
    with tempfile.TemporaryDirectory() as td:
        nd = str(Path(td) / "rows.ndjson")
        n, missing = fetch_and_write(scenes, get_fn, nd, workers=2)
        assert n == 2
        assert missing == 1
        rows = [json.loads(line) for line in Path(nd).read_text().splitlines()]
        assert {r["id"] for r in rows} == {"A1", "A2"}


def test_fetch_and_write_skips_unnormalizable_item_and_keeps_valid_ones(capsys):
    """Fix round, production run 35092766147: an item whose JSON is valid
    but lacks both s2:mgrs_tile and every mgrs:* fallback field must be
    skipped (normalize()'s ValueError), not crash the batch -- exactly like
    a MissingItem, just from a different stage (get_fn succeeded; normalize
    is what fails)."""
    def get_fn(url):
        item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
        if item_id == "BAD":
            return _fake_item_missing_mgrs(item_id)
        return _fake_item(item_id)

    scenes = [("A1", "https://x/A1.json"),
             ("BAD", "https://x/BAD.json"),
             ("A2", "https://x/A2.json")]
    with tempfile.TemporaryDirectory() as td:
        nd = str(Path(td) / "rows.ndjson")
        n, skipped = fetch_and_write(scenes, get_fn, nd, workers=2)
        assert n == 2
        assert skipped == 1
        rows = [json.loads(line) for line in Path(nd).read_text().splitlines()]
        assert {r["id"] for r in rows} == {"A1", "A2"}
        err = capsys.readouterr().err
        assert "skipping unnormalizable item" in err
        assert "BAD" in err


# --------------------------------------------------------------------------
# copy_ndjson_to_parquet (s2_fetch.py, shared): atomic write on failure
# --------------------------------------------------------------------------

def test_copy_ndjson_to_parquet_leaves_no_partial_or_tmp_file_on_failure():
    """Fix round: dest.exists() is the skip-if-exists check every caller
    relies on (s2_fetch.py's fetch_window and s2_repair.py's per-day
    resume alike), so a COPY that fails partway through must never leave a
    partial file at dest -- a process killed mid-COPY (a real event under
    runner eviction) would otherwise poison resume by making a
    half-written chunk look finished. Triggered here with malformed
    NDJSON, which fails after the destination path is already committed to
    but before any bytes are written."""
    with tempfile.TemporaryDirectory() as td:
        dest = Path(td) / "2018-09-05_2018-09-05.parquet"
        nd = Path(td) / "rows.ndjson"
        nd.write_text("{not valid json\n")
        with pytest.raises(Exception):
            copy_ndjson_to_parquet(str(nd), dest)
        assert not dest.exists(), "a failed COPY must not leave a partial dest"
        assert not dest.with_name(dest.name + ".tmp").exists(), "no .tmp litter"


# --------------------------------------------------------------------------
# scene_day / group_scenes_by_day: date extraction from the scene id
# --------------------------------------------------------------------------

def test_scene_day_parses_the_embedded_acquisition_date():
    assert scene_day("S2A_31UFU_20180905_0_L2A") == "2018-09-05"
    assert scene_day("S2C_53HNV_20260910_0_L2A") == "2026-09-10"


def test_scene_day_rejects_unrecognized_ids():
    with pytest.raises(ValueError):
        scene_day("not-a-scene-id")


def test_group_scenes_by_day_buckets_by_date():
    scenes = [("S2A_31UFU_20180905_0_L2A", "https://x/a"),
             ("S2A_31UFU_20180905_1_L2A", "https://x/b"),
             ("S2A_31UFU_20180906_0_L2A", "https://x/c")]
    by_day = group_scenes_by_day(scenes)
    assert set(by_day) == {"2018-09-05", "2018-09-06"}
    assert len(by_day["2018-09-05"]) == 2
    assert len(by_day["2018-09-06"]) == 1


def test_group_scenes_by_day_skips_unparsable_id_and_continues(capsys):
    """Fix round: an unparsable scene id must be logged and skipped, not
    raised -- group_scenes_by_day() runs before any day is fetched, so an
    unhandled exception here would crash the whole month deterministically
    on every retry (discovery re-runs from scratch each attempt)."""
    scenes = [("S2A_31UFU_20180905_0_L2A", "https://x/a"),
             ("not-a-valid-scene-id", "https://x/bad"),
             ("S2A_31UFU_20180906_0_L2A", "https://x/c")]
    by_day = group_scenes_by_day(scenes)
    assert set(by_day) == {"2018-09-05", "2018-09-06"}
    err = capsys.readouterr().err
    assert "skipping unparsable scene id" in err
    assert "not-a-valid-scene-id" in err


# --------------------------------------------------------------------------
# repair_month: end-to-end against injected lister/getter, real parquet out.
# Day-granular (fix round: eviction resilience) -- one chunk per day, so a
# month is many small units of work rather than one big one, and a rerun
# only fetches whatever day never finished.
# --------------------------------------------------------------------------

def _two_day_month_list_fn(prefix):
    if prefix.endswith("2018/9/"):
        return ["sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/",
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180906_0_L2A/"]
    return []


def _get_by_url(url):
    item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
    return _fake_item(item_id)


def test_repair_month_writes_one_chunk_per_day():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         workers=2, list_fn=_two_day_month_list_fn, get_fn=_get_by_url)
        assert n == 2  # one scene per day, two days with scenes
        dest_dir = out / "repair"
        day5 = dest_dir / "2018-09-05_2018-09-05.parquet"
        day6 = dest_dir / "2018-09-06_2018-09-06.parquet"
        assert day5.exists() and day5.stat().st_size > 0
        assert day6.exists() and day6.stat().st_size > 0
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{day5}')").fetchall()
        got = [d[0] for d in desc]
        want = [c[0] for c in DATA_COLUMNS]
        assert got == want, "each day's chunk must match s2_fetch's canonical schema"
        # Every other day in September gets a zero-byte sentinel, exactly
        # like s2_fetch.py's own per-day resumability contract.
        day1 = dest_dir / "2018-09-01_2018-09-01.parquet"
        assert day1.exists() and day1.stat().st_size == 0
        assert len(list(dest_dir.glob("*.parquet"))) == 30  # every day in Sept


def test_repair_month_skips_unparsable_scene_id_and_continues(capsys):
    """Integration proof for the log-and-continue fix: a mixed batch with
    one unparsable scene id alongside two valid ones must NOT raise -- the
    valid days still get written, the bad id is warned about and skipped,
    and repair_month returns normally (exit 0 at the CLI)."""
    def fake_list(prefix):
        if prefix.endswith("2018/9/"):
            return [
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/",
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/not-a-valid-scene-id/",
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180906_0_L2A/",
            ]
        return []

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         workers=2, list_fn=fake_list, get_fn=_get_by_url)
        assert n == 2, "the two valid days must still be fetched"
        dest_dir = out / "repair"
        day5 = dest_dir / "2018-09-05_2018-09-05.parquet"
        day6 = dest_dir / "2018-09-06_2018-09-06.parquet"
        assert day5.exists() and day5.stat().st_size > 0
        assert day6.exists() and day6.stat().st_size > 0
        err = capsys.readouterr().err
        assert "skipping unparsable scene id" in err
        assert "not-a-valid-scene-id" in err


def test_repair_month_skips_missing_item_and_prints_summary(capsys):
    """Integration proof for the 404/410-is-permanent fix: a day with one
    genuinely-missing scene alongside a valid one must still write the
    valid scene, warn about the missing one, and never call get_fn for it
    more than once (no retry delay). The month-end summary line must
    report both counts."""
    def fake_list(prefix):
        if prefix.endswith("2018/9/"):
            return [
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_0_L2A/",
                "sentinel-s2-l2a-cogs/31/U/FU/2018/9/S2A_31UFU_20180905_1_L2A/",
            ]
        return []

    call_counts: dict[str, int] = {}

    def get_fn(url):
        call_counts[url] = call_counts.get(url, 0) + 1
        if url.endswith("S2A_31UFU_20180905_1_L2A.json"):
            raise MissingItem(f"{url}: HTTP 404")
        return _get_by_url(url)

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         workers=2, list_fn=fake_list, get_fn=get_fn)
        assert n == 1, "only the valid scene counts toward rows fetched"
        day5 = out / "repair" / "2018-09-05_2018-09-05.parquet"
        assert day5.exists() and day5.stat().st_size > 0

        missing_url = ("https://sentinel-cogs.s3.us-west-2.amazonaws.com/"
                      "sentinel-s2-l2a-cogs/31/U/FU/2018/9/"
                      "S2A_31UFU_20180905_1_L2A/S2A_31UFU_20180905_1_L2A.json")
        assert call_counts[missing_url] == 1, (
            "a 404-equivalent must be fetched exactly once -- no retry delay")

        out_text = capsys.readouterr()
        assert "skipping scene with missing item JSON: S2A_31UFU_20180905_1_L2A" \
            in out_text.err
        assert "month 2018-09: 1 scenes fetched, 1 scenes skipped " \
            "(missing or unnormalizable)" in out_text.out


def test_repair_month_skips_unnormalizable_item_and_prints_summary(capsys):
    """Integration proof, production run 35092766147: a day with one
    exotic scene whose item JSON exists but fails normalize() (no
    s2:mgrs_tile, no mgrs:* fallback -- exactly S2B_35NKA_20180901_0_L2A)
    alongside a valid scene must still write the valid scene, warn with the
    item id, and count it in the month-end summary. No raise."""
    def fake_list(prefix):
        if prefix.endswith("2018/9/"):
            return [
                "sentinel-s2-l2a-cogs/35/N/KA/2018/9/S2B_35NKA_20180901_0_L2A/",
                "sentinel-s2-l2a-cogs/35/N/KA/2018/9/S2B_35NKA_20180902_0_L2A/",
            ]
        return []

    def get_fn(url):
        item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
        if item_id == "S2B_35NKA_20180901_0_L2A":
            return _fake_item_missing_mgrs(item_id, "2018-09-01T10:00:00Z")
        return _fake_item(item_id, "2018-09-02T10:00:00Z")

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/35/N/KA/"],
                         workers=2, list_fn=fake_list, get_fn=get_fn)
        assert n == 1, "only the valid scene counts toward rows fetched"

        dest_dir = out / "repair"
        day1 = dest_dir / "2018-09-01_2018-09-01.parquet"
        day2 = dest_dir / "2018-09-02_2018-09-02.parquet"
        # The exotic scene was the ONLY scene on day 1, so day 1 gets a
        # zero-byte sentinel (same as a day discovered with zero scenes).
        assert day1.exists() and day1.stat().st_size == 0
        assert day2.exists() and day2.stat().st_size > 0

        out_text = capsys.readouterr()
        assert "skipping unnormalizable item" in out_text.err
        assert "S2B_35NKA_20180901_0_L2A" in out_text.err
        assert "month 2018-09: 1 scenes fetched, 1 scenes skipped " \
            "(missing or unnormalizable)" in out_text.out


def test_repair_month_second_run_skips_already_finished_days():
    """Per-day flush proof: after a first full run, a second run over the
    same month must not re-fetch ANY day -- discovery (list_fn) still runs
    (month-level, unconditional), but get_fn must never be called again."""
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        first = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                             workers=2, list_fn=_two_day_month_list_fn,
                             get_fn=_get_by_url)
        assert first == 2

        def boom(*a, **k):
            raise AssertionError("must not re-fetch a day whose chunk already exists")

        second = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                              workers=2, list_fn=_two_day_month_list_fn, get_fn=boom)
        assert second == 0


def test_repair_month_resumes_partial_progress():
    """The workflow's resume story: a prior (evicted) attempt already
    finished day 1 and uploaded a partial slice-YYYY-MM artifact; the
    workflow downloads it into the SAME staging dir before re-invoking this
    tool. repair_month must skip day 1 (its chunk already exists on disk)
    and only fetch day 2."""
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        dest_dir = out / "repair"
        dest_dir.mkdir(parents=True)
        pre_existing = dest_dir / "2018-09-05_2018-09-05.parquet"
        pre_existing.write_bytes(b"already-finished-before-eviction")

        def get_fn(url):
            if "20180905" in url:
                raise AssertionError("day 1 already has a chunk; must not re-fetch")
            return _get_by_url(url)

        n = repair_month("2018-09", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         workers=2, list_fn=_two_day_month_list_fn, get_fn=get_fn)
        assert n == 1, "only day 2's one scene should be fetched"
        assert pre_existing.read_bytes() == b"already-finished-before-eviction"
        day6 = dest_dir / "2018-09-06_2018-09-06.parquet"
        assert day6.exists() and day6.stat().st_size > 0


def test_repair_month_zero_scenes_writes_sentinel_per_day():
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2019-02", out, ["sentinel-s2-l2a-cogs/31/U/FU/"],
                         list_fn=lambda p: [], get_fn=lambda u: {})
        assert n == 0
        dest_dir = out / "repair"
        for day in ("01", "14", "28"):
            f = dest_dir / f"2019-02-{day}_2019-02-{day}.parquet"
            assert f.exists() and f.stat().st_size == 0
        assert len(list(dest_dir.glob("*.parquet"))) == 28  # 2019 is not a leap year


# --------------------------------------------------------------------------
# --collection sentinel-2-c1-l2a: every bucket-specific constant comes from
# the CollectionConfig; the first collection's defaults are untouched.
# --------------------------------------------------------------------------

def test_c1_static_item_normalizes_to_the_c1_schema():
    """The second live fetch: the C1 bucket's static item JSON (the file
    s2_repair GETs) normalizes onto the frozen C1 schema exactly like an
    API feature does -- the repair path writes rows through
    config.schema.normalize, so this is the shape it must accept."""
    req = urllib.request.Request(C1_STATIC_ITEM_URL,
                                 headers={"User-Agent": "sentinel-2-catalog-tools/1.0"})
    item = json.load(urllib.request.urlopen(req, timeout=60))
    row = C1.schema.normalize(item)
    want = {c[0] for c in s2c1_schema.DATA_COLUMNS if c[0] != "geometry"} | {"_geometry_json"}
    assert set(row) == want
    assert row["id"] == "S2B_T31UET_20260921T105030_L2A"
    assert row["_tile"] == "31UET"


def test_scene_day_uses_the_collection_id_regex():
    assert scene_day("S2B_T31UET_20260921T105030_L2A", C1) == "2026-09-21"
    # A first-collection id is not a C1 id, and vice versa.
    with pytest.raises(ValueError):
        scene_day("S2A_31UFU_20180905_0_L2A", C1)
    with pytest.raises(ValueError):
        scene_day("S2B_T31UET_20260921T105030_L2A")


def test_group_scenes_by_day_skips_an_id_the_c1_regex_rejects(capsys):
    scenes = [("S2B_T31UET_20260921T105030_L2A", "https://x/a"),
              ("S2A_31UFU_20260921_0_L2A", "https://x/first-collection-shape"),
              ("S2A_T31UET_20260922T105031_L2A", "https://x/c")]
    by_day = group_scenes_by_day(scenes, C1)
    assert set(by_day) == {"2026-09-21", "2026-09-22"}
    err = capsys.readouterr().err
    assert "skipping unparsable scene id" in err
    assert "S2A_31UFU_20260921_0_L2A" in err


class _FakeS3:
    """Records every list_objects_v2 call; returns canned CommonPrefixes."""

    def __init__(self, tree: dict[str, list[str]]):
        self.tree = tree
        self.calls: list[dict] = []

    def list_objects_v2(self, **kwargs):
        self.calls.append(kwargs)
        under = self.tree.get(kwargs["Prefix"], [])
        return {"CommonPrefixes": [{"Prefix": p} for p in under], "IsTruncated": False}


def test_list_common_prefixes_lists_the_collection_bucket():
    s3 = _FakeS3({"sentinel-2-c1-l2a/": ["sentinel-2-c1-l2a/31/"]})
    assert _list_common_prefixes(s3, C1.key_root, C1) == ["sentinel-2-c1-l2a/31/"]
    assert s3.calls[0]["Bucket"] == "e84-earth-search-sentinel-data"
    # Default: the first collection's bucket, as before.
    s3 = _FakeS3({})
    _list_common_prefixes(s3, "sentinel-s2-l2a-cogs/")
    assert s3.calls[0]["Bucket"] == "sentinel-cogs"


def test_build_prefix_cache_roots_the_crawl_at_config_key_root(tmp_path, monkeypatch):
    tree = {
        "sentinel-2-c1-l2a/": ["sentinel-2-c1-l2a/31/", "sentinel-2-c1-l2a/2019/"],
        "sentinel-2-c1-l2a/31/": ["sentinel-2-c1-l2a/31/U/"],
        "sentinel-2-c1-l2a/31/U/": ["sentinel-2-c1-l2a/31/U/ET/", "sentinel-2-c1-l2a/31/U/FU/"],
    }
    s3 = _FakeS3(tree)
    monkeypatch.setattr(s2_repair, "_s3_client", lambda: s3)
    out = tmp_path / "cache.txt"
    squares = build_prefix_cache(out, workers=2, config=C1)
    assert squares == ["sentinel-2-c1-l2a/31/U/ET/", "sentinel-2-c1-l2a/31/U/FU/"]
    assert out.read_text() == "sentinel-2-c1-l2a/31/U/ET/\nsentinel-2-c1-l2a/31/U/FU/\n"
    assert s3.calls[0]["Prefix"] == "sentinel-2-c1-l2a/"
    assert {c["Bucket"] for c in s3.calls} == {"e84-earth-search-sentinel-data"}


def test_prefix_cache_path_is_per_collection():
    assert prefix_cache_path().name == "mgrs_prefixes.txt"
    assert prefix_cache_path(cols.get(cols.DEFAULT)).name == "mgrs_prefixes.txt"
    assert prefix_cache_path(C1).name == "mgrs_prefixes_c1.txt"
    assert prefix_cache_path(C1).parent == prefix_cache_path().parent


def test_committed_c1_prefix_cache_is_the_c1_bucket_layout():
    """tools/mgrs_prefixes_c1.txt is built once by a real crawl and
    committed; every line must be a zone/band/square prefix under the C1
    key root."""
    lines = prefix_cache_path(C1).read_text().splitlines()
    assert len(lines) > 30_000
    pat = re.compile(r"^sentinel-2-c1-l2a/\d{1,2}/[C-X]/[A-Z]{2}/$")
    assert [l for l in lines if not pat.match(l)] == []
    assert lines == sorted(lines)


def test_discover_scenes_builds_urls_from_the_collection_https_base():
    scenes = discover_scenes(
        ["sentinel-2-c1-l2a/31/U/ET/"], "2026", "9",
        lambda p: [p + "S2B_T31UET_20260921T105030_L2A/"], workers=1, config=C1)
    assert scenes == [(
        "S2B_T31UET_20260921T105030_L2A",
        "https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com/"
        "sentinel-2-c1-l2a/31/U/ET/2026/9/S2B_T31UET_20260921T105030_L2A/"
        "S2B_T31UET_20260921T105030_L2A.json")]


def _c1_item(item_id: str, dt: str) -> dict:
    """A minimal C1 feature: the live fixture with id/datetime swapped."""
    item = json.loads((ROOT / "tests" / "fixtures" / "c1_item.json").read_text())
    item["id"] = item_id
    item["properties"]["datetime"] = dt
    return item


def test_repair_month_writes_c1_rows_with_the_c1_schema():
    def fake_list(prefix):
        if prefix == "sentinel-2-c1-l2a/31/U/ET/2026/9/":
            return [prefix + "S2B_T31UET_20260921T105030_L2A/",
                    prefix + "S2A_T31UET_20260923T105031_L2A/"]
        return []

    def get_fn(url):
        item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
        day = item_id.split("_")[2][:8]
        return _c1_item(item_id, f"{day[:4]}-{day[4:6]}-{day[6:]}T10:50:30.000Z")

    with tempfile.TemporaryDirectory() as td:
        out = Path(td)
        n = repair_month("2026-09", out, ["sentinel-2-c1-l2a/31/U/ET/"], workers=2,
                         list_fn=fake_list, get_fn=get_fn, config=C1)
        assert n == 2
        dest_dir = out / "repair"
        day21 = dest_dir / "2026-09-21_2026-09-21.parquet"
        day23 = dest_dir / "2026-09-23_2026-09-23.parquet"
        assert day21.stat().st_size > 0 and day23.stat().st_size > 0
        assert len(list(dest_dir.glob("*.parquet"))) == 30
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{day21}')").fetchall()
        assert [d[0] for d in desc] == [c[0] for c in s2c1_schema.DATA_COLUMNS]
        assert con.execute(f"SELECT id, _tile FROM read_parquet('{day21}')").fetchall() == [
            ("S2B_T31UET_20260921T105030_L2A", "31UET")]


def test_repair_cli_takes_collection_and_defaults_to_the_first(tmp_path):
    """repair-slices.yml calls the tool with no --collection; the option
    must exist with the first collection as its default, and the
    pre-existing argument checks must still fire before any network."""
    r = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "s2_repair.py"), "--help"],
        capture_output=True, text=True)
    assert r.returncode == 0
    assert "--collection" in r.stdout
    assert "sentinel-2-c1-l2a" in r.stdout
    r = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "s2_repair.py"),
         "--collection", "sentinel-2-c1-l2a", "--month", "2026-09",
         "--prefix-cache", str(tmp_path / "missing.txt")],
        capture_output=True, text=True)
    assert r.returncode != 0
    assert "--month requires --out" in r.stderr
