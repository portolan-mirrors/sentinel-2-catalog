"""Live-fetch one page and prove normalization lands exactly on the
canonical schema. Schema drift upstream must fail loudly here, never fork
the published schema silently."""
import http.client
import json
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import s2_schema  # noqa: E402
from s2_schema import COLUMNS  # noqa: E402
import s2_collections as cols  # noqa: E402
import s2c1_schema  # noqa: E402
import s2_fetch  # noqa: E402  (module import: needed to monkeypatch s2_fetch.time.sleep)
from s2_fetch import with_retries  # noqa: E402
from s2_repair import MissingItem  # noqa: E402

API = "https://earth-search.aws.element84.com/v1/search"
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]
FIX_C1 = json.loads(
    (Path(__file__).parent / "fixtures" / "c1_item.json").read_text())


def _one_feature():
    body = json.dumps({"collections": ["sentinel-2-l2a"],
                       "datetime": "2026-09-10T00:00:00Z/2026-09-10T06:00:00Z",
                       "limit": 1}).encode()
    req = urllib.request.Request(API, data=body,
                                 headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))["features"][0]


def test_normalize_covers_every_column():
    from s2_schema import normalize
    row = normalize(_one_feature())
    want = {c[0] for c in DATA_COLUMNS if c[0] != "geometry"} | {"_geometry_json"}
    assert set(row) == want
    assert row["s2:mgrs_tile"] and row["thumbnail_url"].startswith("https://")
    a = json.loads(row["assets"])
    assert "red" in a and a["red"]["href"].startswith("https://")
    assert "eo:bands" in a["red"]              # verbatim, nothing stripped
    assert row["sat:relative_orbit"] is not None      # parsed from product_uri
    assert isinstance(row["s2:mean_solar_zenith"], float)


def test_fetch_window_writes_canonical_parquet():
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(
            [sys.executable, "tools/s2_fetch.py", "--start", "2026-09-10",
             "--end", "2026-09-10", "--limit-pages", "1", "--out", td],
            check=True, cwd=Path(__file__).resolve().parent.parent)
        f = next(Path(td, "api").glob("*.parquet"))
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()
        got = [(d[0], d[1]) for d in desc]
        want = [(n, t) for n, t, _ in DATA_COLUMNS]
        # DuckDB reports the stored geometry/tz types in its own spelling.
        assert [g[0] for g in got] == [w[0] for w in want]
        n = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
        assert 0 < n <= 200


def test_fetch_window_two_pages_no_duplicates():
    """A real next-link continuation must preserve the original filters
    (not drift to a different window) and must not repeat rows across
    pages -- proving the pagination fallback's `merge`/`body` handling is
    correct against live Earth Search, not just plausible-looking code."""
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(
            [sys.executable, "tools/s2_fetch.py", "--start", "2026-09-01",
             "--end", "2026-09-01", "--limit-pages", "2", "--out", td],
            check=True, cwd=Path(__file__).resolve().parent.parent)
        f = next(Path(td, "api").glob("*.parquet"))
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
        assert 200 < n <= 400
        distinct_ids = con.execute(
            f"SELECT count(DISTINCT id) FROM read_parquet('{f}')").fetchone()[0]
        assert distinct_ids == n
        bad_dates = con.execute(f"""
            SELECT count(*) FROM read_parquet('{f}')
            WHERE CAST(datetime AT TIME ZONE 'UTC' AS DATE) != DATE '2026-09-01'
        """).fetchone()[0]
        assert bad_dates == 0


# --------------------------------------------------------------------------
# with_retries: connection-phase failures that escape urllib's own wrapping
# must still be retried. Production evidence, run 35097702125, job
# repair (2018-09): a RemoteDisconnected raised mid-response-read from
# urllib.request.urlopen inside with_retries went uncaught (the except
# tuple only matched URLError/HTTPError/TimeoutError) and killed the whole
# month job. urllib only wraps CONNECT-phase failures into URLError --
# failures during the response-read phase surface as raw
# http.client.HTTPException subclasses (RemoteDisconnected, BadStatusLine,
# IncompleteRead) or raw ConnectionError subclasses (ConnectionResetError).
# --------------------------------------------------------------------------

def test_with_retries_retries_remote_disconnected(monkeypatch):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise http.client.RemoteDisconnected("Remote end closed connection")
        return "ok"

    monkeypatch.setattr(s2_fetch.time, "sleep", lambda s: None)  # skip real backoff
    assert with_retries(flaky) == "ok"
    assert calls["n"] == 2, "a RemoteDisconnected must be retried, not escape"


def test_with_retries_retries_connection_reset_error(monkeypatch):
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionResetError("Connection reset by peer")
        return "ok"

    monkeypatch.setattr(s2_fetch.time, "sleep", lambda s: None)  # skip real backoff
    assert with_retries(flaky) == "ok"
    assert calls["n"] == 2, "a ConnectionResetError must be retried, not escape"


def test_with_retries_does_not_catch_missing_item(monkeypatch):
    """Critical invariant: widening the except tuple to catch connection
    failures must NOT start catching s2_repair.MissingItem. MissingItem is
    a plain Exception subclass (not URLError/HTTPError/TimeoutError/
    http.client.HTTPException/ConnectionError), so it must keep propagating
    out of with_retries() on the very first call, with zero retries."""
    calls = {"n": 0}

    def raises_missing():
        calls["n"] += 1
        raise MissingItem("https://x/missing.json: HTTP 404")

    monkeypatch.setattr(s2_fetch.time, "sleep", lambda s: None)  # skip real backoff
    with pytest.raises(MissingItem):
        with_retries(raises_missing)
    assert calls["n"] == 1, "MissingItem must not be retried"


# --------------------------------------------------------------------------
# --collection and the `created` lookback (Collection 1). The first
# collection's request body and output columns must not change at all.
# --------------------------------------------------------------------------

def _stub_post(monkeypatch, feature):
    """Replace the API call; returns the list of bodies that were sent."""
    sent = []

    def fake_post(body, tries=8):
        sent.append(body)
        return {"features": [feature], "links": []}
    monkeypatch.setattr(s2_fetch, "_post", fake_post)
    return sent


def _column_names(f: Path) -> list[str]:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return [c[0] for c in
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f}')").fetchall()]


def test_search_body_by_created_uses_the_query_extension():
    cfg = cols.get("sentinel-2-c1-l2a")
    body = s2_fetch.search_body(cfg, "2026-09-20", "2026-09-21", "created")
    assert body["collections"] == ["sentinel-2-c1-l2a"]
    assert "datetime" not in body
    assert body["query"]["created"] == {"gte": "2026-09-20T00:00:00Z",
                                        "lte": "2026-09-21T23:59:59Z"}
    assert body["limit"] == s2_fetch.PAGE


def test_search_body_by_datetime_is_unchanged_for_the_first_collection():
    body = s2_fetch.search_body(cols.get(cols.DEFAULT), "2026-09-20", "2026-09-21", "datetime")
    assert body == {"collections": ["sentinel-2-l2a"],
                    "datetime": "2026-09-20T00:00:00Z/2026-09-21T23:59:59Z",
                    "limit": s2_fetch.PAGE}


def test_search_body_by_datetime_for_c1_uses_the_api_collection():
    body = s2_fetch.search_body(cols.get("sentinel-2-c1-l2a"), "2026-09-20", "2026-09-21", "datetime")
    assert body == {"collections": ["sentinel-2-c1-l2a"],
                    "datetime": "2026-09-20T00:00:00Z/2026-09-21T23:59:59Z",
                    "limit": s2_fetch.PAGE}


def test_search_body_rejects_an_unknown_field():
    with pytest.raises(ValueError):
        s2_fetch.search_body(cols.get(cols.DEFAULT), "2026-09-20", "2026-09-21", "updated")


def test_data_columns_come_from_the_schema_module():
    """Each schema module owns its DATA_COLUMNS; s2_fetch's module-level
    list is the first collection's (the writers' default), not a second
    derivation."""
    assert s2_fetch.DATA_COLUMNS is s2_schema.DATA_COLUMNS
    assert s2_fetch.DATA_COLUMNS == DATA_COLUMNS
    assert not hasattr(s2_fetch, "data_columns")
    c1 = s2c1_schema.DATA_COLUMNS
    assert [c[0] for c in c1][-2:] == ["_tile", "geometry"]
    assert not any(c[0] in ("_month", "_hilbert") for c in c1)


def test_fetch_window_normalizes_with_the_collection_schema(tmp_path, monkeypatch):
    cfg = cols.get("sentinel-2-c1-l2a")
    sent = _stub_post(monkeypatch, FIX_C1)
    n = s2_fetch.fetch_window("2026-09-21", "2026-09-21", tmp_path, config=cfg)
    assert n == 1
    # The default field is datetime even for C1; --field created is opt-in.
    assert sent == [{"collections": ["sentinel-2-c1-l2a"],
                     "datetime": "2026-09-21T00:00:00Z/2026-09-21T23:59:59Z",
                     "limit": s2_fetch.PAGE}]
    f = tmp_path / "2026-09-21_2026-09-21.parquet"
    names = _column_names(f)
    assert "_tile" in names and "s2:mgrs_tile" not in names
    assert names == [c[0] for c in s2c1_schema.DATA_COLUMNS]
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    row = con.execute(f"SELECT id, _tile, collection FROM read_parquet('{f}')").fetchone()
    assert row == (FIX_C1["id"], "31UET", "sentinel-2-c1-l2a")


def test_fetch_window_by_created_sends_the_query_body(tmp_path, monkeypatch):
    cfg = cols.get("sentinel-2-c1-l2a")
    sent = _stub_post(monkeypatch, FIX_C1)
    n = s2_fetch.fetch_window("2026-09-20", "2026-09-21", tmp_path,
                              config=cfg, field="created")
    assert n == 1
    assert sent == [{"collections": ["sentinel-2-c1-l2a"],
                     "query": {"created": {"gte": "2026-09-20T00:00:00Z",
                                           "lte": "2026-09-21T23:59:59Z"}},
                     "limit": s2_fetch.PAGE}]


def test_fetch_window_default_config_is_the_first_collection(tmp_path, monkeypatch):
    """No config argument: the body and the output columns are exactly
    what the first collection's callers (backfill.yml, refresh-daily.yml)
    have always had."""
    sent = _stub_post(monkeypatch, _one_feature())
    n = s2_fetch.fetch_window("2026-09-10", "2026-09-10", tmp_path)
    assert n == 1
    assert sent == [{"collections": ["sentinel-2-l2a"],
                     "datetime": "2026-09-10T00:00:00Z/2026-09-10T23:59:59Z",
                     "limit": s2_fetch.PAGE}]
    assert _column_names(tmp_path / "2026-09-10_2026-09-10.parquet") == \
        [c[0] for c in DATA_COLUMNS]


def test_cli_collection_and_field_flags(tmp_path):
    """End to end through main(): --collection picks the schema and
    --field created picks the lookback. The live call is one page of
    Collection 1 items created on 2026-09-20, so it also proves the API
    honours the `query.created` form the module docstring records."""
    subprocess.run(
        [sys.executable, "tools/s2_fetch.py", "--collection", "sentinel-2-c1-l2a",
         "--field", "created", "--start", "2026-09-20", "--end", "2026-09-20",
         "--limit-pages", "1", "--out", str(tmp_path)],
        check=True, cwd=Path(__file__).resolve().parent.parent)
    f = next(Path(tmp_path, "api").glob("*.parquet"))
    assert _column_names(f) == [c[0] for c in s2c1_schema.DATA_COLUMNS]
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    n, lo, hi = con.execute(f"""
        SELECT count(*), min(created), max(created) FROM read_parquet('{f}')
    """).fetchone()
    assert 0 < n <= s2_fetch.PAGE
    # DuckDB hands back tz-aware datetimes in the session zone; compare as
    # instants, not as strings.
    day = datetime(2026, 9, 20, tzinfo=timezone.utc)
    assert day <= lo.astimezone(timezone.utc) <= hi.astimezone(timezone.utc) < day + timedelta(days=1)


def test_cli_rejects_created_for_the_first_collection(tmp_path):
    r = subprocess.run(
        [sys.executable, "tools/s2_fetch.py", "--field", "created",
         "--start", "2026-09-20", "--end", "2026-09-20", "--out", str(tmp_path)],
        capture_output=True, text=True, cwd=Path(__file__).resolve().parent.parent)
    assert r.returncode != 0
    assert "created" in r.stderr and "sentinel-2-l2a" in r.stderr
