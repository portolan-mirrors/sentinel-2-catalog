"""Live-fetch one page and prove normalization lands exactly on the
canonical schema. Schema drift upstream must fail loudly here, never fork
the published schema silently."""
import http.client
import json
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from s2_schema import COLUMNS
import s2_fetch  # noqa: E402  (module import: needed to monkeypatch s2_fetch.time.sleep)
from s2_fetch import with_retries  # noqa: E402
from s2_repair import MissingItem  # noqa: E402

API = "https://earth-search.aws.element84.com/v1/search"
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]


def _one_feature():
    body = json.dumps({"collections": ["sentinel-2-l2a"],
                       "datetime": "2026-09-10T00:00:00Z/2026-09-10T06:00:00Z",
                       "limit": 1}).encode()
    req = urllib.request.Request(API, data=body,
                                 headers={"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=60))["features"][0]


def test_normalize_covers_every_column():
    from s2_fetch import normalize
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
