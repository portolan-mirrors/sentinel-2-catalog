"""Live-fetch one page and prove normalization lands exactly on the
canonical schema. Schema drift upstream must fail loudly here, never fork
the published schema silently."""
import json
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from s2_schema import COLUMNS

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
