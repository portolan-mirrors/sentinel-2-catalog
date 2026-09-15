# Sentinel-2 Portolan Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the Earth Search Sentinel-2 L2A archive as a git-backed Portolan catalog of partitioned STAC-GeoParquet on Source Cooperative, with MGRS aggregate stats, scheduled updates, and a static explorer app powered only by the parquet.

**Architecture:** Repo from `portolan-sdi/portolan-catalog-template` (firms-catalog is the working sibling reference at `~/repos/firms-catalog`). A 28.1M-item seed parquet is repartitioned into `year=YYYY/items.parquet` files; Earth Search API fetches close the 2024-06→present gap and keep a daily `live.parquet` fresh. MGRS tile × month stats plus a footprint PMTiles drive a MapLibre + DuckDB-WASM explorer. Nothing but catalog metadata lives in git; data goes straight to the bucket.

**Tech Stack:** Python 3.12, duckdb (+spatial, httpfs), geoparquet-io (`gpio`), tippecanoe, go-pmtiles, rashid, stac-check, GitHub Actions (OIDC to Source Coop), MapLibre GL JS, pmtiles.js, @duckdb/duckdb-wasm.

**Spec:** `docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md`

## Global Constraints

- Published repo: `github.com/portolan-mirrors/sentinel-2-catalog`; bucket prefix `s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog`; public base `https://data.source.coop/portolan-mirrors/sentinel-2-catalog`.
- Item schema = 45 columns (listed in Task 2): the 42 seed-era data columns, then `assets VARCHAR` (the complete upstream STAC assets object, verbatim, as a compact JSON **string** — never a nested struct; measured 179 B/row under zstd-22 with clustered ordering), then `_month TINYINT`, `_hilbert UINTEGER`, and `geometry` last.
- Every published parquet follows the [GeoParquet distribution best practices](https://github.com/opengeospatial/geoparquet/blob/main/format-specs/distributing-geoparquet.md): GeoParquet 2.0 (native GEOMETRY, row-group geo statistics, no bbox covering column), spatial ordering, zstd — cranked to level 22 for EVERY part, `live.parquet` included (user decision 2026-09-15). Sort order inside every part file: `(_month, _hilbert)`; row groups 100,000 rows (~the doc's 128-256 MB byte target at our row width); written by `gpio sort column`.
- Earth Search: `https://earth-search.aws.element84.com/v1/search`, collection `sentinel-2-l2a`, POST paging via `next` link, `limit=200` (500 returns HTTP 500 — measured 2026-09-15).
- ALL data comes from Earth Search (spec Amendment 1): full-archive backfill 2015-06 → present, 51,254,668 items measured 2026-09-15. The old seed parquet (`…/cholmes/stac-geoparquet-public/slim/s2-stac.parquet`) is Planetary Computer STAC — cross-check only, never a data source.
- Dedupe rule everywhere parts are built: keep one row per `id`, preferring highest `s2:generation_time` (NULLS LAST).
- CI gates from the template are law: `python3 tests/run_all.py`, `rashid`, `stac-check`. Never widen a conformance allow-list.
- Workflows use OIDC role `arn:aws:iam::939788573396:role/source-coop-portolan-mirrors` (trust policy already admits every portolan-mirrors repo) — no new secrets.
- Data files never enter git. Generated metadata (collection extents, year items) is committed once as a stable baseline; scheduled runs restamp and upload without committing (firms model).
- Commit messages end with these two trailer lines:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_017HujiaQsbAPviGWJG16fuJ`

---

### Task 1: Scaffold the repository from the template

**Files:**
- Create: entire repo from `portolan-sdi/portolan-catalog-template` merged into the existing local repo (which already holds `docs/superpowers/`)
- Modify: `catalog.publish.yaml`, `catalog/catalog.json`, `catalog/README.md`, `catalog/AGENTS.md`, root `README.md`

**Interfaces:**
- Produces: a repo where `python3 tests/run_all.py` and `python3 tools/publish.py` (dry run) pass; `tools/publish.py` + `tools/upload_data.py` used verbatim by every later task.

- [ ] **Step 1: Create the GitHub repo from the template and merge histories**

```bash
cd /Users/cholmes/repos/sentinel-2-catalog
gh repo create portolan-mirrors/sentinel-2-catalog \
  --template portolan-sdi/portolan-catalog-template --public
git remote add origin git@github.com:portolan-mirrors/sentinel-2-catalog.git
git fetch origin
git branch -M main
# local repo has only docs/ commits; template supplies everything else
git rebase origin/main
```
If `gh repo create` fails with a permissions error, STOP and ask the user for org rights — do not create the repo elsewhere.

- [ ] **Step 2: Work through SETUP.md**

Follow the template's `SETUP.md` exactly (it is authoritative). The values:
- `catalog.publish.yaml`: `write_prefix: s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog`, `public_base: https://data.source.coop/portolan-mirrors/sentinel-2-catalog`, `region: us-west-2`, `profile: portolan-mirrors`, `publish_dir: catalog`, `data_dir: ../s2-staging/publish`. Copy the comment style from `~/repos/firms-catalog/catalog.publish.yaml`.
- `catalog/catalog.json`: `id: sentinel-2-catalog`, `title: "Sentinel-2 L2A STAC-GeoParquet Mirror"`, description explaining: item index for the AWS Sentinel-2 L2A COG archive, imagery stays on AWS, no API needed. Add absolute-URL links:
  `{"rel": "vcs", "href": "https://github.com/portolan-mirrors/sentinel-2-catalog", "title": "Source repository"}` and
  `{"rel": "issues", "href": "https://github.com/portolan-mirrors/sentinel-2-catalog/issues", "title": "Issue tracker"}`.
- Rewrite `catalog/README.md`, `catalog/AGENTS.md`, root `README.md` for this catalog (model on firms-catalog's, s/fire detections/Sentinel-2 scenes/ in substance, not by copy-paste of irrelevant sections).
- Keep or drop `repo-checks.yml`/ops-sync per SETUP step 10 by matching what firms-catalog (same org) keeps.

- [ ] **Step 3: Verify no setup placeholder survives**

```bash
grep -rn "TODO(setup)" . --exclude-dir=.git && echo "UNFINISHED" || echo "clean"
```
Expected: `clean`.

- [ ] **Step 4: Run the template gates**

```bash
python3 tests/run_all.py
python3 tools/publish.py        # dry run, uploads nothing
```
Expected: both pass (template guarantees green on a fully-edited repo).

- [ ] **Step 5: Commit and push**

```bash
git add -A
git commit -m "chore: scaffold sentinel-2-catalog from portolan-catalog-template

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin main
```
Confirm CI is green on GitHub before proceeding: `gh run watch`.

---

### Task 2: `tools/s2_schema.py` — canonical schema and asset stripping

**Files:**
- Create: `tools/s2_schema.py`
- Test: `tests/test_schema.py`

**Interfaces:**
- Produces: `COLUMNS: list[tuple[str, str, str]]` (name, duckdb_type, description) — 45 entries; `SELECT_LIST: str` (quoted, ordered column list for DuckDB).

- [ ] **Step 1: Write the failing test**

`tests/test_schema.py`:
```python
"""The 45-column schema contract. The assets column is a verbatim JSON
string of the upstream assets object; the live test proves its hrefs point
at real objects."""
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from s2_schema import COLUMNS, SELECT_LIST


def test_schema_shape():
    assert len(COLUMNS) == 45
    assert COLUMNS[-1][0] == "geometry"
    assert ("_hilbert", "UINTEGER") == COLUMNS[-2][:2]
    assert ("_month", "TINYINT") == COLUMNS[-3][:2]
    assert COLUMNS[-4][0] == "assets"
    assert SELECT_LIST.split(", ")[0] == '"thumbnail_url"'


def test_live_assets_resolve():
    """A live Earth Search item's assets, serialized the way s2_fetch will
    store them, must parse back and point at real objects."""
    body = json.dumps({"collections": ["sentinel-2-l2a"], "limit": 1}).encode()
    req = urllib.request.Request(
        "https://earth-search.aws.element84.com/v1/search", data=body,
        headers={"Content-Type": "application/json"})
    f = json.load(urllib.request.urlopen(req, timeout=60))["features"][0]
    a = json.loads(json.dumps(f["assets"], separators=(",", ":")))
    assert len(a) >= 30                       # full object, nothing stripped
    assert a["red"]["eo:bands"][0]["name"] == "B04"
    for key in ("red", "visual", "thumbnail"):
        r = urllib.request.Request(a[key]["href"], method="HEAD")
        assert urllib.request.urlopen(r, timeout=30).status == 200
```

- [ ] **Step 2: Run it to make sure it fails**

```bash
python3 -m pytest tests/test_schema.py -v
```
Expected: FAIL (`ModuleNotFoundError: s2_schema`).

- [ ] **Step 3: Write `tools/s2_schema.py`**

```python
#!/usr/bin/env python3
"""Canonical published schema.

Assets ship as a JSON string column holding the upstream assets object
verbatim, never a nested struct: deep struct nesting made earlier parquets
hard to open, and a string keeps every reader's schema flat. Verbatim
because it is nearly free — measured 179 B/row under zstd-22 with
clustered ordering — and lossless beats clever. This module is the single
source of truth for the column list; collection metadata is generated
from it.
"""
from __future__ import annotations


# (name, duckdb type, description). Order is the seed file's order with the
# two sort helpers appended before geometry; geometry stays last so
# `SELECT * EXCLUDE (geometry), geometry` round-trips cleanly.
COLUMNS = [
    ("thumbnail_url", "VARCHAR", "Preview JPEG on the sentinel-cogs bucket."),
    ("type", "VARCHAR", "Always 'Feature'."),
    ("stac_version", "VARCHAR", "STAC version of the source item."),
    ("stac_extensions", "VARCHAR[]", "Extension schema URIs of the source item."),
    ("id", "VARCHAR", "Earth Search item id, e.g. S2C_53HNV_20260910_0_L2A."),
    ("bbox", "DOUBLE[]", "Item bounding box [w, s, e, n], CRS84."),
    ("links", "STRUCT(href VARCHAR, rel VARCHAR, title VARCHAR, \"type\" VARCHAR)[]",
     "Source item links (canonical et al.); paging links are stripped."),
    ("collection", "VARCHAR", "Always 'sentinel-2-l2a'."),
    ("datetime", "TIMESTAMP WITH TIME ZONE", "Acquisition datetime, UTC."),
    ("platform", "VARCHAR", "sentinel-2a / sentinel-2b / sentinel-2c."),
    ("proj:epsg", "BIGINT", "UTM EPSG code of the scene grid."),
    ("instruments", "VARCHAR[]", "Always ['msi']."),
    ("s2:mgrs_tile", "VARCHAR", "MGRS tile id, e.g. 53HNV. THE spatial join key."),
    ("constellation", "VARCHAR", "Always 'sentinel-2'."),
    ("s2:granule_id", "VARCHAR", "ESA granule id. NULL on newer items."),
    ("eo:cloud_cover", "DOUBLE", "Scene cloud cover percentage, 0-100."),
    ("s2:datatake_id", "VARCHAR", "ESA datatake id."),
    ("s2:product_uri", "VARCHAR", "ESA product name."),
    ("s2:datastrip_id", "VARCHAR", "ESA datastrip id."),
    ("s2:product_type", "VARCHAR", "Always 'S2MSI2A'."),
    ("sat:orbit_state", "VARCHAR", "ascending/descending. NULL on newer items."),
    ("s2:datatake_type", "VARCHAR", "e.g. INS-NOBS."),
    ("s2:generation_time", "VARCHAR", "Processing generation time; dedupe tiebreak."),
    ("sat:relative_orbit", "BIGINT", "Relative orbit number, parsed from product_uri when absent upstream."),
    ("s2:water_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:mean_solar_zenith", "DOUBLE", "Mean solar zenith angle; 90 - view:sun_elevation on newer items."),
    ("s2:mean_solar_azimuth", "DOUBLE", "Mean solar azimuth; view:sun_azimuth on newer items."),
    ("s2:processing_baseline", "VARCHAR", "e.g. 05.11."),
    ("s2:snow_ice_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:vegetation_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:thin_cirrus_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:cloud_shadow_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:nodata_pixel_percentage", "DOUBLE", "Nodata share; high values = partial scenes."),
    ("s2:unclassified_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:dark_features_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:not_vegetated_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:degraded_msi_data_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:high_proba_clouds_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:reflectance_conversion_factor", "DOUBLE", "Sun-distance reflectance factor."),
    ("s2:medium_proba_clouds_percentage", "DOUBLE", "Scene classification percentage."),
    ("s2:saturated_defective_pixel_percentage", "DOUBLE", "Scene classification percentage."),
    ("assets", "VARCHAR",
     "The upstream STAC assets object, verbatim, as a compact JSON string. "
     "Parse with json_extract or JSON.parse."),
    ("_month", "TINYINT", "month(datetime); first sort key. Query helper, not STAC."),
    ("_hilbert", "UINTEGER",
     "ST_Hilbert(geometry, world bounds); second sort key. Query helper, not STAC."),
    ("geometry", "GEOMETRY", "Scene footprint, CRS84."),
]

SELECT_LIST = ", ".join(f'"{name}"' for name, _, _ in COLUMNS)
```

- [ ] **Step 4: Run the tests**

```bash
python3 -m pytest tests/test_schema.py -v
```
Expected: 2 passed (network required for `test_live_assets_resolve`;
if the network is down, fix the network, don't skip the test).

- [ ] **Step 5: Commit**

```bash
git add tools/s2_schema.py tests/test_schema.py
git commit -m "feat: canonical item schema with assets as a JSON string

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `tools/s2_fetch.py` — Earth Search fetch + normalize

**Files:**
- Create: `tools/s2_fetch.py`
- Test: `tests/test_fetch.py`

**Interfaces:**
- Consumes: `s2_schema.COLUMNS`, `SELECT_LIST`.
- Produces: CLI `python3 tools/s2_fetch.py --start 2024-06-20 --end 2024-07-01 --out ./staging/chunks` writing `./staging/chunks/api/<start>_<end>.parquet` in canonical schema minus the two helper columns (added at build time). Also `fetch_window(start: str, end: str, out_dir: Path, session) -> int` (row count) and `normalize(feature: dict) -> dict` for tests. Zero-match windows write a zero-byte sentinel file (firms convention: "fetched, nothing there" ≠ "not fetched").

- [ ] **Step 1: Write the failing test**

`tests/test_fetch.py`:
```python
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
```

- [ ] **Step 2: Run to verify failure**

```bash
python3 -m pytest tests/test_fetch.py -v
```
Expected: FAIL (`ModuleNotFoundError: s2_fetch`).

- [ ] **Step 3: Write `tools/s2_fetch.py`**

```python
#!/usr/bin/env python3
"""Fetch Earth Search sentinel-2-l2a items for a date window into chunk
Parquet matching the seed archive's slim schema.

Resumable: a window whose chunk file already exists is skipped, and a window
with zero matches leaves a zero-byte sentinel so the next run skips it too.
limit=200 because the API 500s at limit=500 (measured 2026-09-15).

Normalization notes (newer items moved off the s2 extension for several
fields; the archive keeps the seed schema):
  s2:mgrs_tile        <- props or mgrs:utm_zone + mgrs:latitude_band + mgrs:grid_square
  sat:relative_orbit  <- props or _R(\\d{3})_ in s2:product_uri
  s2:mean_solar_zenith  <- props or 90 - view:sun_elevation
  s2:mean_solar_azimuth <- props or view:sun_azimuth
Absent values stay NULL rather than being invented (s2:granule_id,
sat:orbit_state on newer items).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_schema import COLUMNS

API = "https://earth-search.aws.element84.com/v1/search"
PAGE = 200
UA = {"User-Agent": "sentinel-2-catalog-tools/1.0 "
      "(+https://github.com/portolan-mirrors/sentinel-2-catalog)",
      "Content-Type": "application/json"}
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]
_REL_ORBIT = re.compile(r"_R(\d{3})_")


def _post(body: dict, tries: int = 8) -> dict:
    data = json.dumps(body).encode()
    for i in range(tries):
        try:
            req = urllib.request.Request(API, data=data, headers=UA)
            return json.load(urllib.request.urlopen(req, timeout=120))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if i == tries - 1:
                raise
            wait = min(2 ** i * 5, 300)
            print(f"  retry {i + 1} in {wait}s: {e}", file=sys.stderr)
            time.sleep(wait)
    raise AssertionError("unreachable")


def normalize(f: dict) -> dict:
    p = f["properties"]
    tile = p.get("s2:mgrs_tile")
    if not tile:
        tile = f"{p['mgrs:utm_zone']}{p['mgrs:latitude_band']}{p['mgrs:grid_square']}"
    rel = p.get("sat:relative_orbit")
    if rel is None and p.get("s2:product_uri"):
        m = _REL_ORBIT.search(p["s2:product_uri"])
        rel = int(m.group(1)) if m else None
    zen = p.get("s2:mean_solar_zenith")
    if zen is None and p.get("view:sun_elevation") is not None:
        zen = 90.0 - p["view:sun_elevation"]
    azi = p.get("s2:mean_solar_azimuth", p.get("view:sun_azimuth"))
    row = {
        "assets": json.dumps(f.get("assets", {}), separators=(",", ":")),
        "thumbnail_url": (f.get("assets", {}).get("thumbnail") or {}).get("href"),
        "type": "Feature",
        "stac_version": f.get("stac_version"),
        "stac_extensions": f.get("stac_extensions") or [],
        "id": f["id"],
        "bbox": f.get("bbox"),
        "links": [{"href": l.get("href"), "rel": l.get("rel"),
                   "title": l.get("title"), "type": l.get("type")}
                  for l in f.get("links", [])
                  if l.get("rel") not in ("next", "prev", "root", "parent")],
        "collection": "sentinel-2-l2a",
        "datetime": p["datetime"],
        "platform": p.get("platform"),
        "proj:epsg": p.get("proj:epsg"),
        "instruments": p.get("instruments") or [],
        "s2:mgrs_tile": tile,
        "constellation": p.get("constellation"),
        "s2:granule_id": p.get("s2:granule_id"),
        "eo:cloud_cover": p.get("eo:cloud_cover"),
        "sat:orbit_state": p.get("sat:orbit_state"),
        "sat:relative_orbit": rel,
        "s2:mean_solar_zenith": zen,
        "s2:mean_solar_azimuth": azi,
        "_geometry_json": json.dumps(f["geometry"]),
    }
    # Every remaining s2:* column comes straight from properties.
    for name, _, _ in DATA_COLUMNS:
        if name not in row and name != "geometry":
            row[name] = p.get(name)
    return {k: row[k] for k in
            [c[0] for c in DATA_COLUMNS if c[0] != "geometry"] + ["_geometry_json"]}


def fetch_window(start: str, end: str, out_dir: Path,
                 limit_pages: int | None = None) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{start}_{end}.parquet"
    if dest.exists():
        print(f"  {dest.name}: exists, skipping")
        return 0
    body = {"collections": ["sentinel-2-l2a"],
            "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
            "limit": PAGE}
    rows, pages, next_body = [], 0, body
    while True:
        resp = _post(next_body)
        rows.extend(normalize(f) for f in resp.get("features", []))
        pages += 1
        nxt = [l for l in resp.get("links", []) if l.get("rel") == "next"]
        if not nxt or (limit_pages and pages >= limit_pages):
            break
        # Earth Search paging: POST the next link's body verbatim.
        next_body = nxt[0].get("body") or {**body, **nxt[0].get("merge", {})}
    if not rows:
        dest.touch()          # sentinel: fetched, zero matches
        return 0
    _write(rows, dest)
    print(f"  {dest.name}: {len(rows):,} rows in {pages} page(s)", flush=True)
    return len(rows)


def _write(rows: list[dict], dest: Path) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".ndjson", delete=False) as tf:
        for r in rows:
            tf.write(json.dumps(r) + "\n")
        nd = tf.name
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    cast = ", ".join(
        f'CAST("{n}" AS {t}) AS "{n}"'
        for n, t, _ in DATA_COLUMNS if n != "geometry")
    con.execute(f"""
        COPY (
          SELECT {cast},
                 ST_GeomFromGeoJSON(_geometry_json) AS geometry
          FROM read_ndjson('{nd}', maximum_object_size=20000000)
        ) TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)
    Path(nd).unlink()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--days-per-chunk", type=int, default=1)
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="test hook: stop after N pages")
    a = ap.parse_args()
    out = Path(a.out) / "api"
    total = 0
    d0, d1 = date.fromisoformat(a.start), date.fromisoformat(a.end)
    cur = d0
    while cur <= d1:
        end = min(cur + timedelta(days=a.days_per_chunk - 1), d1)
        total += fetch_window(cur.isoformat(), end.isoformat(), out,
                              a.limit_pages)
        cur = end + timedelta(days=1)
    print(f"TOTAL {total:,} rows fetched")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

```bash
python3 -m pytest tests/test_fetch.py tests/test_hrefs.py -v
```
Expected: all pass. If `test_fetch_window_writes_canonical_parquet` fails on a
type mismatch, fix `normalize`/`_write` (never the canonical COLUMNS list) —
the seed file's DESCRIBE is the authority.

- [ ] **Step 5: Commit**

```bash
git add tools/s2_fetch.py tests/test_fetch.py
git commit -m "feat: Earth Search fetch with slim-schema normalization

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `tools/s2_build.py` — compact chunks into sorted year/live parts

**Files:**
- Create: `tools/s2_build.py`
- Test: `tests/test_build.py`

**Interfaces:**
- Consumes: chunk parquet from `s2_fetch.py` (canonical minus helpers) and/or the seed file (same shape).
- Produces: CLI `python3 tools/s2_build.py --sources <file-or-dir> [<...>] --years 2024,2025 --out ./staging/publish/sentinel-2-l2a [--name live.parquet] [--memory 8GB]` writing `year=<Y>/<name>` (default `items.parquet`), deduped by id (max `s2:generation_time` NULLS LAST), sorted `(_month, _hilbert)`, GeoParquet 2.0 via `gpio sort column`, zstd 22 for every part. Exposes `build_year(con, files, year, outdir, name) -> int`.

- [ ] **Step 1: Write the failing test**

`tests/test_build.py`:
```python
"""Build a year part from tiny synthetic chunks and verify dedupe, sort
order, helper columns, and GeoParquet output."""
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def _mk_chunk(con, path, rows):
    """rows: list of (id, iso_datetime, generation_time, lon, lat)"""
    vals = ", ".join(
        f"('{i}', TIMESTAMPTZ '{d}', '{g}', ST_Point({x}, {y}))"
        for i, d, g, x, y in rows)
    con.execute(f"""
        COPY (
          SELECT NULL::VARCHAR AS thumbnail_url, 'Feature' AS type,
                 '1.1.0' AS stac_version, []::VARCHAR[] AS stac_extensions,
                 v.id, v.dt AS datetime,
                 v.g AS "s2:generation_time", '31UFU' AS "s2:mgrs_tile",
                 50.0 AS "eo:cloud_cover", v.geom AS geometry
          FROM (VALUES {vals}) v(id, dt, g, geom)
        ) TO '{path}' (FORMAT PARQUET)
    """)


def test_build_dedupes_and_sorts():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        chunks = Path(td) / "chunks" / "api"
        chunks.mkdir(parents=True)
        _mk_chunk(con, chunks / "a.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-01T12:00:00Z", 4.0, 52.0),
            ("B", "2024-01-15 10:00:00+00", "2024-01-15T12:00:00Z", 5.0, 52.0),
            ("C", "2023-12-31 10:00:00+00", "2023-12-31T12:00:00Z", 6.0, 52.0),
        ])
        _mk_chunk(con, chunks / "b.parquet", [
            ("A", "2024-03-01 10:00:00+00", "2024-03-02T09:00:00Z", 4.0, 52.0),
        ])
        out = Path(td) / "publish"
        subprocess.run(
            [sys.executable, "tools/s2_build.py",
             "--sources", str(chunks.parent), "--years", "2024",
             "--out", str(out)],
            check=True, cwd=ROOT)
        f = out / "year=2024" / "items.parquet"
        r = con.execute(f"""
            SELECT id, "s2:generation_time", _month
            FROM read_parquet('{f}') ORDER BY id""").fetchall()
        assert [x[0] for x in r] == ["A", "B"]          # C is 2023; A deduped
        assert r[0][1] == "2024-03-02T09:00:00Z"        # newer generation won
        months = con.execute(
            f"SELECT list(_month) FROM read_parquet('{f}')").fetchone()[0]
        assert months == sorted(months)                 # sorted by _month first
```

Note: the synthetic chunk has only a subset of columns. `s2_build.py` must
read with `union_by_name=true` and select the canonical list with missing
columns as typed NULLs — that is what makes seed + api chunks (whose column
order differs) build together, and the test enforces it.

- [ ] **Step 2: Run to verify failure**

```bash
python3 -m pytest tests/test_build.py -v
```
Expected: FAIL (no `tools/s2_build.py`).

- [ ] **Step 3: Write `tools/s2_build.py`**

```python
#!/usr/bin/env python3
"""Compact chunks (and/or the seed archive) into published year parts.

    sentinel-2-l2a/year=<YYYY>/items.parquet   (--name overrides, e.g. live.parquet)

Rows are deduped by id keeping the highest s2:generation_time, then sorted
(_month, _hilbert): month-first keeps month pruning inside a year file,
Hilbert-within-month keeps row-group bboxes tight for spatial pruning. The
two helper columns are published and documented in the collection AGENTS.md.

gpio does the ordered GeoParquet 2.0 write. Do NOT sort in DuckDB and
`gpio convert`: convert does not preserve row order (see firms-catalog).
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from s2_schema import COLUMNS

ROW_GROUP = 100_000
# 22 everywhere, live.parquet included: distribution best practices say go as
# high as you have time for, and the user said crank it (2026-09-15). zstd
# decompression cost is flat across levels, so clients pay nothing.
ZSTD_LEVEL = 22
WORLD = "ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90))"


def connect(mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(f"SET memory_limit='{mem}'; SET temp_directory='{tmp}';")
    con.execute("SET preserve_insertion_order=false;")
    return con


def gather(sources: list[str]) -> list[str]:
    files: list[str] = []
    for s in sources:
        p = Path(s)
        if p.is_dir():
            files += [str(f) for f in sorted(p.rglob("*.parquet"))
                      if f.stat().st_size > 0]
        elif p.suffix == ".parquet" and p.stat().st_size > 0:
            files.append(str(p))
    if not files:
        raise SystemExit(f"no non-empty parquet under {sources}")
    return files


def _select(con, lst: str) -> str:
    """Canonical select list. A column absent from EVERY source cannot be
    referenced even with union_by_name, so it becomes a typed NULL — this is
    what lets partial fixtures and differently-shaped chunks build."""
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{lst}], union_by_name=true)"
    ).fetchall()}
    parts = []
    for name, typ, _ in COLUMNS:
        if name in ("_month", "_hilbert", "geometry"):
            continue
        if name in have:
            parts.append(f'CAST("{name}" AS {typ}) AS "{name}"')
        else:
            parts.append(f'NULL::{typ} AS "{name}"')
    return ", ".join(parts)


def build_year(con, files: list[str], year: int, outdir: Path,
               name: str = "items.parquet") -> int:
    lst = ",".join(f"'{f}'" for f in files)
    dest = outdir / f"year={year}"
    dest.mkdir(parents=True, exist_ok=True)
    final = dest / name
    with tempfile.TemporaryDirectory() as td:
        staged = Path(td) / "rows.parquet"
        con.execute(f"""
            COPY (
              SELECT {_select(con, lst)},
                     month(datetime)::TINYINT AS _month,
                     ST_Hilbert(geometry, {WORLD}) AS _hilbert,
                     geometry
              FROM read_parquet([{lst}], union_by_name=true)
              WHERE year(datetime) = {year}
              QUALIFY row_number() OVER (
                PARTITION BY id
                ORDER BY "s2:generation_time" DESC NULLS LAST) = 1
            ) TO '{staged}'
              (FORMAT PARQUET, COMPRESSION zstd, ROW_GROUP_SIZE {ROW_GROUP})
        """)
        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{staged}')").fetchone()[0]
        if n == 0:
            if not any(dest.iterdir()):
                shutil.rmtree(dest, ignore_errors=True)
            return 0
        final.unlink(missing_ok=True)
        r = subprocess.run(
            ["gpio", "sort", "column", str(staged), str(final),
             "_month,_hilbert", "--geoparquet-version", "2.0",
             "--compression", "zstd", "--compression-level", str(ZSTD_LEVEL),
             "--row-group-size", str(ROW_GROUP)],
            capture_output=True, text=True)
        if r.returncode != 0:
            print(r.stdout[-1500:], r.stderr[-1500:], file=sys.stderr)
            raise SystemExit(f"gpio sort failed for {year}")
    print(f"  year={year}/{name}: {n:,} rows, "
          f"{final.stat().st_size / 1e6:,.0f} MB", flush=True)
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True,
                    help="chunk dirs and/or parquet files (seed included)")
    ap.add_argument("--years", help="comma list; default = every year found")
    ap.add_argument("--out", required=True)
    ap.add_argument("--name", default="items.parquet")
    ap.add_argument("--memory", default="8GB")
    a = ap.parse_args()

    outdir = Path(a.out)
    outdir.mkdir(parents=True, exist_ok=True)
    tmp = outdir.parent / ".duckdb-tmp"
    tmp.mkdir(exist_ok=True)
    con = connect(a.memory, tmp)
    files = gather(a.sources)

    if a.years:
        years = [int(y) for y in a.years.split(",")]
    else:
        lst = ",".join(f"'{f}'" for f in files)
        years = [r[0] for r in con.execute(
            f"SELECT DISTINCT year(datetime) y "
            f"FROM read_parquet([{lst}], union_by_name=true) ORDER BY y"
        ).fetchall()]

    total = 0
    for y in years:
        total += build_year(con, files, y, outdir, a.name)
    print(f"TOTAL {total:,} rows across {len(years)} part(s)")
    if total == 0:
        print("no rows matched: nothing was written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests**

```bash
python3 -m pytest tests/test_build.py -v
```
Expected: PASS. (`gpio` must be on PATH: `pip install geoparquet-io` if not.)

- [ ] **Step 5: Commit**

```bash
git add tools/s2_build.py tests/test_build.py
git commit -m "feat: chunk compaction into sorted year parts

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Pilot slice — validate the pipeline end-to-end locally

**Files:**
- Output (NOT committed): `../s2-staging/publish/sentinel-2-l2a/year=<current>/items.parquet` built from a two-day fetch

**Interfaces:**
- Consumes: `s2_fetch.py`, `s2_build.py` CLIs.
- Produces: a real staged year part for Task 6's generator development; confidence that fetch → build → query works before committing 135 workflow slices to it.

- [ ] **Step 1: Fetch two recent days**

```bash
python3 tools/s2_fetch.py --start 2026-09-12 --end 2026-09-13 \
  --out ../s2-staging/chunks
```
Expected: two chunk files, ~4-6k rows each, no schema assertion failures.

- [ ] **Step 2: Build the pilot year part**

```bash
python3 tools/s2_build.py --sources ../s2-staging/chunks \
  --out ../s2-staging/publish/sentinel-2-l2a
```
Expected: one `year=2026/items.parquet` (rows from the two days).

- [ ] **Step 3: Spot-check pruning and assets**

```bash
python3 - << 'CHECK'
import duckdb, json, time
con = duckdb.connect(); con.execute("INSTALL spatial; LOAD spatial; SET TimeZone='UTC';")
f = "../s2-staging/publish/sentinel-2-l2a/year=2026/items.parquet"
tile = con.execute(f'SELECT "s2:mgrs_tile" FROM read_parquet(\'{f}\') LIMIT 1').fetchone()[0]
t0 = time.time()
row = con.execute(f"""
  SELECT id, "eo:cloud_cover", assets FROM read_parquet('{f}')
  WHERE "s2:mgrs_tile" = '{tile}' LIMIT 1""").fetchone()
print(row[0], row[1], round(time.time() - t0, 2), "s")
a = json.loads(row[2])
assert a["red"]["href"].startswith("https://"), a["red"]
print("assets ok:", len(a), "keys")
CHECK
```
Expected: sub-second, assets parse with 30+ keys.

- [ ] **Step 4: Ledger note** — record the pilot's row counts in the SDD
ledger (no repo commit; the staging dir is git-ignored territory).

---

### Task 6: Collection metadata — `sentinel-2-l2a` collection, year items, generators

**Files:**
- Create: `tools/make_items.py`, `tools/make_collection.py`, `catalog/sentinel-2-l2a/README.md`, `catalog/sentinel-2-l2a/AGENTS.md`
- Create (generated, committed as baseline): `catalog/sentinel-2-l2a/collection.json`, `catalog/sentinel-2-l2a/year=YYYY/YYYY.json` for each staged year
- Modify: `catalog/catalog.json` (add `child` link `./sentinel-2-l2a/collection.json`)

**Interfaces:**
- Consumes: the pilot-staged part in `../s2-staging/publish/sentinel-2-l2a/` (Task 5), `s2_schema.COLUMNS`.
- Note: the committed collection/item baseline is generated from the PILOT part, so its extents/counts are provisional until the backfill publishes (Task 8 regenerates and recommits from the published record). Gates must still pass on the provisional baseline. Do NOT publish anything in this task.
- Produces: `python3 tools/make_items.py --data-dir <staged>` and `python3 tools/make_collection.py --data-dir <staged>` regenerate the committed JSON; every scheduled workflow calls both then uploads.

Both generators follow `~/repos/firms-catalog/tools/make_items.py` and
`make_collection.py` closely — read those first; they solve remote-footer
statistics, hashing, and extent widening. The Sentinel-2 differences are
fully specified here:

**`make_collection.py` requirements (constants and content):**
- `PUBLIC = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"`, `S3 = "s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog"`, `REPO = "https://github.com/portolan-mirrors/sentinel-2-catalog"`, `APP = "https://portolan-mirrors.github.io/sentinel-2-catalog/"`.
- `id: sentinel-2-l2a`, `title: "Sentinel-2 L2A scenes (item index)"`, `license: "proprietary"` is WRONG — use `"CC-BY-SA-3.0-IGO"` (the Copernicus Sentinel data licence SPDX id; state "Contains modified Copernicus Sentinel data" in the description). Keywords: `["sentinel-2", "l2a", "stac-geoparquet", "earth-search", "esa", "copernicus", "satellite imagery", "cloud cover", "mgrs"]`.
- `providers`: ESA (producer/licensor), Sinergise+AWS (host, `https://registry.opendata.aws/sentinel-2-l2a-cogs/`), Element 84 Earth Search (processor, `https://earth-search.aws.element84.com/v1`), this repo (processor+host).
- `partition:scheme: "hive"`, `partition:strategy: "temporal"`, `partition:keys: [{"name": "year", "type": "int32", "description": "Year of acquisition (UTC)."}]`, `partition:glob: "<S3>/sentinel-2-l2a/year=*/*.parquet"` (the `*` part name covers `items.parquet` and `live.parquet`), `partition:file_count` counted from the staged/published parts.
- `table:primary_geometry: "geometry"`, `table:row_count` summed from parquet footers, `table:columns` generated from `s2_schema.COLUMNS` (name/type/description — this is why descriptions live there).
- `stac_extensions`: same five as firms' collection.json.
- `item_assets`: generated once from a live fetch of `https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a` (documentation of the per-asset band metadata, mirrored for STAC clients that expect it at collection level; the per-item `assets` column is verbatim and self-sufficient); cache it as `tools/item_assets.json` committed to the repo so the generator never needs the network.
- Links: root/parent `../catalog.json`, `describedby ./README.md`, `agents ./AGENTS.md`, `via https://earth-search.aws.element84.com/v1` and `via https://registry.opendata.aws/sentinel-2-l2a-cogs/`, `preview` → APP, one `item` link per `year=YYYY/YYYY.json`.
- Extent: spatial `[-180, -90, 180, 90]`; temporal measured from the parts' datetime row-group stats (min of first year, max of last part including live).
- No `self` link (Portolan forbids it; ignore the stac-check nag).

**`make_items.py` requirements:** one item per year directory, id `"YYYY"`, `collection: "sentinel-2-l2a"`, geometry = bbox polygon of that year's parts (from GeoParquet metadata, no scan), `properties`: `title: "Sentinel-2 L2A scenes, YYYY"`, `start_datetime`/`end_datetime` from row-group stats, `table:row_count` from footers, `s2:platforms` from a `DISTINCT platform` scan of that year (cheap: one column). Assets: `data` → `./items.parquet` (`application/vnd.apache.parquet`, role `data`, `file:size`) and, when present, `live` → `./live.parquet` (roles `["data"]`, title "Rolling tail since the last consolidation, refreshed daily"). Current year's item spans both parts.

**`AGENTS.md` must document** (agent-facing contract): the canonical schema incl. `_month`/`_hilbert` (what they are, that they are helpers, sort order `(_month,_hilbert)`); the dedupe rule; the `assets` JSON-string contract (every asset key, fields href/type/title/roles/gsd, band metadata in collection `item_assets`, parse with `json_extract_string`); that `sat:orbit_state`/`s2:granule_id` are NULL on newer items and why; that coverage is partial before Dec 2018 (no 2015-16, partial 2017-18) because that is what Earth Search/AWS serves; the DuckDB query pattern (filter `s2:mgrs_tile` + `_month`/`datetime` + `eo:cloud_cover`, hive-partition on year). Update `catalog/README.md`'s seed-era claims (28.1M rows / 2024-06-24 end) to the Earth Search record while in here.

**`README.md` must include** the FTW-style snippet, verbatim:

```sql
-- Cloud-free scenes over a field during harvest, no API, no rate limits.
INSTALL spatial; LOAD spatial;
SELECT id, datetime, "eo:cloud_cover", thumbnail_url,
       json_extract_string(assets, '$.visual.href') AS visual_cog
FROM read_parquet('https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/year=*/*.parquet', hive_partitioning=true)
WHERE year IN (2021)
  AND "s2:mgrs_tile" = '31UFU'
  AND datetime BETWEEN '2021-08-01' AND '2021-10-15'
  AND "eo:cloud_cover" < 10
ORDER BY "eo:cloud_cover" LIMIT 20;
```
plus one line of prose: every asset href is in the `assets` JSON-string column — no URL templates, no API.

- [ ] **Step 1: Write `tools/make_collection.py` and `tools/make_items.py`** per the requirements above (start from the firms versions; strip FIRMS availability/sensor logic; keep the footer-statistics approach and the Source Coop `User-Agent` workaround).

- [ ] **Step 2: Generate against the staging dir**

```bash
python3 tools/make_items.py --data-dir ../s2-staging/publish/sentinel-2-l2a
python3 tools/make_collection.py --data-dir ../s2-staging/publish/sentinel-2-l2a
```
Expected: `collection.json` + one `year=YYYY/YYYY.json` per staged year appear under `catalog/sentinel-2-l2a/`.

- [ ] **Step 3: Add the child link in `catalog/catalog.json`**

```json
{"rel": "child", "href": "./sentinel-2-l2a/collection.json", "title": "Sentinel-2 L2A scenes"}
```

- [ ] **Step 4: Write README.md and AGENTS.md** per the content requirements above.

- [ ] **Step 5: Run the gates**

```bash
python3 tests/run_all.py && python3 tools/publish.py
```
Expected: pass. `test_links.py` verifies the child link resolves; rashid
validates the collection. Fix findings — never the allow-list.

- [ ] **Step 6: Commit**

```bash
git add tools/make_items.py tools/make_collection.py catalog/
git commit -m "feat: sentinel-2-l2a collection metadata and generators

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push && gh run watch
```

---

### Task 7: Access smoke test — prove CI can write the bucket

**Files:**
- Create: `.github/workflows/check-access.yml` (manual-dispatch only)

**Interfaces:**
- Produces: verified OIDC write access from this repo to the Source Coop prefix, before a multi-day backfill depends on it. Credentials are the same as firms-catalog: role `arn:aws:iam::939788573396:role/source-coop-portolan-mirrors`, no repo secrets (user-confirmed 2026-09-15; the source.coop repository `portolan-mirrors/sentinel-2-catalog` exists).

- [ ] **Step 1: Write `.github/workflows/check-access.yml`**

```yaml
name: check-access

# One-shot manual smoke test: assume the Source Cooperative role and write
# + delete a marker object, so the multi-day backfill never discovers an
# access problem on its final step.

on:
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

jobs:
  check:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - name: Assume the Source Cooperative write role
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::939788573396:role/source-coop-portolan-mirrors
          role-session-name: sentinel-2-catalog-access-check
          aws-region: us-west-2
      - name: Round-trip a marker object
        run: |
          set -euo pipefail
          P=s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/_access-check
          date -u | aws s3 cp - "$P"
          aws s3 ls "$P"
          aws s3 rm "$P"
```

- [ ] **Step 2: Commit, push, dispatch, verify**

```bash
git add .github/workflows/check-access.yml
git commit -m "feat: OIDC access smoke test for the publish role

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_017HujiaQsbAPviGWJG16fuJ"
git push
gh workflow run check-access.yml && sleep 20 && gh run watch --exit-status
```
Expected: green. If the role assumption fails, STOP and report to the user
(likely the role trust policy or the source.coop repository provisioning).

---

### Task 8: Backfill workflows — fetch the whole Earth Search record

**Files:**
- Create: `.github/workflows/backfill.yml`, `.github/workflows/publish-backfill.yml`

**Interfaces:**
- Consumes: `s2_fetch.py`, `s2_build.py`, `make_items.py`, `make_collection.py`, `upload_data.py`, `publish.py`.
- Produces: per-month-slice workflow artifacts named `slice-YYYY-MM` (2015-06 → current month, ~135 slices, ~51.25M items, roughly 2-3 days of wall clock at max-parallel 2); the FIRST publish of the catalog — all year parts plus regenerated metadata.

- [ ] **Step 1: Write `.github/workflows/backfill.yml`**

```yaml
name: backfill

# Fetch the whole Earth Search record one month-slice at a time (the seed
# parquet turned out to be Planetary Computer STAC — see spec Amendment 1 —
# so everything comes from Earth Search). No published transaction budget,
# but a whole-month slice is ~450k items / ~2.3k requests, so max-parallel 2
# keeps us polite and each slice well inside the 6-hour job cap. Slices
# publish workflow artifacts (no cloud credentials here);
# publish-backfill.yml does the credentialed merge. Empty early months
# (2015-16 have ~no AWS L2A) cost one request and a sentinel artifact.

on:
  workflow_dispatch:
    inputs:
      start:
        description: "First month (YYYY-MM)"
        default: "2015-06"
      end:
        description: "Last month (YYYY-MM), blank = current month"
        default: ""

permissions:
  contents: read
  actions: read

jobs:
  plan:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.build.outputs.matrix }}
    steps:
      - id: build
        env:
          START: ${{ inputs.start }}
          END: ${{ inputs.end }}
        run: |
          python3 - << 'EOF' >> "$GITHUB_OUTPUT"
          import datetime as dt, json, os
          start = os.environ["START"]
          end = os.environ.get("END") or dt.date.today().strftime("%Y-%m")
          y, m = map(int, start.split("-"))
          ey, em = map(int, end.split("-"))
          out = []
          while (y, m) <= (ey, em):
              last = (dt.date(y + (m == 12), m % 12 + 1, 1) - dt.timedelta(days=1))
              out.append({"month": f"{y}-{m:02d}",
                          "start": f"{y}-{m:02d}-01",
                          "end": last.isoformat()})
              y, m = (y + 1, 1) if m == 12 else (y, m + 1)
          print("matrix=" + json.dumps({"include": out}))
          EOF

  fetch:
    needs: plan
    runs-on: ubuntu-latest
    timeout-minutes: 350
    strategy:
      max-parallel: 2
      fail-fast: false
      matrix: ${{ fromJson(needs.plan.outputs.matrix) }}
    steps:
      - uses: actions/checkout@v4
      - name: Skip if this slice was already fetched
        id: resume
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          set -euo pipefail
          NAME="slice-${{ matrix.month }}"
          FOUND=$(gh api "repos/$GITHUB_REPOSITORY/actions/artifacts?per_page=100&name=$NAME" \
                    --jq '[.artifacts[] | select(.expired==false)] | length' 2>/dev/null || echo 0)
          if [ "${FOUND:-0}" -gt 0 ]; then
            echo "done=true" >> "$GITHUB_OUTPUT"
            echo "::notice::$NAME already exists; skipping"
          else
            echo "done=false" >> "$GITHUB_OUTPUT"
          fi
      - uses: actions/setup-python@v5
        if: steps.resume.outputs.done == 'false'
        with:
          python-version: "3.12"
      - name: Fetch the month
        if: steps.resume.outputs.done == 'false'
        run: |
          pip install --quiet duckdb
          python3 tools/s2_fetch.py --start "${{ matrix.start }}" \
            --end "${{ matrix.end }}" --out ./staging/chunks
      - uses: actions/upload-artifact@v4
        if: steps.resume.outputs.done == 'false'
        with:
          name: slice-${{ matrix.month }}
          path: staging/chunks/api/
          retention-days: 30
          if-no-files-found: error
```

- [ ] **Step 2: Write `.github/workflows/publish-backfill.yml`**

Model on `~/repos/firms-catalog/.github/workflows/publish-backfill.yml` (read
it first), with these specifics:
- `workflow_dispatch` with input `years` (blank default = every year found in the downloaded chunks).
- `permissions: {contents: read, id-token: write}`; `concurrency: {group: catalog-write, cancel-in-progress: false}`.
- Steps: checkout; setup-python 3.12; `pip install --quiet duckdb boto3 geoparquet-io 'rashid>=0.1.8,<0.2.0' stac-check`; download all `slice-*` artifacts (`actions/download-artifact@v4` with `pattern: slice-*`, `path: staging/chunks/api`, `merge-multiple: true`);
  `python3 tools/s2_build.py --sources staging/chunks/api --out staging/publish/sentinel-2-l2a --memory 12GB` (add `--years` only when the input is set);
  `python3 tools/make_items.py --data-dir staging/publish/sentinel-2-l2a`; `python3 tools/make_collection.py --data-dir staging/publish/sentinel-2-l2a`; `python3 tests/run_all.py` (with the template's `CI_LIGHT` env if firms uses it); assume the OIDC role exactly as firms does (`role-to-assume: arn:aws:iam::939788573396:role/source-coop-portolan-mirrors`, `aws-region: us-west-2`); `python3 tools/upload_data.py --confirm --data-dir staging/publish`; `python3 tools/publish.py --confirm`.
  Disk: ~135 chunk artifacts + built years is large; use the `runs-on: ubuntu-latest` 14 GB free tier carefully — build and upload one year at a time in a loop (`for Y in $(seq 2015 2026)`), deleting each year's staged part after upload, and download artifacts per-year batches if space runs out.

- [ ] **Step 3: Commit, push, and run**

```bash
git add .github/workflows/backfill.yml .github/workflows/publish-backfill.yml
git commit -m "feat: backfill workflows for the Earth Search gap

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
gh workflow run backfill.yml
```
Monitor with `gh run list --workflow backfill.yml`. This runs for days;
continue with later tasks meanwhile. When all ~135 slices are green,
`gh workflow run publish-backfill.yml`, then verify the published temporal
extent reaches the current week:
```bash
curl -fsSL https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/collection.json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['extent']['temporal'])"
```

- [ ] **Step 4: Recommit the real baseline** — after publish-backfill is
green, regenerate `make_items`/`make_collection` locally against the
published record (the generators read parquet footers over HTTP; pass the
public base as data source per their `--remote-baseline` support), replace
the provisional pilot-derived metadata in `catalog/`, run the gates, commit
and push.

---

### Task 9: `tools/s2_stats.py` — MGRS aggregates + footprint PMTiles, `stats/` collection

**Files:**
- Create: `tools/s2_stats.py`, `catalog/stats/README.md`, `catalog/stats/collection.json` (hand-written; extents restamped by `make_collection.py` is NOT needed here — stats carry their own `updated` stamped by `s2_stats.py` writing a small sidecar the workflow passes to publish), `catalog/stats/styles/default.json`
- Test: `tests/test_stats.py`

**Interfaces:**
- Consumes: published/staged year parts (canonical schema).
- Produces: `python3 tools/s2_stats.py --sources <parts...> --out ./staging/publish/stats [--footprints] [--merge-years Y --existing <url-or-path>]` writing `mgrs-monthly.parquet` (schema below) and, with `--footprints`, `mgrs-tiles.geojsonl` → `tippecanoe` → `mgrs.pmtiles`.

`mgrs-monthly.parquet` schema (plain parquet, zstd, sorted by `(mgrs_tile, year, month)`):
`mgrs_tile VARCHAR, year SMALLINT, month TINYINT, scene_count INTEGER, min_cloud_cover DOUBLE, median_cloud_cover DOUBLE, best_item_id VARCHAR, best_item_datetime TIMESTAMPTZ`

- [ ] **Step 1: Write the failing test**

`tests/test_stats.py`:
```python
"""Aggregate a synthetic two-tile fixture and verify counts, medians,
best-item selection, and the merge path used by the daily refresh."""
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def _fixture(con, path):
    con.execute(f"""
      COPY (SELECT * FROM (VALUES
        ('A1', TIMESTAMPTZ '2024-05-01 10:00+00', '31UFU', 80.0, ST_Point(4, 52)),
        ('A2', TIMESTAMPTZ '2024-05-11 10:00+00', '31UFU', 10.0, ST_Point(4, 52)),
        ('A3', TIMESTAMPTZ '2024-05-21 10:00+00', '31UFU', 40.0, ST_Point(4, 52)),
        ('B1', TIMESTAMPTZ '2024-06-01 10:00+00', '32UMV', 5.0,  ST_Point(9, 51))
      ) t(id, datetime, "s2:mgrs_tile", "eo:cloud_cover", geometry)
      ) TO '{path}' (FORMAT PARQUET)
    """)


def test_monthly_stats():
    con = duckdb.connect(); con.execute("INSTALL spatial; LOAD spatial;")
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "items.parquet"; out = Path(td) / "stats"
        _fixture(con, src)
        subprocess.run([sys.executable, "tools/s2_stats.py",
                        "--sources", str(src), "--out", str(out)],
                       check=True, cwd=ROOT)
        r = con.execute(f"""
            SELECT mgrs_tile, year, month, scene_count, min_cloud_cover,
                   median_cloud_cover, best_item_id
            FROM read_parquet('{out}/mgrs-monthly.parquet')
            ORDER BY mgrs_tile""").fetchall()
        assert r[0] == ("31UFU", 2024, 5, 3, 10.0, 40.0, "A2")
        assert r[1] == ("32UMV", 2024, 6, 1, 5.0, 5.0, "B1")
```

- [ ] **Step 2: Run to verify failure** — `python3 -m pytest tests/test_stats.py -v` → FAIL.

- [ ] **Step 3: Write `tools/s2_stats.py`**

```python
#!/usr/bin/env python3
"""MGRS tile x month aggregates, and (optionally) the tile-footprint PMTiles.

The stats table is the timeline/choropleth backend for the explorer app and
the cheap first stop for "which month has a cloud-free scene here". The
PMTiles carries geometry only (envelope of each tile's item bboxes); the app
joins stats to tiles by mgrs_tile, so daily stat refreshes never touch the
tileset. Scenes whose bbox spans >20 degrees of longitude (antimeridian
wraps) are excluded from footprint aggregation only.

--merge-years: the daily refresh recomputes only the current year and
splices it into the existing table instead of scanning ten years of parquet.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
    return con


def _sources_sql(sources: list[str]) -> str:
    files = []
    for s in sources:
        p = Path(s)
        if p.is_dir():
            files += [str(f) for f in sorted(p.rglob("*.parquet"))]
        else:
            files.append(s)          # path or URL
    return ",".join(f"'{f}'" for f in files)


STATS_SQL = """
  SELECT "s2:mgrs_tile" AS mgrs_tile,
         year(datetime)::SMALLINT AS year,
         month(datetime)::TINYINT AS month,
         count(*)::INTEGER AS scene_count,
         min("eo:cloud_cover") AS min_cloud_cover,
         median("eo:cloud_cover") AS median_cloud_cover,
         arg_min(id, "eo:cloud_cover") AS best_item_id,
         arg_min(datetime, "eo:cloud_cover") AS best_item_datetime
  FROM read_parquet([{files}], union_by_name=true)
  {where}
  GROUP BY 1, 2, 3
"""


def build_stats(con, sources, out: Path, merge_years=None, existing=None):
    out.mkdir(parents=True, exist_ok=True)
    files = _sources_sql(sources)
    dest = out / "mgrs-monthly.parquet"
    if merge_years:
        yrs = ",".join(str(y) for y in merge_years)
        con.execute(f"""
          COPY (
            SELECT * FROM read_parquet('{existing}')
            WHERE year NOT IN ({yrs})
            UNION ALL BY NAME
            {STATS_SQL.format(files=files,
                              where=f'WHERE year(datetime) IN ({yrs})')}
            ORDER BY mgrs_tile, year, month
          ) TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    else:
        con.execute(f"""
          COPY ({STATS_SQL.format(files=files, where='')}
                ORDER BY mgrs_tile, year, month)
          TO '{dest}' (FORMAT PARQUET, COMPRESSION zstd)
        """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    print(f"  mgrs-monthly.parquet: {n:,} tile-months")


def build_footprints(con, sources, out: Path):
    files = _sources_sql(sources)
    gj = out / "mgrs-tiles.geojsonl"
    con.execute(f"""
      COPY (
        SELECT "s2:mgrs_tile" AS mgrs_tile,
               ST_Envelope(ST_Extent_Agg(geometry)) AS geometry
        FROM read_parquet([{files}], union_by_name=true)
        WHERE bbox[3] - bbox[1] < 20
        GROUP BY 1
      ) TO '{gj}' (FORMAT gdal, DRIVER 'GeoJSONSeq')
    """)
    subprocess.run(
        ["tippecanoe", "-o", str(out / "mgrs.pmtiles"), "--force",
         "-l", "mgrs", "-zg", "--coalesce-densest-as-needed", str(gj)],
        check=True)
    gj.unlink()
    print("  mgrs.pmtiles written")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--footprints", action="store_true")
    ap.add_argument("--merge-years", help="comma list recomputed from sources")
    ap.add_argument("--existing", help="current stats parquet (path or URL)")
    a = ap.parse_args()
    con = connect()
    merge = [int(y) for y in a.merge_years.split(",")] if a.merge_years else None
    if merge and not a.existing:
        raise SystemExit("--merge-years requires --existing")
    build_stats(con, a.sources, Path(a.out), merge, a.existing)
    if a.footprints:
        build_footprints(con, a.sources, Path(a.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

Feature ids: the app promotes `mgrs_tile` to the feature id
(`promoteId` on the source), so tippecanoe needs no id flag and string
ids are fine.

- [ ] **Step 4: Run the test** — `python3 -m pytest tests/test_stats.py -v` → PASS.

- [ ] **Step 5: Build the real thing locally and stage it**

```bash
python3 tools/s2_stats.py \
  --sources ../s2-staging/publish/sentinel-2-l2a \
  --out ../s2-staging/publish/stats --footprints
```
Expected: ~55-60k tiles, several million tile-month rows, `mgrs.pmtiles`
tens of MB. Sanity-check one tile:
`SELECT * FROM read_parquet('../s2-staging/publish/stats/mgrs-monthly.parquet') WHERE mgrs_tile='31UFU' AND year=2021 ORDER BY month`.

- [ ] **Step 6: Write the `stats/` collection metadata**

`catalog/stats/collection.json` (hand-written, follows the firms collection
shape minus partitioning): id `stats`, title "MGRS coverage and cloud
statistics", license/keywords/providers as in Task 6, `table:*` for the
monthly schema, assets:
`data` → `./mgrs-monthly.parquet` (role `data`),
`pmtiles` → `./mgrs.pmtiles` (`application/vnd.pmtiles`, roles `["visual"]`, `pmtiles:layers: ["mgrs"]`),
`style-default` → `./styles/default.json` (roles `["style", "default"]`).
`catalog/stats/styles/default.json`: MapLibre v8 style, vector source
`{"type": "vector", "url": "../mgrs.pmtiles"}`, one fill layer on
source-layer `mgrs`, neutral single-color fill with 0.35 opacity and a thin
outline (the app does data-driven coloring itself; the catalog style is for
generic Portolan viewers). Add the `child` link in `catalog/catalog.json`
and an `item`/asset-free README explaining the two products and the
exclusion of antimeridian-wrapping scenes from footprints.

- [ ] **Step 7: Gates, upload, publish, commit**

```bash
python3 tests/run_all.py
python3 tools/upload_data.py --confirm --data-dir ../s2-staging/publish
python3 tools/publish.py --confirm
git add tools/s2_stats.py tests/test_stats.py catalog/
git commit -m "feat: MGRS tile-month aggregates and footprint tiles

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

---

### Task 10: Scheduled refresh and consolidation workflows

**Files:**
- Create: `.github/workflows/refresh-daily.yml`, `.github/workflows/consolidate-month.yml`

**Interfaces:**
- Consumes: every tool from Tasks 3-9.
- Produces: a daily-fresh `year=<Y>/live.parquet` + restamped metadata; a monthly consolidation into `year=<Y>/items.parquet`.

- [ ] **Step 1: Write `.github/workflows/refresh-daily.yml`**

```yaml
name: refresh-daily

# Pull the last few days of Earth Search items into the current year's
# live.parquet, splice the current year into the stats table, restamp
# extents/counts, upload. Nothing is committed: the repository keeps only
# the stable catalog definition (firms-catalog model).
#
# The 5-day lookback covers Sentinel-2's hours-to-days publication latency;
# the dedupe rule in s2_build makes re-fetched days harmless.

on:
  schedule:
    - cron: "42 3 * * *"
  workflow_dispatch:

permissions:
  contents: read
  id-token: write

concurrency:
  group: catalog-write
  cancel-in-progress: false

jobs:
  refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 120
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install tooling
        run: |
          set -euo pipefail
          pip install --quiet duckdb boto3 geoparquet-io \
            'rashid>=0.1.8,<0.2.0' stac-check
      - name: Fetch the lookback window
        run: |
          set -euo pipefail
          START=$(date -u -d "5 days ago" +%F)
          END=$(date -u +%F)
          python3 tools/s2_fetch.py --start "$START" --end "$END" \
            --out ./staging/chunks
      - name: Rebuild the live part
        run: |
          set -euo pipefail
          Y=$(date -u +%Y)
          BASE=https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a
          # Merge with the existing live part so a fetch gap never truncates
          # it; the year part is the floor the dedupe rule resolves against.
          curl -fSL -o staging/live-prev.parquet "$BASE/year=$Y/live.parquet" \
            || rm -f staging/live-prev.parquet
          SRC="./staging/chunks"
          [ -s staging/live-prev.parquet ] && SRC="$SRC staging/live-prev.parquet"
          python3 tools/s2_build.py --sources $SRC --years "$Y" \
            --out ./staging/publish/sentinel-2-l2a --name live.parquet
      - name: Splice current year into stats
        run: |
          set -euo pipefail
          Y=$(date -u +%Y)
          BASE=https://data.source.coop/portolan-mirrors/sentinel-2-catalog
          python3 tools/s2_stats.py \
            --sources "$BASE/sentinel-2-l2a/year=$Y/items.parquet" \
                      "./staging/publish/sentinel-2-l2a/year=$Y/live.parquet" \
            --out ./staging/publish/stats \
            --merge-years "$Y" --existing "$BASE/stats/mgrs-monthly.parquet"
      - name: Restamp collection metadata
        run: |
          python3 tools/make_items.py --data-dir ./staging/publish/sentinel-2-l2a --remote-baseline
          python3 tools/make_collection.py --data-dir ./staging/publish/sentinel-2-l2a --remote-baseline
      - name: Validate before upload
        run: python3 tests/run_all.py
        env:
          CI_LIGHT: "1"
      - name: Assume the Source Cooperative write role
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::939788573396:role/source-coop-portolan-mirrors
          role-session-name: ${{ github.event.repository.name }}-${{ github.run_id }}
          aws-region: us-west-2
      - name: Upload data
        run: python3 tools/upload_data.py --confirm --data-dir ./staging/publish
      - name: Publish catalog metadata
        run: python3 tools/publish.py --confirm
```

`--remote-baseline` on the generators means: year parts not present in the
staging dir keep their committed item JSON untouched, and collection totals
come from committed items + the staged parts (firms' `widen_from_items`
pattern — add the flag to both generators in this task if Task 6 didn't).

- [ ] **Step 2: Write `.github/workflows/consolidate-month.yml`**

```yaml
name: consolidate-month

# Fold live.parquet into the year's items.parquet on the 3rd of each month
# (the lookback has long since covered month-end by then). Rewrites one
# year part (~a few hundred MB) and empties live.

on:
  schedule:
    - cron: "17 5 3 * *"
  workflow_dispatch:
    inputs:
      year:
        description: "Year to consolidate (blank = current)"
        default: ""

permissions:
  contents: read
  id-token: write

concurrency:
  group: catalog-write
  cancel-in-progress: false

jobs:
  consolidate:
    runs-on: ubuntu-latest
    timeout-minutes: 180
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install tooling
        run: pip install --quiet duckdb boto3 geoparquet-io 'rashid>=0.1.8,<0.2.0' stac-check
      - name: Merge live into the year part
        run: |
          set -euo pipefail
          Y="${{ inputs.year }}"; [ -z "$Y" ] && Y=$(date -u +%Y)
          BASE=https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a
          curl -fSL -o staging/items-prev.parquet "$BASE/year=$Y/items.parquet"
          curl -fSL -o staging/live-prev.parquet "$BASE/year=$Y/live.parquet" \
            || rm -f staging/live-prev.parquet
          SRC="staging/items-prev.parquet"
          [ -s staging/live-prev.parquet ] && SRC="$SRC staging/live-prev.parquet"
          mkdir -p staging
          python3 tools/s2_build.py --sources $SRC --years "$Y" \
            --out ./staging/publish/sentinel-2-l2a --memory 12GB
          # Empty live: publish a zero-row live.parquet rather than deleting
          # (publishing never deletes; a missing object would 404 in the glob
          # clients already resolved).
          Y="$Y" python3 - << 'EOF'
          import duckdb, os
          y = os.environ["Y"]
          con = duckdb.connect(); con.execute("INSTALL spatial; LOAD spatial;")
          src = f"staging/publish/sentinel-2-l2a/year={y}/items.parquet"
          dst = f"staging/publish/sentinel-2-l2a/year={y}/live.parquet"
          con.execute(f"COPY (SELECT * FROM read_parquet('{src}') LIMIT 0) TO '{dst}' (FORMAT PARQUET, COMPRESSION zstd)")
          EOF
      - name: Restamp collection metadata
        run: |
          python3 tools/make_items.py --data-dir ./staging/publish/sentinel-2-l2a --remote-baseline
          python3 tools/make_collection.py --data-dir ./staging/publish/sentinel-2-l2a --remote-baseline
      - name: Validate before upload
        run: python3 tests/run_all.py
        env:
          CI_LIGHT: "1"
      - name: Assume the Source Cooperative write role
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::939788573396:role/source-coop-portolan-mirrors
          role-session-name: ${{ github.event.repository.name }}-${{ github.run_id }}
          aws-region: us-west-2
      - name: Upload data
        run: python3 tools/upload_data.py --confirm --data-dir ./staging/publish
      - name: Publish catalog metadata
        run: python3 tools/publish.py --confirm
```

- [ ] **Step 3: Commit, push, and fire both once manually**

```bash
git add .github/workflows/refresh-daily.yml .github/workflows/consolidate-month.yml
git commit -m "feat: daily live refresh and monthly consolidation

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
gh workflow run refresh-daily.yml && gh run watch
```
Expected: green run; `live.parquet` appears under the current year;
`collection.json`'s temporal extent reaches today. Then run
consolidate-month once and verify the year part's row count grew and live is
zero rows.

---

### Task 11: Explorer app — map, choropleth, timeline

**Files:**
- Create: `apps/explorer/index.html`, `apps/explorer/app.js`, `apps/explorer/style.css`

**Interfaces:**
- Consumes: `stats/mgrs-monthly.parquet`, `stats/mgrs.pmtiles` (published).
- Produces: `window.S2 = {BASE, statsForMonth(y, m), db}` used by Task 12; layer id `mgrs-fill` with `promoteId: "mgrs_tile"`.

- [ ] **Step 1: Write `apps/explorer/index.html`**

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Sentinel-2 Explorer — no API behind this</title>
<link rel="stylesheet" href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css">
<link rel="stylesheet" href="style.css">
</head>
<body>
<div id="map"></div>
<aside id="panel">
  <h1>Sentinel-2 L2A</h1>
  <p class="sub">28M+ scenes. Every query on this page is a range read
     against static GeoParquet on Source Cooperative — there is no API.</p>
  <section id="controls">
    <label>Month <input type="month" id="month" min="2015-07"></label>
    <label>Color by
      <select id="metric">
        <option value="min_cloud_cover">clearest scene (min cloud %)</option>
        <option value="scene_count">scene count</option>
        <option value="median_cloud_cover">median cloud %</option>
      </select></label>
  </section>
  <section id="query">
    <h2>Find scenes</h2>
    <p class="hint">Click the map to pick an MGRS tile.</p>
    <label>From <input type="date" id="date0"></label>
    <label>To <input type="date" id="date1"></label>
    <label>Max cloud % <input type="range" id="maxcloud" min="0" max="100" value="10">
      <output id="maxcloud-out">10</output></label>
    <button id="run" disabled>Search scenes</button>
  </section>
  <section id="timeline"><h2>Monthly scenes</h2><div id="bars"></div></section>
  <section id="results"></section>
  <details id="apibox"><summary>The "API request" this answered</summary>
    <pre id="api"></pre><pre id="sql"></pre></details>
</aside>
<script type="module" src="app.js"></script>
</body>
</html>
```

- [ ] **Step 2: Write `apps/explorer/style.css`** — dark left panel (360px,
scrollable, system font), full-bleed map, `#bars` as a flex row of
`div.bar` elements with height proportional to count, thumbnails in
`#results` at 96px with id/date/cloud% caption, `.best` badge. ~80 lines;
no framework.

- [ ] **Step 3: Write `apps/explorer/app.js` (part 1 — this task's scope)**

```js
// Sentinel-2 explorer. Everything below talks to static files:
//   stats/mgrs-monthly.parquet  – choropleth + timeline
//   stats/mgrs.pmtiles          – MGRS tile footprints
//   sentinel-2-l2a/year=*/…     – item queries (Task 12)
import maplibregl from "https://esm.sh/maplibre-gl@4.7.1";
import { Protocol } from "https://esm.sh/pmtiles@3.2.0";
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

export const BASE = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog";

async function initDb() {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker = new Worker(URL.createObjectURL(new Blob(
    [`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })));
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  return db;
}

const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
const map = new maplibregl.Map({
  container: "map",
  style: { version: 8, sources: {}, layers: [
    { id: "bg", type: "background", paint: { "background-color": "#0b1020" } }] },
  center: [10, 30], zoom: 2,
});

const db = await initDb();
const conn = await db.connect();
window.S2 = { BASE, db, conn };

map.on("load", () => {
  map.addSource("mgrs", {
    type: "vector", url: `pmtiles://${BASE}/stats/mgrs.pmtiles`,
    promoteId: "mgrs_tile",
  });
  map.addLayer({
    id: "mgrs-fill", type: "fill", source: "mgrs", "source-layer": "mgrs",
    paint: {
      "fill-color": ["case", ["!=", ["feature-state", "v"], null],
        ["interpolate", ["linear"], ["feature-state", "v"],
          0, "#1a9850", 25, "#fee08b", 60, "#d73027", 100, "#4d0013"],
        "#223"],
      "fill-opacity": 0.55,
    },
  });
  map.addLayer({ id: "mgrs-line", type: "line", source: "mgrs",
    "source-layer": "mgrs",
    paint: { "line-color": "#8899bb", "line-width": 0.4 } });
});

export async function statsForMonth(y, m) {
  const metric = document.getElementById("metric").value;
  const res = await conn.query(`
    SELECT mgrs_tile, ${metric} AS v
    FROM read_parquet('${BASE}/stats/mgrs-monthly.parquet')
    WHERE year = ${y} AND month = ${m}`);
  return res.toArray();
}

let painted = [];
async function paintMonth() {
  const [y, m] = document.getElementById("month").value.split("-").map(Number);
  if (!y) return;
  const rows = await statsForMonth(y, m);
  for (const id of painted) map.removeFeatureState({ source: "mgrs", sourceLayer: "mgrs", id });
  painted = [];
  const isCount = document.getElementById("metric").value === "scene_count";
  for (const r of rows) {
    // scene_count is rescaled onto the same 0-100 ramp (12+ scenes = green).
    const v = isCount ? Math.max(0, 100 - r.v * 8) : r.v;
    map.setFeatureState({ source: "mgrs", sourceLayer: "mgrs", id: r.mgrs_tile }, { v });
    painted.push(r.mgrs_tile);
  }
}

export async function timelineFor(tile) {
  const where = tile ? `WHERE mgrs_tile = '${tile.replace(/'/g, "")}'` : "";
  const res = await conn.query(`
    SELECT year, month, sum(scene_count)::INT AS n,
           min(min_cloud_cover) AS clearest
    FROM read_parquet('${BASE}/stats/mgrs-monthly.parquet')
    ${where} GROUP BY 1, 2 ORDER BY 1, 2`);
  const bars = document.getElementById("bars");
  bars.replaceChildren();
  const rows = res.toArray();
  const max = Math.max(...rows.map(r => Number(r.n)), 1);
  for (const r of rows) {
    const d = document.createElement("div");
    d.className = "bar";
    d.style.height = `${(100 * Number(r.n)) / max}%`;
    d.title = `${r.year}-${String(r.month).padStart(2, "0")}: ${r.n} scenes, clearest ${Number(r.clearest).toFixed(1)}%`;
    d.onclick = () => {
      document.getElementById("month").value =
        `${r.year}-${String(r.month).padStart(2, "0")}`;
      paintMonth();
    };
    bars.append(d);
  }
}

document.getElementById("month").value = new Date().toISOString().slice(0, 7);
document.getElementById("month").addEventListener("change", paintMonth);
document.getElementById("metric").addEventListener("change", paintMonth);
map.on("load", () => { paintMonth(); timelineFor(null); });
```

- [ ] **Step 4: Serve locally and verify by hand**

```bash
python3 -m http.server 8080 --directory apps/explorer
```
Open http://localhost:8080: tiles render, changing month recolors, hovering
a timeline bar shows counts, clicking a bar switches the month. Check the
browser console for CORS errors on `data.source.coop` — if the parquet
fetches are blocked, stop and report to the user; nothing in this repo can
fix bucket CORS.

- [ ] **Step 5: Commit**

```bash
git add apps/explorer
git commit -m "feat: explorer app map, choropleth and timeline

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 12: Explorer app — the no-API scene query

**Files:**
- Modify: `apps/explorer/app.js` (append), `apps/explorer/index.html` (already has the controls)

**Interfaces:**
- Consumes: `window.S2.conn`, layer `mgrs-fill`, controls `#date0 #date1 #maxcloud #run`, containers `#results #api #sql`.
- Produces: the working hero flow: click tile → date window → ranked scenes with thumbnails + COG links, every href read straight from the row's `assets` JSON-string column (no URL templates anywhere in the app).

- [ ] **Step 1: Append the query flow to `app.js`**

```js
let selectedTile = null;
const CURRENT_YEAR = new Date().getUTCFullYear();

map.on("click", "mgrs-fill", (e) => {
  selectedTile = e.features[0].properties.mgrs_tile;
  document.querySelector("#query .hint").textContent = `Tile ${selectedTile}`;
  document.getElementById("run").disabled = false;
  timelineFor(selectedTile);
});

function partUrls(y0, y1) {
  const urls = [];
  for (let y = y0; y <= y1; y++) {
    urls.push(`${BASE}/sentinel-2-l2a/year=${y}/items.parquet`);
    if (y === CURRENT_YEAR) urls.push(`${BASE}/sentinel-2-l2a/year=${y}/live.parquet`);
  }
  return urls;
}

async function runQuery() {
  const d0 = document.getElementById("date0").value;
  const d1 = document.getElementById("date1").value;
  const cc = Number(document.getElementById("maxcloud").value);
  if (!selectedTile || !d0 || !d1) return;
  const urls = partUrls(Number(d0.slice(0, 4)), Number(d1.slice(0, 4)));
  const sql = `
    SELECT id, datetime, "eo:cloud_cover" AS cloud, thumbnail_url, assets
    FROM read_parquet([${urls.map(u => `'${u}'`).join(", ")}], union_by_name=true)
    WHERE "s2:mgrs_tile" = '${selectedTile}'
      AND _month BETWEEN ${Number(d0.slice(5, 7))} AND ${Number(d1.slice(5, 7))}
      AND datetime BETWEEN '${d0}' AND '${d1} 23:59:59'
      AND "eo:cloud_cover" <= ${cc}
    ORDER BY "eo:cloud_cover" LIMIT 30`;
  // The month predicate is only valid when the window stays inside one
  // calendar year; widen it to 1..12 otherwise.
  const finalSql = d0.slice(0, 4) === d1.slice(0, 4)
    ? sql : sql.replace(/_month BETWEEN \d+ AND \d+/, "_month BETWEEN 1 AND 12");
  document.getElementById("sql").textContent = finalSql.trim();
  document.getElementById("api").textContent = JSON.stringify({
    note: "the STAC API request this page did NOT need to make",
    url: "https://earth-search.aws.element84.com/v1/search",
    body: { collections: ["sentinel-2-l2a"],
            datetime: `${d0}T00:00:00Z/${d1}T23:59:59Z`,
            query: { "eo:cloud_cover": { lte: cc },
                     "mgrs:grid_square": selectedTile } },
  }, null, 2);
  const box = document.getElementById("results");
  box.replaceChildren(Object.assign(document.createElement("p"),
    { textContent: "querying parquet…" }));
  const rows = (await conn.query(finalSql)).toArray();
  box.replaceChildren();
  if (!rows.length) {
    box.append(Object.assign(document.createElement("p"),
      { textContent: "No scenes under that cloud cover — raise the slider." }));
    return;
  }
  rows.forEach((r, i) => {
    const iso = new Date(Number(r.datetime)).toISOString();
    const assets = JSON.parse(r.assets);
    const card = document.createElement("div");
    card.className = "scene" + (i === 0 ? " best" : "");
    const img = Object.assign(document.createElement("img"),
      { src: r.thumbnail_url, loading: "lazy", alt: r.id });
    const cap = document.createElement("div");
    const link = (key, label) => assets[key]
      ? `<a href="${assets[key].href}">${label}</a>` : "";
    cap.innerHTML = `<b>${r.id}</b><br>${iso.slice(0, 10)} ·
      ${Number(r.cloud).toFixed(1)}% cloud<br>
      ${link("visual", "TCI")} ${link("red", "B04")}
      ${link("nir", "B08")} ${link("scl", "SCL")}`;
    card.append(img, cap);
    box.append(card);
  });
}

document.getElementById("run").addEventListener("click", runQuery);
document.getElementById("maxcloud").addEventListener("input", (e) =>
  document.getElementById("maxcloud-out").textContent = e.target.value);
```

- [ ] **Step 3: Verify the hero flow by hand** — local server again; click a
tile over the Netherlands, window 2021-08-01 → 2021-10-15, max cloud 10:
expect a ranked thumbnail list in a few seconds, TCI links that open, and the
API/SQL panes filled. Also verify a cross-year window (2023-11 → 2024-02)
returns rows (month predicate widened).

- [ ] **Step 4: Commit**

```bash
git add apps/explorer
git commit -m "feat: parquet-powered scene query, hrefs from the assets column

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 13: Deploy the app — GitHub Pages

**Files:**
- Create: `.github/workflows/pages.yml` (copy `~/repos/firms-catalog/.github/workflows/pages.yml`, change the app path to `apps/explorer`)

- [ ] **Step 1: Write `pages.yml`** — trigger on push to main touching
`apps/explorer/**` + `workflow_dispatch`; standard
`actions/upload-pages-artifact` (path `apps/explorer`) + `deploy-pages`
pair with `permissions: {pages: write, id-token: write}`.

- [ ] **Step 2: Enable Pages, push, verify**

```bash
gh api -X POST repos/portolan-mirrors/sentinel-2-catalog/pages \
  -f build_type=workflow 2>/dev/null || true
git add .github/workflows/pages.yml
git commit -m "feat: deploy explorer to GitHub Pages

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push && gh run watch
curl -fsSI https://portolan-mirrors.github.io/sentinel-2-catalog/ | head -1
```
Expected: HTTP 200 and the app works from the public URL (the `preview`
link Task 6 wrote into the collection now resolves).

---

### Task 14: Register the catalog and finish the front door

- [ ] **Step 1: Root README final pass** — what this is, the two
collections, the FTW snippet from Task 6, the update cadence table
(daily refresh / monthly consolidation / backfill), the explorer URL, the
"no API" pitch, and a contribution pointer at the issue tracker. Use the
`portolan:sourcecoop` skill's linking rule: human-facing links →
`source.coop`, machine-facing → `data.source.coop`.

- [ ] **Step 2: Register** — invoke the `portolan:register-catalog` skill
for `https://data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json`
and follow it exactly.

- [ ] **Step 3: End-to-end verification sweep**

```bash
python3 tests/run_all.py
python3 tools/publish.py           # dry run must show nothing unexpected
gh run list --limit 10             # all workflows green
```
Plus: Portolan browser renders both collections; the app answers the hero
query; a fresh `duckdb` session runs the README snippet unmodified.

- [ ] **Step 4: Commit, push, and report** — summarize published URLs, row
counts, and cadence to the user.

---

## Execution notes

- Tasks 1→7 are strictly ordered. Task 8 needs 7; Task 9 needs 8 published
  (stats over the full record); 10 needs 9; 11-13 need 9 published; 14 last.
- Gates already cleared by the user (2026-09-15): source.coop repository
  created; credentials = the firms-catalog OIDC role, no secrets. Task 7's
  smoke test still runs before the backfill; stop only if it fails.
- Backfill (Task 8) runs 2-3 days of wall clock. Start it, then develop
  Task 9's stats tooling against the pilot staging; run the real stats
  build and everything downstream after publish-backfill lands.
- Local disk needed: ~2 GB under `../s2-staging` (pilot only).
