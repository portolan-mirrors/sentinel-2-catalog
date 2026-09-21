#!/usr/bin/env python3
"""Canonical published schema for the Collection 1 collection
(Earth Search `sentinel-2-c1-l2a`).

Same shape and conventions as s2_schema.py, the first collection's schema:
`thumbnail_url` first, then the union of the upstream item's fields and
properties, `assets` as a verbatim JSON string, the query helpers next,
`geometry` last. Frozen on 2026-09-21 from one live item per year across
2017-2026 (processing baselines 05.00, 05.09, 05.10, 05.11, 05.12, 05.13):
baselines <= 05.10 carry `s2:dark_features_percentage`, 05.11+ do not, so
it is a nullable column. tests/fixtures/c1_item.json (05.13) and
c1_item_2019.json (05.00) pin both shapes. Differences that matter to a
reader:

* Collection 1 items carry no `s2:mgrs_tile`; the tile lives in
  `grid:code` as `MGRS-31UET`. The join key is therefore the derived
  `_tile` column (the bare id, `31UET`), which sits with the other helpers
  so `SELECT * EXCLUDE (geometry), geometry` still round-trips.
* `created` / `updated` are real columns: Collection 1 back-processes old
  scenes, so `created` -- not `datetime` -- is the field an incremental
  fetch looks back on.
* Two upstream properties are objects. `proj:centroid` becomes a
  STRUCT(lat, lon); `processing:software` (a name -> version map whose keys
  can change) is a compact JSON string, like `assets`.

A property the schema does not know is not an error: normalize() drops it
from the row and counts it in UNKNOWN_PROPERTIES so a fetch can log what
upstream added since the freeze.
"""
from __future__ import annotations

import collections
import json

from s2_schema import USER_AGENT  # noqa: F401  re-exported: one client name

_LINKS = 'STRUCT(href VARCHAR, rel VARCHAR, title VARCHAR, "type" VARCHAR)[]'
_TS = "TIMESTAMP WITH TIME ZONE"
_PCT = "Scene classification percentage."

# (name, duckdb type, description).
COLUMNS = [
    ("thumbnail_url", "VARCHAR", "Preview JPEG on the e84-earth-search-sentinel-data bucket."),
    ("type", "VARCHAR", "Always 'Feature'."),
    ("stac_version", "VARCHAR", "STAC version of the source item."),
    ("stac_extensions", "VARCHAR[]", "Extension schema URIs of the source item."),
    ("id", "VARCHAR", "Earth Search item id, e.g. S2B_T31UET_20260921T105030_L2A."),
    ("bbox", "DOUBLE[]", "Item bounding box [w, s, e, n], CRS84."),
    ("links", _LINKS, "Source item links (canonical et al.); paging links are stripped."),
    ("collection", "VARCHAR", "Always 'sentinel-2-c1-l2a'."),
    ("datetime", _TS, "Acquisition datetime, UTC."),
    ("created", _TS, "When Earth Search created the item; the incremental-fetch lookback field."),
    ("updated", _TS, "When Earth Search last updated the item."),
    ("platform", "VARCHAR", "sentinel-2a / sentinel-2b / sentinel-2c."),
    ("constellation", "VARCHAR", "Always 'sentinel-2'."),
    ("instruments", "VARCHAR[]", "Always ['msi']."),
    ("grid:code", "VARCHAR", "MGRS grid code, e.g. MGRS-31UET. `_tile` is the bare id."),
    ("mgrs:utm_zone", "BIGINT", "UTM zone number, 1-60."),
    ("mgrs:latitude_band", "VARCHAR", "MGRS latitude band letter."),
    ("mgrs:grid_square", "VARCHAR", "MGRS 100 km grid square."),
    ("proj:epsg", "BIGINT", "UTM EPSG code of the scene grid."),
    ("proj:centroid", "STRUCT(lat DOUBLE, lon DOUBLE)", "Scene centroid, CRS84."),
    ("eo:cloud_cover", "DOUBLE", "Scene cloud cover percentage, 0-100."),
    ("s2:tile_id", "VARCHAR", "ESA tile (granule) id."),
    ("s2:degraded_msi_data_percentage", "DOUBLE", _PCT),
    ("s2:nodata_pixel_percentage", "DOUBLE", "Nodata share; high values = partial scenes."),
    ("s2:saturated_defective_pixel_percentage", "DOUBLE", _PCT),
    ("s2:dark_features_percentage", "DOUBLE",
     _PCT + " Present on processing baselines <= 05.10; NULL on 05.11+."),
    ("s2:cloud_shadow_percentage", "DOUBLE", _PCT),
    ("s2:vegetation_percentage", "DOUBLE", _PCT),
    ("s2:not_vegetated_percentage", "DOUBLE", _PCT),
    ("s2:water_percentage", "DOUBLE", _PCT),
    ("s2:unclassified_percentage", "DOUBLE", _PCT),
    ("s2:medium_proba_clouds_percentage", "DOUBLE", _PCT),
    ("s2:high_proba_clouds_percentage", "DOUBLE", _PCT),
    ("s2:thin_cirrus_percentage", "DOUBLE", _PCT),
    ("s2:snow_ice_percentage", "DOUBLE", _PCT),
    ("s2:product_type", "VARCHAR", "Always 'S2MSI2A'."),
    ("s2:processing_baseline", "VARCHAR", "e.g. 05.13."),
    ("s2:product_uri", "VARCHAR", "ESA product name."),
    ("s2:generation_time", "VARCHAR", "Processing generation time; dedupe tiebreak."),
    ("s2:datatake_id", "VARCHAR", "ESA datatake id."),
    ("s2:datatake_type", "VARCHAR", "e.g. INS-NOBS."),
    ("s2:datastrip_id", "VARCHAR", "ESA datastrip id."),
    ("s2:reflectance_conversion_factor", "DOUBLE", "Sun-distance reflectance factor."),
    ("view:azimuth", "DOUBLE", "Mean viewing azimuth angle, degrees."),
    ("view:incidence_angle", "DOUBLE", "Mean viewing incidence angle, degrees."),
    ("view:sun_azimuth", "DOUBLE", "Mean solar azimuth angle, degrees."),
    ("view:sun_elevation", "DOUBLE", "Mean solar elevation angle, degrees."),
    ("storage:platform", "VARCHAR", "Always 'AWS'."),
    ("storage:region", "VARCHAR", "Always 'us-west-2'."),
    ("storage:requester_pays", "BOOLEAN", "Always false."),
    ("processing:software", "VARCHAR",
     "The upstream processing:software object (name -> version), verbatim, "
     "as a compact JSON string."),
    ("earthsearch:payload_id", "VARCHAR", "Earth Search ingest payload id."),
    ("assets", "VARCHAR",
     "The upstream STAC assets object, verbatim, as a compact JSON string. "
     "Parse with json_extract or JSON.parse."),
    ("_month", "TINYINT", "month(datetime); first sort key. Query helper, not STAC."),
    ("_hilbert", "UINTEGER",
     "ST_Hilbert(geometry, world bounds); second sort key. Query helper, not STAC."),
    ("_tile", "VARCHAR", "MGRS tile id from grid:code, e.g. 31UET. THE spatial join key."),
    ("geometry", "GEOMETRY", "Scene footprint, CRS84."),
]

# Everything normalize() emits: the sort helpers are computed at build time.
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]
_ROW_KEYS = [c[0] for c in DATA_COLUMNS if c[0] != "geometry"] + ["_geometry_json"]
_KNOWN_PROPERTIES = frozenset(c[0] for c in COLUMNS)

# Drift guard: upstream property names normalize() has seen that are not
# in COLUMNS, with counts. Never raises; a fetch reads and logs it.
UNKNOWN_PROPERTIES: collections.Counter = collections.Counter()


def normalize(f: dict) -> dict:
    """One Earth Search Collection 1 item -> one canonical row, with the
    geometry as `_geometry_json` for the NDJSON -> parquet COPY step
    (s2_fetch.copy_ndjson_to_parquet)."""
    p = f["properties"]
    UNKNOWN_PROPERTIES.update(k for k in p if k not in _KNOWN_PROPERTIES)
    code = p.get("grid:code")
    tile = code.removeprefix("MGRS-") if code else None
    if not tile:
        try:
            tile = (f"{p['mgrs:utm_zone']}{p['mgrs:latitude_band']}"
                    f"{p['mgrs:grid_square']}")
        except KeyError as e:
            raise ValueError(
                f"{f.get('id', '<unknown id>')}: missing mgrs field {e} "
                "and no grid:code") from e
    software = p.get("processing:software")
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
        "collection": "sentinel-2-c1-l2a",
        "datetime": p["datetime"],
        "instruments": p.get("instruments") or [],
        "proj:centroid": ({"lat": p["proj:centroid"].get("lat"),
                           "lon": p["proj:centroid"].get("lon")}
                          if p.get("proj:centroid") else None),
        "processing:software": (json.dumps(software, separators=(",", ":"))
                                if software is not None else None),
        "_tile": tile,
        "_geometry_json": json.dumps(f["geometry"]),
    }
    # Every other column comes straight from properties by name.
    for name in _ROW_KEYS:
        if name not in row:
            row[name] = p.get(name)
    return {k: row[k] for k in _ROW_KEYS}
