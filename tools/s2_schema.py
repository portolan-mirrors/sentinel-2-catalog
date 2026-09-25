#!/usr/bin/env python3
"""Canonical published schema.

Assets ship as a JSON string column holding the upstream assets object
verbatim, never a nested struct: deep struct nesting made earlier parquets
hard to open, and a string keeps every reader's schema flat. Verbatim
because it is nearly free — measured 179 B/row under zstd with clustered
ordering on 2026-09-15, before anyone knew the requested compression level
was not reaching the file; parts now publish at zstd 18, which cannot make
that number larger — and lossless beats clever. This module is the single
source of truth for the column list; collection metadata is generated
from it.

normalize() turns one Earth Search sentinel-2-l2a item into one row on this
schema. It lives here, next to the columns, so that s2_collections can
hand every tool a schema module with the same two names (COLUMNS,
normalize) for either collection; s2_fetch re-exports it. Only stdlib is
needed for it, which keeps this module free of duckdb for upload_part.
"""
from __future__ import annotations

import json
import re


# (name, duckdb type, description). Order is the upstream item's property
# order (fixed when the schema was set from Earth Search in 2026) with the
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

# The client name every tool here sends. Source Cooperative's CDN answers
# 403 to Python-urllib's default agent, so a request without this looks like
# a missing file rather than a rejected client. One constant, imported by
# s2_fetch, s2_build, make_items and upload_part; this module has no
# dependencies, so upload_part can import it without pulling duckdb in.
USER_AGENT = ("sentinel-2-catalog-tools/1.0 "
              "(+https://github.com/taylor-geospatial/s2-stac-geoparquet)")

# Everything normalize() emits: the two sort helpers are computed at build
# time, so a chunk parquet carries every column but those.
DATA_COLUMNS = [c for c in COLUMNS if c[0] not in ("_month", "_hilbert")]
_ROW_KEYS = [c[0] for c in DATA_COLUMNS if c[0] != "geometry"] + ["_geometry_json"]
_REL_ORBIT = re.compile(r"_R(\d{3})_")


def normalize(f: dict) -> dict:
    """One Earth Search sentinel-2-l2a item -> one canonical row, with the
    geometry as `_geometry_json` for the NDJSON -> parquet COPY step
    (s2_fetch.copy_ndjson_to_parquet).

    Newer items moved off the s2 extension for several fields; the archive
    keeps the seed schema:
      s2:mgrs_tile          <- props or mgrs:utm_zone + mgrs:latitude_band + mgrs:grid_square
      sat:relative_orbit    <- props or _R(\\d{3})_ in s2:product_uri
      s2:mean_solar_zenith  <- props or 90 - view:sun_elevation
      s2:mean_solar_azimuth <- props or view:sun_azimuth
    Absent values stay NULL rather than being invented (s2:granule_id,
    sat:orbit_state on newer items). Raises ValueError for an item with
    neither s2:mgrs_tile nor the three mgrs:* fields."""
    p = f["properties"]
    tile = p.get("s2:mgrs_tile")
    if not tile:
        try:
            tile = (f"{p['mgrs:utm_zone']}{p['mgrs:latitude_band']}"
                    f"{p['mgrs:grid_square']}")
        except KeyError as e:
            raise ValueError(
                f"{f.get('id', '<unknown id>')}: missing mgrs field {e} "
                "and no s2:mgrs_tile") from e
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
    for name in _ROW_KEYS:
        if name not in row:
            row[name] = p.get(name)
    return {k: row[k] for k in _ROW_KEYS}
