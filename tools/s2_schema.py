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
