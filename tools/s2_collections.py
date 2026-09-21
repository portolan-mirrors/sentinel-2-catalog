#!/usr/bin/env python3
"""One record per published collection. Every tool reads its collection
from here, so the two collections cannot drift apart in code.

Named s2_collections, not collections: the stdlib module of that name is
imported during interpreter startup, so a tools/collections.py could never
be reached by `import collections` -- sys.modules already holds the stdlib
one by the time any tool or test runs."""
from __future__ import annotations

import re
from dataclasses import dataclass

import s2_schema
import s2c1_schema

DEFAULT = "sentinel-2-l2a"
PUBLIC = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"


@dataclass(frozen=True)
class CollectionConfig:
    id: str
    api_collection: str
    schema: object              # module with COLUMNS and normalize()
    tile_column: str
    id_re: re.Pattern           # groups: tile, day (YYYYMMDD)
    bucket: str
    key_root: str               # prefix under the bucket, with trailing /
    https_base: str
    item_key: str               # format: {zone}/{band}/{sq}/{year}/{month}/{id}/{id}.json
    inventory_bucket: str
    inventory_prefix: str
    catalog_dir: str
    stats_dir: str
    zone_split: bool
    row_group_mode: str         # "uniform" | "month_aligned"
    row_group_size: int | None  # None: s2_build falls back to its ROW_GROUP
    live_zstd_level: int
    lookback_field: str         # "datetime" | "created"

    @property
    def public_base(self) -> str:
        return f"{PUBLIC}/{self.catalog_dir}"


_FIRST = CollectionConfig(
    id="sentinel-2-l2a", api_collection="sentinel-2-l2a", schema=s2_schema,
    tile_column="s2:mgrs_tile",
    id_re=re.compile(r"^S2[A-Z]_(?P<tile>\d{1,2}[A-Z]{3})_(?P<day>\d{8})_\d+_L2A$"),
    bucket="sentinel-cogs", key_root="sentinel-s2-l2a-cogs/",
    https_base="https://sentinel-cogs.s3.us-west-2.amazonaws.com",
    item_key="{zone}/{band}/{sq}/{year}/{month}/{id}/{id}.json",
    inventory_bucket="sentinel-cogs-inventory",
    inventory_prefix="sentinel-cogs/sentinel-cogs/hive/",
    catalog_dir="sentinel-2-l2a", stats_dir="stats",
    zone_split=True, row_group_mode="uniform",
    # s2_build.ROW_GROUP is the single source; None means "use it".
    row_group_size=None,
    live_zstd_level=18, lookback_field="datetime")

_C1 = CollectionConfig(
    id="sentinel-2-c1-l2a", api_collection="sentinel-2-c1-l2a", schema=s2c1_schema,
    tile_column="_tile",
    id_re=re.compile(r"^S2[A-Z]_T(?P<tile>\d{1,2}[A-Z]{3})_(?P<day>\d{8})T\d{6}_L2A$"),
    bucket="e84-earth-search-sentinel-data", key_root="sentinel-2-c1-l2a/",
    https_base="https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com",
    item_key="{zone}/{band}/{sq}/{year}/{month}/{id}/{id}.json",
    inventory_bucket="e84-earth-search-sentinel-data-inventory",
    inventory_prefix="",        # Task 3 discovers and pins the real prefix
    catalog_dir="sentinel-2-c1-l2a", stats_dir="stats-c1",
    zone_split=False, row_group_mode="month_aligned", row_group_size=20_000,
    live_zstd_level=3, lookback_field="created")

_ALL = {c.id: c for c in (_FIRST, _C1)}
NAMES = tuple(_ALL)


def get(name: str) -> CollectionConfig:
    try:
        return _ALL[name]
    except KeyError:
        raise SystemExit(f"unknown collection {name!r}; valid: {', '.join(NAMES)}")


def add_collection_arg(ap) -> None:
    """The shared --collection option. Default keeps every existing call as is."""
    ap.add_argument("--collection", choices=NAMES, default=DEFAULT,
                    help="which published collection this run is for")
