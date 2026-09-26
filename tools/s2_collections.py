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
    title: str                  # what the collection is called in links and item titles
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
    sort_key: str               # gpio sort columns of a year's parts (s2_build.sort_key)
    row_group_mode: str         # "uniform" | "month_aligned"
    row_group_size: int | None  # None: s2_build falls back to its ROW_GROUP
    live_zstd_level: int
    lookback_field: str         # "datetime" | "created"
    # Is the year's live tail cut into one file per month
    # (live-01.parquet .. live-12.parquet, s2_build.live_part_names) instead
    # of one live.parquet? The daily refresh rewrites and re-uploads every
    # live file its lookback touches, so a monthly tail caps that cost at one
    # month, whatever the year holds. The first collection keeps one
    # live.parquet: consolidate-month.yml folds it every month, so it never
    # grows past a month either way.
    monthly_live: bool = False

    @property
    def public_base(self) -> str:
        return f"{PUBLIC}/{self.catalog_dir}"


_FIRST = CollectionConfig(
    id="sentinel-2-l2a", api_collection="sentinel-2-l2a",
    title="Sentinel-2 L2A scenes", schema=s2_schema,
    tile_column="s2:mgrs_tile",
    id_re=re.compile(r"^S2[A-Z]_(?P<tile>\d{1,2}[A-Z]{3})_(?P<day>\d{8})_\d+_L2A$"),
    bucket="sentinel-cogs", key_root="sentinel-s2-l2a-cogs/",
    https_base="https://sentinel-cogs.s3.us-west-2.amazonaws.com",
    item_key="{zone}/{band}/{sq}/{year}/{month}/{id}/{id}.json",
    inventory_bucket="sentinel-cogs-inventory",
    inventory_prefix="sentinel-cogs/sentinel-cogs/hive/",
    catalog_dir="sentinel-2-l2a", stats_dir="stats",
    zone_split=True,
    # The key of parts from s2_build.TILE_SORT_FROM; earlier vintages keep
    # (_month, _hilbert) -- sort_key() applies that rule for this one.
    sort_key="_month,s2:mgrs_tile,_hilbert", row_group_mode="uniform",
    # s2_build.ROW_GROUP is the single source; None means "use it".
    row_group_size=None,
    live_zstd_level=18, lookback_field="datetime")

_C1 = CollectionConfig(
    id="sentinel-2-c1-l2a", api_collection="sentinel-2-c1-l2a",
    title="Sentinel-2 Collection 1 L2A scenes", schema=s2c1_schema,
    tile_column="_tile",
    id_re=re.compile(r"^S2[A-Z]_T(?P<tile>\d{1,2}[A-Z]{3})_(?P<day>\d{8})T\d{6}_L2A$"),
    bucket="e84-earth-search-sentinel-data", key_root="sentinel-2-c1-l2a/",
    https_base="https://e84-earth-search-sentinel-data.s3.us-west-2.amazonaws.com",
    item_key="{zone}/{band}/{sq}/{year}/{month}/{id}/{id}.json",
    inventory_bucket="e84-earth-search-sentinel-data-inventory",
    # Listed live 2026-09-21: hive/dt=YYYY-MM-DD-HH-MM/symlink.txt manifests,
    # daily since 2024-04-02, pointing at Parquet data files (not CSV).
    inventory_prefix="e84-earth-search-sentinel-data/primary/hive/",
    catalog_dir="sentinel-2-c1-l2a", stats_dir="stats-c1",
    zone_split=False,
    # Spec Amendment 1 (issue #9): tile-major, so one tile's year is one
    # contiguous run and any tile window is one or two row groups; uniform
    # groups near 6,000 rows (DuckDB fills them in 2,048-row steps, so
    # 6,144). The month-aligned writer stays behind --row-group-mode.
    sort_key="_tile,datetime", row_group_mode="uniform", row_group_size=6_000,
    live_zstd_level=3, lookback_field="created",
    # One live file per month: nothing on GitHub folds this collection, so a
    # single live.parquet grew by ~15,000 rows (~24 MB) a day and the daily
    # refresh rewrote and re-uploaded all of it. A month caps that.
    monthly_live=True)

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
