#!/usr/bin/env python3
"""Inventory-vs-catalog completeness audit: is every scene the S3 Inventory
says exists in a collection's source bucket actually in that collection's
published catalog?

The bucket is ground truth here (S3 has no outage windows; the API does --
see s2_repair.py). Each collection's S3 Inventory lists every object in
its source bucket as of that inventory run, under hive-partitioned
hive/dt=YYYY-MM-DD-HH-MM/symlink.txt manifests; --collection (default
sentinel-2-l2a) picks inventory bucket, manifest prefix, key root and
catalog directory from s2_collections:
  * sentinel-2-l2a: s3://sentinel-cogs-inventory/sentinel-cogs/sentinel-cogs/hive/
    (checked live 2026-09-16 against dt=2026-09-15-01-00): manifest.json
    says fileFormat "CSV", fileSchema "Bucket, Key, Size, LastModifiedDate"
    -- plain gzipped CSV with NO header row, not ORC.
  * sentinel-2-c1-l2a: s3://e84-earth-search-sentinel-data-inventory/
    e84-earth-search-sentinel-data/primary/hive/ (listed live 2026-09-21,
    daily partitions since 2024-04-02): fileFormat "Parquet", columns
    bucket, key, size, last_modified_date; 420 data files of ~20 MB under
    primary/data/, anonymous HTTPS GET verified.
DuckDB reads either directly; inventory_source() picks read_csv (explicit
column list, no header) or read_parquet from the data files' suffix, so
the format is discovered from the manifest rather than pinned in code.

Each scene directory holds more than one JSON file -- "{id}.json" (the
item) plus a sidecar ("tileinfo_metadata.json" in the first collection's
bucket, "tileInfo.json" in C1's), both ending in ".json" -- so the item is
identified by the brief's actual rule: the file is SELF-NAMED, i.e. the
filename stem equals the enclosing directory name ({id}/{id}.json). RE2 (
DuckDB's regex engine) has no backreferences, so this is checked by
extracting the directory-name and filename-stem groups separately and
comparing them for equality in SQL, not inside one regex.

The first collection's bucket also holds ~64k keys (see s2_repair.py's
_is_valid_zone() note) that end in ".json" and even happen to be
self-named, but sit directly under a bogus "sentinel-s2-l2a-cogs/2019/"
root prefix with no {yyyy}/{m}/ segments at all -- these fail the
year/month regex entirely and MUST be filtered out of the WHERE clause with
regexp_matches(), not just left for the CAST to sort out: DuckDB's
CAST('' AS INTEGER) raises rather than returning NULL, so a
regexp_extract() that found no match crashes the whole query instead of
producing an excludable row.

Truth: the audit defines "expected" from the bucket. Earth Search keeps
multiple processing sequences (s2:sequence) for some scenes, so a small
positive delta (published fewer than bucket, i.e. delta < 0) or the
reverse can legitimately occur -- use --tolerance to allow it.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
import s2_collections as cols  # noqa: E402
from s2_collections import CollectionConfig  # noqa: E402

DEFAULT_CONFIG = cols.get(cols.DEFAULT)

# Anchors the tail of an item key: .../{yyyy}/{m}/{dirname}/{filestem}.json.
# m is 1 or 2 digits (neither bucket zero-pads month path segments).
# Groups 3 (dirname) and 4 (filestem) are compared for equality in SQL to
# enforce the self-named {id}/{id}.json rule -- RE2 has no backreferences.
# The same tail serves both collections; the key root ahead of it is the
# collection-specific part and comes from config.key_root.
KEY_RE = r'/(\d{4})/(\d{1,2})/([^/]+)/([^/]+)\.json$'

CSV_COLUMNS = {"bucket": "VARCHAR", "key": "VARCHAR",
              "size": "BIGINT", "last_modified": "VARCHAR"}


def _s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="us-west-2")


def resolve_manifest_key(s3, inventory_date: str | None = None,
                         config: CollectionConfig = DEFAULT_CONFIG) -> str:
    """The hive/dt=.../symlink.txt manifest key: the one named explicitly,
    or the newest partition found by listing."""
    if not config.inventory_prefix:
        raise SystemExit(f"no inventory for collection {config.id}: "
                         "its CollectionConfig has no inventory_prefix")
    if inventory_date:
        return f"{config.inventory_prefix}dt={inventory_date}/symlink.txt"
    paginator = s3.get_paginator("list_objects_v2")
    dts = []
    for page in paginator.paginate(Bucket=config.inventory_bucket,
                                   Prefix=config.inventory_prefix, Delimiter="/"):
        dts.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    if not dts:
        raise SystemExit(f"no inventory partitions found under "
                         f"s3://{config.inventory_bucket}/{config.inventory_prefix}")
    return sorted(dts)[-1] + "symlink.txt"


def manifest_data_urls(s3, manifest_key: str,
                       config: CollectionConfig = DEFAULT_CONFIG) -> list[str]:
    """The symlink.txt manifest is itself a text file listing s3:// URLs of
    the actual data files (gzipped CSV or Parquet, see the module
    docstring); turn those into plain HTTPS URLs (both inventory buckets
    allow anonymous GET, verified live) DuckDB's httpfs can read with no
    credentials."""
    body = s3.get_object(Bucket=config.inventory_bucket,
                         Key=manifest_key)["Body"].read().decode()
    urls = []
    for line in body.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if not line.startswith("s3://"):
            raise SystemExit(f"unexpected manifest line (not s3://): {line}")
        bucket, key = line[len("s3://"):].split("/", 1)
        urls.append(f"https://{bucket}.s3.us-west-2.amazonaws.com/{key}")
    return urls


def _month_where(months: list[str] | None,
                 config: CollectionConfig = DEFAULT_CONFIG) -> str:
    # The LIKE is a cheap pre-filter (ends in .json under the collection's
    # key root); regexp_matches excludes keys with no {yyyy}/{m}/ pair at
    # all (the bogus root-level "2019/" scenes) BEFORE the CAST below ever
    # sees them -- required, not just tidy: CAST('' AS INTEGER) raises in
    # DuckDB, it does not return NULL, so an unmatched regexp_extract()
    # would crash the whole query rather than being excludable. The final
    # clause enforces the self-named {id}/{id}.json rule (drops the sidecar
    # json and any other non-self-named json alongside a real item).
    root = config.key_root
    where = (
        f"key LIKE '{root}%.json' "
        f"AND regexp_matches(key, '{KEY_RE}') "
        f"AND regexp_extract(key, '{KEY_RE}', 3) = regexp_extract(key, '{KEY_RE}', 4)"
    )
    if not months:
        return where
    clauses = []
    for ym in months:
        y, m = ym.split("-")
        clauses.append(f"key LIKE '{root}%/{y}/{int(m)}/%.json'")
    return where + " AND (" + " OR ".join(clauses) + ")"


def inventory_source(data_paths: list[str]) -> str:
    """The FROM-clause source for the inventory data files: read_parquet
    when they are Parquet (C1's inventory), read_csv with the header-less
    CSV column list when they are CSV (the first collection's, .csv.gz).
    The suffix comes from the manifest, so this follows whatever format
    the inventory is configured with; a mixed or unknown set is refused
    rather than half-read."""
    if not data_paths:
        raise SystemExit("inventory manifest lists no data files")
    files = ",".join(f"'{p}'" for p in data_paths)
    if all(p.endswith(".parquet") for p in data_paths):
        return f"read_parquet([{files}])"
    if all(p.endswith((".csv", ".csv.gz")) for p in data_paths):
        columns = ", ".join(f"'{k}': '{v}'" for k, v in CSV_COLUMNS.items())
        return f"read_csv([{files}], columns={{{columns}}}, header=false)"
    kinds = sorted({Path(p).suffix or "(none)" for p in data_paths})
    raise SystemExit(f"inventory data files must all be .parquet or .csv[.gz]; "
                     f"got suffixes {kinds}")


def expected_month_counts(con, data_paths: list[str],
                          months: list[str] | None = None,
                          config: CollectionConfig = DEFAULT_CONFIG,
                          ) -> dict[tuple[int, int], int]:
    """Expected item counts per (year, month) from inventory data file(s).
    `data_paths` are local paths or URLs DuckDB can open directly -- real
    use passes manifest_data_urls() output, tests pass a local fixture.
    Only keys under config.key_root count, so a fixture holding both
    collections' keys audits one at a time."""
    rows = con.execute(f"""
        SELECT
          CAST(regexp_extract(key, '{KEY_RE}', 1) AS INTEGER) AS year,
          CAST(regexp_extract(key, '{KEY_RE}', 2) AS INTEGER) AS month,
          count(*) AS expected
        FROM {inventory_source(data_paths)}
        WHERE {_month_where(months, config)}
        GROUP BY 1, 2
    """).fetchall()
    return {(y, m): n for y, m, n in rows}


def have_month_counts(con, published_base: str,
                      months: list[str] | None = None) -> dict[tuple[int, int], int]:
    """Published item counts per (year, month) from the year-partitioned
    catalog parts under `published_base`: the COLLECTION's base (the
    directory holding year=*/ -- config.public_base for real use, or a
    local staging directory for tests, where nothing is published yet).
    Both collections' layouts are year=YYYY/*.parquet (zone parts for the
    first; items.parquet + live.parquet for C1) and both carry `datetime`
    and `_month`, so nothing here is per collection."""
    glob = f"{published_base.rstrip('/')}/year=*/*.parquet"
    rows = con.execute(f"""
        SELECT year(datetime)::INTEGER AS year, _month::INTEGER AS month,
               count(*) AS have
        FROM read_parquet('{glob}', hive_partitioning=true)
        GROUP BY 1, 2
    """).fetchall()
    have = {(y, m): n for y, m, n in rows}
    if months:
        want = {tuple(int(x) for x in ym.split("-")) for ym in months}
        have = {k: v for k, v in have.items() if k in want}
    return have


def audit(published_base: str, data_paths: list[str], months: list[str] | None,
          tolerance: int = 0, out_csv: str | None = None,
          config: CollectionConfig = DEFAULT_CONFIG) -> tuple[list[tuple], bool]:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL httpfs; LOAD httpfs;")
    expected = expected_month_counts(con, data_paths, months, config)
    have = have_month_counts(con, published_base, months)
    rows = []
    bad = False
    for key in sorted(set(expected) | set(have)):
        y, m = key
        e, h = expected.get(key, 0), have.get(key, 0)
        d = h - e
        rows.append((y, m, e, h, d))
        if abs(d) > tolerance:
            bad = True

    print(f"{'year':>6} {'month':>6} {'expected':>10} {'have':>10} {'delta':>8}")
    for y, m, e, h, d in rows:
        print(f"{y:>6} {m:>6} {e:>10} {h:>10} {d:>8}")
    if out_csv:
        with open(out_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["year", "month", "expected", "have", "delta"])
            w.writerows(rows)
    return rows, bad


def main() -> int:
    ap = argparse.ArgumentParser()
    cols.add_collection_arg(ap)
    ap.add_argument("--published-base",
                    help="the collection's public base URL (default: its "
                         "config.public_base, .../<catalog_dir>), or a local "
                         "staging dir holding year=*/ for testing")
    ap.add_argument("--months", help="comma list YYYY-MM; default = every month")
    ap.add_argument("--inventory-date", help="hive dt=... partition; default newest")
    ap.add_argument("--tolerance", type=int, default=0)
    ap.add_argument("--out", help="write the delta table to this CSV path")
    a = ap.parse_args()
    config = cols.get(a.collection)
    published_base = a.published_base or config.public_base
    months = [m.strip() for m in a.months.split(",")] if a.months else None

    s3 = _s3_client()
    manifest_key = resolve_manifest_key(s3, a.inventory_date, config)
    print(f"inventory manifest: s3://{config.inventory_bucket}/{manifest_key}",
          file=sys.stderr)
    data_urls = manifest_data_urls(s3, manifest_key, config)
    print(f"  {len(data_urls)} data file(s) -- a full scan may read tens of "
          "GB of compressed inventory", file=sys.stderr)

    _, bad = audit(published_base, data_urls, months, a.tolerance, a.out, config)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
