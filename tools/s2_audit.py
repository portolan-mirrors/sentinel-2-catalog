#!/usr/bin/env python3
"""Inventory-vs-catalog completeness audit: is every scene the S3 Inventory
says exists in sentinel-cogs actually in the published catalog?

The bucket is ground truth here (S3 has no outage windows; the API does --
see s2_repair.py). S3 Inventory (s3://sentinel-cogs-inventory/sentinel-cogs/
sentinel-cogs/, hive-partitioned hive/dt=YYYY-MM-DD-HH-MM/symlink.txt
manifests) lists every object in the source bucket as of that inventory run.

Inventory format (checked live 2026-09-16 against dt=2026-09-15-01-00):
manifest.json says fileFormat "CSV", fileSchema "Bucket, Key, Size,
LastModifiedDate" -- plain gzipped CSV with NO header row, not ORC. DuckDB
reads it directly via read_csv with an explicit column list; no separate
ORC path needed.

Each scene directory holds two JSON files -- "{id}.json" (the item) and
"tileinfo_metadata.json" (excluded here). Both end in ".json", so the item
is identified by the brief's actual rule: the file is SELF-NAMED, i.e. the
filename stem equals the enclosing directory name ({id}/{id}.json). RE2 (
DuckDB's regex engine) has no backreferences, so this is checked by
extracting the directory-name and filename-stem groups separately and
comparing them for equality in SQL, not inside one regex.

The bucket also holds ~64k keys (see s2_repair.py's _is_valid_zone() note)
that end in ".json" and even happen to be self-named, but sit directly
under a bogus "sentinel-s2-l2a-cogs/2019/" root prefix with no {yyyy}/{m}/
segments at all -- these fail the year/month regex entirely and MUST be
filtered out of the WHERE clause with regexp_matches(), not just left for
the CAST to sort out: DuckDB's CAST('' AS INTEGER) raises rather than
returning NULL, so a regexp_extract() that found no match crashes the
whole query instead of producing an excludable row.

Truth: the audit defines "expected" from the bucket. Earth Search keeps
multiple processing sequences (s2:sequence) for some scenes, so a small
positive delta (published fewer than bucket, i.e. delta < 0) or the
reverse can legitimately occur -- use --tolerance to allow it.
"""
from __future__ import annotations

import argparse
import csv
import sys

import duckdb

INV_BUCKET = "sentinel-cogs-inventory"
INV_PREFIX = "sentinel-cogs/sentinel-cogs/hive/"
DEFAULT_PUBLISHED_BASE = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"

# Anchors the tail of an item key: .../{yyyy}/{m}/{dirname}/{filestem}.json.
# m is 1 or 2 digits (the bucket does not zero-pad month path segments).
# Groups 3 (dirname) and 4 (filestem) are compared for equality in SQL to
# enforce the self-named {id}/{id}.json rule -- RE2 has no backreferences.
KEY_RE = r'/(\d{4})/(\d{1,2})/([^/]+)/([^/]+)\.json$'

CSV_COLUMNS = {"bucket": "VARCHAR", "key": "VARCHAR",
              "size": "BIGINT", "last_modified": "VARCHAR"}


def _s3_client():
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config
    return boto3.client(
        "s3", config=Config(signature_version=UNSIGNED), region_name="us-west-2")


def resolve_manifest_key(s3, inventory_date: str | None = None) -> str:
    """The hive/dt=.../symlink.txt manifest key: the one named explicitly,
    or the newest partition found by listing."""
    if inventory_date:
        return f"{INV_PREFIX}dt={inventory_date}/symlink.txt"
    paginator = s3.get_paginator("list_objects_v2")
    dts = []
    for page in paginator.paginate(Bucket=INV_BUCKET, Prefix=INV_PREFIX, Delimiter="/"):
        dts.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    if not dts:
        raise SystemExit(f"no inventory partitions found under {INV_PREFIX}")
    return sorted(dts)[-1] + "symlink.txt"


def manifest_data_urls(s3, manifest_key: str) -> list[str]:
    """The symlink.txt manifest is itself a text file listing s3:// URLs of
    the actual gzipped-CSV data files; turn those into plain HTTPS URLs
    (the bucket allows anonymous GET, verified live) DuckDB's httpfs can
    read with no credentials."""
    body = s3.get_object(Bucket=INV_BUCKET, Key=manifest_key)["Body"].read().decode()
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


def _month_where(months: list[str] | None) -> str:
    # The LIKE is a cheap pre-filter (ends in .json under the tile root);
    # regexp_matches excludes keys with no {yyyy}/{m}/ pair at all (the
    # bogus root-level "2019/" scenes) BEFORE the CAST below ever sees them
    # -- required, not just tidy: CAST('' AS INTEGER) raises in DuckDB, it
    # does not return NULL, so an unmatched regexp_extract() would crash the
    # whole query rather than being excludable. The final clause enforces
    # the self-named {id}/{id}.json rule (drops tileinfo_metadata.json and
    # any other non-self-named json alongside a real item).
    where = (
        "key LIKE 'sentinel-s2-l2a-cogs/%.json' "
        f"AND regexp_matches(key, '{KEY_RE}') "
        f"AND regexp_extract(key, '{KEY_RE}', 3) = regexp_extract(key, '{KEY_RE}', 4)"
    )
    if not months:
        return where
    clauses = []
    for ym in months:
        y, m = ym.split("-")
        clauses.append(f"key LIKE 'sentinel-s2-l2a-cogs/%/{y}/{int(m)}/%.json'")
    return where + " AND (" + " OR ".join(clauses) + ")"


def expected_month_counts(con, data_paths: list[str],
                          months: list[str] | None = None) -> dict[tuple[int, int], int]:
    """Expected item counts per (year, month) from inventory data file(s).
    `data_paths` are local paths or URLs read_csv can open directly -- real
    use passes manifest_data_urls() output, tests pass a local fixture."""
    files = ",".join(f"'{p}'" for p in data_paths)
    cols = ", ".join(f"'{k}': '{v}'" for k, v in CSV_COLUMNS.items())
    rows = con.execute(f"""
        SELECT
          CAST(regexp_extract(key, '{KEY_RE}', 1) AS INTEGER) AS year,
          CAST(regexp_extract(key, '{KEY_RE}', 2) AS INTEGER) AS month,
          count(*) AS expected
        FROM read_csv([{files}], columns={{{cols}}}, header=false)
        WHERE {_month_where(months)}
        GROUP BY 1, 2
    """).fetchall()
    return {(y, m): n for y, m, n in rows}


def have_month_counts(con, published_base: str,
                      months: list[str] | None = None) -> dict[tuple[int, int], int]:
    """Published item counts per (year, month) from the year-partitioned
    catalog parts. `published_base` may be a local staging directory (what
    the tests use -- nothing is published yet) or the public base URL."""
    glob = f"{published_base.rstrip('/')}/sentinel-2-l2a/year=*/*.parquet"
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
          tolerance: int = 0, out_csv: str | None = None) -> tuple[list[tuple], bool]:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'; INSTALL httpfs; LOAD httpfs;")
    expected = expected_month_counts(con, data_paths, months)
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
    ap.add_argument("--published-base", default=DEFAULT_PUBLISHED_BASE,
                    help="public base URL, or a local staging dir for testing")
    ap.add_argument("--months", help="comma list YYYY-MM; default = every month")
    ap.add_argument("--inventory-date", help="hive dt=... partition; default newest")
    ap.add_argument("--tolerance", type=int, default=0)
    ap.add_argument("--out", help="write the delta table to this CSV path")
    a = ap.parse_args()
    months = [m.strip() for m in a.months.split(",")] if a.months else None

    s3 = _s3_client()
    manifest_key = resolve_manifest_key(s3, a.inventory_date)
    print(f"inventory manifest: s3://{INV_BUCKET}/{manifest_key}", file=sys.stderr)
    data_urls = manifest_data_urls(s3, manifest_key)
    print(f"  {len(data_urls)} data file(s) -- a full scan may read tens of "
          "GB of compressed CSV", file=sys.stderr)

    _, bad = audit(a.published_base, data_urls, months, a.tolerance, a.out)
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
