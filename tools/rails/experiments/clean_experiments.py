#!/usr/bin/env python3
"""Delete the catalog bucket's `_experiments/` objects once their numbers are
recorded. Nothing else, ever.

    AWS_PROFILE=source-coop python3 tools/rails/experiments/clean_experiments.py \
        --prefix _experiments/layout --dry-run
    AWS_PROFILE=source-coop python3 tools/rails/experiments/clean_experiments.py \
        --prefix _experiments/layout --yes

`tools/rails/upload.py` never deletes, by design, which leaves the experiment
prefix to be cleaned by hand. This does it with three guards: the key prefix
comes from `catalog.publish.yaml`'s `write_prefix` (never typed), the
`--prefix` must start with `_experiments`, and every key is re-checked against
the full `<write_prefix>/_experiments/` prefix before the delete call. A
collection directory cannot be reached from here.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
from publish import load_config, split_s3_uri  # noqa: E402

GUARD = "_experiments"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--prefix", default=GUARD,
                    help=f"key prefix below the catalog prefix; must start with {GUARD}")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE", "source-coop"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--yes", action="store_true", help="actually delete")
    a = ap.parse_args()
    rel = a.prefix.strip("/")
    if not (rel == GUARD or rel.startswith(GUARD + "/")):
        sys.exit(f"--prefix must start with {GUARD}/ — refusing {a.prefix!r}")

    import boto3
    config = load_config()
    bucket, base = split_s3_uri(config["write_prefix"])
    root = f"{base.rstrip('/')}/{GUARD}/"
    target = f"{base.rstrip('/')}/{rel}/"
    client = boto3.Session(profile_name=a.profile,
                           region_name=config.get("region")).client("s3")
    print(f"listing s3://{bucket}/{target}")
    keys, total = [], 0
    for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=target):
        for obj in page.get("Contents", []):
            if not obj["Key"].startswith(root):
                sys.exit(f"refusing: {obj['Key']} is not under {root}")
            keys.append(obj["Key"])
            total += obj["Size"]
    print(f"{len(keys):,} object(s), {total / 1e9:,.2f} GB")
    if not keys:
        return 0
    if not a.yes or a.dry_run:
        for k in keys[:5]:
            print(f"  would delete {k}")
        print(f"  … pass --yes to delete all {len(keys):,}")
        return 0
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        res = client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
        errs = res.get("Errors") or []
        print(f"  deleted {len(res.get('Deleted') or [])}, {len(errs)} error(s)")
        for e in errs[:5]:
            print(f"    {e}")
    left = client.list_objects_v2(Bucket=bucket, Prefix=target).get("KeyCount", 0)
    print(f"remaining under {target}: {left}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
