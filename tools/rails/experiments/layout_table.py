#!/usr/bin/env python3
"""Reduce the per-variant `layout.json` manifests to the layout table of
docs/c1-layout-experiments.md.

    python3 tools/rails/experiments/layout_table.py /tmp/manifests/2018/*/layout.json

Per variant: file count, total bytes, total row groups, the footer and
row-group distribution across files, and the build wall time (the fold cost).
"""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path


def q(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    if not xs:
        return 0
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def main(paths: list[str]) -> None:
    rows = []
    for p in paths:
        m = json.loads(Path(p).read_text())
        parts = m["parts"]
        if not parts:
            continue
        footers = [f["footer"] for f in parts.values()]
        groups = [f["row_groups"] for f in parts.values()]
        sizes = [f["bytes"] for f in parts.values()]
        rows.append(dict(
            variant=m["variant"], note=m["note"], prune=m["prune"],
            row_group=m["row_group"], files=len(parts),
            total_gb=m["total_bytes"] / 1e9, rows=m["total_rows"],
            row_groups=sum(groups),
            footer_med=statistics.median(footers), footer_max=max(footers),
            footer_total=sum(footers),
            groups_med=statistics.median(groups), groups_max=max(groups),
            size_med=statistics.median(sizes), size_max=max(sizes),
            rows_med=statistics.median([f["rows"] for f in parts.values()]),
            rows_max=max(f["rows"] for f in parts.values()),
            part_secs_max=max(f.get("seconds") or 0 for f in parts.values()),
            part_secs_total=sum(f.get("seconds") or 0 for f in parts.values()),
            build_min=m["build_seconds"] / 60,
            failed=len(m.get("failed") or {})))
    rows.sort(key=lambda r: r["variant"])
    base = next((r["total_gb"] for r in rows if r["variant"] == "V0"), None)
    print("| id | partitioning | rows/group | prune by | files | total GB |"
          " vs V0 | row groups | footer per file, median (max) |"
          " rows per file, median (max) | largest part | fold wall |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        vs = f"{(r['total_gb'] / base - 1) * 100:+.1f} %" if base else "—"
        print(f"| {r['variant']} | {r['note']} | {r['row_group']:,} | "
              f"{r['prune']} | {r['files']:,} | {r['total_gb']:.2f} | {vs} | "
              f"{r['row_groups']:,} | "
              f"{r['footer_med'] / 1024:,.0f} KB ({r['footer_max'] / 1024:,.0f} KB) | "
              f"{r['rows_med']:,.0f} ({r['rows_max']:,}) | "
              f"{r['size_max'] / 1e6:,.0f} MB / {r['part_secs_max']:,.0f} s | "
              f"{r['build_min']:,.1f} min |"
              + ("  **FAILED PARTS**" if r["failed"] else ""))
    print()
    for r in rows:
        print(f"{r['variant']}: {r['files']:,} files, {r['total_gb']:.3f} GB, "
              f"{r['rows']:,} rows, {r['row_groups']:,} groups, "
              f"footer total {r['footer_total'] / 1e6:.2f} MB, "
              f"fold {r['build_min']:.1f} min wall, "
              f"{r['part_secs_total'] / 60:.0f} core-min of gpio, "
              f"slowest part {r['part_secs_max']:.0f} s")


if __name__ == "__main__":
    main(sys.argv[1:])
