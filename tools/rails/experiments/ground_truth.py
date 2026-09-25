#!/usr/bin/env python3
"""The rows a scene search must return, from DuckDB over the published part.

    python3 tools/rails/experiments/ground_truth.py \
        --jsonl /tmp/search-2024.jsonl

An independent answer to the same question `sceneSearch` answers, computed by a
different engine over the same object: tile, UTC day window, cloud ceiling,
coverage floor, `ORDER BY cloud, id LIMIT 30`. Given a measurement JSONL it
checks every distinct (url, tileColumn, tile, window, cc, cov) the sweep asked
for, and reports any arm whose ids differ.

This is what makes the measurement a correctness check as well as a timing one,
and it is the only way to check an arm whose *baseline* cannot complete -- the
committed client fails outright on the sentinel-2-l2a parts, so there is nothing
to diff it against.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def truth(con, url: str, tile_column: str, tile: str, d0: str, d1: str,
          cc: float, cov: float) -> list[str]:
    q = f'''
        SELECT id FROM read_parquet('{url}')
        WHERE "{tile_column}" = ?
          AND datetime >= TIMESTAMP '{d0} 00:00:00'
          AND datetime <= TIMESTAMP '{d1} 23:59:59.999'
          AND "eo:cloud_cover" <= {cc}
          AND ({cov} <= 0 OR 100 - "s2:nodata_pixel_percentage" >= {cov})
        ORDER BY "eo:cloud_cover", id
        LIMIT 30
    '''
    return [r[0] for r in con.execute(q, [tile]).fetchall()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--jsonl", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args()

    import duckdb
    con = duckdb.connect(config={"memory_limit": "8GB"})
    con.execute("INSTALL httpfs; LOAD httpfs;")

    recs = [json.loads(l) for l in a.jsonl.read_text().splitlines() if l.strip()]
    # One truth per distinct query, whichever arms asked it.
    queries: dict[tuple, list[dict]] = {}
    for r in recs:
        g = r["args"]
        key = (g["urls"][0], g["tileColumn"], g["tile"], g["d0"], g["d1"],
               g["cc"], g["cov"])
        queries.setdefault(key, []).append(r)

    bad, checked = [], 0
    report = {}
    for key, rs in sorted(queries.items()):
        want = truth(con, *key)
        arms = {}
        for r in rs:
            got = (r.get("cold") or {}).get("ids")
            if got is None:
                arms.setdefault(r["arm"], set()).add("<no rows: the search failed>")
                continue
            arms.setdefault(r["arm"], set()).add(",".join(got))
        for arm, seen in sorted(arms.items()):
            for s in seen:
                checked += 1
                where = f"{arm} {key[2]} {key[3]}..{key[4]} cc{key[5]:g}"
                if s.startswith("<no rows"):
                    bad.append(f"{where}: {s}")
                    continue
                got = s.split(",") if s else []
                if got != want:
                    bad.append(f"{where}: {len(got)} row(s), DuckDB says "
                               f"{len(want)}; first difference "
                               f"{next((f'{g!r} vs {w!r}' for g, w in zip(got, want) if g != w), 'length only')}")
        report["|".join(str(k) for k in key[1:])] = dict(
            url=key[0], want=want, arms={k: sorted(v) for k, v in arms.items()})
        print(f"{key[2]} {key[3]}..{key[4]} cc{key[5]:g} cov{key[6]:g}: "
              f"DuckDB {len(want)} row(s); arms "
              + ", ".join(
                  f"{k}={'ok' if all((s.split(',') if s else []) == want for s in v) else 'DIFF'}"
                  for k, v in sorted(arms.items())))
    if a.out:
        a.out.write_text(json.dumps(report, indent=1))
    if bad:
        print("\n" + "\n".join(bad))
        sys.exit(1)
    print(f"\n{checked} arm-result(s) match DuckDB over {len(queries)} distinct query(s)")


if __name__ == "__main__":
    main()
