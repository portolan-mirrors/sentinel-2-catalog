#!/usr/bin/env python3
"""Build one Collection 1 layout variant from a published year part.

    python3 tools/rails/experiments/build_layout.py \
        --src /u/cholmes/s2-c1/publish/sentinel-2-c1-l2a/year=2018/items.parquet \
        --variant V4 --out /u/cholmes/s2-c1/layout/2018 --jobs 8 --memory 240GB

Experiment-only. `tools/s2_build.py` is the production builder and is not
touched: it writes one `items.parquet` per Collection 1 year and knows nothing
about these partitionings. This script exists because the variants the layout
experiment needs (monthly, per-zone, octant x month, grid-zone prefix) are
partitionings the builder deliberately does not support.

What it does, per part, is exactly what `s2_build._sort_and_check` does, so
every variant is a real GeoParquet 2.0 file the explorer's client can read:

  1. DuckDB stages the rows -- ONE scan of the source for the whole variant,
     via `COPY ... PARTITION_BY`, at zstd 3 (the staging files are deleted).
     V1/V2 skip staging: they repartition nothing, so gpio sorts the
     published file directly.
  2. `gpio sort column <staged> <.tmp> _tile,datetime --geoparquet-version 2.0
     --compression zstd --compression-level 18 --row-group-size N`
  3. `gpio check all <.tmp>`  -- the same gate, at the same severity.
  4. os.replace onto the final name, so an unchecked part never sits at one.

Then it writes `<out>/<variant>/layout.json`: per part the rows, bytes,
footer bytes and row-group count, plus the build wall time. That manifest is
the layout table of docs/c1-layout-experiments.md and the input the
measurement harness reads.

`--plan` prints the part list and its row counts and builds nothing.
`--only PART[,PART]` rebuilds named parts (a retry after a failure).
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from layout_parts import ALL, DISCOVERED, fixed_variants  # noqa: E402

SORT_KEY = "_tile,datetime"
ZSTD_LEVEL = 18          # what the published parts are written with
STAGE_LEVEL = 3          # staging files are deleted; spend nothing on them
STAGE_ROW_GROUP = 122_880


def say(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def footer_len(path: Path) -> int:
    """The parquet footer length, from the 8-byte tail (thrift + the 4-byte
    length + PAR1). This is what a client without a sidecar must download
    before it can read a single column chunk."""
    with path.open("rb") as fh:
        fh.seek(-8, os.SEEK_END)
        tail = fh.read(8)
    return struct.unpack("<I", tail[:4])[0] + 8


def connect(memory: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={"memory_limit": memory,
                                 "temp_directory": str(tmp)})
    con.execute("INSTALL spatial; LOAD spatial;")
    # The partitioned write must stream, not buffer the year. gpio re-sorts
    # every part afterwards, so insertion order is worth nothing here, and
    # keeping it is what made DuckDB spill -- and a spill on /u is exactly the
    # failure tools/rails/env.sh warns about ("a spill on the network
    # filesystem failed"): this script saw both "Stale file handle" and "Could
    # not read enough bytes" out of it, plus segfaults.
    con.execute("SET preserve_insertion_order = false;")
    # The partitioned write keeps per-thread buffers for every open partition,
    # so the widest variant (1,011 grid-zone prefixes of the 4.4M-row 2024
    # year) ran out of memory at DuckDB's default thread count. The split is
    # I/O bound anyway -- it took 25-90 s for 2018 -- so fewer threads costs
    # nothing and bounds the buffers.
    con.execute("SET threads = 8;")
    return con


def variant_spec(con, src: Path, variant: str) -> dict:
    """The variant's row-group size, pruning class and part list. V6/V7
    enumerate the keys present in this year, so an unpopulated zone or band
    costs no object."""
    fixed = fixed_variants()
    if variant in fixed:
        return dict(fixed[variant], name=variant)
    if variant not in DISCOVERED:
        sys.exit(f"unknown variant {variant!r}; known: {', '.join(ALL)}")
    d = DISCOVERED[variant]
    rows = con.execute(
        f"SELECT {d['key_sql']} AS k, count(*) AS n "
        f"FROM read_parquet('{src}') GROUP BY 1 ORDER BY 1"
    ).fetchall()
    parts = [(d["stem"](k), d["pred"](k)) for k, n in rows if k is not None and n]
    return dict(d, name=variant, parts=parts,
                counts={d["stem"](k): n for k, n in rows if k is not None and n})


def stage(con, src: Path, spec: dict, stage_dir: Path) -> dict[str, Path]:
    """One DuckDB scan of the source for the whole variant.

    `COPY ... PARTITION_BY` writes every part in that one pass, which is the
    difference between one scan and sixty (V6) or seven hundred (V7). The
    partition key is computed in SQL from `_tile` / `datetime` exactly as the
    part predicates spell it, and DuckDB drops the key column from the files
    it writes, so a staged part has the published schema.

    V1/V2 need no split at all -- they return the source itself, and gpio
    sorts the published file directly.
    """
    parts = spec["parts"]
    if len(parts) == 1 and parts[0][1] == "TRUE":
        return {parts[0][0]: src}
    shutil.rmtree(stage_dir, ignore_errors=True)
    # `_lp` is the partition key: a CASE over the part predicates, which keeps
    # the split and the per-part SQL literally the same expression. Its value
    # is the part's index, not its stem, because DuckDB percent-escapes a
    # partition value ('t=31U' becomes the directory '_lp=t%3D31U') and a slug
    # that needs no unescaping is one less thing to get wrong.
    slug = {f"p{i:04d}": stem for i, (stem, _) in enumerate(parts)}
    cases = "\n".join(f"WHEN {pred} THEN 'p{i:04d}'"
                      for i, (_, pred) in enumerate(parts))
    t0 = time.monotonic()
    con.execute(f"""
        COPY (SELECT *, CASE {cases} END AS _lp FROM read_parquet('{src}')
              WHERE CASE {cases} END IS NOT NULL)
        TO '{stage_dir}' (FORMAT PARQUET, PARTITION_BY (_lp),
             COMPRESSION zstd, COMPRESSION_LEVEL {STAGE_LEVEL},
             ROW_GROUP_SIZE {STAGE_ROW_GROUP}, OVERWRITE_OR_IGNORE true);
    """)
    say(f"staged {len(parts)} part(s) in one scan, "
        f"{time.monotonic() - t0:,.1f}s")
    staged = {}
    empty = 0
    for key, stem in slug.items():
        # DuckDB writes <dir>/_lp=<key>/data_0.parquet, one file per partition.
        cand = sorted((stage_dir / f"_lp={key}").glob("*.parquet"))
        if not cand:
            empty += 1
            continue
        if len(cand) > 1:
            # With insertion order off, DuckDB may flush one partition from
            # several threads and leave data_0/data_1/… behind. gpio sorts one
            # input file, so glue them first -- cheap, and it happened for a
            # handful of the 864 grid-zone prefixes only.
            one = cand[0].with_name("merged.parquet")
            lst = ", ".join(f"'{c}'" for c in cand)
            con.execute(f"""COPY (SELECT * FROM read_parquet([{lst}]))
                            TO '{one}' (FORMAT PARQUET, COMPRESSION zstd,
                            COMPRESSION_LEVEL {STAGE_LEVEL},
                            ROW_GROUP_SIZE {STAGE_ROW_GROUP});""")
            say(f"  {stem}: merged {len(cand)} staged files")
            cand = [one]
        staged[stem] = cand[0]
    if empty:
        say(f"  {empty} part(s) held no rows and were skipped")
    return staged


def sort_and_check(staged: Path, final: Path, row_group: int,
                   memory: str, scratch: Path) -> None:
    """s2_build._sort_and_check, minus the collection plumbing: gpio writes
    a dotfile, `gpio check all` gates it there, and only a part that passed
    is renamed onto its final name.

    `scratch` is a working directory of this part's own. It matters: gpio's own
    DuckDB spills to the RELATIVE path `./.tmp/duckdb_temp_storage_DEFAULT-0.tmp`,
    so N gpio processes sharing a working directory overwrite each other's spill
    file and fail with "Could not read enough bytes from .tmp/…". That is what
    killed the octant variant twice before this was isolated.
    """
    final.parent.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(f".{final.stem}.tmp.parquet")
    tmp.unlink(missing_ok=True)
    try:
        r = subprocess.run(
            ["gpio", "sort", "column", str(staged), str(tmp), SORT_KEY,
             "--geoparquet-version", "2.0", "--compression", "zstd",
             "--compression-level", str(ZSTD_LEVEL),
             "--row-group-size", str(row_group),
             "--write-memory", memory],
            capture_output=True, text=True, cwd=scratch)
        if r.returncode != 0:
            # A negative code is a signal: -9 is the cgroup OOM killer, which
            # is how a whole-year single-part sort fails when --mem is small.
            raise RuntimeError(f"gpio sort failed for {final.name} "
                               f"(exit {r.returncode}):\n"
                               f"{r.stdout[-2000:]}\n{r.stderr[-2000:]}")
        chk = subprocess.run(["gpio", "check", "all", str(tmp)],
                             capture_output=True, text=True, cwd=scratch)
        if chk.returncode != 0:
            raise RuntimeError(f"gpio check failed for {final.name}:\n"
                               f"{chk.stdout[-1500:]}\n{chk.stderr[-1500:]}")
        os.replace(tmp, final)
    finally:
        tmp.unlink(missing_ok=True)
        shutil.rmtree(scratch, ignore_errors=True)


def describe(path: Path) -> dict:
    import pyarrow.parquet as pq
    md = pq.ParquetFile(path).metadata
    return dict(bytes=path.stat().st_size, rows=md.num_rows,
                row_groups=md.num_row_groups, columns=md.num_columns,
                footer=footer_len(path))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, type=Path)
    ap.add_argument("--variant", required=True)
    ap.add_argument("--out", required=True, type=Path,
                    help="directory; the variant gets a subdirectory of it")
    ap.add_argument("--jobs", type=int, default=8)
    ap.add_argument("--memory", default="200GB")
    ap.add_argument("--gpio-memory", default=None,
                    help="--write-memory per gpio worker (default: memory/jobs)")
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--only", default="")
    args = ap.parse_args()

    out = args.out / args.variant
    # Per variant, never shared: the eight variants of a year run as eight
    # concurrent jobs against the same --out, and a shared spill directory got
    # one job's DuckDB reading a file another job had just deleted ("Stale file
    # handle", and segfaults).
    tmp = args.out / f".duckdb-tmp-{args.variant}"
    con = connect(args.memory, tmp)
    spec = variant_spec(con, args.src, args.variant)
    say(f"{args.variant}: {spec['note']}; {len(spec['parts'])} part(s), "
        f"row groups {spec['row_group']:,}, pruning '{spec['prune']}'")
    if args.plan:
        counts = spec.get("counts")
        for stem, pred in spec["parts"]:
            n = f"{counts[stem]:,}" if counts else "?"
            say(f"  {stem:<24} {n:>12} rows   {pred[:70]}")
        return

    only = {s for s in args.only.split(",") if s}
    gpio_mem = args.gpio_memory or f"{max(4, int(args.memory.rstrip('GB')) // max(1, args.jobs))}GB"
    t_all = time.monotonic()
    staged = stage(con, args.src, spec, args.out / f".stage-{args.variant}")
    con.execute("SET memory_limit='2GB';")   # the budget goes to gpio now

    todo = [(stem, path) for stem, path in staged.items()
            if not only or stem in only]
    done, failed = {}, {}
    t_sort = time.monotonic()

    scratch_root = args.out / f".gpio-{args.variant}"

    def one(item):
        stem, src = item
        final = out / f"{stem}.parquet"
        t0 = time.monotonic()
        try:
            sort_and_check(src, final, spec["row_group"], gpio_mem,
                           scratch_root / stem.replace("/", "_"))
        except Exception as e:  # noqa: BLE001
            return stem, None, str(e)
        return stem, dict(describe(final), seconds=round(time.monotonic() - t0, 1)), None

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for i, (stem, info, err) in enumerate(pool.map(one, todo), 1):
            if err:
                failed[stem] = err
                say(f"  FAILED {stem}:\n{err}")
            else:
                done[stem] = info
                if i % 25 == 0 or len(todo) <= 25:
                    say(f"  [{i}/{len(todo)}] {stem}: {info['rows']:,} rows, "
                        f"{info['bytes'] / 1e6:,.1f} MB, {info['row_groups']} groups, "
                        f"footer {info['footer'] / 1024:,.0f} KB, {info['seconds']}s")

    sort_secs = time.monotonic() - t_sort
    shutil.rmtree(args.out / f".stage-{args.variant}", ignore_errors=True)
    shutil.rmtree(scratch_root, ignore_errors=True)
    shutil.rmtree(tmp, ignore_errors=True)

    manifest = dict(
        variant=args.variant, note=spec["note"], prune=spec["prune"],
        row_group=spec["row_group"], source=str(args.src),
        sort_key=SORT_KEY, zstd_level=ZSTD_LEVEL,
        files=len(done), total_bytes=sum(f["bytes"] for f in done.values()),
        total_rows=sum(f["rows"] for f in done.values()),
        total_row_groups=sum(f["row_groups"] for f in done.values()),
        build_seconds=round(time.monotonic() - t_all, 1),
        sort_seconds=round(sort_secs, 1),
        jobs=args.jobs, parts=done, failed=failed)
    out.mkdir(parents=True, exist_ok=True)
    (out / "layout.json").write_text(json.dumps(manifest, indent=1))
    say(f"{args.variant}: {manifest['files']} file(s), "
        f"{manifest['total_bytes'] / 1e9:,.2f} GB, "
        f"{manifest['total_rows']:,} rows, "
        f"{manifest['total_row_groups']:,} row groups, "
        f"{manifest['build_seconds'] / 60:,.1f} min wall")
    if failed:
        sys.exit(f"{len(failed)} part(s) failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
