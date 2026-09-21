# Layout experiments (placeholder)

Nothing here runs yet. This directory is reserved for the row-group and
partition experiments the
[Collection 1 design](../../../docs/superpowers/specs/2026-09-21-sentinel-2-c1-design.md)
defers until after the backfill. Each experiment is one sbatch script
beside this file, and its result is a section of
[`docs/query-performance.md`](../../../docs/query-performance.md).

## Planned

1. **Row-group size** on one published year (2018, 1.3M rows): the same
   file at 3,000, 6,000, 12,000 and 20,000-row groups. Measure the footer
   size, the bytes and GETs of the explorer's tile-window query, and the
   gpio write time.
2. **Month-aligned groups** against uniform groups on the same year
   (`s2_build.py --row-group-mode month_aligned`): does the last group of
   each month cost more reads than it saves?
3. **Column statistics on `assets`**: the footer with and without them.
   `assets` is the widest column and its min/max strings are long.
4. **gpio against DuckDB `COPY`** for the level-18 write: wall time on a
   full compute node.
5. **Partition tiers** for the largest years (2025: 5.1M rows): one file
   against a split by UTM zone, for the tile-window query and for a
   whole-year scan.

## Method

The method is the one `docs/query-performance.md` records: DuckDB with
`httpfs`, one fresh connection per measurement, `EXPLAIN ANALYZE` for the
wall time, the HTTPFS statistics block for GETs and bytes, three cold runs
per cell and the median reported, the same three tiles and the same
windows for every layout. Every experiment file is published under the
`_experiments/` key prefix of the catalog bucket (upload.py
`--key-prefix _experiments`), never under a collection directory, and is
deleted when its section is written.
