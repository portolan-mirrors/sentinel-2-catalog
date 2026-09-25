# Layout experiments

## Done: the Collection 1 item-part layout sweep

Eight partitionings and three row-group sizes of a Collection 1 year, built
here and measured against the shipped explorer client over the live bucket.
Results and recommendation:
[`docs/c1-layout-experiments.md`](../../../docs/c1-layout-experiments.md).

| file | what it does |
|---|---|
| `layout_parts.py` | the variants as part lists, and how a client prunes to a part |
| `build_layout.py` | one variant from one published year: DuckDB split, `gpio sort column`, `gpio check all`, `layout.json` |
| `build_layout.sbatch` | one variant per Slurm job, then upload to `_experiments/layout/` |
| `layout_table.py` | the `layout.json` manifests as the document's layout table |
| `measure_layout.py` | the measurement: a local server plus `chrome-headless-shell`, driving `apps/explorer/search.js` unchanged |
| `harness.html` | the page it serves; imports the shipped module and counts every request |
| `coalesce_probe.py` | what the per-search request count costs, and what coalescing or distinct URLs would save |
| `fold_cost.sh` | one part's write: wall time and peak RSS at a runner-sized memory budget |
| `clean_experiments.py` | delete the `_experiments/` prefix when the numbers are recorded |

Covered by that sweep, from the list below: (1) row-group size on 2018, and
(5) partition tiers. Row-group *mode* (2), `assets` statistics (3) and gpio
against DuckDB `COPY` (4) are still open.

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
