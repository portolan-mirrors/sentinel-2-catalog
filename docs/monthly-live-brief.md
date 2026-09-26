# Monthly live parts for Collection 1

## Why

The daily refresh rewrites and re-uploads `sentinel-2-c1-l2a/year=YYYY/live.parquet`
whole on every run. Measured on 2026-09-25: 36,159 rows, 57 MB, about 1.6 KB a
row, growing about 15,000 rows a day. After a month that is about 700 MB a day
of rewrite and upload; after three months about 2 GB, which is where a GitHub
runner (16 GB RAM, 14 GB disk) starts to struggle. That cost, not the year
file, is what forces a fold on the RAILS cluster every couple of months.

A live part per month caps the daily cost: the refresh rewrites only the
months its lookback touches, which in steady state is one.

The item layout does not change. A year is still one `items.parquet`, which
the user wants to keep as a sensible download unit. Collection 1 only; the
first collection keeps its single `live.parquet`, folded monthly by
`consolidate-month.yml`.

## The shape

`sentinel-2-c1-l2a/year=YYYY/live-MM.parquet`, `MM` zero-padded, zstd 3,
sorted like the archive (`_tile, datetime`).

Rulings, settled — implement these rather than re-deciding:

1. **Builder.** `tools/s2_build.py` gains `--months` beside `--years` (a comma
   list, filtering on `month(datetime)` in the same staging query). The
   refresh calls it once per touched (year, month) with
   `--years Y --months M --name live-MM.parquet`.
2. **Refresh.** `refresh-c1` in `.github/workflows/refresh-daily.yml` derives
   the touched (year, month) pairs from the fetched `created` slice
   (`SELECT DISTINCT year(datetime), month(datetime)`), and for each one
   merges the published `live-MM.parquet` when it exists (the HEAD/GET rules
   already there: 404 means none, any other failure is fatal) and excludes
   ids already in that year's `items.parquet` when it is published. Upload
   only the months it wrote.
3. **Migration.** The published `year=2026/live.parquet` holds rows today. The
   first run under the new scheme must fold its rows into the right
   `live-MM.parquet` files and then publish a zero-row `live.parquet` (the
   catalog never deletes), so nothing is counted twice and no client that
   still reads the old name sees stale rows. Do this inside the workflow, and
   make it a no-op on later runs. Say in the report how you made it idempotent.
4. **Metadata.** `tools/make_items.py`: Collection 1's parts become
   `items.parquet` plus `live-01` … `live-12`, probed the way the first
   collection probes its fourteen candidates. Row counts and extents sum over
   whatever exists. `make_collection.py` follows.
5. **App.** `apps/explorer/app.js` builds a year's part list. For Collection 1
   the current year adds only the `live-MM.parquet` files whose month
   intersects the query window (the search already knows `d0`/`d1`), so a
   search never probes twelve months. A part that answers 404 must be treated
   as empty, not as an error — check how `search.js` handles a missing part
   today and make it graceful if it is not. Keep the prefetch (`warmWindowParts`)
   consistent with whatever the search reads.
6. **Fold.** `tools/rails/fold_live.sbatch` folds every `live-MM.parquet` of a
   year into `items.parquet`, then publishes zero-row replacements for the
   months it folded. Its year enumeration already finds published lives;
   extend it to the monthly names.
7. **Stats.** Wherever a workflow enumerates parts for the stats build
   (`publish-stats.yml`, the refresh's splice), the monthly live names must be
   included. `s2_build.archive_part_names` and friends are the single source
   of those lists — extend them rather than hard-coding names in YAML.

## Verification

- Unit tests for `--months`, for the metadata probing, and for the app's
  window-to-months mapping (a December-to-January window must ask for the
  right months of the right years).
- A dry run of the refresh's C1 job body locally against the real bucket, with
  the upload step stubbed, showing which months it would write.
- `CI_LIGHT=1 python3 tests/run_all.py`, `CI_LIGHT=1 python3 -m pytest tests -q`,
  `python3 -m pyflakes tools tests`, YAML parse and `bash -n` on every run
  block you touch, `node --check` on the app modules.
- Do not run anything on RAILS and do not write to any published bucket path.

## Constraints

Simplified Technical English in lasting comments, docs and commit bodies.
Update `tools/README.md`, `catalog/sentinel-2-c1-l2a/README.md` and its
`AGENTS.md` to describe the monthly live. Commit on a branch named
`s2-monthly-live`, explicit paths, one commit per numbered item where that is
natural. Do not push.
