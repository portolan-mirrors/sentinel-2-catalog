# Collection 1 layout experiment — brief

**Goal, in the user's words:** "the top thing I'm actually interested in is
the lowest query times possible"; "the ideal is no sidecars, though the
current optional sidecar works decently". Automation (folding on GitHub
runners instead of the RAILS cluster) is a secondary prize: take it only
where it does not cost read speed.

## The question

The explorer's scene search runs `apps/explorer/search.js` (hyparquet):
metadata once per part per session, then parallel range GETs of the eight
search columns from the row groups whose tile statistics admit the tile.
Today one Collection 1 year is a single file of about 700 row groups and
57 columns, so its footer is about 7.5 MB. `tools/make_search_sidecar.mjs`
exists to avoid that footer (about 100 KB on the wire instead).

A smaller file has a smaller footer. Partitioning may therefore make the
plain footer path as fast as today's sidecar path, or faster, and delete a
whole moving part.

**Find the partition organization and row-group size that minimise wall
time for the app's real search, with no sidecar. Beat the current
sidecar-assisted single-file year.**

## Layout variants (build for one year, then confirm the best two)

Sweep on **2018** (1,329,973 rows, 1.68 GB — about 50 minutes per variant
at the measured 2.3 ms/row); confirm the top two on **2024** (4,369,942
rows). Every variant keeps the published schema, zstd 18, GeoParquet 2.0
and the sort `(_tile, datetime)` unless the variant says otherwise.

| id | partitioning | row groups |
|----|--------------|-----------|
| V0 | one file per year (published today) | 6,144 |
| V1 | one file per year | 20,000 |
| V2 | one file per year | 100,000 |
| V3 | 12 files, one per month | 6,144 |
| V4 | 8 files, UTM-zone octants (s2_build ZONE_PARTS_8) | 6,144 |
| V5 | 8 octants x 12 months (96 files) | 6,144 |
| V6 | 60 files, one per UTM zone | 6,144 |
| V7 | your choice: one more organization you expect to win | your choice |

V7 is deliberate room to think. Candidates worth considering: MGRS
latitude-band buckets, a Hilbert-bucketed set of equal-size files, tile
prefix buckets sized so each file holds roughly one row group per tile, or
a two-level scheme. Say why you picked it.

Record for every variant: file count, total bytes, footer bytes per file,
row groups per file, and the build wall time (the fold cost).

## Measurement

Measure the **real client over the real network**, not a local server:
upload every variant to
`s3://us-west-2.opendata.source.coop/portolan-mirrors/sentinel-2-catalog/_experiments/layout/<variant>/`
(public read; `AWS_PROFILE=source-coop` on RAILS) and drive
`apps/explorer/search.js` unchanged from a headless Chrome page that
imports it as a module. Report per query: wall time, request count, bytes.
Use `PerformanceObserver`/`performance.getEntriesByType("resource")` for
the tally.

Query shapes, each on three tiles in different UTM zones (pick tiles with
a realistic scene count, e.g. 31UFU, 33UUP, 23KKQ), cold (fresh page) and
warm (second identical search in the same page):

1. tile + one month
2. tile + three months (the FTW planting or harvest window)
3. tile + the whole year
4. tile + three months with a cloud filter of 20 percent

Five repetitions, report the median and the spread. Note the laptop's
round-trip time to the bucket at the start and end of the run; the earlier
work found request depth dominates, so a changed RTT invalidates a
comparison.

Baselines to include in the same table: V0 **with** its published sidecar,
and V0 without it (the footer path) — the numbers the new layouts must
beat.

## Deliverable

`docs/c1-layout-experiments.md`: method, the table of layouts, the timing
tables, a plot-free but explicit statement of which layout wins each query
shape, and a recommendation that answers three questions.

1. Which layout gives the lowest search time with no sidecar?
2. Does it also remove the RAILS dependency (is a fold of one unit inside a
   GitHub runner's 6 hours, 14 GB disk, 16 GB RAM)? State the per-fold
   cost.
3. What does it cost: total bytes, file count, and any query shape that
   gets worse.

Commit the document and any experiment scripts under
`tools/rails/experiments/`. Do not change the production builder, the app,
or any published path. Delete the `_experiments/` prefix when the numbers
are recorded.
