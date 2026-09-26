# Monthly live parts for Collection 1 — what was done

Answers `docs/monthly-live-brief.md`. Branch `s2-monthly-live-work`, seven
commits, one per ruling where that was natural.

Collection 1 only. Nothing under `catalog/sentinel-2-l2a/`, and no code path
of the first collection, changed: `live_part_names(DEFAULT_CONFIG)` is still
`("live",)`, so every list it drives is the list it was.

## The shape, as built

`sentinel-2-c1-l2a/year=YYYY/live-MM.parquet`, `MM` zero-padded, zstd 3,
sorted `(_tile, datetime)` like the archive. A year is still one
`items.parquet`. The single `live.parquet` the collection published before
the move stays in the bucket at zero rows, because this catalog never
deletes, and stays in every list that enumerates a year's parts.

Two functions in `tools/s2_build.py` are the only place those names are
spelled (ruling 1, committed before this report): `live_month_name(m)` and
`live_part_names(config)`. Every workflow, the fold, the metadata generators
and the explorer take their list from there; a test refuses a literal
`live-MM.parquet` on any line of any workflow that runs.

## Ruling 2 — the refresh

`refresh-c1` in `.github/workflows/refresh-daily.yml`.

**Plan.** The step now takes `(year, month)` pairs from the slice
(`SELECT DISTINCT year(datetime), month(datetime)`, session zone UTC, the
build's own zone) instead of years alone, and writes per year:

| file | holds |
|---|---|
| `archive.urls` | the year's published archive parts (`archive_part_names`) |
| `months` | the months to build: the slice's, plus every month the pre-monthly `live.parquet` still holds |
| `migrate` | does `live.parquet` still hold rows? |
| `prev-MM` | one marker per published monthly part |
| `splice.urls` | the year's published monthly parts this run does **not** rebuild |

Every probe is `s2_build.published_part` (retried; anything but 200 or 404
stops the run), and the row count of `live.parquet` is one footer read
(`published_rows`).

**Build.** One `s2_build.py --years Y --months M --name live-MM.parquet
--zstd-level 3` per pair, sources being the slice, the published
`live-MM.parquet` when the plan saw one (its GET must then succeed), and the
pre-monthly `live.parquet` while it still holds rows. `--exclude-ids-from`
the year file, unchanged. The step fails if the build wrote no file for a
month the plan named, because the splice below would then be short a month.

**Splice.** `s2_stats.py --merge-years` replaces a year's whole table, so
the sources must be the year's whole row set: the year file, the monthly
parts this run built, and the year's other published monthly parts where
they are. Without that last list a year whose August part was not touched
today would lose August from `mgrs-monthly.parquet`. The pre-monthly
`live.parquet` is never a source: it holds no rows, or this run has just
moved the rows it held into the monthly parts.

**Upload.** Two passes, the idiom `consolidate-month.yml` already uses:
every part but `live.parquet` through `--only`, one at a time, then one pass
over the rest. See the migration below for why the order matters.

## Ruling 3 — migrating the published `year=2026/live.parquet`

The published file held **51,505 rows on 2026-09-26**, all acquired in
September 2026 (a fold ran recently, so the tail is one month today). The
migration is therefore one month in practice, and written for any number.

**What the run does.** A year whose `live.parquet` answers 200 *and* whose
footer says it holds rows is a year to migrate. The plan reads the months in
it (`SELECT DISTINCT month(datetime)` over HTTP, the `datetime` column only)
and adds them to the year's `months`. The build then has that file in the
source list of every month of the year, so each row lands in the month it
was acquired in, deduped on `id` against the slice and the published monthly
part. After the year's months are written, the step stages a **zero-row**
`live.parquet` over it (`s2_build.write_empty_part`, the schema of the
downloaded file, the same writer the fold and `consolidate-month` use).

**Why it is idempotent.** The trigger is the row count of the published
`live.parquet`, which the migration itself drives to zero:

* Run 1 sees rows, folds them into the months, publishes the empty file.
* Run 2 reads zero rows in the same probe and does nothing: no download, no
  extra month, no rewrite. The file stays in the bucket, emptied, for the
  clients that still read the old name.
* A run that writes the months and then fails before the empty file is
  published leaves the rows in **both** places. Nothing is lost; a glob
  counts those scenes twice until the next run, which the dedupe on `id`
  already removes for any reader that follows the documented recipe. The
  next run sees rows again and folds them again, which is a no-op on the
  monthly parts (same ids, same generation times).
* The merge is by `id` with the highest `s2:generation_time`, so folding the
  same rows twice cannot duplicate a scene inside a part.

**Why the upload order is the safe one.** The monthly parts go up first,
each through `--only`, and the emptied `live.parquet` only in the pass after
them. The dangerous order is the other one: an empty `live.parquet` on the
bucket while the months that hold its rows are not there yet is a hole, and
those scenes are older than the five-day lookback, so nothing would fetch
them again. The order above can only ever produce the harmless state
(double-counted for minutes, or until tomorrow).

**What it is not.** No step deletes, renames or moves a published object,
and no step writes to the bucket outside the two upload passes. A dry run
was made against the real bucket with the upload stubbed (below).

## Ruling 4 — metadata

`make_items.py` (committed earlier on the branch) probes `items.parquet`
plus `live_part_names(config)`, gives every month present its own asset with
its own row count and time range, and sums the year's totals over whatever
exists. `make_collection.py` follows with no new enumeration of its own: it
calls the same `discover()` for a staged year, so `partition:file_count`,
`table:row_count` and the temporal extent count the months that are there,
and its prose (`year_file_text`, `description`) names `live-01.parquet` to
`live-12.parquet`, says a day of refresh rewrites only the months it
fetched, and sends a reprocessed scene to the live part of the month it was
acquired in. The committed `collection.json` still carries the sentence its
last restamp wrote; the daily refresh restamps it from the bucket, as it
does every measured field.

**One bug found and fixed here, and it would have stopped the migration.**
A part with no rows has no `datetime` statistics to read, and `build_item`
wrote `"start_datetime": null, "end_datetime": null` into its asset. Null is
not a string: stac-check and rashid (PTL-STR-001) both reject the item, and
that gate runs in every workflow before it uploads. The run that empties the
pre-monthly `live.parquet` would therefore have failed its own "Validate
before upload" step and uploaded nothing. Measured on the real 2026 item with
an emptied `live` asset added: 2 rashid errors with the nulls, 0 with the
pair left out. `make_items` now leaves it out, and a test in
`tests/test_make_items.py` builds an emptied part and refuses the null. The
same bug was reachable without this work -- any fold or consolidation, or a
day the year file already held every staged row, empties a part the same
way.

## Ruling 5 — the app

`apps/explorer/app.js`:

* `windowMonths(year, d0, d1)` — the months of `year` the search window
  touches. A window inside one month gives one month; a window that starts
  before the year starts at January and one that ends after it ends at
  December; a year outside the window gives none. A December-to-January
  window therefore asks December of the first year and January of the
  second, which a unit test pins in node over nine cases.
* Collection 1's `parts(year, tile, months)` is `["items", "live",
  ...months.map(live-MM)]`. A one-month window probes three names, never
  fourteen.
* `partUrlsFor(year, tile, d0 = TODAY, d1 = TODAY)` is the one place a part
  URL is built, so the search (`partUrls`) and the prefetch
  (`warmWindowParts`) cannot read different files. The callers that only ask
  whether a year is published pass no window and get today's month — the
  part the refresh rewrites every morning, and the only file a year the
  archive has not reached can have.

`apps/explorer/search.js`: a part that answers **404 is now read as empty**.
It was not graceful before: `partMeta` threw `range read of … got HTTP 404`,
`Promise.all` rejected, and one missing file threw out the rows of every
other part in the search. The page HEAD-probes each part before reading it,
so this was unreachable in practice, but the two answers can disagree — a
fold can empty and replace a part between the probe and the read, and a
monthly part a window asks for may never have existed. Now `partMeta`
resolves an absent part with no row group, `searchPart` returns no row and
counts it, the printed plan says `N part(s) answered 404 and were read as
empty`, and `keyedRows` returns nothing for it. 403 counts as absent too,
which is what an object store answers for a key it will not discuss.

## Ruling 6 — the fold

`tools/rails/fold_live.sbatch` takes the candidate names from
`live_part_names()` and, per year, probes every one, downloads each part that
holds rows, folds all of them into `items.parquet` in the single existing
`s2_build` call (it already reads the whole input directory and dedupes by
`id`), writes a zero-row replacement for each part it folded, and uploads the
year file first and those replacements after it — the rule it always had,
now over more files. The `YEARS`-unset enumeration folds a year when its live
parts hold rows **between them**. Its dry run now prints one put for the year
file and thirteen for the live names, which the rails test pins in order.

## Ruling 7 — stats enumeration

`publish-stats.yml` asked one live name. It now resolves both lists in one
`python3 -c` (`archive_part_names` and `live_part_names` with the
collection's config) and probes every live name for the years `LIVE_YEARS`
selects. The first collection is unchanged (one `live.parquet`, current year
only); a Collection 1 rebuild pays thirteen HEADs a year over twelve years,
in a workflow a person dispatches by hand. The refresh's splice is described
under ruling 2.

## Docs

`tools/README.md` (the collection table, the refresh paragraph, the daily
step of the Collection 1 lane, the fold section and "if the fold is
skipped"), `catalog/sentinel-2-c1-l2a/README.md` (the layout block and the
tail paragraph), its `AGENTS.md` (the URL block, the layout paragraph, the
dedupe note and the cadence list), and one row of the root `README.md`
cadence table.

## Verification

| Gate | Result |
|---|---|
| `CI_LIGHT=1 python3 tests/run_all.py` | all gates passed |
| `CI_LIGHT=1 python3 -m pytest tests -q` | 225 passed (222 before this work) |
| `python3 -m pyflakes tools tests` | one pre-existing warning in `tools/rails/experiments/measure_layout.py`, untouched here |
| YAML parse + `bash -n` on every `run:` block | `refresh-daily.yml` 21 blocks, `publish-stats.yml` 9 blocks, all parse |
| `bash -n tools/rails/fold_live.sbatch` | passes (and the rails suite runs it under `DRY_RUN=1`) |
| `node --check apps/explorer/app.js search.js` | passes |

New tests: `--months` and `live_part_names` (ruling 1, earlier commit), the
monthly probing in `make_items` (ruling 4, earlier commit), the app's
window-to-months mapping run in node, the refusal of a hand-typed part name
in any workflow, an emptied part's asset stating no time range, and the
fold's per-part upload order.

**The dry run.** `refresh-c1`'s plan, build, splice and upload steps were
lifted out of the YAML by name and run against a local fake bucket (two
years; a pre-monthly tail spanning July, August and September; a published
`live-05.parquet` the slice never touches; a published `live-09.parquet` it
does), with `s2_stats.py`, `upload_data.py` and the generators stubbed. The
run built `year=2019/live-03.parquet`, `year=2026/live-07/08/09.parquet` and
a zero-row `year=2026/live.parquet`; `live-09` came out at 60 rows, the union
of its published 50 and the slice's 10 with the ids deduped; the splice
named the two year files, the four built parts and the untouched
`live-05.parquet`, and no `live.parquet`; the upload printed the four monthly
parts through `--only` before the pass that carries the emptied file. The
same body run a second time, with those parts copied into the fake bucket,
read zero rows in `live.parquet`, built month 9 alone, and spliced
`live-05/07/08` from the bucket — the idempotence claim above, executed.

The plan step was then run against the **real** bucket with a stand-in slice
(100 rows of the published 2026 tail, 20 rows of published 2019 items, both
read over HTTP). It reported `year=2026/live.parquet: 51,505 row(s)`, month 9
to fold, `migrate: true`, months to build `9`, and for 2019 `migrate: false`
with the months of the stand-in slice. Nothing was written to the bucket.

Two things the laptop could not run: the build step against the real 4.7 GB
`year=2026/items.parquet` (the `--exclude-ids-from` read is minutes and the
fake bucket exercises the same code), and `mapfile`, which bash 3.2 does not
have — the dry run rewrote each `mapfile -t NAME < SRC` as the read loop it
is and ran every other line verbatim. The runners have bash 5, and the
`mapfile` lines the C1 job uses are the ones the first collection's job has
used since the year-loop change.

## What to watch on the first run under this scheme

1. The step log should say `year=2026/live.parquet: 51,505 row(s)` (or
   whatever the tail holds that morning) and `month(s) 9 to fold`.
2. The upload should show `--only sentinel-2-c1-l2a/year=2026/live-09.parquet`
   *before* the pass that uploads `live.parquet`.
3. Afterwards, `year=2026/live.parquet` should be a few KB and
   `year=2026/live-09.parquet` should hold the rows. The year item
   (`year=2026/2026.json`) should carry a `live-09` asset and a `live` asset
   with `table:row_count: 0`.
4. The day after, the log should say `year=2026/live.parquet: 0 row(s)` and
   build month 9 alone.

If run 1 fails between the two upload passes, do nothing: the next run
repeats the fold and the emptying. Do not delete the old file by hand.
