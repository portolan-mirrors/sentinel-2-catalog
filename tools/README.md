# tools/

The scripts that fetch, build, describe and publish the catalog. None of
them is published; `catalog/` is. The root [README](../README.md) lists
what each tool does and the workflow that runs it; this file covers what
the root README does not: how the two collections are kept in sync with
Earth Search, and how each was backfilled.

The fetch, repair, audit, build, stats and metadata tools take
`--collection` (`sentinel-2-l2a`, the default, or `sentinel-2-c1-l2a`)
and read everything collection-specific from
[`s2_collections.py`](s2_collections.py): schema, tile column, bucket,
sort key, row-group size, live compression, the lookback field.

## Sync & backfill

Two techniques, one per collection. The first collection runs on GitHub
Actions end to end. Collection 1 is backfilled and folded on the TGI RAILS
cluster, and GitHub only appends its daily tail.

| | `sentinel-2-l2a` | `sentinel-2-c1-l2a` |
|---|---|---|
| Backfill | `backfill` then `publish-backfill` (GitHub) | `tools/rails/` on RAILS |
| Daily tail | `refresh-daily`: `live.parquet` per year, zstd 18 | `refresh-daily`'s `refresh-c1` job, on when the repository variable `C1_LIVE_ENABLED` is `true`: `live.parquet` per year, zstd 3, lookback on `created` |
| Consolidation | `consolidate-month`, the 3rd of each month | `fold_live.sbatch` on RAILS, by hand, every 1 to 2 months |
| Repair | `repair-slices` (bucket crawl) | `repair_month.sbatch` |
| Audit | `s2_audit.py` by hand | `audit_year.sbatch` |
| Metadata | regenerated and published by each workflow | regenerated on a laptop, committed, `publish-catalog` |

### GitHub technique (first collection; not used for Collection 1)

Four workflows, in the order they ran once and the order two of them keep
running.

**`backfill.yml`** (manual, `start`/`end` months). A matrix with one job
per month, `max-parallel: 2` to stay polite to Earth Search. Each job runs
`s2_fetch.py --start --end` for its month into `staging/chunks` and
uploads the day chunks as a workflow artifact `slice-YYYY-MM` with 30-day
retention. A month whose artifact exists is skipped, so a re-dispatch
sweeps only the months that failed. No credentials: artifacts are the
hand-off.

**`publish-backfill.yml`** (manual, `years`, `run_id`). Three stages.
`plan` lists every non-expired `slice-*` artifact and groups the ids by
year. `build-year` is one job per year, `max-parallel: 1`: it downloads
that year's slice artifacts only (a runner has 14 GB of disk), assumes the
Source Cooperative role, and runs `s2_build.py` with `--split zones` from
2019 (four parts for 2019-2020, eight from 2021), `--skip-existing-url` so
a part the bucket already holds is not rebuilt, and
`--on-part-done "python3 tools/upload_part.py"` so each part is uploaded
on a fresh one-hour session the moment it passes `gpio check`. A job that
hits the ceiling has published every part it finished, and the next
dispatch builds the rest. `finalize` regenerates every year item and the
collection from the published parts (`--remote-baseline`), runs the
gates, and publishes the metadata.

**`refresh-daily.yml`** (03:42 UTC). Fetches the last five days by
`datetime`, finds the years in the slice, and rebuilds one `live.parquet`
per year from the previous live plus the slice, minus every id the year's
archive parts hold (`--exclude-ids-from`), so live and archive never
overlap. It splices those years into the stats table, restamps the two
collections, validates, uploads and publishes. Nothing is committed.

**`consolidate-month.yml`** (the 3rd, 05:17 UTC). Folds each year's live
into its archive parts, one job per (year, part) because eight octants at
zstd 18 in one job is 8 to 12 hours. `finalize` checks every planned part
arrived, writes a zero-row live per folded year, restamps, uploads the
parts and then the lives (a reader must never see the empty live before
the new parts), and publishes.

**Its limits**, and why Collection 1 does not use it:

- 6 hours per job. One first-collection octant of ~1 million rows is 50
  to 95 minutes at zstd 18; a Collection 1 year is up to 5 million rows in
  one file.
- Runner disk. `publish-backfill` downloads one year's slices at a time
  because 135 slices do not fit the 14 GB it has for them, and a whole
  7-million-row year spilled past the runner's disk in gpio's sort; that
  is what the zone split exists for.
- Artifacts expire after 30 days, and a matrix of 136 months at two in
  parallel takes days.
- Runner evictions during long bucket crawls (11 of 11 `repair-slices`
  attempts on one month), which is why the repair writes one chunk per day.

**To run Collection 1 on it instead** (if RAILS goes away), the edits are:

1. `tools/s2_collections.py`: set `zone_split=True` in `_C1`. `s2_build`
   then writes years from 2019 as zone parts (`zone_parts_for`), the tile
   column already drives `zone_sql`, and `make_items.parts_for` discovers
   whichever candidates a year has, so the single-file years already
   published stay valid.
2. `backfill.yml`: add a `collection` input, pass it as
   `s2_fetch.py --collection`, and name the artifacts
   `slice-c1-YYYY-MM` so the two collections' slices never collide.
3. `publish-backfill.yml`: filter the artifacts by that prefix; pass
   `--collection sentinel-2-c1-l2a` to `s2_build.py`, `make_items.py` and
   `make_collection.py`; stage under `staging/publish/sentinel-2-c1-l2a`
   and probe `--skip-existing-url "$PUBLIC_BASE/sentinel-2-c1-l2a"`.
   `upload_part.py` needs no change.
4. `consolidate-month.yml`: run its plan for both collections
   (`consolidation_plans(..., config)` takes the config; the `BASE` env
   and the staging path become per collection), so the monthly fold
   replaces the RAILS fold.
5. `refresh-daily.yml`: drop the fold duty from the `refresh-c1` job's
   header; nothing else changes, its live build already excludes the
   archive's ids.

### RAILS technique (Collection 1)

The full instructions, the credentials setup and every script are in
[`tools/rails/README.md`](rails/README.md). The shape:

1. **Login**: `ssh rails` (ssh ControlMaster with Kerberos and Duo; one
   login per session, by a person).
2. **Deploy**: `rsync -av --delete tools/ rails:s2-catalog/tools/` and
   `rsync -av catalog.publish.yaml rails:s2-catalog/`; `mkdir -p ~/s2-catalog/logs`
   once.
3. **Environment**: `micromamba create -f tools/rails/environment.yml -p /u/cholmes/micromamba/envs/s2`
   once; `env.sh` puts it on the PATH.
4. **Credentials**: an IAM user `rails-sentinel-2-catalog` in the bucket's
   account, either with `tools/rails/iam-policy.json` attached or allowed
   to assume the Source Cooperative role (`role-trust-statement.json`);
   either way a `source-coop` profile in `~/.aws` on RAILS. Only the two
   upload jobs use it.
5. **Run order**: smoke (`SMOKE=1`, one month under `_smoke/`) → 2017 end
   to end → 2018 with the layout experiment → the array fetch of every
   month → year builds smallest first (`build_ready_years.sh`) and
   uploads → audit per year, repair and rebuild a short month → the
   metadata commit from a laptop (`make_items.py` and `make_collection.py`
   with `--collection sentinel-2-c1-l2a --remote-baseline`, the gates,
   commit, `publish-catalog`) → set the repository variable
   `C1_LIVE_ENABLED` to `true` → dispatch `publish-stats` (its Collection
   1 entry runs only with the variable set; it seeds `stats-c1`) →
   flip the explorer's default.
6. **Daily**: `refresh-daily`'s `refresh-c1` job, on while
   `C1_LIVE_ENABLED` is `true`, fetches the lookback by
   `created` (so a scene ESA reprocessed last week, whatever its
   acquisition date, is caught), appends to `year=YYYY/live.parquet` at
   zstd 3 with the year file's ids excluded, splices the touched years
   into `stats-c1`, and restamps both collections. It never
   consolidates. Set the variable only once every year it appends to is
   uploaded and `publish-stats` has seeded `stats-c1` (its splice reads
   the published table); the fold below has the same condition.

**The periodic duty.** Every one to two months, and at the end of each
year, a person runs the fold:

```bash
ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEARS=2026 tools/rails/fold_live.sbatch'
# January: YEARS=2025,2026
```

It downloads each year's `items.parquet` and `live.parquet`, rebuilds the
year (`s2_build --collection sentinel-2-c1-l2a`, dedupe by id keeping the
highest `s2:generation_time`, sort `(_tile, datetime)`, zstd 18), uploads
the new year file and then a zero-row live, and prints the laptop
commands: `make_items.py` and `make_collection.py` with
`--collection sentinel-2-c1-l2a --remote-baseline`, the gates, a commit,
and the `publish-catalog` workflow.

**If the fold is skipped**: live keeps growing (a whole year in live is
about 5 million rows at zstd 3, a larger and slower file than the year
file would be); every query stays correct, because the year item lists
both files and the collection glob reads both; and the year file lags the
truth by however long the fold is late, with reprocessed scenes present
twice across the two files (the newer `s2:generation_time` in live) until
the fold dedupes them. Nothing is lost, and nothing else needs to change
when the fold finally runs.
