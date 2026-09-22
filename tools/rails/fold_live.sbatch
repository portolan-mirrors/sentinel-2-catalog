#!/bin/bash
# Fold each year's live.parquet into its items.parquet and empty live.
#
#     ssh rails 'cd ~/s2-catalog && sbatch tools/rails/fold_live.sbatch'
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEARS=2022,2026 tools/rails/fold_live.sbatch'
#
# The daily refresh (refresh-daily.yml) appends to year=YYYY/live.parquet
# at zstd 3 and never consolidates Collection 1; this job is the
# consolidation, run by hand every month or two and at year end (see
# tools/README.md, "Sync & backfill"). The refresh looks back on
# `created`, and ESA is reprocessing old years, so a live part can sit
# under any year, not only the current one. YEARS unset (the usual run)
# folds every year from 2015 to now whose published live.parquet holds
# rows: one HEAD per year (s2_build.published_part, retried) and a
# footer read of each live found (s2_build.published_rows); a probe that
# cannot answer stops the job before anything is folded. YEARS=Y1,Y2
# names the years by hand. For each year:
#
#   1. HEAD the year's published live.parquet; skip the year when it is
#      not published or holds no rows.
#   2. Download live.parquet and, when published, items.parquet to
#      $PUBLISH/fold/in/year=YYYY/ (on /u; /tmp is too small). A year with
#      no published items.parquet is folded from live alone only when the
#      backfill never fetched it (no $SLICES/YYYY-*.parquet): a year whose
#      backfill is fetched but not uploaded stops the job.
#   3. s2_build.py --collection sentinel-2-c1-l2a over both: dedupe by id
#      keeping the highest s2:generation_time, sort (_tile, datetime),
#      uniform 6,144-row groups, zstd 18, gpio check.
#   4. Write a zero-row live.parquet with the same schema
#      (s2_build.write_empty_part, what consolidate-month.yml publishes).
#   5. Upload items.parquet, then the empty live.parquet (--force: a fold
#      replaces both). The order matters: a reader that sees the new
#      items and the old live sees some scenes twice, which dedupe on id
#      removes; one that saw the empty live before the new items would
#      see a hole.
#   6. Move the new items.parquet to $PUBLISH/sentinel-2-c1-l2a/year=YYYY/
#      so the copy on /u matches the bucket (audit_year.sbatch reads it).
#
# Then regenerate and commit the metadata from a laptop; the job prints
# the commands. They include one search sidecar (items.idx.json) per
# folded year: the fold rewrites items.parquet, which makes the published
# sidecar stale. A stale sidecar only slows the search (the explorer
# detects the size mismatch and falls back to the footer), but regenerate
# it the same day. Needs the source-coop AWS profile (README.md).
#
# Timing: the daily refresh rewrites live.parquet at 03:42 UTC. A fold
# whose window (the download in step 2 to the upload in step 5) crosses
# that moment puts the empty live over a live the refresh had just
# rewritten, so that day's `created` slice drops out of live. The next
# refresh's five-day lookback fetches those items again, so the loss
# heals itself within a day; a fold that starts after 04:00 UTC and ends
# before the next 03:42 never meets it.
#
# SMOKE=1 folds the smoke year under _smoke/. DRY_RUN=1 prints the
# commands, assumes every probe answers 200 and, with YEARS unset, takes
# the current year (no HEAD is made).
#SBATCH --job-name=s2c1-fold
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=300g
#SBATCH --output=logs/s2c1-fold-%j.out
set -euo pipefail
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
export AWS_PROFILE="${AWS_PROFILE:-source-coop}"
if [ "$SMOKE" = 1 ]; then YEARS="${SMOKE_MONTH%-*}"; fi
MEMORY="${MEMORY:-250GB}"
BASE="$PUBLIC_BASE/$COLLECTION"
if [ -z "${YEARS:-}" ]; then
  if [ "$DRY_RUN" = 1 ]; then
    YEARS="$(date -u +%Y)"
    echo "dry-run: YEARS unset; taking the current year ($YEARS) instead of probing the bucket"
  else
    # Every year with a published, non-empty live: the list this run
    # folds. published_part stops on a HEAD that is not 200 or 404, and
    # published_rows on a footer it cannot read, so an unanswered probe
    # stops the job here, before any download.
    YEARS="$(python3 - "$REPO" "$BASE" <<'EOF'
import sys
from datetime import datetime, timezone
import duckdb
repo, base = sys.argv[1], sys.argv[2]
sys.path.insert(0, f"{repo}/tools")
from s2_build import published_part, published_rows
con = duckdb.connect()
years = []
for y in range(2015, datetime.now(timezone.utc).year + 1):
    if not published_part(base, y, "live.parquet"):
        continue
    n = published_rows(con, f"{base}/year={y}/live.parquet")
    print(f"year={y}: live.parquet holds {n:,} row(s)", file=sys.stderr)
    if n:
        years.append(str(y))
print(",".join(years))
EOF
)"
    [ -n "$YEARS" ] || { echo "no year has a published live.parquet with rows; nothing to fold"; exit 0; }
    echo "YEARS unset; folding every year with rows in live: $YEARS"
  fi
fi
FOLD="$PUBLISH/fold"
IN="$FOLD/in"; OUT="$FOLD/out/$COLLECTION"

# published URL: 0 when a HEAD answers 200, 1 on 404; anything else stops
# the job (a guess either way would fold the wrong thing).
published() {
  if [ "$DRY_RUN" = 1 ]; then echo "dry-run: HEAD $1 (assumed 200)"; return 0; fi
  local code
  code=$(curl -sS -o /dev/null -w '%{http_code}' -I --retry 5 --retry-all-errors "$1")
  case "$code" in
    200) return 0 ;;
    404) return 1 ;;
    *) echo "$1: HEAD answered $code; not folding on a guess"; exit 1 ;;
  esac
}
rows_in() {
  python3 -c "import duckdb,sys; print(duckdb.connect().execute('SELECT num_rows FROM parquet_file_metadata(?)', [sys.argv[1]]).fetchone()[0])" "$1"
}

FOLDED=()
for Y in ${YEARS//,/ }; do
  LIVE_URL="$BASE/year=$Y/live.parquet"; ITEMS_URL="$BASE/year=$Y/items.parquet"
  if ! published "$LIVE_URL"; then echo "year=$Y: no live.parquet published; nothing to fold"; continue; fi
  run mkdir -p "$IN/year=$Y" "$OUT"
  run curl -fSL --retry 5 --retry-all-errors -o "$IN/year=$Y/live.parquet" "$LIVE_URL"
  if [ "$DRY_RUN" != 1 ]; then
    N=$(rows_in "$IN/year=$Y/live.parquet")
    if [ "$N" -eq 0 ]; then echo "year=$Y: live.parquet has no rows; nothing to fold"; rm -f "$IN/year=$Y/live.parquet"; continue; fi
    echo "year=$Y: live.parquet holds $N row(s)"
  fi
  if published "$ITEMS_URL"; then
    run curl -fSL --retry 5 --retry-all-errors -o "$IN/year=$Y/items.parquet" "$ITEMS_URL"
  elif compgen -G "$SLICES/$Y-*.parquet" > /dev/null; then
    # The backfill fetched this year but its items.parquet is not in the
    # bucket: a fold now would publish a year file holding the tail alone,
    # and build_year would then refuse to replace it. Upload the year first.
    echo "year=$Y: no items.parquet published, but $SLICES holds this year's slices;"
    echo "year=$Y: build and upload the year (build_year, upload_year) before folding it"
    exit 1
  else
    echo "year=$Y: no items.parquet published and no slices fetched; live is the whole year (first fold of a new year)"
  fi
  run rm -f "$OUT/year=$Y/items.parquet"
  run python3 "$REPO/tools/s2_build.py" --collection "$COLLECTION" \
    --sources "$IN/year=$Y" --years "$Y" --out "$OUT" --memory "$MEMORY"
  # The empty live: the schema of the part just written, zero rows.
  run python3 - "$REPO" "$OUT/year=$Y" "$Y" <<'EOF'
import sys
from pathlib import Path
repo, year_dir, year = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3])
sys.path.insert(0, f"{repo}/tools")
from s2_build import connect, write_empty_part
con = connect("2GB", year_dir)
write_empty_part(con, year_dir / "items.parquet", year_dir / "live.parquet", year)
EOF
  # Parts before live, each verified by size after the put.
  run python3 "$REPO/tools/rails/upload.py" --data-dir "$FOLD/out" --key-prefix "$KEY_PREFIX" \
    --force "$COLLECTION/year=$Y/items.parquet"
  run python3 "$REPO/tools/rails/upload.py" --data-dir "$FOLD/out" --key-prefix "$KEY_PREFIX" \
    --force "$COLLECTION/year=$Y/live.parquet"
  run mkdir -p "$PUBLISH/$COLLECTION/year=$Y"
  run mv -f "$OUT/year=$Y/items.parquet" "$PUBLISH/$COLLECTION/year=$Y/items.parquet"
  run rm -rf "$IN/year=$Y" "$OUT/year=$Y"
  FOLDED+=("$Y")
  echo "year=$Y: folded"
done

[ "${#FOLDED[@]}" -gt 0 ] || { echo "nothing folded"; exit 0; }
cat <<EOF
Folded: ${FOLDED[*]}. Next, on a laptop with the repository checked out:
$(for Y in "${FOLDED[@]}"; do echo "  mkdir -p staging/publish/$COLLECTION/year=$Y"; done)
  python3 tools/make_items.py --collection $COLLECTION --data-dir staging/publish/$COLLECTION --remote-baseline
  python3 tools/make_collection.py --collection $COLLECTION --data-dir staging/publish/$COLLECTION --remote-baseline
$(for Y in "${FOLDED[@]}"; do cat <<SIDECAR
  node tools/make_search_sidecar.mjs $PUBLIC_BASE/$COLLECTION/year=$Y/items.parquet
  mv items.idx.json staging/publish/$COLLECTION/year=$Y/
  python3 tools/upload_data.py --data-dir staging/publish --only $COLLECTION/year=$Y/items.idx.json --confirm
SIDECAR
done)
  CI_LIGHT=1 python3 tests/run_all.py
  git add catalog/$COLLECTION && git commit -m "catalog(c1): fold ${FOLDED[*]}"
then run the publish-catalog workflow. It restamps every collection from
the bucket before it uploads, so the first collection's daily restamp is
kept.
EOF
