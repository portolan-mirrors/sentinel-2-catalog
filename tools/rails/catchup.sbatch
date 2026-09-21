#!/bin/bash
# Catch up on items Earth Search created after the array fetch started.
#
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,START=2026-09-19 tools/rails/catchup.sbatch'
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,START=2026-09-19,END=2026-10-02 tools/rails/catchup.sbatch'
#
# The array fetch (fetch_months.sbatch) windows on `datetime`: each month
# holds what the API had for that month on the day the task ran. The
# daily refresh (refresh-daily.yml, refresh-c1) windows on `created` and
# looks back five days from the day the repository variable is set. An
# item created between those two moments -- a new acquisition of the
# current month, or an old scene ESA reprocessed -- is in neither. This
# job closes the gap: one s2_fetch on `created` from START (the day the
# array fetch started) to END (default: today, UTC), day chunks under
# $SLICES/created-START_END/api/, folded by fold_month.py into one
# $SLICES/created-START_END.parquet. build_year.sbatch reads every
# $SLICES/created-*.parquet next to the year's month slices; s2_build
# keeps the rows of the year it builds and dedupes by id, keeping the
# highest s2:generation_time, so a scene both fetches saw is published
# once. A year built before the catch-up must be rebuilt to include it
# (rm its items.parquet, re-run build_year). Set the variable the day the
# catch-up ends, so the two `created` windows meet.
#
# Idempotent: s2_fetch skips a day chunk that exists, and a slice that
# exists exits at once. A second catch-up with a later END is a new slice
# (a new START_END name); the earlier one stays and the dedupe handles
# the overlap. Runs the API at the same rate as one array task. No AWS.
#
# DRY_RUN=1 prints the commands. SMOKE=1 keeps the slice under
# $SLICES/_smoke.
#SBATCH --job-name=s2c1-catchup
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=48g
#SBATCH --output=logs/s2c1-catchup-%j.out
set -euo pipefail
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
START="${START:?set START=YYYY-MM-DD (the day the array fetch started) via sbatch --export=ALL,START=...}"
END="${END:-$(date -u +%F)}"
for d in "$START" "$END"; do
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || { echo "not a YYYY-MM-DD date: $d"; exit 1; }
done
[[ "$START" < "$END" || "$START" = "$END" ]] || { echo "START $START is after END $END"; exit 1; }
NAME="created-${START}_${END}"
DEST="$SLICES/$NAME.parquet"
# A slice that exists, even empty, means this window was fetched and folded.
if [ -e "$DEST" ] && [ ! -e "$DEST.tmp" ]; then echo "$NAME: $DEST exists, nothing to do"; exit 0; fi
run mkdir -p "$SLICES/$NAME"
echo "$NAME: fetching items created $START..$END into $SLICES/$NAME"
run python3 "$REPO/tools/s2_fetch.py" --collection "$COLLECTION" --field created \
  --start "$START" --end "$END" --days-per-chunk 1 --out "$SLICES/$NAME"
run python3 "$REPO/tools/rails/fold_month.py" "$SLICES/$NAME" "$DEST"
cat <<EOF
$NAME: folded into $DEST.
Next: build_year.sbatch reads it with the month slices. A year already
built under $PUBLISH/$COLLECTION does not have these rows: delete its
items.parquet and re-run build_year (then upload_year with FORCE=1 if it
was uploaded). Set the repository variable C1_LIVE_ENABLED the day this
window ends ($END), so the daily refresh's five-day lookback meets it.
EOF
