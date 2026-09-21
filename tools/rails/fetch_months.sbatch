#!/bin/bash
# Fetch Collection 1 items from Earth Search, one month per array task.
#
# Run on a compute node through Slurm: the login node reaps long jobs.
#
#     python3 tools/rails/months.py 2015-10 2026-09 > ~/s2-catalog/months.txt
#     ssh rails 'cd ~/s2-catalog && sbatch --array=0-131%8 tools/rails/fetch_months.sbatch'
#     ssh rails 'squeue -u $USER'
#
# Each task runs s2_fetch.py for its month. The day chunks land under
# $SLICES/YYYY-MM/api/, then fold_month.py folds them into one
# $SLICES/YYYY-MM.parquet (zstd 3, sorted by datetime). A task is
# idempotent: s2_fetch.py skips a day chunk that exists, and a month whose
# slice exists exits at once. Re-run the same array to sweep stragglers.
# Set MONTHS to another list file to fetch a subset.
#
# SMOKE=1 fetches $SMOKE_MONTH into $SLICES/_smoke/ and needs no array.
# DRY_RUN=1 prints the commands (set MONTH=YYYY-MM outside an array).
#SBATCH --job-name=s2c1-fetch
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8g
#SBATCH --output=logs/s2c1-fetch-%A_%a.out
set -euo pipefail
JOB_NAME=s2c1-fetch
# Slurm copies the script to a spool directory, so env.sh is found from
# the submit directory, not from $BASH_SOURCE.
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
if [ "$SMOKE" = 1 ]; then
  MONTH="$SMOKE_MONTH"
elif [ -n "${MONTH:-}" ]; then
  : # a month named by hand, for a dry run or one straggler
else
  MONTHS="${MONTHS:-$REPO/months.txt}"
  IDX="${SLURM_ARRAY_TASK_ID:?run through sbatch --array, or set MONTH=YYYY-MM}"
  MONTH="$(sed -n "$((IDX + 1))p" "$MONTHS")"
  [ -n "$MONTH" ] || { echo "no month at line $((IDX + 1)) of $MONTHS"; exit 1; }
fi
Y="${MONTH%-*}"; M="${MONTH#*-}"
LAST_DAY="$(python3 -c "import calendar,sys; print(calendar.monthrange(int(sys.argv[1]), int(sys.argv[2]))[1])" "$Y" "$M")"
START="$MONTH-01"; END="$MONTH-$LAST_DAY"
DEST="$SLICES/$MONTH.parquet"
# A month file that exists, even empty, means the month was folded once.
if [ -e "$DEST" ] && [ ! -e "$DEST.tmp" ]; then echo "$MONTH: $DEST exists, nothing to do"; exit 0; fi
run mkdir -p "$SLICES/$MONTH"
echo "$MONTH: fetching $START..$END into $SLICES/$MONTH"
run python3 "$REPO/tools/s2_fetch.py" --collection "$COLLECTION" \
  --start "$START" --end "$END" --days-per-chunk 1 --out "$SLICES/$MONTH"
# Fold the day chunks into one month file (fold_month.py skips the
# zero-row sentinel days and writes an empty slice for an empty month).
run python3 "$REPO/tools/rails/fold_month.py" "$SLICES/$MONTH" "$DEST"
