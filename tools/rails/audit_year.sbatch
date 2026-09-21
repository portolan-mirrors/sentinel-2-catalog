#!/bin/bash
# Audit one Collection 1 year against the source bucket's S3 Inventory.
#
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEAR=2019 tools/rails/audit_year.sbatch'
#
# s2_audit.py counts the scenes the inventory of
# s3://e84-earth-search-sentinel-data lists per month (the bucket is the
# ground truth) and compares them with the year files under
# $PUBLISH/sentinel-2-c1-l2a/ -- the copy on /u, the same bytes
# upload_year.sbatch put in the bucket (DuckDB expands year=*/ on a
# filesystem, not over https). The delta table goes to
# $PUBLISH/audit/YYYY.csv and the job exits 1 when a month is off by more
# than TOLERANCE (default 0). A short month is repair_month.sbatch's job.
#
# Anonymous reads only; no AWS profile. The inventory has ~420 data files
# of ~20 MB, all read once per audit, so one job per year is the unit.
#
# SMOKE=1 audits the smoke year under $PUBLISH/_smoke. DRY_RUN=1 prints
# the command.
#SBATCH --job-name=s2c1-audit
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64g
#SBATCH --output=logs/s2c1-audit-%j.out
set -euo pipefail
JOB_NAME=s2c1-audit
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
if [ "$SMOKE" = 1 ]; then YEAR="${SMOKE_MONTH%-*}"; fi
YEAR="${YEAR:?set YEAR=YYYY via sbatch --export=ALL,YEAR=...}"
PUBLISHED_BASE="${PUBLISHED_BASE:-$PUBLISH/$COLLECTION}"
TOLERANCE="${TOLERANCE:-0}"
MONTHS=$(python3 "$REPO/tools/rails/months.py" "$YEAR-01" "$YEAR-12" | paste -sd, -)
if [ ! -s "$PUBLISHED_BASE/year=$YEAR/items.parquet" ] && [ "$DRY_RUN" != 1 ]; then
  echo "year=$YEAR: $PUBLISHED_BASE/year=$YEAR/items.parquet is not built; nothing to audit"; exit 1
fi
run mkdir -p "$PUBLISH/audit"
echo "year=$YEAR: auditing $PUBLISHED_BASE against the inventory (tolerance $TOLERANCE)"
run python3 "$REPO/tools/s2_audit.py" --collection "$COLLECTION" \
  --published-base "$PUBLISHED_BASE" --months "$MONTHS" \
  --tolerance "$TOLERANCE" --out "$PUBLISH/audit/$YEAR.csv"
echo "year=$YEAR: delta table at $PUBLISH/audit/$YEAR.csv"
