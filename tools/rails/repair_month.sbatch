#!/bin/bash
# Re-fetch one month from the Collection 1 bucket, bypassing the API, and
# fold it into the month's slice with the API rows.
#
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,MONTH=2019-03 tools/rails/repair_month.sbatch'
#
# For a month the API lane lost to 502 windows, or one audit_year.sbatch
# reports short. s2_repair.py lists every scene directory of the month
# under s3://e84-earth-search-sentinel-data/sentinel-2-c1-l2a/ (anonymous)
# and GETs each <id>.json, one day chunk at a time, into
# $SLICES/YYYY-MM/repair/. fold_month.py then rewrites $SLICES/YYYY-MM.parquet
# from api/ and repair/ together; s2_build.py dedupes by id keeping the
# highest s2:generation_time, so a scene both lanes fetched is published
# once. A day chunk that exists is not fetched again, so a re-run only
# sweeps what an earlier run did not finish.
#
# After the repair, rebuild the year: delete
# $PUBLISH/sentinel-2-c1-l2a/year=YYYY/items.parquet and submit
# build_year.sbatch, then upload_year.sbatch with FORCE=1 (the size check
# alone must not decide whether a repaired year replaces the old one).
#
# SMOKE=1 repairs $SMOKE_MONTH under $SLICES/_smoke. DRY_RUN=1 prints the
# commands.
#SBATCH --job-name=s2c1-repair
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=12:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16g
#SBATCH --output=logs/s2c1-repair-%j.out
set -euo pipefail
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
if [ "$SMOKE" = 1 ]; then MONTH="$SMOKE_MONTH"; fi
MONTH="${MONTH:?set MONTH=YYYY-MM via sbatch --export=ALL,MONTH=...}"
WORKERS="${WORKERS:-32}"
DEST="$SLICES/$MONTH.parquet"
run mkdir -p "$SLICES/$MONTH"
echo "$MONTH: repairing from the bucket into $SLICES/$MONTH/repair ($WORKERS workers)"
run python3 "$REPO/tools/s2_repair.py" --collection "$COLLECTION" \
  --month "$MONTH" --out "$SLICES/$MONTH" --workers "$WORKERS"
run python3 "$REPO/tools/rails/fold_month.py" "$SLICES/$MONTH" "$DEST" --subdirs api repair
Y="${MONTH%-*}"
cat <<EOF
$MONTH: slice rewritten. To publish the repair, rebuild and re-upload the year:
  rm -f $PUBLISH/$COLLECTION/year=$Y/items.parquet
  sbatch --export=ALL,YEAR=$Y tools/rails/build_year.sbatch
  sbatch --export=ALL,YEAR=$Y,FORCE=1 tools/rails/upload_year.sbatch
EOF
