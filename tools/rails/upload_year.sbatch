#!/bin/bash
# Upload one built Collection 1 year file to the catalog bucket.
#
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEAR=2019 tools/rails/upload_year.sbatch'
#
# Input: $PUBLISH/sentinel-2-c1-l2a/year=YEAR/items.parquet, written by
# build_year.sbatch. The upload runs as the `source-coop` AWS profile
# (README.md, "Credentials"); nothing else in this lane needs one.
# upload.py HEADs the key first and skips an object of the same size, so
# a re-run of a finished year does one HEAD; FORCE=1 uploads regardless
# (a rebuilt year after a repair). The metadata (year item and
# collection.json) is regenerated and committed from a laptop afterwards;
# the job prints the commands.
#
# SMOKE=1 uploads the smoke year under the _smoke/ key prefix. DRY_RUN=1
# prints the command.
#SBATCH --job-name=s2c1-upload
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=04:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8g
#SBATCH --output=logs/s2c1-upload-%j.out
set -euo pipefail
JOB_NAME=s2c1-upload
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
export AWS_PROFILE="${AWS_PROFILE:-source-coop}"
if [ "$SMOKE" = 1 ]; then YEAR="${SMOKE_MONTH%-*}"; fi
YEAR="${YEAR:?set YEAR=YYYY via sbatch --export=ALL,YEAR=...}"
REL="$COLLECTION/year=$YEAR/items.parquet"
if [ ! -s "$PUBLISH/$REL" ] && [ "$DRY_RUN" != 1 ]; then
  echo "year=$YEAR: $PUBLISH/$REL is not built; run build_year.sbatch first"; exit 1
fi
FORCE_FLAG=(); [ "${FORCE:-0}" = 1 ] && FORCE_FLAG=(--force)
echo "year=$YEAR: uploading $PUBLISH/$REL as $AWS_PROFILE"
run python3 "$REPO/tools/rails/upload.py" --data-dir "$PUBLISH" \
  --key-prefix "$KEY_PREFIX" ${FORCE_FLAG[@]+"${FORCE_FLAG[@]}"} "$REL"
run curl -fsI "$PUBLIC_BASE/$REL"
cat <<EOF
year=$YEAR: published at $PUBLIC_BASE/$REL
Next, on a laptop with the repository checked out (the item and the
collection are regenerated from the published parts and committed):
  mkdir -p staging/publish/$COLLECTION/year=$YEAR
  python3 tools/make_items.py --collection $COLLECTION --data-dir staging/publish/$COLLECTION --remote-baseline
  python3 tools/make_collection.py --collection $COLLECTION --data-dir staging/publish/$COLLECTION --remote-baseline
  CI_LIGHT=1 python3 tests/run_all.py
  git add catalog/$COLLECTION && git commit -m "catalog(c1): year=$YEAR published"
then run the publish-catalog workflow.
EOF
