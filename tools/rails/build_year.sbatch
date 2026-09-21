#!/bin/bash
# Build one Collection 1 year file from its fetched month slices.
#
#     ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEAR=2019 tools/rails/build_year.sbatch'
#
# Input: $SLICES/YEAR-MM.parquet for every month of the year (an empty file
# is a month with no items; a missing file means the fetch is not done and
# the job stops). Output: $PUBLISH/sentinel-2-c1-l2a/year=YEAR/items.parquet,
# sorted (_tile, datetime), uniform 6,144-row groups, zstd 18, GeoParquet
# 2.0, checked by `gpio check all` inside s2_build.py. The upload is a
# separate job (upload_year.sbatch), so a build never needs AWS
# credentials. A year whose file exists is not rebuilt: delete the file
# first (after a repair, for example).
#
# A large share of one node: DuckDB stages the year in memory and gpio
# writes with the allocated cores. --memory is DuckDB's limit and gpio's
# write budget; keep it under --mem. --exclusive never schedules while
# the fetch array occupies every node, so the allocation is shared.
#
# SMOKE=1 builds $SMOKE_MONTH's year from the slices under $SLICES/_smoke
# and accepts the short year. DRY_RUN=1 prints the commands.
#SBATCH --job-name=s2c1-build
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=08:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=300g
#SBATCH --output=logs/s2c1-build-%j.out
set -euo pipefail
JOB_NAME=s2c1-build
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
if [ "$SMOKE" = 1 ]; then YEAR="${SMOKE_MONTH%-*}"; fi
YEAR="${YEAR:?set YEAR=YYYY via sbatch --export=ALL,YEAR=...}"
OUT="$PUBLISH/$COLLECTION"
MEMORY="${MEMORY:-250GB}"
DEST="$OUT/year=$YEAR/items.parquet"
if [ -s "$DEST" ]; then echo "year=$YEAR: $DEST exists, nothing to do"; exit 0; fi
# Every month of the year must be folded; the current year stops at this month.
NOW_Y=$(date -u +%Y); NOW_M=$(date -u +%-m)
LAST=12; [ "$YEAR" = "$NOW_Y" ] && LAST=$NOW_M
SRC=(); MISSING=()
for m in $(seq 1 "$LAST"); do
  f="$SLICES/$YEAR-$(printf '%02d' "$m").parquet"
  if [ -e "$f" ]; then
    [ -s "$f" ] && SRC+=("$f")
  else
    MISSING+=("$f")
  fi
done
if [ "${#MISSING[@]}" -gt 0 ]; then
  echo "year=$YEAR: ${#MISSING[@]} month(s) not fetched yet:"; printf '  %s\n' "${MISSING[@]}"
  if [ "$SMOKE" = 1 ]; then
    echo "year=$YEAR: SMOKE=1, building the short year from what is there"
  elif [ "$DRY_RUN" = 1 ]; then
    echo "year=$YEAR: DRY_RUN=1, showing the build anyway"
  else
    echo "year=$YEAR: not building a short year"; exit 1
  fi
fi
if [ "${#SRC[@]}" -eq 0 ] && [ "$DRY_RUN" != 1 ]; then echo "year=$YEAR: no rows in any month"; exit 0; fi
echo "year=$YEAR: ${#SRC[@]} month slice(s), memory $MEMORY, out $OUT"
run python3 "$REPO/tools/s2_build.py" --collection "$COLLECTION" \
  --sources ${SRC[@]+"${SRC[@]}"} --years "$YEAR" --out "$OUT" --memory "$MEMORY"
run ls -la "$OUT/year=$YEAR/"
run python3 - "$DEST" <<'EOF'
import sys, duckdb
p = sys.argv[1]
con = duckdb.connect()
print(con.execute(f"SELECT count(*) AS rows FROM read_parquet('{p}')").fetchone())
print(con.execute(f"SELECT num_row_groups FROM parquet_file_metadata('{p}')").fetchone())
EOF
NEXT="--export=ALL,YEAR=$YEAR"; [ "$SMOKE" = 1 ] && NEXT="--export=ALL,SMOKE=1"
echo "year=$YEAR: built $DEST; next: sbatch $NEXT tools/rails/upload_year.sbatch"
