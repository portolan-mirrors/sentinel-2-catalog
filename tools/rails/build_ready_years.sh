#!/bin/bash
# Submit build_year.sbatch for every year that is ready and not yet built.
#
#     ssh rails 'cd ~/s2-catalog && bash tools/rails/build_ready_years.sh'
#     DRY_RUN=1 bash tools/rails/build_ready_years.sh      # list, submit nothing
#
# A year is ready when every month slice of it exists under $SLICES (the
# current year: up to this month). A year is done when
# $PUBLISH/sentinel-2-c1-l2a/year=YYYY/items.parquet exists, and it is
# in flight when a job named s2c1-build-YYYY is queued or running. Ready
# years are submitted smallest first, by the size of their slices, so
# the big years go last and a problem shows on a cheap year. Run it
# again as the fetch array folds more months: it submits only what is
# new. Runs on the login node in seconds; the builds are the jobs.
set -euo pipefail
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
FIRST_YEAR="${FIRST_YEAR:-2015}"
NOW_Y=$(date -u +%Y); NOW_M=$(date -u +%-m)
QUEUED=""
if command -v squeue >/dev/null 2>&1; then
  QUEUED=$(squeue -u "$USER" -h -o '%j' 2>/dev/null || true)
fi
# A build submitted by hand keeps the script's own name, s2c1-build, and
# says nothing about its year; this script cannot skip that year.
if printf '%s\n' "$QUEUED" | grep -qx "s2c1-build"; then
  echo "warning: a job named s2c1-build (submitted by hand, year unknown) is queued or running; its year may be submitted twice"
fi
READY=()
for Y in $(seq "$FIRST_YEAR" "$NOW_Y"); do
  if [ -s "$PUBLISH/$COLLECTION/year=$Y/items.parquet" ]; then echo "year=$Y: built"; continue; fi
  if printf '%s\n' "$QUEUED" | grep -qx "s2c1-build-$Y"; then echo "year=$Y: job queued or running"; continue; fi
  LAST=12; [ "$Y" = "$NOW_Y" ] && LAST=$NOW_M
  MISSING=0; BYTES=0
  for m in $(seq 1 "$LAST"); do
    f="$SLICES/$Y-$(printf '%02d' "$m").parquet"
    if [ -e "$f" ]; then
      BYTES=$((BYTES + $(stat -c %s "$f" 2>/dev/null || stat -f %z "$f")))
    else
      MISSING=$((MISSING + 1))
    fi
  done
  if [ "$MISSING" -gt 0 ]; then echo "year=$Y: $MISSING month(s) not folded yet"; continue; fi
  echo "year=$Y: ready ($((BYTES / 1048576)) MB of slices)"
  READY+=("$BYTES $Y")
done
[ "${#READY[@]}" -gt 0 ] || { echo "nothing to submit"; exit 0; }
for Y in $(printf '%s\n' "${READY[@]}" | sort -n | awk '{print $2}'); do
  run sbatch --job-name="s2c1-build-$Y" --export=ALL,YEAR="$Y" "$REPO/tools/rails/build_year.sbatch"
done
