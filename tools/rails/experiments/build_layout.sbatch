#!/bin/bash
# Build one Collection 1 layout variant for one year and upload it under the
# bucket's `_experiments/layout/` prefix, for docs/c1-layout-experiments.md.
#
#     ssh rails 'cd ~/s2-catalog && sbatch \
#         --export=ALL,YEAR=2018,VARIANT=V4,JOBS=8 \
#         tools/rails/experiments/build_layout.sbatch'
#
# Input:  $PUBLISH/sentinel-2-c1-l2a/year=$YEAR/items.parquet (the published
#         part; nothing is refetched).
# Output: $LAYOUT/$YEAR/$VARIANT/*.parquet plus layout.json (the per-part
#         rows/bytes/footer/row-group table and the build wall time), then the
#         parquet files uploaded to
#         <catalog>/_experiments/layout/$YEAR/$VARIANT/.
#
# VARIANT=V0 is the special case: no rebuild at all, just a copy of the
# published part into the experiment prefix, so the footer path can be
# measured against it with no sidecar beside it (the published path keeps its
# sidecar and is the other baseline).
#
# No sidecar is ever built here: the whole question is what the layout costs
# a client that has none.
#
# A partial variant is resumable: a part whose final file exists is rebuilt
# anyway (gpio is the only cost), but the upload skips objects of equal size.
# DRY_RUN=1 prints the commands.
# Resources: a *small* share of a node, unlike build_year.sbatch's 64/300g.
# A variant reads one 1.7-5.4 GB year part and hands each output part to gpio;
# nothing here stages a year in RAM, and the eight variants want to run at the
# same time. At 64 cores / 300 GB apiece the scheduler put the eight of them
# seventeen hours out (2026-09-25, a cluster busy with another project's
# array); even 32 cores / 96 GB queued them hours apart. At 16 cores / 32 GB
# the eight start at once, and nothing here is memory-hungry: DuckDB streams
# the split and spills to $LAYOUT/.duckdb-tmp, and gpio writes one part at a
# time per worker. The wall limit matters as much as the size: Slurm's
# backfill only slots a job into a gap it certainly fits, so an 8-hour
# request on a busy cluster queued these behind everything while a 2-hour
# one starts at once. The widest variant measured here (864 parts of a
# 4.4M-row year) takes under an hour.
#SBATCH --job-name=s2c1-layout
#SBATCH --partition=cpu
#SBATCH --account=bgtj-tgirails
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32g
#SBATCH --output=logs/s2c1-layout-%j.out
set -euo pipefail
REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"
[ -f "$REPO/tools/rails/env.sh" ] || { echo "no checkout at $REPO; set REPO"; exit 1; }
source "$REPO/tools/rails/env.sh"
YEAR="${YEAR:?set YEAR=YYYY}"
VARIANT="${VARIANT:?set VARIANT=V0..V7}"
JOBS="${JOBS:-8}"
MEMORY="${MEMORY:-24GB}"
LAYOUT="${LAYOUT:-/u/cholmes/s2-c1/layout}"
SRC="$PUBLISH/$COLLECTION/year=$YEAR/items.parquet"
DEST="$LAYOUT/$YEAR/$VARIANT"
EXP_PREFIX="_experiments/layout"

[ -s "$SRC" ] || { echo "no source part at $SRC"; exit 1; }
echo "year=$YEAR variant=$VARIANT: src $SRC ($(stat -c %s "$SRC") bytes), jobs $JOBS, out $DEST"

if [ "$VARIANT" = V0 ]; then
  # The published layout, republished without its sidecar.
  run mkdir -p "$DEST"
  run cp -f "$SRC" "$DEST/items.parquet"
  run python3 - "$DEST" <<'EOF'
import json, os, struct, sys
from pathlib import Path
import pyarrow.parquet as pq
d = Path(sys.argv[1]); p = d / "items.parquet"
md = pq.ParquetFile(p).metadata
with p.open("rb") as fh:
    fh.seek(-8, os.SEEK_END); tail = fh.read(8)
footer = struct.unpack("<I", tail[:4])[0] + 8
info = dict(bytes=p.stat().st_size, rows=md.num_rows,
            row_groups=md.num_row_groups, columns=md.num_columns, footer=footer)
(d / "layout.json").write_text(json.dumps(dict(
    variant="V0", note="one file per year, as published today",
    prune="none", row_group=6000, sort_key="_tile,datetime", zstd_level=18,
    files=1, total_bytes=info["bytes"], total_rows=info["rows"],
    total_row_groups=info["row_groups"], build_seconds=0.0,
    parts={"items": info}, failed={}), indent=1))
print(info)
EOF
else
  run python3 "$REPO/tools/rails/experiments/build_layout.py" \
    --src "$SRC" --variant "$VARIANT" --out "$LAYOUT/$YEAR" \
    --jobs "$JOBS" --memory "$MEMORY"
fi

# No `| head` here: under `set -o pipefail` a truncated pipe is SIGPIPE on the
# left-hand side and kills the job between the build and the upload, which is
# how one variant's 96 finished parts sat on /u unpublished.
echo "year=$YEAR variant=$VARIANT: $(find "$DEST" -name '*.parquet' | wc -l) parquet file(s), $(du -sh "$DEST" | cut -f1)"

# Upload every parquet part (never the manifest, never a sidecar).
cd "$LAYOUT"
mapfile -t FILES < <(cd "$LAYOUT" && find "$YEAR/$VARIANT" -name '*.parquet' -printf '%p\n' | sort)
echo "uploading ${#FILES[@]} file(s) to $EXP_PREFIX/$YEAR/$VARIANT/"
# In batches, so one very wide variant (V7's hundreds of parts) does not
# build an unbounded argv.
for ((i = 0; i < ${#FILES[@]}; i += 200)); do
  run env AWS_PROFILE=source-coop python3 "$REPO/tools/rails/upload.py" \
    --data-dir "$LAYOUT" --key-prefix "$EXP_PREFIX" "${FILES[@]:i:200}"
done
echo "year=$YEAR variant=$VARIANT: done"
