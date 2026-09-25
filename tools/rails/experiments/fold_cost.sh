#!/bin/bash
# What one part of a partitioned Collection 1 year costs to write: wall time and
# peak RSS of the `gpio sort column` that writes it, at a runner-sized memory
# budget. This is the number behind the "does a fold fit in a GitHub runner
# (6 h, 14 GB disk, 16 GB RAM)" question of docs/c1-layout-experiments.md.
#
#     ssh rails 'bash ~/s2-catalog/tools/rails/experiments/fold_cost.sh \
#         /u/cholmes/s2-c1/layout/2018/V6/z=31.parquet 2GB 6000'
#
# The input is an already-built part, so the measurement is the write alone --
# the same operation a fold performs on a part that gained a month of rows.
# Peak RSS comes from /usr/bin/time -v, which reports the maximum resident set
# of the gpio process tree.
set -euo pipefail
SRC="${1:?a parquet part}"
BUDGET="${2:-2GB}"
# Not `GROUPS`: that is a bash built-in array (the caller's group ids) and an
# assignment to it is silently ignored -- it read back as the numeric gid.
ROWGROUP="${3:-6000}"
export PATH="${S2_ENV:-/u/cholmes/micromamba/envs/s2}/bin:$PATH"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"     # gpio's own DuckDB spills to ./.tmp; give it a private one
echo "part: $SRC ($(stat -c %s "$SRC") bytes), --write-memory $BUDGET, row groups $ROWGROUP"
/usr/bin/time -v gpio sort column "$SRC" "$WORK/out.parquet" _tile,datetime \
  --geoparquet-version 2.0 --compression zstd --compression-level 18 \
  --row-group-size "$ROWGROUP" --write-memory "$BUDGET" 2>&1 \
  | grep -E "Elapsed \(wall|Maximum resident|User time|System time|Sorted|Error|error"
echo "out: $(stat -c %s "$WORK/out.parquet") bytes"
/usr/bin/time -v gpio check all "$WORK/out.parquet" 2>&1 \
  | grep -E "Elapsed \(wall|Maximum resident" || true
