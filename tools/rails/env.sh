# Sourced by every script in this directory. Sets the toolchain, the AWS
# defaults and the shared paths on /u.
#
# Every sbatch finds this file through $REPO, never through $BASH_SOURCE:
# Slurm copies a submitted script to a spool directory, so the script's
# own path says nothing about where the checkout is. $REPO is the submit
# directory under Slurm and ~/s2-catalog otherwise; a laptop dry run sets
# REPO to its checkout.
#
# The toolchain is a micromamba env on shared /u, reachable from compute
# nodes without `module load`. See README.md for how to create it.
export PATH="${S2_ENV:-/u/cholmes/micromamba/envs/s2}/bin:$PATH"
export AWS_DEFAULT_REGION=us-west-2
# Jobs that upload set AWS_PROFILE=source-coop themselves (upload_year,
# fold_live); the fetch, the build and the audit need no AWS identity.
# DuckDB and Python both honour TZ; every tool buckets by UTC month.
export TZ=UTC

# DRY_RUN=1 prints each command instead of running it and creates nothing.
# SMOKE=1 keeps every path under a _smoke/ directory or key prefix and
# fetches one small month (2017-07; the whole of 2017 is 24,664 items).
export DRY_RUN="${DRY_RUN:-0}"
export SMOKE="${SMOKE:-0}"
export SMOKE_MONTH=2017-07

# /tmp on a RAILS node is a 64 GB tmpfs: too small for a year file or a
# year build's DuckDB spill, which stay on /u. The month fold's spill
# (fold_month.py, FOLD_TMP) is the one thing that goes there: a month
# fits, and a spill on the network filesystem failed. Sourcing this file
# creates nothing.

# Where fetched month slices live between the fetch and the build: the
# shared project space, so a build job on another node can read them.
export SLICES="${SLICES:-/u/cholmes/s2-c1/slices}"
# Where built year files wait for their upload, and where s2_build.py
# spills (its temp dir is $PUBLISH/.duckdb-tmp, on /u, not in /tmp).
export PUBLISH="${PUBLISH:-/u/cholmes/s2-c1/publish}"
export REPO="${REPO:-$HOME/s2-catalog}"

# The published catalog, and the collection this lane builds.
export COLLECTION=sentinel-2-c1-l2a
export PUBLIC_BASE="${PUBLIC_BASE:-https://data.source.coop/portolan-mirrors/sentinel-2-catalog}"
# upload.py --key-prefix: a directory inserted between the catalog prefix
# and the file's path. Empty for the real catalog.
export KEY_PREFIX="${KEY_PREFIX:-}"

if [ "$SMOKE" = 1 ]; then
  SLICES="$SLICES/_smoke"
  PUBLISH="$PUBLISH/_smoke"
  PUBLIC_BASE="$PUBLIC_BASE/_smoke"
  KEY_PREFIX="_smoke"
fi

# run CMD...: run it, or under DRY_RUN=1 print it, shell-quoted, and do
# nothing. A heredoc on stdin is consumed either way.
run() {
  if [ "$DRY_RUN" = 1 ]; then
    printf 'dry-run:'; printf ' %q' "$@"; printf '\n'
    return 0
  fi
  "$@"
}
