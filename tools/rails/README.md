# RAILS lane: Collection 1 on the TGI Slurm cluster

The `sentinel-2-c1-l2a` collection (Earth Search's Sentinel-2 Collection 1,
30.4 million items) is fetched, built and uploaded from the TGI RAILS
cluster, not from GitHub runners. A year of Collection 1 is one file of up
to 5 million rows; a GitHub job has 6 hours and 14 GB of disk, a RAILS
node has 192 CPUs, 512 GB of memory and shared storage. This directory is
the whole lane: one Slurm script per step, one `env.sh` they all source,
and this README. The tools they run are the ones in `tools/`, with
`--collection sentinel-2-c1-l2a`. The wider picture, and the GitHub
technique this replaces for Collection 1, is in
[`tools/README.md`](../README.md), "Sync & backfill".

## The cluster

| Fact | Consequence |
|---|---|
| Slurm; account `bgtj-tgirails`, partition `cpu` | Every script carries both `#SBATCH` lines. |
| Nodes: 192 CPUs, 512 GB | A year build asks for a shared slice (`--cpus-per-task=64 --mem=300g`). `--exclusive` never schedules while the fetch array occupies every node. |
| `/tmp` is a 64 GB tmpfs | Too small for a year file or a DuckDB spill. `$WORK` is for logs and small files; slices, builds and spills live on `/u`. |
| `/u` is shared project space (925 TB) | `$SLICES=/u/cholmes/s2-c1/slices`, `$PUBLISH=/u/cholmes/s2-c1/publish`; a job on any node reads what another wrote. |
| The login node reaps long processes | Everything runs through `sbatch`. `build_ready_years.sh` is the one script that runs on the login node, for seconds. |
| Slurm copies a submitted script to a spool directory | A script finds `env.sh` through `$REPO` (`$SLURM_SUBMIT_DIR`, or `~/s2-catalog`), never through `$BASH_SOURCE`. Submit from the checkout. |
| Outbound HTTPS and S3 | The fetch, the repair and the audit need no credentials. Uploads need the `source-coop` profile (below). |
| Login: ssh ControlMaster, Kerberos, Duo | `ssh rails` once per session; the controller cannot log in for you. |

## Setup

**Deploy the code.** The scripts import `tools/*.py` and `upload.py` reads
`catalog.publish.yaml`, so both go over. `logs/` must exist before the
first `sbatch`: Slurm does not create the directory of `--output`.

```bash
ssh rails 'mkdir -p ~/s2-catalog/logs'
rsync -av --delete tools/ rails:s2-catalog/tools/
rsync -av catalog.publish.yaml rails:s2-catalog/
```

**Create the environment** once. It lives on `/u`, so compute nodes reach
it without `module load`. pip prints a note about botocore's urllib3 pin
while it resolves; the environment works.

```bash
ssh rails
micromamba create -y -f ~/s2-catalog/tools/rails/environment.yml -p /u/cholmes/micromamba/envs/s2
/u/cholmes/micromamba/envs/s2/bin/gpio --version   # 1.5.0
```

`env.sh` puts that `bin/` first on `PATH` (`S2_ENV` overrides the
location), sets `TZ=UTC` and `AWS_DEFAULT_REGION=us-west-2`, and defines
`$WORK`, `$SLICES`, `$PUBLISH`, `$REPO`, `$COLLECTION`, `$PUBLIC_BASE` and
the `run` helper.

## Credentials

Only `upload_year.sbatch` and `fold_live.sbatch` write to the bucket. They
run as the AWS profile `source-coop`. The `[default]` profile in
`~/.aws/credentials` on RAILS is another account (417712557820) and gets
`AccessDenied` on the catalog prefix; leave it alone, the scripts name
their profile. Two ways to create `source-coop`; (b) is preferred because
the role is the identity every GitHub workflow already writes with, and
the user's own keys then hold no S3 permission at all.

**Both ways start with an IAM user** in the bucket's account
(939788573396): create the user `rails-sentinel-2-catalog` with no
console access, create one access key pair for it, and write the keys to
`/u/cholmes/.aws/credentials` (`~/.aws` on RAILS) with `chmod 600`.

**(a) The user writes directly.** Attach
[`iam-policy.json`](iam-policy.json) to the user: `s3:ListBucket` on the
bucket with an `s3:prefix` condition of `portolan-mirrors/sentinel-2-catalog/*`,
and `s3:GetObject`, `s3:PutObject`, `s3:AbortMultipartUpload`,
`s3:ListMultipartUploadParts` on the objects under that prefix. Nothing
else, and no delete: publishing never deletes.

```ini
# ~/.aws/credentials
[source-coop]
aws_access_key_id = AKIA...
aws_secret_access_key = ...

# ~/.aws/config
[profile source-coop]
region = us-west-2
```

**(b) The user assumes the Source Cooperative role.** Add the statement in
[`role-trust-statement.json`](role-trust-statement.json) to the trust
policy of `arn:aws:iam::939788573396:role/source-coop-portolan-mirrors`
(IAM console, the role, "Trust relationships", "Edit trust policy"; paste
it as one more element of `Statement`). It names the user's ARN
explicitly, so the user needs no policy of its own: a same-account
principal named in a role's trust policy can assume it without an
identity-based `sts:AssumeRole` allow. (If STS still answers
`AccessDenied`, attach this inline policy to the user:
`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::939788573396:role/source-coop-portolan-mirrors"}]}`.)
Then the profile chain:

```ini
# ~/.aws/credentials
[rails-user]
aws_access_key_id = AKIA...
aws_secret_access_key = ...

# ~/.aws/config
[profile rails-user]
region = us-west-2

[profile source-coop]
role_arn = arn:aws:iam::939788573396:role/source-coop-portolan-mirrors
source_profile = rails-user
region = us-west-2
```

boto3 assumes the role for one hour and refreshes the session by itself
from `source_profile`, so an upload that runs longer than an hour does
not fail on an expired session.

**Test the profile** before the first upload (boto3 is in the
environment; the aws CLI is not). Under (a) the last line answers
`AccessDenied`, which is correct: the policy has no delete, and the one
marker object is overwritten by the next test rather than accumulating.

```bash
source ~/s2-catalog/tools/rails/env.sh
python3 - <<'EOF'
import boto3, datetime
s = boto3.Session(profile_name="source-coop"); print(s.client("sts").get_caller_identity()["Arn"])
s3, b, k = s.client("s3"), "us-west-2.opendata.source.coop", "portolan-mirrors/sentinel-2-catalog/_work/write-test.txt"
s3.put_object(Bucket=b, Key=k, Body=datetime.datetime.now(datetime.timezone.utc).isoformat().encode()); print("put ok:", s3.head_object(Bucket=b, Key=k)["ContentLength"], "bytes")
s3.delete_object(Bucket=b, Key=k); print("delete ok")
EOF
```

`_work/` is scratch under the catalog prefix that no catalog document
references; the marker is the only thing this lane ever puts there.

## The scripts

Every sbatch: `set -euo pipefail`, `source "$REPO/tools/rails/env.sh"`,
`--account=bgtj-tgirails --partition=cpu`, output in
`logs/<name>-<jobid>.out`, idempotent (a finished step is skipped on the
next run). Two switches, both environment variables passed with
`--export=ALL,...` or set in the shell for a laptop run:

- `DRY_RUN=1` prints each command, shell-quoted, and creates nothing. A
  laptop needs `REPO=$PWD`: `REPO=$PWD DRY_RUN=1 YEAR=2017 bash tools/rails/build_year.sbatch`.
- `SMOKE=1` keeps every path under `_smoke/` (`$SLICES/_smoke`,
  `$PUBLISH/_smoke`, the `_smoke/` key prefix and public URL) and
  processes one month, 2017-07, so the whole chain runs end to end in
  minutes without touching the catalog.

| Script | Submit | Does |
|---|---|---|
| `fetch_months.sbatch` | `sbatch --array=0-131%8 tools/rails/fetch_months.sbatch` | One month per array task from `months.txt` (`months.py`): `s2_fetch` day chunks into `$SLICES/YYYY-MM/api/`, then `fold_month.py` writes `$SLICES/YYYY-MM.parquet` (zstd 3). An empty month writes an empty sentinel file. 2 CPUs, 8 GB, 12 h. |
| `repair_month.sbatch` | `sbatch --export=ALL,MONTH=2019-03 tools/rails/repair_month.sbatch` | `s2_repair` from the bucket into `$SLICES/YYYY-MM/repair/`, then re-folds the month from `api/` and `repair/` together. The year must be rebuilt afterwards (the job prints how). |
| `build_year.sbatch` | `sbatch --export=ALL,YEAR=2019 tools/rails/build_year.sbatch` | `s2_build --collection sentinel-2-c1-l2a` over the year's twelve slices into `$PUBLISH/sentinel-2-c1-l2a/year=YYYY/items.parquet`: sorted `(_tile, datetime)`, uniform 6,144-row groups, zstd 18, `gpio check all`. Refuses a year with a month not yet folded. 64 CPUs, 300 GB (`--memory 250GB`), 8 h. No AWS. |
| `build_ready_years.sh` | `bash tools/rails/build_ready_years.sh` | Login node. Submits `build_year.sbatch` for every year whose months are all folded, that is not built and has no `s2c1-build-YYYY` job queued; smallest first. Run it again as the fetch progresses. |
| `upload_year.sbatch` | `sbatch --export=ALL,YEAR=2019 tools/rails/upload_year.sbatch` | `upload.py` puts the year file under the catalog prefix as `source-coop`; a HEAD first skips an object of the same size (`FORCE=1` to replace). Prints the laptop commands for the metadata. |
| `audit_year.sbatch` | `sbatch --export=ALL,YEAR=2019 tools/rails/audit_year.sbatch` | `s2_audit` of the built year files on `/u` against the source bucket's S3 Inventory; delta table in `$PUBLISH/audit/YYYY.csv`; exit 1 when a month is off by more than `TOLERANCE`. |
| `fold_live.sbatch` | `sbatch --export=ALL,YEARS=2026 tools/rails/fold_live.sbatch` | The periodic duty: merges each year's published `live.parquet` into its `items.parquet`, uploads the year then an empty live, and prints the laptop commands. |
| `upload.py` | called by the two upload jobs | Profile-based upload with HEAD skip-existing; see its docstring for why `upload_data.py` cannot be used here. |
| `fold_month.py`, `months.py` | called by the scripts | The month fold and the month list. |
| `experiments/` | later | Layout experiments after the backfill; see its README. |

Watch jobs with `squeue -u $USER`, read a log with `tail -f logs/s2c1-build-<jobid>.out`,
cancel with `scancel <jobid>`.

## Run order

1. **Smoke.** `SMOKE=1` through fetch, build, upload and audit; then
   check `https://data.source.coop/portolan-mirrors/sentinel-2-catalog/_smoke/sentinel-2-c1-l2a/year=2017/items.parquet`
   answers a HEAD. The smoke objects are not catalog data; delete them
   from a laptop with the `portolan-mirrors` profile when done.
2. **2017** (24,664 items), the first real year: `fetch_months` for its
   twelve months, `build_year`, `upload_year`, then the metadata commit
   (step 7) and a look at the year in the explorer.
3. **2018** (1.3 million items): the same, plus the layout experiment the
   design asks for (footer size with and without `assets` statistics; gpio
   against DuckDB `COPY`), recorded in `docs/query-performance.md`.
4. **The array fetch** for every month: `months.py 2015-10 $(date +%Y-%m) > months.txt`,
   `sbatch --array=0-N%8` with N one less than the line count. Re-submit
   the same array to sweep stragglers; a folded month exits at once.
5. **Builds, largest last.** `build_ready_years.sh` whenever more months
   have folded; it orders the ready years by slice size. Then
   `upload_year` per built year.
6. **Audit** each uploaded year; `repair_month` any short month, rebuild
   and re-upload that year with `FORCE=1`.
7. **Metadata, from a laptop** (the repository is not on RAILS):
   `make_items.py --collection sentinel-2-c1-l2a --data-dir staging/publish/sentinel-2-c1-l2a --remote-baseline`
   with an empty `year=YYYY/` directory staged per published year, the
   same for `make_collection.py`, `CI_LIGHT=1 python3 tests/run_all.py`,
   commit, run `publish-catalog`.
8. **Stats**: `publish-stats` for `sentinel-2-c1-l2a` (the `stats-c1`
   collection).
9. **Explorer**: flip the default collection when the backfill and the
   stats are complete.

## The periodic duty: fold live

The daily GitHub refresh appends Collection 1's new and reprocessed items
to `year=YYYY/live.parquet` at zstd 3 and never consolidates. Every one to
two months, and at the end of each year:

```bash
ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEARS=2026 tools/rails/fold_live.sbatch'
# in January: YEARS=2025,2026
```

Then, on a laptop, the metadata commands the job prints (`make_items` and
`make_collection` with `--remote-baseline`, the gates, a commit) and the
`publish-catalog` workflow. The daily refresh needs nothing: its next run
finds the emptied live and starts a new tail, disjoint from the year
file.

If the fold is skipped, nothing breaks. `live.parquet` keeps growing at
zstd 3 (a whole year of Collection 1 in live is about 5 million rows in a
larger, less compressed file), every query stays correct because the year
item lists both files and the collection's glob reads both, and the year
file lags the truth by however long the fold is late. A reprocessed scene
sits in live with a newer `s2:generation_time` next to the archive's copy
until the fold dedupes them.
