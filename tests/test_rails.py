"""The RAILS lane under tools/rails/: every Slurm script parses and carries
the cluster's account, partition and env.sh conventions; the month list,
the month fold and the profile-based upload do what the scripts rely on;
the IAM documents are valid JSON naming the catalog prefix and nothing
else. Nothing here talks to Slurm or to AWS."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
RAILS = ROOT / "tools" / "rails"
sys.path.insert(0, str(RAILS))
sys.path.insert(0, str(ROOT / "tools"))
import fold_month  # noqa: E402
import months      # noqa: E402
import upload      # noqa: E402

SBATCH = sorted(RAILS.glob("*.sbatch"))
SCRIPTS = SBATCH + [RAILS / "build_ready_years.sh"]
EXPECTED_SBATCH = {"audit_year", "build_year", "fetch_months", "fold_live",
                   "repair_month", "upload_year"}
BUCKET_ARN = "arn:aws:s3:::us-west-2.opendata.source.coop"
OBJECTS_ARN = f"{BUCKET_ARN}/portolan-mirrors/sentinel-2-catalog/*"


def _bash(script: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", str(script)], cwd=ROOT, env=env,
                          capture_output=True, text=True)


def _dry_env(**extra: str) -> dict[str, str]:
    """A laptop dry run: no Slurm, the checkout named by REPO, nothing
    written (env.sh skips its mkdir under DRY_RUN=1)."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("SLURM_") and k not in ("YEAR", "YEARS", "MONTH")}
    env.update(REPO=str(ROOT), DRY_RUN="1", HOME=env.get("HOME", "/nonexistent"))
    env.update(extra)
    return env


# --- the scripts -----------------------------------------------------------

def test_every_planned_sbatch_exists():
    assert {p.stem for p in SBATCH} == EXPECTED_SBATCH


@pytest.mark.parametrize("script", SCRIPTS, ids=[p.name for p in SCRIPTS])
def test_script_parses(script):
    proc = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("script", SBATCH, ids=[p.name for p in SBATCH])
def test_sbatch_conventions(script):
    """Account and partition lines, strict mode, env.sh reached through
    $REPO (never $BASH_SOURCE: Slurm runs a spooled copy of the script),
    a logs/ output file, and every job-specific command through run so
    DRY_RUN=1 prints instead of acting."""
    text = script.read_text()
    assert "#SBATCH --account=bgtj-tgirails" in text
    assert "#SBATCH --partition=cpu" in text
    assert re.search(r"#SBATCH --output=logs/s2c1-\w+-%[jA]", text)
    assert "set -euo pipefail" in text
    assert 'REPO="${REPO:-${SLURM_SUBMIT_DIR:-$HOME/s2-catalog}}"' in text
    assert 'source "$REPO/tools/rails/env.sh"' in text
    assert "$BASH_SOURCE" not in text.replace("not from $BASH_SOURCE", "")
    assert re.search(r"^\s*run python3 ", text, re.M)


@pytest.mark.parametrize("script,extra", [
    ("build_year.sbatch", {"YEAR": "2017"}),
    ("fetch_months.sbatch", {"MONTH": "2019-03"}),
    ("upload_year.sbatch", {"YEAR": "2019"}),
    ("fold_live.sbatch", {"YEARS": "2025,2026"}),
    ("repair_month.sbatch", {"MONTH": "2019-03"}),
    ("audit_year.sbatch", {"YEAR": "2019"}),
], ids=lambda x: x if isinstance(x, str) else "")
def test_dry_run_prints_the_tools_and_touches_nothing(script, extra):
    """DRY_RUN=1 on a laptop (no Slurm, no slices, no AWS) exits 0 and
    prints the s2_* command it would run, with --collection
    sentinel-2-c1-l2a, and writes nothing under $SLICES or $PUBLISH."""
    with tempfile.TemporaryDirectory() as td:
        env = _dry_env(SLICES=f"{td}/slices", PUBLISH=f"{td}/publish", **extra)
        proc = _bash(RAILS / script, env)
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert "dry-run: python3 " in proc.stdout
        if script != "upload_year.sbatch":
            assert "--collection sentinel-2-c1-l2a" in proc.stdout
        assert os.listdir(td) == []


def test_smoke_keeps_every_path_under_smoke():
    """SMOKE=1: the smoke month, slices and publish under _smoke/, the key
    prefix _smoke, no array index needed."""
    fetch = _bash(RAILS / "fetch_months.sbatch", _dry_env(SMOKE="1"))
    assert fetch.returncode == 0, fetch.stdout + fetch.stderr
    assert "--start 2017-07-01 --end 2017-07-31" in fetch.stdout
    assert "/slices/_smoke/2017-07" in fetch.stdout
    up = _bash(RAILS / "upload_year.sbatch", _dry_env(SMOKE="1", PUBLISH="/p"))
    assert up.returncode == 0, up.stdout + up.stderr
    assert "--data-dir /p/_smoke --key-prefix _smoke sentinel-2-c1-l2a/year=2017/items.parquet" in up.stdout
    assert "sentinel-2-catalog/_smoke/sentinel-2-c1-l2a/year=2017/items.parquet" in up.stdout


def test_build_year_refuses_a_short_year_outside_dry_run():
    env = _dry_env(YEAR="2017")
    env["DRY_RUN"] = "0"
    with tempfile.TemporaryDirectory() as td:
        env.update(SLICES=td, PUBLISH=td)
        proc = _bash(RAILS / "build_year.sbatch", env)
    assert proc.returncode == 1
    assert "not building a short year" in proc.stdout


def test_fold_live_uploads_parts_before_live_with_force():
    out = _bash(RAILS / "fold_live.sbatch", _dry_env(YEARS="2026")).stdout
    puts = [line for line in out.splitlines() if "upload.py" in line]
    assert len(puts) == 2
    assert puts[0].endswith("--force sentinel-2-c1-l2a/year=2026/items.parquet")
    assert puts[1].endswith("--force sentinel-2-c1-l2a/year=2026/live.parquet")
    assert out.index("s2_build.py") < out.index("upload.py")
    assert "make_items.py --collection sentinel-2-c1-l2a" in out


def test_build_ready_years_submits_only_complete_unbuilt_years():
    """Two years of slices, one already built, one short: one sbatch,
    named for its year."""
    env = _dry_env()
    with tempfile.TemporaryDirectory() as td:
        slices = Path(td) / "slices"; slices.mkdir()
        for y in (2016, 2017):
            for m in range(1, 13):
                (slices / f"{y}-{m:02d}.parquet").write_bytes(b"x" * m)
        for m in range(1, 12):
            (slices / f"2018-{m:02d}.parquet").write_bytes(b"x")
        publish = Path(td) / "publish" / "sentinel-2-c1-l2a" / "year=2016"
        publish.mkdir(parents=True)
        (publish / "items.parquet").write_bytes(b"built")
        env.update(SLICES=str(slices), PUBLISH=str(Path(td) / "publish"),
                   FIRST_YEAR="2016", PATH=f"/nonexistent:{env['PATH']}")
        proc = _bash(RAILS / "build_ready_years.sh", env)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "year=2016: built" in proc.stdout
    assert "year=2018: 1 month(s) not folded yet" in proc.stdout
    submits = [line for line in proc.stdout.splitlines() if "sbatch" in line]
    assert len(submits) == 1 and "--job-name=s2c1-build-2017" in submits[0]
    # printf %q escapes the comma in the dry-run echo.
    assert "--export=ALL\\,YEAR=2017" in submits[0]


# --- months.py --------------------------------------------------------------

def test_months_range():
    assert months.months("2015-06", "2026-09") == \
        [f"{y}-{m:02d}" for y in range(2015, 2027) for m in range(1, 13)][5:-3]
    assert len(months.months("2015-06", "2026-09")) == 136
    assert len(months.months("2015-10", "2026-09")) == 132
    assert months.months("2019-12", "2020-01") == ["2019-12", "2020-01"]
    assert months.months("2020-03", "2020-03") == ["2020-03"]
    assert months.months("2020-04", "2020-03") == []
    proc = subprocess.run([sys.executable, str(RAILS / "months.py"), "2017-01", "2017-12"],
                          capture_output=True, text=True)
    assert proc.stdout.split() == [f"2017-{m:02d}" for m in range(1, 13)]


# --- fold_month.py ----------------------------------------------------------

def _chunk(con, path: Path, ids: list[str], day: int):
    con.execute(f"""
        COPY (SELECT id, make_timestamptz(2019, 3, {day}, 10, 0, 0) AS datetime,
                     '2019-01-01T00:00:00Z' AS "s2:generation_time"
              FROM (SELECT unnest({ids!r}) AS id))
        TO '{path}' (FORMAT PARQUET)""")


def test_fold_month_unions_api_and_repair_and_skips_empty_days():
    con = duckdb.connect()
    with tempfile.TemporaryDirectory() as td:
        month = Path(td) / "2019-03"
        (month / "api").mkdir(parents=True); (month / "repair").mkdir()
        _chunk(con, month / "api" / "2019-03-02_2019-03-02.parquet", ["b", "c"], 2)
        _chunk(con, month / "api" / "2019-03-01_2019-03-01.parquet", ["a"], 1)
        (month / "api" / "2019-03-03_2019-03-03.parquet").write_bytes(b"")
        _chunk(con, month / "repair" / "2019-03-05_2019-03-05.parquet", ["c", "d"], 5)
        dest = Path(td) / "2019-03.parquet"
        assert fold_month.fold(str(month), str(dest), ["api"]) == 3
        assert con.execute(f"SELECT list(id ORDER BY datetime) FROM '{dest}'").fetchone()[0] == ["a", "b", "c"]
        # The repair lane adds its rows; duplicates are the build's to drop.
        assert fold_month.fold(str(month), str(dest), ["api", "repair"]) == 5
        assert con.execute(f"SELECT count(*), count(DISTINCT id) FROM '{dest}'").fetchone() == (5, 4)
        assert not dest.with_name(dest.name + ".tmp").exists()
        # A month with no rows at all: the empty sentinel.
        empty = Path(td) / "2019-04"; (empty / "api").mkdir(parents=True)
        (empty / "api" / "2019-04-01_2019-04-01.parquet").write_bytes(b"")
        sentinel = Path(td) / "2019-04.parquet"
        assert fold_month.fold(str(empty), str(sentinel), ["api"]) == 0
        assert sentinel.exists() and sentinel.stat().st_size == 0


# --- upload.py --------------------------------------------------------------

class _FakeS3:
    """head_object / upload_file over a dict; 404 as botocore would raise it."""

    class exceptions:
        class ClientError(Exception):
            def __init__(self, code):
                super().__init__(code)
                self.response = {"Error": {"Code": code}}

    def __init__(self, objects: dict[str, int] | None = None):
        self.objects = dict(objects or {})
        self.puts: list[tuple[str, str]] = []

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self.exceptions.ClientError("404")
        return {"ContentLength": self.objects[Key]}

    def upload_file(self, filename, bucket, key, ExtraArgs=None, Config=None):
        self.puts.append((key, ExtraArgs["ContentType"]))
        self.objects[key] = Path(filename).stat().st_size


def test_upload_plans_keys_under_the_catalog_prefix():
    with tempfile.TemporaryDirectory() as td:
        base = Path(td); year = base / "sentinel-2-c1-l2a" / "year=2019"
        year.mkdir(parents=True)
        (year / "items.parquet").write_bytes(b"p" * 10)
        (year / "notes.txt").write_text("no")
        prefix = "portolan-mirrors/sentinel-2-catalog"
        [u] = upload.plan_uploads(base, ["sentinel-2-c1-l2a/year=2019/items.parquet"], prefix)
        assert u.key == f"{prefix}/sentinel-2-c1-l2a/year=2019/items.parquet"
        assert u.content_type == "application/vnd.apache.parquet"
        [u] = upload.plan_uploads(base, [str(year / "items.parquet")], prefix, "_smoke")
        assert u.key == f"{prefix}/_smoke/sentinel-2-c1-l2a/year=2019/items.parquet"
        with pytest.raises(SystemExit, match="not a publishable"):
            upload.plan_uploads(base, ["sentinel-2-c1-l2a/year=2019/notes.txt"], prefix)
        with pytest.raises(SystemExit, match="no such file"):
            upload.plan_uploads(base, ["sentinel-2-c1-l2a/year=2020/items.parquet"], prefix)
        with pytest.raises(SystemExit, match="not under"):
            upload.plan_uploads(base, ["/etc/hosts"], prefix)


def test_upload_skips_same_size_and_force_replaces():
    with tempfile.TemporaryDirectory() as td:
        f = Path(td) / "items.parquet"; f.write_bytes(b"p" * 10)
        u = upload.Upload(f, "k/items.parquet", "application/vnd.apache.parquet")
        s3 = _FakeS3({"k/items.parquet": 10})
        assert upload.upload_one(s3, "b", u, force=False) == "skipped"
        assert s3.puts == []
        s3 = _FakeS3({"k/items.parquet": 7})
        assert upload.upload_one(s3, "b", u, force=False) == "uploaded"
        assert s3.puts == [("k/items.parquet", "application/vnd.apache.parquet")]
        s3 = _FakeS3({"k/items.parquet": 10})
        assert upload.upload_one(s3, "b", u, force=True) == "uploaded"
        assert upload.upload_one(_FakeS3(), "b", u, force=False, dry_run=True) == "would upload"


def test_upload_main_uses_the_profile_and_the_real_write_prefix(capsys):
    with tempfile.TemporaryDirectory() as td:
        year = Path(td) / "sentinel-2-c1-l2a" / "year=2019"; year.mkdir(parents=True)
        (year / "items.parquet").write_bytes(b"p" * 5)
        s3 = _FakeS3()
        rc = upload.main(["--data-dir", td, "--profile", "source-coop",
                          "sentinel-2-c1-l2a/year=2019/items.parquet"], client=s3)
    assert rc == 0
    assert s3.puts == [("portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=2019/items.parquet",
                        "application/vnd.apache.parquet")]
    out = capsys.readouterr().out
    assert "profile: source-coop" in out and "1 uploaded, 0 skipped" in out


# --- IAM documents ------------------------------------------------------------

def test_iam_policy_names_only_the_catalog_prefix():
    policy = json.loads((RAILS / "iam-policy.json").read_text())
    assert policy["Version"] == "2012-10-17"
    resources = {s["Resource"] for s in policy["Statement"]}
    assert resources == {BUCKET_ARN, OBJECTS_ARN}
    by_resource = {s["Resource"]: s for s in policy["Statement"]}
    lst = by_resource[BUCKET_ARN]
    assert lst["Action"] == "s3:ListBucket"
    assert lst["Condition"]["StringLike"]["s3:prefix"] == "portolan-mirrors/sentinel-2-catalog/*"
    objs = by_resource[OBJECTS_ARN]
    assert set(objs["Action"]) == {"s3:GetObject", "s3:PutObject",
                                   "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}
    assert all(s["Effect"] == "Allow" for s in policy["Statement"])


def test_role_trust_statement_names_the_rails_user():
    stmt = json.loads((RAILS / "role-trust-statement.json").read_text())
    assert stmt["Effect"] == "Allow" and stmt["Action"] == "sts:AssumeRole"
    assert stmt["Principal"] == {"AWS": "arn:aws:iam::939788573396:user/rails-sentinel-2-catalog"}
    readme = (RAILS / "README.md").read_text()
    assert "arn:aws:iam::939788573396:role/source-coop-portolan-mirrors" in readme
    assert "[profile source-coop]" in readme and "rails-sentinel-2-catalog" in readme
