#!/usr/bin/env python3
"""Upload one finished year part, with credentials that cannot have expired.

    python3 tools/upload_part.py --data-dir staging/publish <part.parquet>

s2_build.py --on-part-done runs this with each part's path the moment the
part passes its gpio check, so a build-year job that times out has already
published everything it finished. The job can run six hours; the OIDC
session configure-aws-credentials assumed before the build lasts one hour
unless the role allows more, and the role's MaxSessionDuration is not ours
to read (iam:GetRole is denied to every identity this repo holds; checked
2026-09-18). publish-backfill.yml asks for a six-hour session and falls
back to the default when the role refuses -- but this script does not
depend on which it got.

Before each upload it mints its own session: the runner's OIDC token
(ACTIONS_ID_TOKEN_REQUEST_URL / _TOKEN, present in every step of a job with
`id-token: write`) exchanged for a fresh one-hour session through STS
AssumeRoleWithWebIdentity -- the same exchange configure-aws-credentials
does -- and exported to the environment of the upload_data.py child, and
nowhere else. The role comes from S2_UPLOAD_ROLE_ARN. When the exchange
fails the upload still runs on whatever credentials the environment holds
(the long session, if the role granted one); when there is no OIDC
environment at all (a local run) it runs on the environment or the
catalog.publish.yaml profile, exactly as upload_data.py alone would.

Every failure exits non-zero, and s2_build.py stops the year on that: a
part whose upload did not happen must not be counted as published.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from s2_fetch import UA as _FETCH_UA  # noqa: E402

UA = {"User-Agent": _FETCH_UA["User-Agent"]}
AUDIENCE = "sts.amazonaws.com"
SESSION_SECONDS = 3600


def oidc_token(request_url: str, request_token: str,
               audience: str = AUDIENCE) -> str:
    """The runner's OIDC JWT for `audience`, from the token endpoint GitHub
    hands every step of a job with id-token: write."""
    sep = "&" if "?" in request_url else "?"
    req = urllib.request.Request(
        f"{request_url}{sep}audience={audience}",
        headers={"Authorization": f"bearer {request_token}",
                 "Accept": "application/json; api-version=2.0", **UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["value"]


def fresh_credentials(role_arn: str, token: str, session_name: str,
                      region: str | None, sts=None) -> dict[str, str]:
    """Exchange an OIDC token for a one-hour session on `role_arn`, as the
    three AWS_* variables boto3 reads. AssumeRoleWithWebIdentity is
    unsigned, so an expired session in the environment does not get in
    the way."""
    if sts is None:
        import boto3
        sts = boto3.client("sts", region_name=region or None)
    got = sts.assume_role_with_web_identity(
        RoleArn=role_arn, RoleSessionName=session_name,
        WebIdentityToken=token, DurationSeconds=SESSION_SECONDS)["Credentials"]
    return {"AWS_ACCESS_KEY_ID": got["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": got["SecretAccessKey"],
            "AWS_SESSION_TOKEN": got["SessionToken"]}


def upload_env(env: dict[str, str], mint=None) -> dict[str, str]:
    """The environment for the upload: `env` plus a fresh session when the
    runner's OIDC endpoint and S2_UPLOAD_ROLE_ARN are both there. A failed
    exchange is reported and the upload proceeds on `env` as it is, so a
    long-lived session from the assume step still works; without one, the
    upload fails on expired credentials and the build stops, which is the
    right outcome for a part that was not published."""
    url = env.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    bearer = env.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    role = env.get("S2_UPLOAD_ROLE_ARN")
    if not (url and bearer and role):
        print("upload_part: no OIDC endpoint or S2_UPLOAD_ROLE_ARN in the "
              "environment; uploading with the credentials already there",
              flush=True)
        return dict(env)
    mint = mint or (lambda: fresh_credentials(
        role, oidc_token(url, bearer),
        env.get("S2_UPLOAD_SESSION_NAME") or "sentinel-2-catalog-upload-part",
        env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")))
    try:
        creds = mint()
    except Exception as exc:  # noqa: BLE001 - any failure: say so, fall back
        print(f"upload_part: could not mint a fresh session for {role} "
              f"({type(exc).__name__}: {exc}); uploading with the "
              f"credentials already in the environment", file=sys.stderr,
              flush=True)
        return dict(env)
    print(f"upload_part: fresh {SESSION_SECONDS}s session on {role}",
          flush=True)
    return {**env, **creds}


def main(argv: list[str] | None = None, run=subprocess.run,
         environ: dict[str, str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data-dir", required=True,
                    help="the staging tree upload_data.py walks; the part "
                         "must be under it")
    ap.add_argument("part", help="the finished part's path")
    a = ap.parse_args(argv)
    env = upload_env(dict(os.environ if environ is None else environ))
    cmd = [sys.executable, str(HERE / "upload_data.py"), "--confirm",
           "--data-dir", a.data_dir, "--only", a.part]
    return run(cmd, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
