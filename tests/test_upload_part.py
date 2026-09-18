"""tools/upload_part.py: the per-part upload s2_build.py --on-part-done runs.

No network beyond a loopback token endpoint, no AWS: the STS exchange is a
stub, and the upload_data.py child is a recorded call."""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import upload_part  # noqa: E402


class _TokenEndpoint:
    """GitHub's ACTIONS_ID_TOKEN_REQUEST_URL, on loopback: answers {"value":
    <jwt>} and records the request it saw."""

    def __init__(self):
        endpoint = self
        self.seen = []

        class H(BaseHTTPRequestHandler):
            def do_GET(self):
                endpoint.seen.append(
                    (self.path, self.headers.get("Authorization"),
                     self.headers.get("User-Agent")))
                body = json.dumps({"value": "jwt-for-" + self.path}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.srv = HTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_port}/token?api-version=2.0"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()


class _Sts:
    def __init__(self):
        self.calls = []

    def assume_role_with_web_identity(self, **kw):
        self.calls.append(kw)
        return {"Credentials": {"AccessKeyId": "ASIAFRESH",
                                "SecretAccessKey": "fresh-secret",
                                "SessionToken": "fresh-token"}}


def test_oidc_token_asks_the_runner_endpoint_for_the_sts_audience():
    ep = _TokenEndpoint()
    try:
        tok = upload_part.oidc_token(ep.url, "runner-bearer")
    finally:
        ep.close()
    assert tok == "jwt-for-/token?api-version=2.0&audience=sts.amazonaws.com"
    path, auth, ua = ep.seen[0]
    assert auth == "bearer runner-bearer"
    assert ua.startswith("sentinel-2-catalog-tools/")


def test_fresh_credentials_exchange_the_token_for_one_hour():
    sts = _Sts()
    creds = upload_part.fresh_credentials(
        "arn:aws:iam::1:role/r", "the-jwt", "sess", "us-west-2", sts=sts)
    assert creds == {"AWS_ACCESS_KEY_ID": "ASIAFRESH",
                     "AWS_SECRET_ACCESS_KEY": "fresh-secret",
                     "AWS_SESSION_TOKEN": "fresh-token"}
    assert sts.calls == [{"RoleArn": "arn:aws:iam::1:role/r",
                          "RoleSessionName": "sess",
                          "WebIdentityToken": "the-jwt",
                          "DurationSeconds": 3600}]


def test_upload_env_replaces_the_stale_session_with_a_fresh_one():
    env = {"ACTIONS_ID_TOKEN_REQUEST_URL": "http://x/token?api-version=2.0",
           "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "b",
           "S2_UPLOAD_ROLE_ARN": "arn:aws:iam::1:role/r",
           "AWS_ACCESS_KEY_ID": "ASIASTALE", "AWS_SECRET_ACCESS_KEY": "old",
           "AWS_SESSION_TOKEN": "old-token", "PATH": "/usr/bin"}
    out = upload_part.upload_env(env, mint=lambda: {
        "AWS_ACCESS_KEY_ID": "ASIAFRESH", "AWS_SECRET_ACCESS_KEY": "new",
        "AWS_SESSION_TOKEN": "new-token"})
    assert out["AWS_ACCESS_KEY_ID"] == "ASIAFRESH"
    assert out["AWS_SESSION_TOKEN"] == "new-token"
    assert out["PATH"] == "/usr/bin"
    # The caller's environment is not touched.
    assert env["AWS_ACCESS_KEY_ID"] == "ASIASTALE"


def test_upload_env_falls_back_to_the_environment_when_the_exchange_fails(capsys):
    env = {"ACTIONS_ID_TOKEN_REQUEST_URL": "http://x/token",
           "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "b",
           "S2_UPLOAD_ROLE_ARN": "arn:aws:iam::1:role/r",
           "AWS_ACCESS_KEY_ID": "ASIALONG"}

    def boom():
        raise RuntimeError("sts is down")

    out = upload_part.upload_env(env, mint=boom)
    assert out == env
    assert "could not mint a fresh session" in capsys.readouterr().err


def test_upload_env_without_oidc_is_the_environment_as_is(capsys):
    env = {"AWS_ACCESS_KEY_ID": "AKIALOCAL", "HOME": "/h"}
    calls = []
    out = upload_part.upload_env(env, mint=lambda: calls.append(1))
    assert out == env and calls == []
    assert "no OIDC endpoint" in capsys.readouterr().out


def test_main_runs_upload_data_only_on_the_part_and_returns_its_exit():
    seen = []

    class R:
        def __init__(self, code):
            self.returncode = code

    def run(cmd, env):
        seen.append((cmd, env))
        return R(7)

    code = upload_part.main(
        ["--data-dir", "staging/publish", "/abs/year=2021/z01-15.parquet"],
        run=run, environ={"AWS_ACCESS_KEY_ID": "AKIA"})
    assert code == 7
    cmd, env = seen[0]
    assert cmd[0] == sys.executable
    assert cmd[1].endswith("tools/upload_data.py")
    assert cmd[2:] == ["--confirm", "--data-dir", "staging/publish",
                       "--only", "/abs/year=2021/z01-15.parquet"]
    assert env["AWS_ACCESS_KEY_ID"] == "AKIA"
