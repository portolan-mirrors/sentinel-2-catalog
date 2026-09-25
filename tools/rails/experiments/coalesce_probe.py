#!/usr/bin/env python3
"""What the per-search request count costs, and what coalescing would save.

    python3 tools/rails/experiments/coalesce_probe.py layout-2018.jsonl \
        --variant V7 --tile 31UFU --reps 5

This is NOT a measurement of `apps/explorer/search.js`; measure_layout.py is.
It is the network half of a question that measurement raises. On this path a
range GET to one object costs 0.25-0.8 s and the browser serves several of them
to the *same* object one after another, so a search's wall time tracks its
per-object request count and almost ignores its bytes. `search.js` fetches one
range per (column, row group) — eight per admitted group — and the eight land in
three to five nearly adjacent byte runs.

So this replays the byte ranges a measured search actually used, from the same
headless Chrome against the same objects, two ways:

  * as the client issues them, one GET per column chunk;
  * merged, any two runs closer than --gap bytes fetched as one GET.

The difference is what a coalescing client would save, measured rather than
assumed, and it is not a layout property — except that a layout with small row
groups puts the chunks closer together and merges further.
"""
from __future__ import annotations

import argparse
import http.server
import json
import shutil
import socketserver
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from measure_layout import find_chrome  # noqa: E402

PAGE = """<!doctype html><meta charset=utf-8><title>coalesce probe</title>
<pre id=log></pre><script type=module>
const plan = await fetch("/plan").then(r => r.json());
const log = document.getElementById("log");
// `distinct` is the whole point of the probe: whether the concurrent range
// reads go to ONE url (what search.js does) or to one url per range (the same
// object, the same bytes, a query parameter S3 ignores).
const get = async (url, lo, hi, distinct, salt) => {
  const u = distinct ? `${url}?cb=${salt}-${lo}` : `${url}?cb=${salt}`;
  const res = await fetch(u, { headers: { Range: `bytes=${lo}-${hi}` } });
  if (res.status !== 206) throw new Error("HTTP " + res.status);
  return (await res.arrayBuffer()).byteLength;
};
const out = [];
for (const round of plan.rounds) {
  const t0 = performance.now();
  const sizes = await Promise.all(round.spans.map(
    ([lo, hi]) => get(plan.url, lo, hi, round.distinct, round.label)));
  out.push({ label: round.label, kind: round.kind, gets: round.spans.length,
             bytes: sizes.reduce((a, b) => a + b, 0),
             ms: +(performance.now() - t0).toFixed(1) });
  log.textContent += `\\n${round.label}: ${round.spans.length} GET, `
    + `${out.at(-1).bytes} B, ${out.at(-1).ms} ms`;
}
await fetch("/report", { method: "POST", body: JSON.stringify(out) });
document.title = "PROBE-DONE";
</script>
"""


def spans_of(path: Path, variant: str, tile: str) -> tuple[str, list[tuple[int, int]]]:
    """The column-chunk byte ranges of one measured warm search."""
    for line in path.read_text().splitlines():
        r = json.loads(line)
        if r.get("variant") != variant or r.get("tile") != tile or not r.get("warm"):
            continue
        url, spans = None, []
        for q in r["warm"]["requests"]:
            rg = q.get("range") or ""
            if not rg.startswith("bytes=") or rg == "bytes=-8":
                continue
            lo, hi = rg[6:].split("-")
            if not lo or not hi:
                continue
            spans.append((int(lo), int(hi)))
            url = q["url"].split("?")[0]
        if spans:
            return url, sorted(spans)
    sys.exit(f"no warm search for {variant} {tile} in {path}")


def merge(spans: list[tuple[int, int]], gap: int) -> list[tuple[int, int]]:
    out = [list(spans[0])]
    for lo, hi in spans[1:]:
        if lo - out[-1][1] <= gap:
            out[-1][1] = max(out[-1][1], hi)
        else:
            out.append([lo, hi])
    return [(lo, hi) for lo, hi in out]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("jsonl", type=Path)
    ap.add_argument("--variant", default="V7")
    ap.add_argument("--tile", default="31UFU")
    ap.add_argument("--gap", type=int, default=64 * 1024)
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--port", type=int, default=8791)
    a = ap.parse_args()

    path, spans = spans_of(a.jsonl, a.variant, a.tile)
    url = "https://data.source.coop" + path
    merged = merge(spans, a.gap)
    print(f"{a.variant} {a.tile}: {url}")
    print(f"  as issued: {len(spans)} GETs, "
          f"{sum(h - l + 1 for l, h in spans) / 1024:,.0f} KiB")
    print(f"  merged at {a.gap // 1024} KiB: {len(merged)} GETs, "
          f"{sum(h - l + 1 for l, h in merged) / 1024:,.0f} KiB")
    # Three rounds per repetition, interleaved:
    #   as-issued  what search.js does -- N ranges, all on the SAME url
    #   distinct   the same N ranges, each on its own url (?cb=…), same object
    #   merged     the runs merged at --gap, on the same url
    kinds = (("as-issued", spans, False), ("distinct", spans, True),
             ("merged", merged, False))
    rounds = []
    for i in range(a.reps):
        for kind, sp, distinct in kinds:
            rounds.append(dict(label=f"{kind}-{i}", kind=kind, distinct=distinct,
                               spans=[list(s) for s in sp]))

    results: list[dict] = []

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path == "/plan":
                body = json.dumps(dict(url=url, rounds=rounds)).encode()
                ct = "application/json"
            else:
                body, ct = PAGE.encode(), "text/html"
            self.send_response(200)
            self.send_header("content-type", ct)
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            n = int(self.headers.get("content-length", 0))
            results.extend(json.loads(self.rfile.read(n)))
            self.send_response(200)
            self.send_header("content-length", "0")
            self.end_headers()

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    srv = socketserver.ThreadingTCPServer(("127.0.0.1", a.port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    profile = Path(f"/tmp/coalesce-{a.port}")
    shutil.rmtree(profile, ignore_errors=True)
    proc = subprocess.Popen(
        [find_chrome(), "--disable-gpu", "--no-first-run", "--disk-cache-size=1",
         f"--user-data-dir={profile}", f"http://127.0.0.1:{a.port}/"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.monotonic()
    while len(results) < len(rounds) and time.monotonic() - t0 < 600:
        time.sleep(2)
    proc.kill()
    shutil.rmtree(profile, ignore_errors=True)
    for kind in ("as-issued", "distinct", "merged"):
        rs = [r for r in results if r.get("kind") == kind]
        if not rs:
            continue
        print(f"  {kind:10} median {statistics.median(r['ms'] for r in rs):,.0f} ms "
              f"(min {min(r['ms'] for r in rs):,.0f}, max {max(r['ms'] for r in rs):,.0f}) "
              f"over {len(rs)} runs, {rs[0]['gets']} GET, {rs[0]['bytes'] / 1024:,.0f} KiB")


if __name__ == "__main__":
    main()
