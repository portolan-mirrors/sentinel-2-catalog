#!/usr/bin/env python3
"""Four ways to issue a search's concurrent range reads, timed in real Chrome.

    python3 tools/rails/experiments/parallel_probe.py \
        --url https://data.source.coop/portolan-mirrors/sentinel-2-catalog/\
sentinel-2-c1-l2a/year=2024/items.parquet --tile 31UFU --reps 5

Section A of docs/c1-search-speed-brief.md. The previous experiment
(docs/c1-layout-experiments.md) found that Chrome serialises concurrent range
GETs that *share a URL*, so `search.js`'s eight column-chunk reads of one row
group cost ~2.1 s instead of ~0.44 s, and that giving each read its own URL
removes the stall. A distinct URL is a distinct CDN cache key, though, so this
probe times the candidates that keep one URL against it:

  as-issued   N ranges, one URL, default cache mode -- what search.js does
  no-store    N ranges, one URL, `cache: "no-store"` on the fetch
  coalesced   ONE range spanning min(off)..max(off+len) of the N, sliced in
              memory; fewer requests, some wasted bytes (reported)
  coal-ns     the same single range with `cache: "no-store"`
  distinct    the N ranges, one URL each (`?cb=…`, which S3 ignores) -- the
              reference number, NOT a candidate: it shifts load to the origin

The spans are the real ones: the probe reads the part's footer over HTTP,
admits the row groups whose `_tile` statistics cannot exclude the tile, and
replays exactly the (column, group) chunk ranges search.js would fetch.
Rounds are interleaved so the path's latency drift lands on all of them.
"""
from __future__ import annotations

import argparse
import http.server
import io
import json
import shutil
import socketserver
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from measure_layout import UA, find_chrome  # noqa: E402

# The columns search.js decodes: the tile column plus SEARCH_COLUMNS.
COLUMNS = ("_tile", "id", "datetime", "eo:cloud_cover",
           "s2:nodata_pixel_percentage", "thumbnail_url", "bbox",
           "s2:processing_baseline")


class RemoteFile(io.RawIOBase):
    """A seekable read-only file over HTTP range GETs, so pyarrow can read a
    remote part's footer without downloading the part."""

    def __init__(self, url: str):
        self.url = url
        self.pos = 0
        req = urllib.request.Request(url, method="HEAD", headers=UA)
        with urllib.request.urlopen(req, timeout=120) as r:
            self.size = int(r.headers["content-length"])

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, off, whence=0):
        self.pos = off if whence == 0 else (self.pos + off if whence == 1
                                            else self.size + off)
        return self.pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self.pos
        if n == 0 or self.pos >= self.size:
            return b""
        hi = min(self.size, self.pos + n) - 1
        req = urllib.request.Request(
            self.url, headers={**UA, "Range": f"bytes={self.pos}-{hi}"})
        with urllib.request.urlopen(req, timeout=300) as r:
            buf = r.read()
        self.pos += len(buf)
        return buf


def spans_for(url: str, tile: str, tile_column: str) -> tuple[list[tuple[int, int]], dict]:
    """The (lo, hi) byte ranges search.js would fetch for `tile`, plus a
    little metadata about the part."""
    import pyarrow.parquet as pq

    fh = RemoteFile(url)
    md = pq.ParquetFile(fh).metadata
    spans, groups = [], 0
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        cols = [rg.column(c) for c in range(rg.num_columns)]
        names = [c.path_in_schema.split(".")[0] for c in cols]
        lo = hi = None
        for name, cc in zip(names, cols):
            if name != tile_column:
                continue
            st = cc.statistics
            if st is not None and st.has_min_max:
                lo, hi = st.min, st.max
        if lo is not None and not (lo <= tile <= hi):
            continue
        groups += 1
        for name, cc in zip(names, cols):
            if name not in COLUMNS:
                continue
            off = cc.dictionary_page_offset or cc.data_page_offset
            spans.append((int(off), int(off) + int(cc.total_compressed_size) - 1))
    info = dict(size=fh.size, row_groups=md.num_row_groups, admitted=groups,
                footer=footer_len(fh))
    return sorted(spans), info


def footer_len(fh: RemoteFile) -> int:
    fh.seek(-8, 2)
    tail = fh.read(8)
    return int.from_bytes(tail[:4], "little") + 8


PAGE = """<!doctype html><meta charset=utf-8><title>parallel probe</title>
<pre id=log></pre><script type=module>
const plan = await fetch("/plan").then(r => r.json());
const log = document.getElementById("log");
// One fetch of one range. `distinct` gives every range its own url (same
// object -- S3 ignores an unknown query parameter); `store` false sets
// cache:"no-store", which is the candidate that keeps one url.
const get = async (url, lo, hi, round) => {
  const u = `${url}?cb=${round.label}` + (round.distinct ? `-${lo}` : "");
  const init = { headers: { Range: `bytes=${lo}-${hi}` } };
  if (round.nostore) init.cache = "no-store";
  const res = await fetch(u, init);
  if (res.status !== 206) throw new Error("HTTP " + res.status);
  return (await res.arrayBuffer()).byteLength;
};
const out = [];
for (const round of plan.rounds) {
  const t0 = performance.now();
  let bytes = 0, err = null;
  try {
    const sizes = await Promise.all(round.spans.map(
      ([lo, hi]) => get(plan.url, lo, hi, round)));
    bytes = sizes.reduce((a, b) => a + b, 0);
  } catch (e) { err = String(e); }
  out.push({ label: round.label, kind: round.kind, gets: round.spans.length,
             bytes, err, ms: +(performance.now() - t0).toFixed(1) });
  log.textContent += `\\n${round.label}: ${round.spans.length} GET, `
    + `${bytes} B, ${out.at(-1).ms} ms ${err ?? ""}`;
}
await fetch("/report", { method: "POST", body: JSON.stringify(out) });
document.title = "PROBE-DONE";
</script>
"""

KINDS = ("as-issued", "no-store", "coalesced", "coal-ns", "distinct")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--url", required=True)
    ap.add_argument("--tile", default="31UFU")
    ap.add_argument("--tile-column", default="_tile")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--port", type=int, default=8793)
    ap.add_argument("--label", default="")
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args()

    spans, info = spans_for(a.url, a.tile, a.tile_column)
    if not spans:
        sys.exit(f"no admitted row group for {a.tile} in {a.url}")
    want = sum(h - l + 1 for l, h in spans)
    one = [(spans[0][0], spans[-1][1])]
    span_bytes = one[0][1] - one[0][0] + 1
    print(f"{a.label or a.url}")
    print(f"  {info['size']:,} B part, {info['row_groups']} row group(s), "
          f"{info['admitted']} admitted, footer {info['footer'] / 1024:,.0f} KB")
    print(f"  as issued: {len(spans)} GET, {want / 1024:,.1f} KiB")
    print(f"  coalesced: 1 GET, {span_bytes / 1024:,.1f} KiB "
          f"(+{100 * (span_bytes - want) / want:,.1f} % waste)")

    rounds = []
    for i in range(a.reps):
        for kind in KINDS:
            sp = one if kind.startswith("coal") else spans
            rounds.append(dict(label=f"{kind}-{i}", kind=kind,
                               distinct=kind == "distinct",
                               nostore=kind in ("no-store", "coal-ns"),
                               spans=[list(s) for s in sp]))

    results: list[dict] = []

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            if self.path == "/plan":
                body = json.dumps(dict(url=a.url, rounds=rounds)).encode()
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
    profile = Path(f"/tmp/parallel-{a.port}")
    shutil.rmtree(profile, ignore_errors=True)
    proc = subprocess.Popen(
        [find_chrome(), "--disable-gpu", "--no-first-run",
         "--no-default-browser-check", "--disk-cache-size=1",
         f"--user-data-dir={profile}", f"http://127.0.0.1:{a.port}/"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    t0 = time.monotonic()
    while len(results) < len(rounds) and time.monotonic() - t0 < 900:
        time.sleep(2)
    proc.kill()
    shutil.rmtree(profile, ignore_errors=True)

    table = {}
    for kind in KINDS:
        rs = [r for r in results if r.get("kind") == kind and not r.get("err")]
        if not rs:
            print(f"  {kind:10} no result")
            continue
        med = statistics.median(r["ms"] for r in rs)
        table[kind] = dict(ms=round(med), lo=round(min(r["ms"] for r in rs)),
                           hi=round(max(r["ms"] for r in rs)), n=len(rs),
                           gets=rs[0]["gets"], kib=round(rs[0]["bytes"] / 1024, 1))
        print(f"  {kind:10} median {med:,.0f} ms "
              f"(min {min(r['ms'] for r in rs):,.0f}, "
              f"max {max(r['ms'] for r in rs):,.0f}) over {len(rs)} runs, "
              f"{rs[0]['gets']} GET, {rs[0]['bytes'] / 1024:,.0f} KiB")
    if a.out:
        a.out.write_text(json.dumps(dict(url=a.url, label=a.label, tile=a.tile,
                                         info=info, want=want,
                                         coalesced_bytes=span_bytes,
                                         table=table, raw=results), indent=1))


if __name__ == "__main__":
    main()
