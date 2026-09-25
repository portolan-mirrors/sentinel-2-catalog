#!/usr/bin/env python3
"""Measure the explorer's scene search against every uploaded layout variant,
from this laptop, over HTTPS, with the shipped client.

    python3 tools/rails/experiments/measure_layout.py --year 2018 \
        --variants V0s,V0,V1,V2,V3,V4,V5,V6,V7 --reps 5 \
        --out /tmp/layout-2018.jsonl

How it works: the script is a tiny HTTP server plus a browser.

  * It serves the repository at `/`, so `harness.html` can `import
    "/apps/explorer/search.js"` -- the shipped module, unchanged.
  * It builds the plan: one cell per (variant, tile, query shape, repetition),
    each carrying the exact `sceneSearch({urls, tileColumn, tile, d0, d1, cc,
    cov})` arguments, including the list of part URLs a client would ask for.
    Which parts those are IS the variant: see `part_stems`.
  * `chrome-headless-shell` (Playwright's, the same binary the earlier
    measurement tasks used) loads `/run?i=0`. Each page load runs one cell cold
    then warm, POSTs the result, and navigates to the next -- so the plan walks
    itself and there is no CDP client to keep alive.
  * Cells are ordered so that every variant is measured back to back for the
    same (repetition, tile, shape). The path's per-request latency drifts by
    2-3x over minutes (docs/query-performance.md), and interleaving is what
    stops that drift from landing on one variant.
  * The round-trip time to the bucket is measured before and after the sweep,
    because a changed RTT invalidates the comparison.

Raw results go to the --out JSONL (one line per cell, every request recorded).
`--summarise FILE` re-reduces an existing JSONL without measuring again.
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import shutil
import socketserver
import statistics
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
sys.path.insert(0, str(HERE))
from layout_parts import octant_of, prefix_of, zone_of  # noqa: E402

BASE = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"
PUBLISHED = f"{BASE}/sentinel-2-c1-l2a"
EXPERIMENT = f"{BASE}/_experiments/layout"
TILE_COLUMN = "_tile"
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36"}

# Three tiles in three UTM zones, both hemispheres, all with a realistic 2018
# scene count (67 / 63 / 50 scenes): the Netherlands, Poland, Brazil.
TILES = ("31UFU", "33UUP", "23KKQ")

# The four query shapes of the brief. Only the tile prunes row groups in this
# client -- the date window and the cloud ceiling are applied to decoded rows --
# so the shapes differ in cost only for a date-partitioned layout, where they
# change how many PARTS a client asks for. That is exactly what they are here
# to expose.
SHAPES = {
    "1mo": dict(months=(6,), cc=100, cov=0,
                window=lambda y: (f"{y}-06-01", f"{y}-06-30")),
    "3mo": dict(months=(4, 5, 6), cc=100, cov=0,
                window=lambda y: (f"{y}-04-01", f"{y}-06-30")),
    "year": dict(months=tuple(range(1, 13)), cc=100, cov=0,
                 window=lambda y: (f"{y}-01-01", f"{y}-12-31")),
    "3mo-cc20": dict(months=(4, 5, 6), cc=20, cov=0,
                     window=lambda y: (f"{y}-04-01", f"{y}-06-30")),
}

# Where a variant's parts live, and how a client names the ones that may hold a
# tile in a window -- the `COLLECTIONS[...].parts(year, tile)` of
# apps/explorer/app.js, one entry per variant.
#  V0s  the published part WITH its published sidecar (the number to beat)
#  V0   the same layout republished with no sidecar beside it (the footer path)


def part_stems(variant: str, tile: str, months: tuple[int, ...]) -> list[str]:
    if variant in ("V0s", "V0", "V1", "V2"):
        return ["items"]
    if variant == "V3":
        return [f"m={m:02d}" for m in months]
    if variant == "V4":
        return [octant_of(tile)]
    if variant == "V5":
        return [f"{octant_of(tile)}-m={m:02d}" for m in months]
    if variant == "V6":
        return [f"z={zone_of(tile):02d}"]
    if variant == "V7":
        return [f"t={prefix_of(tile)}"]
    raise SystemExit(f"unknown variant {variant}")


def variant_dir(variant: str, year: int) -> str:
    if variant == "V0s":
        return f"{PUBLISHED}/year={year}"
    return f"{EXPERIMENT}/{year}/{variant}"


def head(url: str) -> int | None:
    """The object's size, or None on 404. Anything else raises: a variant
    measured against a part that is not really there is worse than no number."""
    req = urllib.request.Request(url, method="HEAD", headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return int(r.headers["content-length"])
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def rtt(url: str, n: int = 7) -> dict:
    """Time-to-first-byte for a 1-byte range read, n times. A transient 5xx
    from the CDN is skipped, not fatal: losing the closing RTT reading would
    throw away the whole sweep it is supposed to bracket."""
    out, errors = [], []
    for _ in range(n):
        t0 = time.monotonic()
        try:
            req = urllib.request.Request(url, headers={**UA, "Range": "bytes=0-0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                r.read()
        except Exception as e:  # noqa: BLE001
            errors.append(str(e))
            continue
        out.append(round(time.monotonic() - t0, 3))
    if not out:
        return dict(samples=[], median=None, lo=None, hi=None, errors=errors)
    return dict(samples=out, median=statistics.median(out),
                lo=min(out), hi=max(out), errors=errors)


def wire_size(url: str) -> int | None:
    """The object's size *as it crosses the wire*, compressed. The bucket
    serves the JSON sidecar zstd- or gzip-encoded and then sends no
    Content-Length, so the browser cannot report it; this is how the document
    gets the real number (385 KB of sidecar JSON is 30 KB on the wire)."""
    req = urllib.request.Request(url, headers={**UA, "Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return len(r.read())
    except urllib.error.HTTPError:
        return None


def footer_of(url: str) -> tuple[int, int]:
    """(size, footer length) of a remote parquet file, from its 8-byte tail."""
    req = urllib.request.Request(url, headers={**UA, "Range": "bytes=-8"})
    with urllib.request.urlopen(req, timeout=120) as r:
        buf = r.read()
        size = int(r.headers["content-range"].split("/")[1])
    return size, struct.unpack("<I", buf[:4])[0] + 8


def build_plan(year: int, variants: list[str], reps: int,
               shapes: list[str]) -> list[dict]:
    """One cell per (repetition, tile, shape, variant), in that nesting, so
    consecutive cells compare variants under the same network weather.

    Every part URL is HEADed once here: a stem with no object is dropped, the
    way app.js's `partExists` probe drops one, and a variant left with no part
    for a cell stops the run rather than scoring an empty search as fast.
    """
    exists: dict[str, int | None] = {}
    plan = []
    index = 0
    for rep in range(1, reps + 1):
        for tile in TILES:
            for shape in shapes:
                sh = SHAPES[shape]
                d0, d1 = sh["window"](year)
                for variant in variants:
                    urls = []
                    for stem in part_stems(variant, tile, sh["months"]):
                        u = f"{variant_dir(variant, year)}/{stem}.parquet"
                        if u not in exists:
                            exists[u] = head(u)
                        if exists[u] is not None:
                            urls.append(u)
                    if not urls:
                        raise SystemExit(
                            f"{variant} {tile} {shape}: no published part "
                            f"among {part_stems(variant, tile, sh['months'])}")
                    plan.append(dict(
                        index=index, rep=rep, tile=tile, shape=shape,
                        variant=variant, year=year, parts=len(urls),
                        args=dict(urls=urls, tileColumn=TILE_COLUMN, tile=tile,
                                  d0=d0, d1=d1, cc=sh["cc"], cov=sh["cov"])))
                    index += 1
    for cell in plan:
        cell["total"] = len(plan)
    return plan


def sidecar_wire(plan: list[dict]) -> dict[str, int]:
    """Compressed wire size of every sidecar any part in the plan has, keyed
    the way the harness keys a request (the path below the host)."""
    out = {}
    seen = set()
    for cell in plan:
        for u in cell["args"]["urls"]:
            idx = u.replace(".parquet", ".idx.json")
            if idx in seen:
                continue
            seen.add(idx)
            n = wire_size(idx)
            if n is not None:
                out[idx.split("data.source.coop")[1]] = n
    return out


class Harness(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, plan, out: Path):
        self.plan = plan
        self.served = 0
        self.results: list[dict] = []
        self.out = out.open("a")
        self.last = time.monotonic()
        self.lock = threading.Lock()
        super().__init__(addr, Handler)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(REPO), **kw)

    def log_message(self, *a):  # quiet
        pass

    def _json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.send_header("cache-control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        srv = self.server
        if self.path.startswith("/run") or self.path == "/":
            self.path = "/tools/rails/experiments/harness.html"
            return super().do_GET()
        if self.path == "/assign":
            with srv.lock:
                if srv.served >= len(srv.plan):
                    return self._json(dict(done=True))
                run = srv.plan[srv.served]
                srv.served += 1
                srv.last = time.monotonic()
            return self._json(dict(done=False, run=run))
        return super().do_GET()

    def do_POST(self):
        srv = self.server
        n = int(self.headers.get("content-length", 0))
        rec = json.loads(self.rfile.read(n) or b"{}")
        with srv.lock:
            srv.results.append(rec)
            srv.out.write(json.dumps(rec) + "\n")
            srv.out.flush()
            srv.last = time.monotonic()
            done, total = len(srv.results), len(srv.plan)
        cold = rec.get("cold") or {}
        warm = rec.get("warm") or {}
        print(f"  [{done}/{total}] {rec.get('variant'):<4} {rec.get('tile')} "
              f"{rec.get('shape'):<9} rep{rec.get('rep')}  "
              f"cold {cold.get('ms', '-')} ms / {len(cold.get('requests', []))} req  "
              f"warm {warm.get('ms', '-')} ms / {len(warm.get('requests', []))} req  "
              f"rows {cold.get('rows', '-')}"
              + (f"  ERROR {rec['error'].splitlines()[0]}" if rec.get("error") else ""),
              flush=True)
        self._json(dict(ok=True))


CHROME_CANDIDATES = (
    Path.home() / "Library/Caches/ms-playwright",
    Path.home() / ".cache/ms-playwright",
)


def find_chrome() -> str:
    for root in CHROME_CANDIDATES:
        if not root.exists():
            continue
        hits = sorted(root.glob("chromium_headless_shell-*/*/chrome-headless-shell"))
        if hits:
            return str(hits[-1])
    raise SystemExit("no Playwright chrome-headless-shell found")


def sequential_depth(reqs: list[dict]) -> int:
    """The longest chain of requests that had to happen one after another --
    each starting only after the previous one finished. This is the quantity
    docs/query-performance.md found dominates wall time on this path."""
    order = sorted(reqs, key=lambda r: (r["t0"], r["t1"]))
    best = 0
    depth = [1] * len(order)
    for i, r in enumerate(order):
        for j in range(i):
            if order[j]["t1"] <= r["t0"]:
                depth[i] = max(depth[i], depth[j] + 1)
        best = max(best, depth[i])
    return best


def cell_bytes(reqs: list[dict], wire: dict[str, int]) -> int:
    """Bytes on the wire for one search. A request whose response was
    content-encoded carries no Content-Length, so the harness recorded its
    DECODED length; `wire` substitutes the measured compressed size for the
    objects that happens to (the sidecars)."""
    total = 0
    for q in reqs:
        if q.get("decoded"):
            key = q["url"].split("?")[0]
            total += wire.get(key, q["bytes"])
        else:
            total += q["bytes"]
    return total


def summarise(records: list[dict], wire: dict[str, int] | None = None) -> dict:
    wire = wire or {}
    cells: dict[tuple, list[dict]] = {}
    for r in records:
        if r.get("error"):
            continue
        cells.setdefault((r["variant"], r["shape"], r["tile"]), []).append(r)
    rows = {}
    for (variant, shape, tile), rs in sorted(cells.items()):
        entry = {}
        for phase in ("cold", "warm"):
            ms = [r[phase]["ms"] for r in rs if r.get(phase)]
            req = [len(r[phase]["requests"]) for r in rs if r.get(phase)]
            byt = [cell_bytes(r[phase]["requests"], wire)
                   for r in rs if r.get(phase)]
            dep = [sequential_depth(r[phase]["requests"]) for r in rs if r.get(phase)]
            if not ms:
                continue
            entry[phase] = dict(
                n=len(ms), ms=round(statistics.median(ms)),
                ms_lo=round(min(ms)), ms_hi=round(max(ms)),
                requests=round(statistics.median(req)),
                kib=round(statistics.median(byt) / 1024, 1),
                depth=round(statistics.median(dep)))
        entry["rows"] = sorted({r["cold"]["rows"] for r in rs if r.get("cold")})
        entry["ids_hash"] = sorted({hash(tuple(r["cold"]["ids"])) for r in rs
                                    if r.get("cold")})
        rows[f"{variant}|{shape}|{tile}"] = entry
    # Across the three tiles, per (variant, shape).
    agg = {}
    for key, entry in rows.items():
        variant, shape, _ = key.split("|")
        a = agg.setdefault(f"{variant}|{shape}", {"cold": [], "warm": []})
        for phase in ("cold", "warm"):
            if phase in entry:
                a[phase].append(entry[phase])
    out = {}
    for key, a in agg.items():
        out[key] = {}
        for phase in ("cold", "warm"):
            if not a[phase]:
                continue
            out[key][phase] = dict(
                ms=round(statistics.median([e["ms"] for e in a[phase]])),
                ms_lo=min(e["ms_lo"] for e in a[phase]),
                ms_hi=max(e["ms_hi"] for e in a[phase]),
                requests=round(statistics.median([e["requests"] for e in a[phase]])),
                kib=round(statistics.median([e["kib"] for e in a[phase]]), 1),
                depth=round(statistics.median([e["depth"] for e in a[phase]])))
    return dict(per_tile=rows, per_shape=out)


def markdown(summary: dict, variants: list[str], shapes: list[str]) -> str:
    lines = []
    for phase in ("cold", "warm"):
        lines.append(f"\n### {phase}: median wall ms / requests / KiB "
                     f"(3 tiles x reps)\n")
        lines.append("| variant | " + " | ".join(shapes) + " |")
        lines.append("|---|" + "---|" * len(shapes))
        for v in variants:
            cells = []
            for s in shapes:
                e = summary["per_shape"].get(f"{v}|{s}", {}).get(phase)
                cells.append("—" if not e
                             else f"{e['ms']} / {e['requests']} / {e['kib']:,.0f}")
            lines.append(f"| {v} | " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=2018)
    ap.add_argument("--variants", default="V0s,V0,V1,V2,V3,V4,V5,V6,V7")
    ap.add_argument("--shapes", default="1mo,3mo,year,3mo-cc20")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--port", type=int, default=8771)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--stall", type=int, default=180,
                    help="seconds with no report before the browser is restarted")
    ap.add_argument("--summarise", type=Path, default=None,
                    help="reduce an existing JSONL and exit")
    a = ap.parse_args()
    variants = a.variants.split(",")
    shapes = a.shapes.split(",")

    if a.summarise:
        recs = [json.loads(l) for l in a.summarise.read_text().splitlines() if l.strip()]
        wire = sidecar_wire(build_plan(a.year, variants, 1, shapes))
        s = summarise(recs, wire)
        s["sidecar_wire_bytes"] = wire
        print(json.dumps(s["per_shape"], indent=1))
        print(markdown(s, variants, shapes))
        (a.summarise.with_suffix(".summary.json")).write_text(json.dumps(s, indent=1))
        return

    probe = f"{PUBLISHED}/year={a.year}/items.parquet"
    before = rtt(probe)
    print(f"RTT before: median {before['median']} s "
          f"({before['lo']}-{before['hi']}); samples {before['samples']}"
          + (f"; errors {before['errors']}" if before.get("errors") else ""))

    print(f"planning {len(variants)} variant(s) x {len(TILES)} tiles x "
          f"{len(shapes)} shapes x {a.reps} reps …", flush=True)
    plan = build_plan(a.year, variants, a.reps, shapes)
    wire = sidecar_wire(plan)
    print(f"{len(plan)} cells; sidecars on the wire: "
          + (", ".join(f"{k.split('/')[-2]}/{k.split('/')[-1]} {v:,}B"
                       for k, v in wire.items()) or "none"))

    srv = Harness(("127.0.0.1", a.port), plan, a.out)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    chrome = find_chrome()
    profile = Path(f"/tmp/hz-profile-{os.getpid()}")
    proc = None

    def launch(index: int):
        shutil.rmtree(profile, ignore_errors=True)
        profile.mkdir(parents=True)
        return subprocess.Popen(
            [chrome, "--disable-gpu", "--no-first-run", "--no-default-browser-check",
             "--disk-cache-size=1", "--media-cache-size=1",
             f"--user-data-dir={profile}",
             f"http://127.0.0.1:{a.port}/run?i={index}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print(f"chrome: {chrome}", flush=True)
    proc = launch(0)
    t_start = time.monotonic()
    try:
        while True:
            time.sleep(5)
            with srv.lock:
                done, served, last = len(srv.results), srv.served, srv.last
            if done >= len(plan):
                break
            if time.monotonic() - last > a.stall:
                print(f"  stalled {a.stall}s after {done} cell(s); "
                      f"restarting the browser", flush=True)
                proc.kill()
                with srv.lock:
                    srv.served = done      # re-serve the cell that never reported
                    srv.last = time.monotonic()
                proc = launch(done)
    finally:
        if proc:
            proc.kill()
        shutil.rmtree(profile, ignore_errors=True)

    print(f"done in {(time.monotonic() - t_start) / 60:,.1f} min", flush=True)
    after = rtt(probe)
    print(f"RTT after: median {after['median']} s ({after['lo']}-{after['hi']})"
          + (f"; errors {after['errors']}" if after.get("errors") else ""))

    s = summarise(srv.results, wire)
    s["rtt"] = dict(before=before, after=after)
    s["sidecar_wire_bytes"] = wire
    a.out.with_suffix(".summary.json").write_text(json.dumps(s, indent=1))
    print(markdown(s, variants, shapes))


if __name__ == "__main__":
    main()
