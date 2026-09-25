#!/usr/bin/env python3
"""Measure the committed scene-search client against the candidate one, on the
published layout and on V7, with and without a sidecar.

    python3 tools/rails/experiments/measure_search.py --year 2024 \
        --arms prod,prod-AB,foot-old,foot-AB,foot-AB-nosc,v7-old,v7-AB,v7-AB-nosc \
        --reps 5 --out /tmp/search-2024.jsonl

Sections A-C of docs/c1-search-speed-brief.md. measure_layout.py compares
*layouts* with one client; this compares *clients* — the file at git HEAD
against the working tree's — over the same objects, in one interleaved sweep.

The arms (`--arms`), all on one year:

  prod          HEAD client, published part, its published sidecar.
                Today's production number.
  prod-AB       candidate client, same part and sidecar.
  foot-old      HEAD client, published part, sidecar 404'd.  The old footer
                path: probe, 8-byte tail, footer.
  foot-AB       candidate client, published part, sidecar 404'd, probe kept.
  foot-AB-nosc  candidate client, published part, `sidecars: false`.
  v7-old        HEAD client, V7 prefix part (which has no sidecar).
  v7-AB         candidate client, V7 part, probe kept.
  v7-AB-nosc    candidate client, V7 part, `sidecars: false`.

"Sidecar 404'd" is the harness rewriting `<stem>.idx.json` to a key the bucket
really does not hold, so the probe costs a real round trip on the real path.
That models "this collection publishes no sidecar" without copying a 5.4 GB
part to a sidecar-free prefix.

The metadata phase is derived, not instrumented: the warm phase is exactly the
column-chunk GETs, so the cold phase's first (cold_n - warm_n) requests are the
metadata phase, and their count, bytes and span are section B's numbers.
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
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
from layout_parts import octant_of, prefix_of  # noqa: E402
from measure_layout import (BASE, EXPERIMENT, PUBLISHED, SHAPES,  # noqa: E402
                            TILE_COLUMN, TILES, cell_bytes, find_chrome, head,
                            rtt, sequential_depth, sidecar_wire, wire_size)

CANDIDATE = "/apps/explorer/search.js"
BASELINE = "/baseline/search.js"

# arm -> (client module, layout, sidecar reachable, `sidecars` argument)
ARMS = {
    "prod":         (BASELINE,  "V0", True,  None),
    "prod-AB":      (CANDIDATE, "V0", True,  True),
    "foot-old":     (BASELINE,  "V0", False, None),
    "foot-AB":      (CANDIDATE, "V0", False, True),
    "foot-AB-nosc": (CANDIDATE, "V0", False, False),
    "v7-old":       (BASELINE,  "V7", False, None),
    "v7-AB":        (CANDIDATE, "V7", False, True),
    "v7-AB-nosc":   (CANDIDATE, "V7", False, False),
    # The other collection, which publishes no sidecar for any part and whose
    # zone-octant parts are not tile-major enough to admit one row group: a
    # tile's scenes sit in 34 of 118 groups, so a search is 238 chunk reads of
    # one object. That is the worst case the cache lock can produce.
    "l2a-old":      (BASELINE,  "L2A", False, None),
    "l2a-AB":       (CANDIDATE, "L2A", False, False),
}
# The tile column differs per collection, as apps/explorer/app.js has it.
L2A_TILE_COLUMN = "s2:mgrs_tile"


def part_url(layout: str, year: int, tile: str) -> str:
    if layout == "V0":
        return f"{PUBLISHED}/year={year}/items.parquet"
    if layout == "L2A":
        return f"{BASE}/sentinel-2-l2a/year={year}/{octant_of(tile)}.parquet"
    return f"{EXPERIMENT}/{year}/V7/t={prefix_of(tile)}.parquet"


def build_plan(year: int, arms: list[str], reps: int, shapes: list[str]) -> list[dict]:
    """One cell per (repetition, tile, shape, arm), in that nesting, so
    consecutive cells compare arms under the same network weather."""
    exists: dict[str, int | None] = {}
    plan, index = [], 0
    for rep in range(1, reps + 1):
        for tile in TILES:
            for shape in shapes:
                sh = SHAPES[shape]
                d0, d1 = sh["window"](year)
                for arm in arms:
                    client, layout, sidecar, flag = ARMS[arm]
                    url = part_url(layout, year, tile)
                    if url not in exists:
                        exists[url] = head(url)
                    if exists[url] is None:
                        raise SystemExit(f"{arm} {tile}: no object at {url}")
                    args = dict(urls=[url], tile=tile, d0=d0, d1=d1,
                                cc=sh["cc"], cov=sh["cov"],
                                tileColumn=(L2A_TILE_COLUMN if layout == "L2A"
                                            else TILE_COLUMN))
                    if flag is not None:
                        args["sidecars"] = flag
                    plan.append(dict(index=index, rep=rep, tile=tile,
                                     shape=shape, arm=arm, year=year,
                                     client=client, blockSidecar=not sidecar,
                                     args=args))
                    index += 1
    for cell in plan:
        cell["total"] = len(plan)
    return plan


class Harness(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, plan, out: Path, baseline: Path):
        self.plan = plan
        self.served = 0
        self.results: list[dict] = []
        self.out = out.open("a")
        self.baseline = baseline
        self.last = time.monotonic()
        self.lock = threading.Lock()
        super().__init__(addr, Handler)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(HERE.parent.parent.parent), **kw)

    def log_message(self, *a):
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
            self.path = "/tools/rails/experiments/search_harness.html"
            return super().do_GET()
        if self.path.startswith(BASELINE):
            body = srv.baseline.read_bytes()
            self.send_response(200)
            self.send_header("content-type", "text/javascript")
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            return self.wfile.write(body)
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
        print(f"  [{done}/{total}] {rec.get('arm'):<13} {rec.get('tile')} "
              f"{rec.get('shape'):<9} rep{rec.get('rep')}  "
              f"cold {cold.get('ms', '-')} ms / {len(cold.get('requests', []))} req  "
              f"warm {warm.get('ms', '-')} ms / {len(warm.get('requests', []))} req  "
              f"rows {cold.get('rows', '-')}"
              + (f"  ERROR {rec['error'].splitlines()[0]}" if rec.get("error") else ""),
              flush=True)
        self._json(dict(ok=True))


def meta_phase(cold: dict, warm: dict, wire: dict) -> dict | None:
    """Section B's numbers. The warm phase is exactly the column-chunk GETs, so
    the cold phase's leading (cold_n - warm_n) requests are the metadata
    phase."""
    cn, wn = len(cold["requests"]), len(warm["requests"])
    if cn <= wn:
        return None
    order = sorted(cold["requests"], key=lambda r: r["t0"])
    reqs = order[: cn - wn]
    # The main-thread gap between the last metadata request finishing and the
    # first chunk request starting: hyparquet parsing the footer's thrift. On a
    # 6.7 MB footer it is the largest single term in a cold sidecar-free search.
    gap = round(order[cn - wn]["t0"] - max(r["t1"] for r in reqs)) if cn > wn else 0
    return dict(requests=len(reqs), bytes=cell_bytes(reqs, wire),
                ms=round(max(r["t1"] for r in reqs) - min(r["t0"] for r in reqs)),
                parse=max(0, gap))


def summarise(records: list[dict], wire: dict[str, int] | None = None) -> dict:
    wire = wire or {}
    cells: dict[tuple, list[dict]] = {}
    for r in records:
        if r.get("error"):
            continue
        cells.setdefault((r["arm"], r["shape"], r["tile"]), []).append(r)
    rows = {}
    for (arm, shape, tile), rs in sorted(cells.items()):
        entry = {}
        for phase in ("cold", "warm"):
            ms = [r[phase]["ms"] for r in rs if r.get(phase)]
            if not ms:
                continue
            entry[phase] = dict(
                n=len(ms), ms=round(statistics.median(ms)),
                ms_lo=round(min(ms)), ms_hi=round(max(ms)),
                requests=round(statistics.median(
                    [len(r[phase]["requests"]) for r in rs if r.get(phase)])),
                kib=round(statistics.median(
                    [cell_bytes(r[phase]["requests"], wire)
                     for r in rs if r.get(phase)]) / 1024, 1),
                depth=round(statistics.median(
                    [sequential_depth(r[phase]["requests"]) for r in rs if r.get(phase)])))
        mp = [meta_phase(r["cold"], r["warm"], wire) for r in rs
              if r.get("cold") and r.get("warm")]
        mp = [m for m in mp if m]
        if mp:
            entry["meta"] = dict(
                requests=round(statistics.median([m["requests"] for m in mp])),
                kib=round(statistics.median([m["bytes"] for m in mp]) / 1024, 1),
                ms=round(statistics.median([m["ms"] for m in mp])),
                ms_lo=min(m["ms"] for m in mp), ms_hi=max(m["ms"] for m in mp),
                parse=round(statistics.median([m["parse"] for m in mp])))
        entry["rows"] = sorted({r["cold"]["rows"] for r in rs if r.get("cold")})
        entry["ids"] = sorted({",".join(r["cold"]["ids"]) for r in rs if r.get("cold")})
        rows[f"{arm}|{shape}|{tile}"] = entry
    agg: dict[str, dict] = {}
    for key, entry in rows.items():
        arm, shape, _ = key.split("|")
        a = agg.setdefault(f"{arm}|{shape}", {"cold": [], "warm": [], "meta": []})
        for k in ("cold", "warm", "meta"):
            if k in entry:
                a[k].append(entry[k])
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
        if a["meta"]:
            out[key]["meta"] = dict(
                requests=round(statistics.median([e["requests"] for e in a["meta"]])),
                kib=round(statistics.median([e["kib"] for e in a["meta"]]), 1),
                ms=round(statistics.median([e["ms"] for e in a["meta"]])),
                ms_lo=min(e["ms_lo"] for e in a["meta"]),
                ms_hi=max(e["ms_hi"] for e in a["meta"]),
                parse=round(statistics.median([e["parse"] for e in a["meta"]])))
    # Row identity: every arm must return the same ids for the same cell.
    per_cell: dict[tuple, set] = {}
    for key, entry in rows.items():
        arm, shape, tile = key.split("|")
        per_cell.setdefault((shape, tile), set()).update(entry["ids"])
    mismatches = {f"{s}|{t}": len(v) for (s, t), v in per_cell.items() if len(v) != 1}
    return dict(per_tile=rows, per_shape=out, id_mismatches=mismatches)


def markdown(summary: dict, arms: list[str], shapes: list[str]) -> str:
    lines = []
    for phase in ("cold", "warm"):
        lines.append(f"\n### {phase}: median wall ms / requests / KiB\n")
        lines.append("| arm | " + " | ".join(shapes) + " |")
        lines.append("|---|" + "---|" * len(shapes))
        for v in arms:
            cells = []
            for s in shapes:
                e = summary["per_shape"].get(f"{v}|{s}", {}).get(phase)
                cells.append("—" if not e
                             else f"{e['ms']:,} / {e['requests']} / {e['kib']:,.0f}")
            lines.append(f"| {v} | " + " | ".join(cells) + " |")
    lines.append("\n### metadata phase (cold): requests / KiB / ms / footer-parse ms\n")
    lines.append("| arm | " + " | ".join(shapes) + " |")
    lines.append("|---|" + "---|" * len(shapes))
    for v in arms:
        cells = []
        for s in shapes:
            e = summary["per_shape"].get(f"{v}|{s}", {}).get("meta")
            cells.append("—" if not e
                         else f"{e['requests']} / {e['kib']:,.0f} / {e['ms']:,} / {e['parse']:,}")
        lines.append(f"| {v} | " + " | ".join(cells) + " |")
    lines.append(f"\nid mismatches across arms: {summary['id_mismatches'] or 'none'}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--arms", default=",".join(ARMS))
    ap.add_argument("--shapes", default="1mo,3mo,year,3mo-cc20")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--port", type=int, default=8781)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--stall", type=int, default=180)
    ap.add_argument("--summarise", type=Path, default=None)
    a = ap.parse_args()
    arms = a.arms.split(",")
    shapes = a.shapes.split(",")
    for arm in arms:
        if arm not in ARMS:
            raise SystemExit(f"unknown arm {arm}; known: {', '.join(ARMS)}")

    if a.summarise:
        recs = [json.loads(l) for l in a.summarise.read_text().splitlines() if l.strip()]
        # The sidecar URLs come from the records, not from a fresh plan: a
        # summary must reduce even when the CDN is answering 520s.
        seen = {u.replace(".parquet", ".idx.json")
                for r in recs for u in r["args"]["urls"]}
        wire = {}
        for u in sorted(seen):
            n = wire_size(u)
            if n is not None:
                wire[u.split("data.source.coop")[1]] = n
        s = summarise(recs, wire)
        s["sidecar_wire_bytes"] = wire
        a.summarise.with_suffix(".summary.json").write_text(json.dumps(s, indent=1))
        print(markdown(s, arms, shapes))
        return

    # The baseline client: the committed file, served untouched from a temp copy.
    repo = HERE.parent.parent.parent
    baseline = Path(f"/tmp/baseline-search-{os.getpid()}.js")
    baseline.write_bytes(subprocess.run(
        ["git", "show", "HEAD:apps/explorer/search.js"], cwd=repo,
        capture_output=True, check=True).stdout)
    print(f"baseline: git HEAD:apps/explorer/search.js ({baseline.stat().st_size:,} B)")

    probe = f"{PUBLISHED}/year={a.year}/items.parquet"
    before = rtt(probe)
    print(f"RTT before: median {before['median']} s "
          f"({before['lo']}-{before['hi']}); samples {before['samples']}")

    plan = build_plan(a.year, arms, a.reps, shapes)
    wire = sidecar_wire(plan)
    print(f"{len(plan)} cells over {len(arms)} arm(s); sidecars on the wire: "
          + (", ".join(f"{k.split('/')[-2]}/{k.split('/')[-1]} {v:,}B"
                       for k, v in wire.items()) or "none"), flush=True)

    srv = Harness(("127.0.0.1", a.port), plan, a.out, baseline)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    chrome = find_chrome()
    profile = Path(f"/tmp/hs-profile-{os.getpid()}")

    def launch(index: int):
        shutil.rmtree(profile, ignore_errors=True)
        profile.mkdir(parents=True)
        return subprocess.Popen(
            [chrome, "--disable-gpu", "--no-first-run", "--no-default-browser-check",
             "--disk-cache-size=1", "--media-cache-size=1",
             f"--user-data-dir={profile}",
             f"http://127.0.0.1:{a.port}/run?i={index}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    proc = launch(0)
    t_start = time.monotonic()
    try:
        while True:
            time.sleep(5)
            with srv.lock:
                done, last = len(srv.results), srv.last
            if done >= len(plan):
                break
            if time.monotonic() - last > a.stall:
                print(f"  stalled {a.stall}s after {done} cell(s); restarting",
                      flush=True)
                proc.kill()
                with srv.lock:
                    srv.served = done
                    srv.last = time.monotonic()
                proc = launch(done)
    finally:
        proc.kill()
        shutil.rmtree(profile, ignore_errors=True)
        baseline.unlink(missing_ok=True)

    print(f"done in {(time.monotonic() - t_start) / 60:,.1f} min", flush=True)
    after = rtt(probe)
    print(f"RTT after: median {after['median']} s ({after['lo']}-{after['hi']})")

    s = summarise(srv.results, wire)
    s["rtt"] = dict(before=before, after=after)
    s["sidecar_wire_bytes"] = wire
    a.out.with_suffix(".summary.json").write_text(json.dumps(s, indent=1))
    print(markdown(s, arms, shapes))


if __name__ == "__main__":
    main()
