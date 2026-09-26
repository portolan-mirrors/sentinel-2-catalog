#!/usr/bin/env python3
"""Run the real explorer headless and check that a scene search still returns
the same rows, on both collections, with the committed client and the candidate.

    python3 tools/rails/experiments/check_app.py

This is the gate for a change to apps/explorer/search.js. measure_search.py
drives `sceneSearch` directly; this loads `apps/explorer/index.html` itself in
chrome-headless-shell, lets it build its map, its stats and its month picker,
and then fires the same map click a user's mouse fires, so the whole page --
partUrls, the HEAD probes, warmPart, runQuery, the result cards -- runs.

Two served variants of the app, from the same repository files:

  candidate/  apps/explorer/* exactly as the working tree has it
  baseline/   the same, with `search.js` replaced by git HEAD's

`app.js` has `window.__map =` and `window.__hit =` inserted into its map
constructor and its hit index in both, because both are module-local consts and
a headless driver has no mouse. Nothing else is rewritten, and the two
insertions are identical in both variants.

Exits non-zero if the candidate's rows differ from DuckDB's for any case, if
the candidate returns nothing, or if the committed client returns different
rows (a case the committed client cannot finish at all is reported, not
excused: it is checked against DuckDB instead).
"""
from __future__ import annotations

import argparse
import http.server
import json
import os
import re
import shutil
import socketserver
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
sys.path.insert(0, str(HERE))
from measure_layout import find_chrome  # noqa: E402

# (label, collection, lng, lat, window). The tile is whatever the page's own
# hit index names under that point -- the driver reports it and the check asks
# DuckDB about that tile, so no case depends on guessing an MGRS id.
#
# The two collections need different windows because their parts differ in
# kind, not just in name. Collection 1 is one tile-major file per year, so a
# 2024 search is one admitted row group. sentinel-2-l2a's post-2021 years are
# zone octants whose `s2:mgrs_tile` statistics overlap across nearly every row
# group -- 43 to 70 of 70 admitted for one tile, 300-490 range reads, 14 MB --
# so a search there takes minutes in the page with EITHER client and is no use
# as a gate. Its pre-split years are single files: 2016 is 4.4 MB in one row
# group, 7 reads, and exercises exactly the same client path.
CASES = [
    ("c1-nl", "sentinel-2-c1-l2a", 5.5, 52.0, ("2024-06-01", "2024-06-30")),
    ("c1-br", "sentinel-2-c1-l2a", -45.5, -22.5, ("2024-06-01", "2024-06-30")),
    ("l2a-se", "sentinel-2-l2a", 15.0, 59.0, ("2016-11-01", "2016-12-31")),
    ("l2a-pl", "sentinel-2-l2a", 19.0, 52.0, ("2016-11-01", "2016-12-31")),
]

DRIVER = """<!doctype html><meta charset=utf-8><title>app check</title>
<pre id=log></pre><script type=module>
// Drive one (variant, collection, point) case: load the real page in an
// iframe, set the window, fire the map click the mouse would fire, read the
// ids off the result cards.
const log = document.getElementById("log");
// Progress is echoed to the server as well as the page: a headless shell has
// no console anyone reads, and a case that hangs should say where.
const say = (m) => { log.textContent += `\\n${m}`;
  fetch("/log", { method: "POST", body: String(m) }).catch(() => {}); };
window.addEventListener("error", (e) => say(`window error: ${e.message}`));
window.addEventListener("unhandledrejection", (e) => say(`rejection: ${e.reason}`));
const plan = await fetch("/assign").then((r) => r.json());
if (plan.done) { say("done"); document.title = "CHECK-DONE"; }
else {
  const c = plan.case;
  say(`${c.variant} ${c.collection} ${c.lng},${c.lat}`);
  const out = { ...c, ids: [], tile: null, plan: null, error: null, ms: null };
  const t0 = performance.now();
  try {
    const frame = document.createElement("iframe");
    frame.width = 1400; frame.height = 900;
    frame.src = `/appvar/${c.variant}/index.html?collection=${c.collection}`;
    document.body.append(frame);
    const w = await new Promise((ok, no) => {
      frame.addEventListener("load", () => ok(frame.contentWindow));
      setTimeout(() => no(new Error("the app frame never loaded")), 60000);
    });
    say("  frame loaded");
    w.addEventListener("error", (e) => say(`  app error: ${e.message} `
      + `(${e.filename}:${e.lineno})`));
    w.addEventListener("unhandledrejection", (e) => say(`  app rejection: ${e.reason}`));
    const $ = (id) => w.document.getElementById(id);
    const until = async (what, pred, ms = 90000) => {
      const t = performance.now();
      while (performance.now() - t < ms) {
        try { if (pred()) return true; } catch (e) { /* still building */ }
        await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`timed out waiting for ${what}`);
    };
    // The page is ready enough when its map exists; the window comes from
    // this driver, not from the month picker, so the case is deterministic.
    await until("the map", () => w.__map);
    // Wait for the hit index itself, not for a click to happen to land. The
    // app translates a click into an MGRS tile through `hitIndex`, which fills
    // from the MGRS tileset's one z0 tile as deck.gl decodes it, and a click
    // before that names no tile and is ignored. Do NOT nudge the camera to
    // hurry it along: the index keeps only the first zoom it sees and the
    // archive holds one z0 tile, so a jumpTo races the very thing being
    // waited for -- which is what made this check fail for one variant and
    // not the other, on a difference that had nothing to do with search.js.
    await until("the MGRS hit index", () => w.__hit?.polys.length > 0, 240000);
    say(`  hit index: ${w.__hit.polys.length} polygons`);
    // Pin the window and both filters, the filters through the app's own
    // slider listener, so the query is the one ground_truth.py checks: every
    // cloud, no coverage floor.
    $("date0").value = c.d0;
    $("date1").value = c.d1;
    for (const [id, v] of [["maxcloud", "100"], ["mincoverage", "0"]]) {
      $(id).value = v;
      $(id).dispatchEvent(new w.Event("input", { bubbles: true }));
    }
    // The app ignores a click that names no tile, so fire until it takes —
    // and the signal that it took is the hint the click handler writes
    // ("Tile 31UFT."), NOT the run button. runQuery disables that button in
    // its own first synchronous statement and only re-enables it when the
    // search is over, so waiting on it means firing another click every
    // 250 ms through the whole search, and a dozen overlapping searches is
    // not what this is measuring.
    const tileHint = () => $("query").querySelector(".hint")?.textContent || "";
    let fired = 0;
    await until("a tile under the click", () => {
      if (/\\bTile \\d/.test(tileHint())) return true;
      fired += 1;
      if (fired % 40 === 0) say(`  still no tile after ${fired} click(s)`);
      w.__map.fire("click", { lngLat: { lng: c.lng, lat: c.lat,
        wrap: () => ({ lng: c.lng, lat: c.lat }) } });
      return false;
    }, 120000);
    say(`  clicked after ${fired} attempt(s): ${tileHint().trim()}`);
    // runQuery writes the plan into #sql when it is done, and the cards into
    // #results; a search that found nothing writes a .hint instead.
    try {
      await until("the search to finish", () =>
        ($("sql").textContent || "").includes("range-read plan")
        && ($("results").querySelector("b, .hint")), 300000);
    } catch (e) {
      // Say what the page said, so a failure is diagnosable from the log.
      say(`  status: ${($("status").textContent || "").slice(0, 300)}`);
      say(`  results: ${($("results").textContent || "").slice(0, 300)}`);
      say(`  sql: ${($("sql").textContent || "").slice(0, 200)}`);
      throw e;
    }
    out.tile = ($("query").querySelector(".hint")?.textContent || "").trim();
    out.plan = $("sql").textContent;
    out.ids = (w.S2 && w.S2.viewIds ? w.S2.viewIds()
      : [...$("results").querySelectorAll(".card b, b")].map((b) => b.textContent.trim()))
      .filter((t) => /^S2[A-Z]/.test(t)).slice(0, 30);
    say(`  ${out.ids.length} row(s): ${out.ids.slice(0, 3).join(", ")}`);
  } catch (e) {
    out.error = `${e}\\n${e.stack ?? ""}`;
    say(`  ERROR ${e}`);
  }
  out.ms = Math.round(performance.now() - t0);
  await fetch("/report", { method: "POST",
    headers: { "content-type": "application/json" }, body: JSON.stringify(out) });
  // The server launches a fresh browser for the next case: one page per
  // browser, because the explorer is a heavy page (deck.gl, a multi-MB footer,
  // a PMTiles index) and eight of them in one process started failing fetches.
  document.title = "CASE-DONE";
}
</script>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--out", type=Path, default=None)
    a = ap.parse_args()

    baseline = subprocess.run(["git", "show", "HEAD:apps/explorer/search.js"],
                              cwd=REPO, capture_output=True, check=True).stdout
    candidate = (REPO / "apps/explorer/search.js").read_bytes()
    # One token added, the same in both variants: the maplibre Map is a
    # module-local const and the driver has no mouse.
    src = (REPO / "apps/explorer/app.js").read_text()
    for needle, with_ in (("const map = new maplibregl.Map({",
                           "const map = window.__map = new maplibregl.Map({"),
                          ("const hitIndex = {",
                           "const hitIndex = window.__hit = {")):
        if needle not in src:
            raise SystemExit(f"app.js no longer spells {needle!r}; this driver "
                             f"needs it to reach the map and the hit index — "
                             f"update check_app.py")
        src = src.replace(needle, with_, 1)
    app_js = src.encode()
    served = {"baseline": baseline, "candidate": candidate}
    # A module that throws while it evaluates throws before the driver can
    # attach a listener to the frame, so the reporter goes in the page.
    reporter = ("<script>"
                "const p=(m)=>fetch('/log',{method:'POST',body:'app: '+m});"
                "addEventListener('error',e=>p(e.message+' @'+e.filename+':'+e.lineno));"
                "addEventListener('unhandledrejection',e=>p('rejection '+e.reason));"
                "</script>\n")
    index_html = (REPO / "apps/explorer/index.html").read_text().replace(
        "</head>", reporter + "</head>", 1).encode()

    cases = []
    for label, collection, lng, lat, window in CASES:
        for variant in ("baseline", "candidate"):
            cases.append(dict(label=label, collection=collection, lng=lng,
                              lat=lat, variant=variant,
                              d0=window[0], d1=window[1]))
    for i, c in enumerate(cases):
        c["index"] = i

    results: list[dict] = []
    lock = threading.Lock()
    state = {"served": 0, "reported": -1}
    CASE_KEYS = ("label", "collection", "lng", "lat", "variant", "d0", "d1")

    def rec_case(rec: dict) -> dict:
        return {k: rec[k] for k in CASE_KEYS}

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kw):
            super().__init__(*args, directory=str(REPO), **kw)

        def log_message(self, *_):
            pass

        def _send(self, body: bytes, ct: str):
            self.send_response(200)
            self.send_header("content-type", ct)
            self.send_header("content-length", str(len(body)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path in ("/", "/drive"):
                return self._send(DRIVER.encode(), "text/html")
            if path == "/assign":
                with lock:
                    if state["served"] >= len(cases):
                        return self._send(json.dumps(dict(done=True)).encode(),
                                          "application/json")
                    c = cases[state["served"]]
                    state["served"] += 1
                return self._send(json.dumps(dict(done=False, case=c)).encode(),
                                  "application/json")
            if path.startswith("/appvar/"):
                _, _, variant, name = path.split("/", 3)
                if name == "search.js":
                    return self._send(served[variant], "text/javascript")
                if name == "app.js":
                    return self._send(app_js, "text/javascript")
                if name == "index.html":
                    return self._send(index_html, "text/html")
                self.path = "/apps/explorer/" + name
                return super().do_GET()
            return super().do_GET()

        def do_POST(self):
            n = int(self.headers.get("content-length", 0))
            raw = self.rfile.read(n)
            if self.path == "/log":
                print(f"    . {raw.decode('utf-8', 'replace')}", flush=True)
                return self._send(b"{}", "application/json")
            rec = json.loads(raw or b"{}")
            with lock:
                results.append(rec)
                done = len(results)
                # The MGRS hit index arrives over the network; a case that
                # never got a tile gets one more turn before it is a failure.
                if rec.get("error") and not rec.get("retry"):
                    again = dict(rec_case(rec), retry=True, index=len(cases))
                    cases.append(again)
            print(f"  [{done}/{len(cases)}] {rec['variant']:<9} "
                  f"{rec['collection']:<18} {rec['label']:<8} "
              f"{(rec.get('tile') or '?')[:20]:<20} "
                  f"{len(rec.get('ids') or [])} row(s) in {rec.get('ms')} ms"
                  + (f"  ERROR {rec['error'].splitlines()[0]}" if rec.get("error") else ""),
                  flush=True)
            self._send(b"{}", "application/json")

    class Srv(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
        allow_reuse_address = True

    srv = Srv(("127.0.0.1", a.port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    profile = Path(f"/tmp/checkapp-{os.getpid()}")
    shutil.rmtree(profile, ignore_errors=True)

    def launch(i):
        return subprocess.Popen(
            # NOT --disable-gpu: maplibre needs a WebGL context, and without one
            # `new maplibregl.Map` throws and the page never builds. SwiftShader
            # is the software rasteriser chrome-headless-shell ships.
            [find_chrome(), "--no-first-run", "--use-gl=angle",
             "--use-angle=swiftshader", "--enable-unsafe-swiftshader",
             "--no-default-browser-check", "--disk-cache-size=1",
             "--window-size=1400,900", f"--user-data-dir={profile}",
             f"http://127.0.0.1:{a.port}/drive?i={i}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    t0 = time.monotonic()
    proc = None
    try:
        while time.monotonic() - t0 < 3600:
            with lock:
                done, total = len(results), len(cases)
            if done >= total:
                break
            if proc is None or proc.poll() is not None or done > state["reported"]:
                if proc is not None:
                    proc.kill()
                    proc.wait()
                with lock:
                    state["reported"] = done
                shutil.rmtree(profile, ignore_errors=True)
                proc = launch(done)
            time.sleep(3)
    finally:
        if proc is not None:
            proc.kill()
        shutil.rmtree(profile, ignore_errors=True)

    if a.out:
        a.out.write_text(json.dumps(results, indent=1))

    # The last report for a (variant, label) wins, so a retry that succeeded
    # replaces the attempt that did not.
    by = {(r["variant"], r["label"]): r for r in results}
    # An independent answer, so a case is checkable even where the committed
    # client cannot finish it -- which is the case the change is worth most.
    import duckdb
    sys.path.insert(0, str(HERE))
    from ground_truth import truth
    con = duckdb.connect(config={"memory_limit": "8GB"})
    con.execute("INSTALL httpfs; LOAD httpfs;")
    BUCKET = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog"

    def part_of(collection: str, year: str) -> tuple[str, str]:
        """The one part the app reads for that (collection, year), and its tile
        column. Both are single files for the years these cases use."""
        if collection == "sentinel-2-c1-l2a":
            return f"{BUCKET}/sentinel-2-c1-l2a/year={year}/items.parquet", "_tile"
        return f"{BUCKET}/sentinel-2-l2a/year={year}/items.parquet", "s2:mgrs_tile"

    bad = []
    print()
    for label, collection, lng, lat, window in CASES:
        b = by.get(("baseline", label))
        c = by.get(("candidate", label))
        if not c:
            bad.append(f"{label}: the candidate never reported")
            continue
        got = c.get("ids") or []
        # The tile is the one the page's own hit index named ("Tile 31UFT.").
        m = re.search(r"\b(\d{1,2}[C-X][A-Z]{2})\b", c.get("tile") or "")
        tile = m.group(1) if m else None
        want = (truth(con, *part_of(collection, window[0][:4]), tile,
                      window[0], window[1], 100, 0) if tile else None)
        ok_truth = want is not None and got == want
        base = "failed" if (not b or b.get("error")) else (
            "same" if b.get("ids") == got else "DIFFERENT")
        if not tile:
            bad.append(f"{label}: the page never named a tile")
        elif not got:
            bad.append(f"{label} {tile}: the candidate returned no rows")
        elif not ok_truth:
            bad.append(f"{label} {tile}: the candidate returned {len(got)} "
                       f"row(s), DuckDB says {len(want)}")
        if base == "DIFFERENT":
            bad.append(f"{label} {tile}: the committed client returned "
                       f"different ids ({len(b['ids'])} vs {len(got)})")
        print(f"{'OK  ' if ok_truth and got and base != 'DIFFERENT' else 'FAIL'} "
              f"{collection:<18} {tile or '?':<6} {window[0][:4]}  candidate "
              f"{len(got)} row(s), matches DuckDB: {ok_truth}; "
              f"committed client: {base}")
    if bad:
        print("\n" + "\n".join(bad))
        sys.exit(1)
    print(f"\nall {len(CASES)} case(s) on both collections: the candidate "
          f"client returns exactly DuckDB's rows in the real page, and the "
          f"committed client returns the same rows")


if __name__ == "__main__":
    main()
