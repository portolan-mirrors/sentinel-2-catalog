# Collection 1 item-part layout: eight partitionings against the real client

Measured 2026-09-25 from a laptop in Copenhagen against the live bucket
(`https://data.source.coop/portolan-mirrors/sentinel-2-catalog/`), driving
`apps/explorer/search.js` **unchanged** in headless Chrome. The question of
[the brief](c1-layout-experiment-brief.md): *which partition organisation and
row-group size minimise the wall time of the explorer's scene search with no
sidecar, and can any of them beat today's sidecar-assisted single-file year?*

## Summary

1. **The fastest sidecar-free layout is V7** — one file per MGRS grid-zone
   prefix (`t=31U`), 2,000-row groups. It wins every query shape among the
   sidecar-free layouts on both years measured, cuts per-search bytes to a
   third of today's (61 KiB against 188 KiB warm) and the footer to 1/160th
   (13 KB against 2,044 KB).
2. **It does not beat the published sidecar, and no layout can.** Without a
   sidecar `search.js` resolves a part's metadata in **three sequential
   requests** (a 404 probe for `<stem>.idx.json`, the 8-byte tail, the footer)
   against **one** with it, and on this path a request costs 0.26–0.8 s. V7
   lands 0.45–0.65 s behind V0+sidecar and 0.3–0.8 s ahead of every other
   sidecar-free layout — and its margin over the unpartitioned footer path
   *grows* with the year (+0.3 s on 2018, +0.8 s on 2024).
3. **It does remove the RAILS dependency.** One V7 part folds in 6 s with a
   1.3 GB peak RSS; the largest 2018 part is 6,438 rows, the largest 2024 part
   18,215. Today's single-file year peaked at **83 GB** of RSS in this
   experiment's own build jobs — five times a GitHub runner's 16 GB, which is
   exactly why the fold lives on the cluster.
4. **It costs +1.6 % bytes (2018), +1.1 % (2024) and 864–1,011 objects a
   year** — roughly 9,500 for 2015–2026, against 12 today. No query shape gets
   worse.
5. **The measurement found a bigger lever than any layout.** Chrome serialises
   concurrent range GETs that share a URL, ~260 ms apiece, and `search.js`
   issues eight of them to one part per admitted row group. Giving each range
   its own URL — an extra query parameter S3 ignores, same object, same bytes —
   takes those eight from **2,077 ms to 438 ms** on V7. That is a one-line
   change in `rangeGet`, worth 1.6 s per search, against the 0.6 s the whole
   layout question spans. With that fix in place, bytes are all that is left to
   optimise, and V7 is the layout with the fewest.
   *(Part two below settles this: `cache: "no-store"` buys the same 4–5× with
   no extra CDN cache key, and it is what shipped. Points 2 and 5 of this
   summary are superseded there — with the client fixed, V7 without a sidecar
   is 2.4–2.8× faster than today's published layout with one.)*

## Method

`tools/rails/experiments/` holds everything. Nothing in `tools/s2_build.py`,
`apps/explorer/` or any published path changed.

**Building the variants.** `build_layout.py` takes a published year part
(`/u/cholmes/s2-c1/publish/sentinel-2-c1-l2a/year=YYYY/items.parquet` on
RAILS — nothing was refetched) and, per variant, splits it in **one** DuckDB
scan with `COPY … PARTITION_BY`, then puts every part through the pipeline
`s2_build._sort_and_check` uses: `gpio sort column … _tile,datetime
--geoparquet-version 2.0 --compression zstd --compression-level 18
--row-group-size N`, `gpio check all` on a dotfile, atomic rename. So every
variant is a real GeoParquet 2.0 file with the published schema (63 columns),
the published sort and zstd 18 — the artefact a reader would get if the catalog
were published this way. `build_layout.sbatch` runs one variant per Slurm job
and uploads the parts to the bucket's `_experiments/layout/<year>/<variant>/`
prefix. **No sidecar was ever built**: the whole question is what a layout
costs a client that has none.

**Measuring.** `measure_layout.py` is a local HTTP server plus
`chrome-headless-shell` (Playwright's, the binary the earlier measurement
tasks used). It serves the repository, so the harness page imports the shipped
`/apps/explorer/search.js` as a module and calls `sceneSearch({urls,
tileColumn, tile, d0, d1, cc, cov})` exactly as `apps/explorer/app.js` does.
One page load is one (variant, tile, shape, repetition) cell: a fresh module
registry is what makes it cold. The page then asks the server for the next cell
and navigates, so the plan walks itself and there is no CDP client to keep
alive.

Everything the harness adds sits outside the module:

* a counting wrapper around `globalThis.fetch`, installed before `search.js`
  is imported, recording each request's URL, `Range`, status, `Content-Length`
  and start/end instant. (`performance.getEntriesByType("resource")` is useless
  for bytes here — the bucket sends no `Timing-Allow-Origin`, so `transferSize`
  is 0. It does send `access-control-expose-headers: *`, which is why
  `Content-Length` is readable at all.) A content-encoded response carries no
  `Content-Length`; the sidecar's size on the wire was measured separately
  (`items.idx.json` is 385 KB of JSON, **26 KB** compressed for 2018 and 86 KB
  for 2024).
* a per-run cache-busting query parameter on bucket URLs. S3 ignores an unknown
  query parameter and this prefix is `cf-cache-status: DYNAMIC` regardless, so
  the same object comes back over the same path — but Chrome's disk cache
  cannot serve a repeat. One token per *phase*, shared by every request of that
  phase, so the client's own request pattern (many ranges, one URL) is
  preserved exactly.

**Cold** is a fresh page: the client's metadata cache is empty. **Warm** is a
second identical search in the same page: metadata resolved, chunks re-fetched
over the wire. Warm is the app's real steady state, because `warmPart()`
resolves the window's part metadata on load, before any click.

Cells are ordered so that all nine variants are measured back to back for the
same (repetition, tile, shape): per-request latency on this path drifts 2–3×
over minutes (docs/query-performance.md), and interleaving keeps the drift off
any one variant. Round-trip time to the bucket is measured before and after
every sweep.

**Which parts a client asks for is the variant.** `search.js` fetches metadata
for every URL it is handed, so a partitioning only pays if the app can name the
part that holds a tile before asking for a byte. `layout_parts.py` records that
per variant as `prune`, in the same terms `COLLECTIONS[…].parts(year, tile)` in
`app.js` already has:

* `none` — every part of the year is read (V0–V2: there is one).
* `tile` — the tile id alone names the part, for every query shape: V4 octant,
  V6 UTM zone, V7 grid-zone prefix. One regex on `_tile`; no index, no extra
  input, works on the first click.
* `date` — the search window names the parts (V3 months): one part for a
  one-month search, three for a quarter, **twelve** for a year.
* `both` — V5, octant × month.

`build_plan` HEADs every part URL once and drops a stem with no object, the way
`app.js`'s `partExists` probe does.

**Tiles and shapes.** Three tiles in three UTM zones and both hemispheres, each
with a realistic scene count: `31UFU` (Netherlands, 67 scenes in 2018), `33UUP`
(Poland, 63), `23KKQ` (Brazil, 50). Four shapes, as the app issues them
(`ORDER BY cloud, id LIMIT 30`): one month (June), three months (April–June),
the whole year, and three months with `eo:cloud_cover <= 20`. Five repetitions
per cell — fifteen runs per (variant, shape) across the three tiles; medians
reported, min–max as the spread. **Every variant returned identical rows** for
every (tile, shape): 0 mismatches over 780 cells, 0 errors.

One structural note the shape columns make visible: in this client the date
window and the cloud ceiling **prune nothing** — they are applied to decoded
rows. Only the tile prunes, through `_tile` row-group statistics. So for any
layout a client cannot prune by date, all four shapes cost exactly the same,
and the shape columns differ only for V3 and V5.

## The layouts, 2018

1,329,973 rows, 28,103 distinct tiles (46 scenes per tile at the median), 864
grid-zone prefixes, 60 UTM zones, 1.68 GB, 63 columns, all sorted
`(_tile, datetime)` at zstd 18.

| id | partitioning | rows/group | prune by | files | total GB | vs V0 | row groups | footer per file, median (max) | rows per file, median (max) | fold wall |
|---|---|---|---|---|---|---|---|---|---|---|
| V0 | one file per year, as published today | 6,000 | none | 1 | 1.68 | +0.0 % | 217 | 2,044 KB (2,044 KB) | 1,329,973 | — |
| V1 | one file per year | 20,000 | none | 1 | 1.66 | −0.9 % | 65 | 625 KB (625 KB) | 1,329,973 | 5.5 min |
| V2 | one file per year | 100,000 | none | 1 | 1.65 | −2.0 % | 14 | 137 KB (137 KB) | 1,329,973 | 8.1 min |
| V3 | 12 files, one per calendar month | 6,000 | date | 12 | 1.72 | +2.4 % | 223 | 149 KB (519 KB) | 91,478 (326,217) | 19.1 min |
| V4 | 8 files, the `ZONE_PARTS_8` UTM-zone octants | 6,000 | tile | 8 | 1.68 | +0.0 % | 221 | 260 KB (327 KB) | 163,368 (203,165) | 11.3 min |
| V5 | 96 files, octant × month | 6,000 | both | 96 | 1.72 | +2.5 % | 265 | 22 KB (88 KB) | 10,888 (53,151) | 13.7 min |
| V6 | 60 files, one per UTM zone | 6,000 | tile | 60 | 1.68 | +0.0 % | 246 | 41 KB (70 KB) | 23,821 (40,597) | 8.3 min |
| **V7** | **864 files, one per MGRS grid-zone prefix (`t=31U`)** | **2,000** | **tile** | **864** | **1.71** | **+1.6 %** | **1,274** | **13 KB (41 KB)** | **1,770 (6,438)** | **17.9 min** |

Fold wall is the whole-variant Slurm job (one node, 12–24 concurrent `gpio`
workers, 16–32 cores); per-part costs are in the fold section below.

A footer costs about **9.4 KB per row group** at 63 columns, plus ~4 KB of
schema, and that is the whole story of the footer column: V0's 217 groups make
2.04 MB, V7's one-to-four groups per file make 13 KB. Note the two independent
directions out of V0 — coarsening row groups shrinks the footer in place (V2's
14 groups, 137 KB) and partitioning shrinks it per file.

Bytes barely move in any direction. Month and octant×month partitioning costs
+2.4/+2.5 % (more groups, smaller dictionaries); zone and octant partitioning
is free to two decimals; V7's 864 files cost +1.6 %; and coarsening row groups
*saves* 1–2 %. Nobody has to trade bytes for layout here.

### Why V7 is the variant I chose

The brief left V7 open. The reasoning, then the arithmetic:

* The grid-zone prefix is the **finest partition key a client can derive from a
  tile id with no index** — `tile.match(/^(\d{1,2}[A-Z])/)`. Unlike a month
  partition it prunes on every query shape, including the whole-year one where
  a month partition prunes nothing; unlike an equal-size bucketing it needs no
  published tile→file map.
* At that grain a part holds 1,770 rows at the median (max 6,438), so the
  footer is a rounding error *and* the per-search chunk read is capped by the
  part's own row count. Row groups of 2,000 rather than 6,000 sit near the
  optimum of `(9.4 KB × R/g) + (27 B × g)` — footer plus one admitted group's
  eight search columns — which for `R` in the 1–6,438 range these parts hold is
  `g = sqrt(9.4e3·R/27)` ≈ 590–4,300.

Both predictions held: 13 KB footers and 61 KiB per warm search, the smallest
of any variant by 3×.

### A data-quality aside

Two `_tile` spellings exist in the published data — `1CCV` (75,381 rows of
2018) and `01CDS` (1,254,592) — so a single-digit zone appears both padded and
not. Nothing here normalises them: every partition key is derived from the tile
*string*, which keeps each spelling self-consistent, and `search.js` matches
`_tile` by exact string too. It deserves a separate look (a zone 1–9 tile's
scenes are split across two ids today, so a search finds only half of them) but
it is not a layout problem.

## Results: 2018, five repetitions × three tiles

RTT to the bucket (time-to-first-byte, 1-byte range read): **before** median
0.776 s (0.697–2.294), **after** median 0.763 s (0.554–0.964). Stable, so the
comparison holds.

**Cold** (fresh page) — median ms (min–max of 15 runs) / requests / KiB

| variant | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| **V0 + sidecar** | **2,645** (2,356–4,060) / 9 / 214 | **2,458** (2,327–2,973) / 9 / 214 | **2,400** (2,280–2,664) / 9 / 214 | **2,463** (2,286–3,068) / 9 / 214 |
| V0 (footer path) | 3,351 (3,147–4,430) / 11 / 2,232 | 3,378 (3,120–4,216) / 11 / 2,232 | 3,456 (3,140–4,195) / 11 / 2,232 | 3,387 (3,140–4,215) / 11 / 2,232 |
| V1 | 3,572 (3,170–4,231) / 11 / 1,152 | 3,593 (3,155–3,883) / 11 / 1,152 | 3,584 (3,223–3,887) / 11 / 1,152 | 3,460 (3,201–4,192) / 11 / 1,152 |
| V2 | 3,551 (3,122–4,225) / 11 / 2,427 | 3,422 (3,338–3,934) / 11 / 2,427 | 3,605 (3,304–4,134) / 11 / 2,427 | 3,596 (3,172–4,130) / 11 / 2,427 |
| V3 | 3,177 (3,040–4,202) / 11 / 521 | 3,408 (3,141–3,822) / 33 / 1,100 | 3,672 (3,432–4,225) / **132** / **4,750** | 3,349 (3,123–3,785) / 33 / 1,100 |
| V4 | 3,710 (3,133–5,339) / 11 / 482 | 3,225 (2,959–3,990) / 11 / 482 | 3,146 (2,961–4,738) / 11 / 482 | 3,166 (2,978–4,037) / 11 / 482 |
| V5 | 3,136 (2,892–4,044) / 11 / 235 | 3,232 (3,078–3,899) / 33 / 593 | 3,631 (3,191–6,311) / **132** / 2,536 | 3,142 (2,890–3,360) / 33 / 593 |
| V6 | 3,194 (2,859–4,024) / 11 / 213 | 3,094 (2,866–3,343) / 11 / 213 | 3,137 (2,853–3,482) / 11 / 213 | 3,163 (2,822–3,721) / 11 / 213 |
| **V7** | **3,092** (2,822–3,533) / 11 / **84** | **2,987** (2,819–3,454) / 11 / **84** | **3,055** (2,813–3,547) / 11 / **84** | **3,091** (2,831–3,643) / 11 / **84** |

**Warm** (second identical search, metadata resolved)

| variant | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| V0 + sidecar | 2,076 (1,922–2,171) / 8 / 188 | 2,083 (2,007–2,553) / 8 / 188 | 2,082 (2,033–2,545) / 8 / 188 | 2,096 (2,010–3,127) / 8 / 188 |
| V0 (footer path) | 2,076 (2,002–2,320) / 8 / 188 | 2,088 (1,937–2,561) / 8 / 188 | 2,081 (2,011–2,804) / 8 / 188 | 2,105 (2,017–2,560) / 8 / 188 |
| V1 | 2,196 (1,995–2,665) / 8 / 526 | 2,105 (1,968–2,401) / 8 / 526 | 2,311 (2,049–3,032) / 8 / 526 | 2,223 (2,003–2,708) / 8 / 526 |
| V2 | 2,370 (2,284–2,954) / 8 / 2,289 | 2,383 (2,247–2,929) / 8 / 2,289 | 2,368 (2,155–2,648) / 8 / 2,289 | 2,526 (2,282–3,044) / 8 / 2,289 |
| V3 | 2,113 (1,996–2,480) / 8 / 194 | 2,165 (2,107–2,533) / 24 / 652 | 2,305 (2,261–3,105) / **96** / 2,590 | 2,163 (2,088–2,390) / 24 / 652 |
| V4 | 2,199 (2,051–2,702) / 8 / 175 | 2,073 (2,020–3,064) / 8 / 175 | 2,083 (2,033–2,413) / 8 / 175 | 2,083 (2,000–2,495) / 8 / 175 |
| V5 | 2,083 (1,989–2,508) / 8 / 193 | 2,205 (2,066–2,573) / 24 / 516 | 2,300 (2,100–4,217) / **96** / 2,192 | 2,269 (2,046–2,440) / 24 / 516 |
| V6 | 2,088 (2,020–2,471) / 8 / 173 | 2,082 (2,057–2,289) / 8 / 173 | 2,132 (2,065–2,690) / 8 / 173 | 2,088 (2,068–2,631) / 8 / 173 |
| **V7** | 2,095 (1,991–2,718) / 8 / **61** | 2,102 (2,016–2,627) / 8 / **61** | 2,090 (1,981–2,450) / 8 / **61** | 2,110 (2,046–2,614) / 8 / **61** |

**Which layout wins each shape, with no sidecar: V7, all four.** Its median is
the lowest sidecar-free number in every column (2,987–3,092 ms), and it is the
only variant whose spread never exceeds 3.7 s. V6 is second everywhere
(3,094–3,194); V4, V3 and V5 follow; V0, V1, V2 are last. Warm is a tie for
every layout that reads one part — the eight chunk GETs are the whole cost —
and V7 wins on bytes by 3×.

## Confirmation: 2024

4,369,942 rows, 5.43 GB, footer 6.71 MB as published; sidecar 86 KB on the
wire. The top two sidecar-free variants were rebuilt and re-measured. V1–V5
were not: V3 and V5 are dominated by the whole-year shape (132 requests, 2.5–4.8
MB), V4 by V6 at the same request count and worse bytes, and V1/V2 lose warm
time to their huge row groups — none of them beats V6 on any 2018 cell, so a
confirm run would only re-establish that.

| id | files | total GB | vs V0 | row groups | footer per file, median (max) | rows per file, median (max) | fold wall |
|---|---|---|---|---|---|---|---|
| V0 | 1 | 5.43 | +0.0 % | 712 | 6,710 KB | 4,369,942 | — |
| V6 | 60 | 5.43 | +0.0 % | 742 | 126 KB (203 KB) | 76,606 (124,468) | 28.2 min |
| **V7** | **1,011** | **5.49** | **+1.1 %** | **2,699** | **31 KB (88 KB)** | **4,254 (18,215)** | **38.0 min** |

RTT: before median 0.755 s (0.566–0.852), after 0.715 s (0.603–0.991).

**Cold** — median ms (min–max of 15) / requests / KiB

| variant | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| **V0 + sidecar** | **2,635** (2,370–4,185) / 9 / 248 | **2,544** (2,330–2,961) / 9 / 248 | **2,460** (2,334–3,123) / 9 / 248 | **2,473** (2,308–3,223) / 9 / 248 |
| V0 (footer path) | 3,679 (3,591–4,767) / 11 / 6,875 | 4,164 (3,660–4,720) / 11 / 6,875 | 3,676 (3,393–4,610) / 11 / 6,875 | 3,679 (3,374–3,933) / 11 / 6,875 |
| V6 | 3,145 (2,869–3,172) / 11 / 294 | 3,154 (2,823–3,674) / 11 / 294 | 3,492 (2,920–4,078) / 11 / 294 | 3,162 (2,849–3,675) / 11 / 294 |
| **V7** | **3,122** (2,842–3,490) / 11 / **100** | **2,958** (2,838–3,441) / 11 / **100** | **2,878** (2,840–3,674) / 11 / **100** | 3,291 (2,846–4,007) / 11 / **100** |

**Warm**

| variant | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| V0 + sidecar | 2,093 / 8 / 164 | 2,374 / 8 / 164 | 2,079 / 8 / 164 | 2,087 / 8 / 164 |
| V0 (footer path) | 2,085 / 8 / 164 | 2,086 / 8 / 164 | 2,100 / 8 / 164 | 2,102 / 8 / 164 |
| V6 | 2,097 / 8 / 167 | 2,079 / 8 / 167 | 2,076 / 8 / 167 | 2,081 / 8 / 167 |
| **V7** | 2,091 / 8 / **58** | 2,097 / 8 / **58** | 2,091 / 8 / **58** | 2,245 / 8 / **58** |

The 2018 ranking holds, and the gap between V7 and the unpartitioned footer
path **widens with the year**: +0.3 s on 2018's 2.0 MB footer, +0.8 s on
2024's 6.7 MB. V7's own numbers are flat between the two years (2,878–3,291 ms
cold, 100 KiB) because a prefix part does not grow much — 1,770 rows in 2018,
4,254 in 2024.

## What dominates, and why the variants land where they do

One V0 cold search, tile 31UFU, as the harness recorded it:

```
   37 ->  254 ms  404         349 B  items.idx.json    the sidecar probe
  254 ->  737 ms  206           8 B  bytes=-8          the footer's length
  737 ->  967 ms  206   2,092,938 B  the footer
  967 -> 1921 ms   (no request: hyparquet parsing 2.04 MB of thrift)
 1921 -> 2263 ms  206      22,144 B  _tile       chunk
 1921 -> 2574 ms  206      10,595 B  datetime    chunk
 1921 -> 2962 ms  206      61,916 B  id          chunk
 1921 -> 3313 ms  206      28,103 B  cloud       chunk
 1921 -> 3542 ms  206      37,991 B  nodata      chunk
 1921 -> 3891 ms  206      32,979 B  thumbnail   chunk
 1921 -> 4315 ms  206          69 B  bbox        chunk
 1921 -> 4847 ms  206         425 B  baseline    chunk
```

Four mechanisms, in order of size:

1. **The eight chunk GETs are issued together and served in a staircase**, one
   completing every ~240–290 ms: ~2.1 s to move 190 KB. They are *not*
   serialised by the server — eight concurrent range reads of the same object
   from eight separate connections (a threaded Python client) all finish inside
   1.0 s — and not by the connection either: requests to *different objects* do
   run in parallel, which is why V3's whole-year search makes 132 requests
   across 12 parts and still finishes in 3.7 s. They are serialised because
   they **share a URL** (see the next section).
2. **A missing sidecar costs two extra sequential requests** — the 404 probe
   plus the tail read — because `search.js` asks for `<stem>.idx.json` first
   and only then reads the 8-byte tail to learn the footer's length. That is
   the whole 0.5–0.9 s gap between V0+sidecar and every sidecar-free layout,
   and no layout can close it. (One of the two is free to recover: a collection
   that declares it publishes no sidecars can skip the probe.)
3. **A large footer costs main-thread CPU**: 950 ms of thrift parsing for V0's
   2.04 MB, against 38 ms for V6's 42 KB. This is the one place the footer's
   *size* shows up, and it is why V0/V1/V2 sit at the bottom of the cold table
   while V6/V7 sit at the top of the sidecar-free group.
4. **Footer bytes themselves are nearly free**: that 2.04 MB read took 230 ms,
   a 42 KB read took 371 ms. Below a few MB this path charges for requests, not
   for bytes.

So the ranking needs little more explanation:

* **V7** — three metadata requests on a 13 KB footer (≈0 parse), then eight
  chunk GETs covering only its own ≤ 2,000-row groups. Fewest requests-times-
  latency and fewest bytes.
* **V6** — the same request profile on a 41 KB footer (126 KB in 2024); 173 KiB
  warm. A close second, and a tenth of V7's object count.
* **V4** — tile-prunable, but a 260 KB footer, 6,000-row groups, and parts too
  big to fold on a runner (below). Dominated by V6.
* **V3 / V5** — prune by date, so a whole-year tile history asks 12 parts and
  makes 132 requests. Parallel across objects, so the wall time survives; the
  bytes do not — 4.75 MB (V3) and 2.54 MB (V5) against 84 KiB for V7. Dominated.
* **V1 / V2** — coarser row groups shrink the footer in one file with no
  partitioning and no client change at all, which is genuinely attractive; but
  a search then reads a 20,000- or 100,000-row group to return ~13 rows, so
  warm bytes go 188 KiB → 526 KiB (V1) → 2.29 MB (V2), and the cold win never
  arrives (the footer is not what costs). Dominated.
* **V0 with its sidecar** — still the fastest thing measured, by one request.

## The next win is not a layout

`coalesce_probe.py` replays the byte ranges a measured search actually used,
from the same headless Chrome against the same objects, three ways: as
`search.js` issues them (N ranges, one URL), the same N ranges each on its own
URL (`?cb=…`, which S3 ignores — same object, same bytes), and the ranges
merged wherever they sit within 64 KiB of each other (fewer, larger reads on
one URL). Tile 31UFU, five repetitions each, interleaved:

| part | as issued (N ranges, 1 URL) | same ranges, 1 URL each | merged at 64 KiB |
|---|---|---|---|
| V7 `t=31U` | 2,077 ms — 8 GET, 61 KiB | **438 ms** — 8 GET, 61 KiB | 778 ms — 3 GET, 133 KiB |
| V6 `z=31` | 2,196 ms — 8 GET, 171 KiB | **449 ms** — 8 GET, 171 KiB | 1,062 ms — 4 GET, 259 KiB |
| V0 `items` | 2,097 ms — 8 GET, 190 KiB | **515 ms** — 8 GET, 190 KiB | 1,411 ms — 5 GET, 217 KiB |
| V2 `items` | 2,327 ms — 8 GET, 2,289 KiB | **538 ms** — 8 GET, 2,289 KiB | 1,574 ms — 6 GET, 2,289 KiB |

The eight range reads are not slow because they are eight, and not because of
their bytes (V2 moves 2.3 MB in 538 ms). They are slow because they **share a
URL**: Chrome queues concurrent requests for one resource behind each other.
Give each range a distinct URL and the same eight reads of the same object take
438 ms instead of 2,077 — **4.7× on the per-search phase, 1.6 s of wall time**,
from a one-line change to `rangeGet` in `apps/explorer/search.js`. Coalescing
adjacent chunks helps too (and V7 merges furthest, 8 → 3 reads, because its
row groups are small), but it is the worse of the two fixes and it fetches more
bytes.

That reorders the whole problem. The layout question spans 0.6 s; this spans
1.6 s and costs no bytes, no objects and no rebuild. And once it is fixed,
bytes are the only thing left in the per-search phase — where V7's 61 KiB beats
V6's 171 and V0's 190, and shows up as 438 ms against 449 and 515.

This is an analysis of the network, not a measurement of `search.js`; the
client was not modified. It should be verified as a client change before
anything is rebuilt, because it is cheaper than every layout in this document
and it makes the layout choice less urgent, not more.

## Fold cost, and whether a GitHub runner can do it

`fold_cost.sh` measures one part's write in isolation — `/usr/bin/time -v`
around `gpio sort column … --write-memory 2GB`, the operation a fold performs
on a part that gained a month of rows. `sacct` MaxRSS gives what each
whole-variant job peaked at.

| unit folded | rows | file | wall | CPU | peak RSS |
|---|---|---|---|---|---|
| **one V7 prefix part, largest** (`t=23X`) | 6,438 | 8.1 MB | **5.9 s** | 28 s | **1.3 GB** |
| one V7 prefix part, median (`t=50R`) | ~1,800 | 2.5 MB | 4.6 s | 12 s | 0.54 GB |
| one V6 zone part, largest (`z=19`) | 40,597 | 51 MB | 16 s | 116 s | 4.0 GB |
| one V4 octant part, largest (`z01-15`) | 203,165 | 258 MB | 30 s | 748 s | **17.6 GB** |
| `gpio check all` on any of them | — | — | ~1 s | — | 0.42 GB |

And the whole-variant jobs, from `sacct` (2018 unless noted):

| variant | job wall | gpio core-time | job peak RSS |
|---|---|---|---|
| V1 (one file, 20k groups) | 5.5 min | 6 core-min | **83.0 GB** |
| V2 (one file, 100k groups) | 8.1 min | 8 core-min | **84.6 GB** |
| V3 (12 months, 12 workers) | 19.1 min | 83 core-min | 33.4 GB |
| V4 (8 octants, 8 workers) | 11.3 min | 71 core-min | 30.5 GB |
| V6 (60 zones, 16 workers) | 8.3 min | 110 core-min | 22.6 GB |
| V7 (864 prefixes, 24 workers) | 17.9 min | 381 core-min | 29.9 GB |
| V6, 2024 (60 zones, 16 workers) | 28.2 min | 350 core-min | 50.3 GB |
| V7, 2024 (1,011 prefixes, 24 workers) | 38.0 min | 765 core-min | 85.9 GB |

**Answer: yes for V7 and V6, no for anything unpartitioned, and no for V4.**

* **Today's layout cannot fold on a runner.** One part *is* the whole year, and
  writing it peaked at 83–85 GB — five times a runner's 16 GB. This is the
  reason the fold is on RAILS, and it is a property of the layout, not of the
  tool.
* **V7's fold unit is a prefix part: 6 s, 1.3 GB, 8 MB of output at the very
  largest.** A whole 2018 year is 381 core-minutes, a whole 2024 year 765 — so
  on a 4-core runner a full-year rebuild is 1.6 h (2018) or 3.2 h (2024),
  inside the 6-hour ceiling, and disk peaks at the source plus one part
  (≤ 6 GB) if parts are written and uploaded one at a time, well inside 14 GB.
  A routine monthly fold only touches the parts that gained rows, and each one
  is a six-second job that can be a matrix entry.
* **V6's fold unit is a zone part: 16 s, 4.0 GB.** Also fine on a runner, with
  a tenth of V7's objects — but 350–765 core-minutes is not cheaper, since the
  work is proportional to bytes either way.
* **V4's fold unit is an octant part: 17.6 GB peak.** Over a runner's RAM, on
  2018, the smallest published year of the three tiers. Octants do not solve
  the automation problem.
* The 2018 fold of any partitioned variant is 6–13 min of wall on one shared
  node at 16 cores; the *shape* of the cost, not its size, is what changes.
  Note `--write-memory 2GB` is advisory: peak RSS tracks the part's size (gpio
  decodes the table), which is exactly why the per-part row count is the number
  that decides whether a runner can do it.

## Recommendation (superseded by sections A–C below)

The recommendation as it stood before the request-shape experiment. Section C
overturns its point 2: with the client fixed, V7 *does* beat the sidecar.

**Do the client fix first, and do not rebuild for latency.**

1. **Give each range read its own URL** in `search.js`'s `rangeGet` (an ignored
   query parameter keyed by the offset). Measured 4.7× on the per-search phase,
   1.6 s per search, on every layout including today's, at zero cost in bytes,
   objects or build time. Nothing in this document comes close. Verify it as a
   client change, with the sidecar exactly as published.
2. **Keep the sidecar.** It is one request against three, it is the fastest
   thing measured on both years, it is 26–86 KB on the wire, and
   `make_search_sidecar.mjs` derives it mechanically from the footer. The
   brief's hope — that a small enough footer would make the sidecar
   unnecessary — does not survive the measurement: what the sidecar buys is not
   bytes but two round trips, and no layout can return them. If the sidecar is
   to go anyway, the cheap half is recoverable: teach the client that a
   collection publishes no sidecars, so it skips the 404 probe, and the gap
   halves.
3. **If Collection 1 is repartitioned, repartition for the fold, not for the
   search, and choose V7** — one file per MGRS grid-zone prefix, 2,000-row
   groups. It is the fastest sidecar-free layout on every query shape and both
   years; it is the only variant whose per-search bytes are under 100 KiB; its
   fold unit is a 6-second, 1.3 GB job that a GitHub runner runs trivially,
   which is the one thing today's layout genuinely cannot do; and it costs
   +1.6 % bytes and 864–1,011 objects a year. Choose **V6** instead if object
   count matters more than the last 0.1 s and the last 100 KiB: 60 files a
   year, +0.0 % bytes, a 16-second fold unit, second place on every cell.
4. **Do not choose V3, V5 or V4.** Month partitioning makes the whole-year tile
   history — the explorer's timeline — cost 12 parts, 132 requests and 4.75 MB.
   Octants keep the fold off a runner.
5. **Row-group size is not a lever on its own.** 20,000 and 100,000 shrink the
   footer without helping (it is not the footer that costs) and make every
   search read a bigger group: V2 moves 2.29 MB per warm search against V0's
   188 KiB. 2,000 is worth having *inside* small parts, where it costs no
   footer, and that is how V7 uses it.

**What it costs.** V7 for the whole archive: 864 objects for 2018, 1,011 for
2024, so roughly 9,500 for 2015–2026 against 12 today; +1.1–1.6 % total bytes
(2018 1.680 → 1.706 GB, 2024 5.432 → 5.494 GB); a client change in
`COLLECTIONS[…].parts(year, tile)` to compute one regex; and ~380 core-minutes
per 1.3M-row year, 765 per 4.4M-row year, to build. **No query shape gets
worse** — all four shapes are identical in requests and bytes for V7, cold and
warm, on both years, and every variant returned identical rows in all 780
cells.

## Reproducing

```bash
# on RAILS, per variant (V0 = the published part copied, with no sidecar beside it)
ssh rails 'cd ~/s2-catalog && sbatch --export=ALL,YEAR=2018,VARIANT=V7,JOBS=24 \
  tools/rails/experiments/build_layout.sbatch'
ssh rails 'bash ~/s2-catalog/tools/rails/experiments/fold_cost.sh \
  /u/cholmes/s2-c1/layout/2018/V7/t=23X.parquet 2GB 2000'

# on the laptop
python3 tools/rails/experiments/measure_layout.py --year 2018 \
  --variants V0s,V0,V1,V2,V3,V4,V5,V6,V7 --reps 5 --out /tmp/layout-2018.jsonl
python3 tools/rails/experiments/measure_layout.py --year 2018 --out /dev/null \
  --summarise /tmp/layout-2018.jsonl
python3 tools/rails/experiments/layout_table.py manifests/2018/*/layout.json
python3 tools/rails/experiments/coalesce_probe.py /tmp/layout-2018.jsonl --variant V7

# afterwards
AWS_PROFILE=source-coop python3 tools/rails/experiments/clean_experiments.py \
  --prefix _experiments/layout --yes
```

The `_experiments/layout/` prefix was deleted once these numbers were recorded.

## Notes for whoever runs this next

Five things cost time here and none of them is obvious:

* **`gpio`'s own DuckDB spills to the relative path `./.tmp/`.** N concurrent
  `gpio sort column` processes sharing a working directory overwrite each
  other's spill file and fail with "Could not read enough bytes from
  `.tmp/…`". `build_layout.py` gives every part its own `cwd`.
* **A DuckDB spill on `/u` is unreliable**, as `tools/rails/env.sh` warns: a
  shared spill directory produced "Stale file handle" and segfaults until
  `SET preserve_insertion_order = false` (plus `SET threads = 8` for the widest
  variant) removed the need to spill at all. With insertion order off DuckDB
  may also write several files for one partition; `build_layout.py` merges them
  before handing the part to gpio.
* **`GROUPS` is a bash built-in array** (the caller's group ids); assigning to
  it is silently ignored, so `--row-group-size "$GROUPS"` passed the numeric
  gid, and gpio happily wrote 202-row groups. `fold_cost.sh` uses `ROWGROUP`.
* **`ls … | head -5` under `set -o pipefail` kills a job.** One variant's 96
  finished parts sat unpublished on `/u` because SIGPIPE on the left-hand side
  of a truncated pipe ended the script between the build and the upload.
* **Slurm's wall-clock request decides when a job runs.** Eight variants at
  `--cpus-per-task=64 --mem=300g --time=08:00:00` were scheduled seventeen
  hours out on a busy cluster; at 16 cores, 32–48 GB and two hours they all
  started at once, and none of them needs more. Only the whole-year single-part
  variants (V1, V2) need a big-memory profile, for the reason the fold table
  gives.

---

# Part two: the request shape, and V7 with the client fixed

Measured 2026-09-25, same laptop, same bucket, the question of
[the follow-on brief](c1-search-speed-brief.md): the sweep above concluded that
no sidecar-free layout can beat the sidecar *because the footer path spends
three sequential metadata round trips*. That premise was not fixed. With it
removed, the conclusion reverses.

`tools/rails/experiments/measure_search.py` runs the sweep this part needs.
Where `measure_layout.py` compares layouts with one client, this compares
**clients** — the file at git HEAD against the working tree's — over the same
objects, in one interleaved sweep, with the same harness, the same three tiles,
the same four shapes and the same cache-busting discipline. `search_harness.html`
imports whichever module the cell names (`/apps/explorer/search.js`, or
`/baseline/search.js` served straight from the committed blob), so "before" is
literally the committed file.

## Summary of part two

1. **`cache: "no-store"` is the fix, not a distinct URL.** Chrome serialises
   concurrent range GETs that share a URL behind its HTTP cache lock.
   `cache: "no-store"` takes the request out of the cache entirely and the
   eight reads of one row group go from 2,423 ms to **472 ms** — statistically
   the same as the 462 ms a distinct URL per read buys, without making each
   read its own CDN cache key. It is a one-word change and it is now shipped.
2. **It is worth 1.2–1.3 s of every search on the layout published today**:
   cold 2,871 → 1,703 ms, warm **2,121 → 829 ms** (2.6×), sidecar and all.
3. **It fixes a search that was failing outright.** On the *other* collection
   (`sentinel-2-l2a`, whose octant parts admit 34 of 118 row groups for one
   tile, so a search is 272 chunk reads of one object) the committed client
   throws `TypeError: Failed to fetch` on all three tiles — Chrome gives up on
   requests queued too long behind the cache lock. The candidate completes and
   returns exactly DuckDB's rows. The same failure appeared twice in 60
   `foot-old` cells on Collection 1's own footer path.
4. **One speculative tail read replaces two metadata round trips**, and a
   collection that says it publishes no sidecar skips the 404 probe: on V7 the
   metadata phase goes **3 requests / 830 ms → 1 request / 276–298 ms**.
5. **V7 + A + B beats today's sidecar, by 2.4–2.8× cold and 3.1–6.6× warm.**
   V7 with no sidecar: **1,018–1,222 ms cold, 316–681 ms warm, 122 KiB**.
   Today, published: 2,713–3,060 ms cold, 2,090–2,121 ms warm, 248 KiB.
6. **The sidecar can be dropped — on V7, not on the year file.** What the
   sidecar buys is now one request against one, so all it buys is bytes and
   parse: 84 KiB of JSON against a 6.7 MB footer. Against a 48 KB footer there
   is nothing left to buy.

## A. How to issue the concurrent range reads

`parallel_probe.py` reads a part's footer over HTTPS, admits the row groups
whose tile statistics cannot exclude the tile, and replays exactly the
(column, group) chunk ranges `search.js` would fetch — five ways, interleaved,
five repetitions, in the same headless Chrome:

| part (admitted reads, bytes) | as issued<br>N ranges, 1 URL | **`no-store`**<br>N ranges, 1 URL | coalesced<br>1 range, 1 URL | coalesced<br>+ `no-store` | *distinct URLs*<br>*(reference)* |
|---|---|---|---|---|---|
| C1 2024 `items` (8 GET, 158 KiB) | 2,423 ms | **472 ms** | 792 ms | 815 ms | *462 ms* |
| C1 2018 `items` (8 GET, 190 KiB) | 2,094 ms | **396 ms** | 782 ms | 779 ms | *406 ms* |
| C1 2024 V7 `t=31U` (8 GET, 59 KiB) | 2,622 ms | **806 ms** | 1,295 ms | 1,379 ms | *821 ms* |
| L2A 2024 `z21-31` (238 GET, 7.3 MB) | 38,084 ms | **3,308 ms** | 10,367 ms | 9,761 ms | *5,806 ms* |

Medians of five. The coalesced column is one GET spanning `min(off)` to
`max(off + len)` of the same reads, sliced in memory.

**`cache: "no-store"` wins, and it is the candidate that costs nothing.**

* It **matches the distinct-URL reference** on every part (472/462, 396/406,
  806/821) and *beats* it on the 238-read L2A case (3,308 against 5,806),
  because 238 distinct query strings are 238 separate CDN cache keys and 238
  origin fetches. That is the cost the brief said to discount; here it is
  measurable rather than hypothetical.
* It sends **no extra request header** (unlike `reload` or `no-cache`, which add
  `Cache-Control`), so the object, the range, the bytes and the edge cache entry
  are exactly what they were. The only thing it gives up is the browser's own
  disk cache for that response — which was buying nothing, because consecutive
  searches read different ranges and the metadata is already cached in the
  module for the session.
* **Coalescing is the worse fix and it is expensive.** The eight search columns
  of one row group are spread across that group's whole byte range in a
  63-column file, so one spanning read costs **+4,554 %** bytes on the published
  layout (158 KiB → 7.37 MB), **+4,171 %** on V7 (59 KiB → 2.52 MB) and
  **+2,558 %** on L2A (7.3 MB → 194 MB). It is 1.6–3.1× slower than `no-store`
  and adding `no-store` to it does not help. Rejected.

The mechanism is Chrome's cache lock: only one in-flight request per cache entry
may write it, so the others wait, ~260 ms apiece, and one that waits too long
fails. That is why the fix is not "fewer requests" and not "different URLs" but
"do not involve the cache".

## B. One-request metadata

Two changes to the footer path, both in `partMeta`:

* **One speculative suffix read of 64 KiB** instead of an 8-byte tail read for
  the footer's length followed by the footer itself. The speculative read's
  Content-Range still names the file size, its last 8 bytes still name the
  footer length, and the footer is normally already inside the buffer. A footer
  that does not fit costs exactly the second request the old path always paid.
* **`sidecars: false` from the caller** drops the 404 probe. `app.js` now says
  it per collection, and the truth was worth stating: `sentinel-2-l2a` has never
  published a sidecar for any part, and no stats product has one — only a
  Collection 1 year's `items.parquet` gets one, in `fold_live.sbatch`. So the
  probe was pure loss on the other collection and on every tile-timeline read.

The metadata phase, isolated (the warm phase is exactly the chunk GETs, so the
cold phase's leading requests are the metadata phase), medians over 15 runs:

| arm | metadata requests | bytes | wall ms | footer parse ms |
|---|---|---|---|---|
| published + sidecar, either client | 1 | 84 KiB | 247–272 | 13–656 |
| published, footer, committed client | 3 | 6,710 KiB | 754–872 | 701–1,217 |
| published, footer, candidate | 3 | 6,774 KiB | 786–886 | 546–1,207 |
| published, footer, candidate, `sidecars: false` | **2** | 6,774 KiB | **566–664** | 582–1,266 |
| V7, footer, committed client | 3 | 40 KiB | 762–909 | 2–3 |
| V7, footer, candidate | 2 | 64 KiB | 540–574 | 6–147 |
| **V7, footer, candidate, `sidecars: false`** | **1** | **64 KiB** | **276–298** | **6–9** |

Read the two halves separately:

* **On V7 the speculative read does its job.** A 48 KB footer fits in 64 KiB, so
  the tail read *is* the footer read: three requests become two, and dropping the
  probe makes it one. 830 ms → 540 ms → **287 ms**, for +24 KiB that were going
  to be fetched anyway. This is the gap the first sweep said no layout could
  close, closed.
* **On the year file it cannot.** A 6.7 MB footer does not fit in 64 KiB, so the
  candidate still makes two reads and the probe is the only request available to
  drop: 3 → 2, ~800 ms → ~600 ms. Raising `TAIL_BYTES` to 8 MB would buy the
  third request at the cost of fetching 8 MB for every part; 64 KiB is the size
  that is free when the footer is small and harmless when it is not.
* **The footer's own parse is the other half of why the year file loses**:
  0.7–1.2 s of main-thread thrift for 6.7 MB against 2–9 ms for 48 KB. (In one
  earlier sweep, with another job holding four of this laptop's twelve cores,
  the same parse measured 12–15 s. The number is load-sensitive; its sign is
  not.)

## C. Does V7 + A + B beat the sidecar?

2024 only, three tiles, four shapes, five repetitions, eight arms interleaved
cell by cell: 479 cells, 4 errors (two CDN 520s, and two `foot-old` cache-lock
failures). Every arm that completed returned **exactly DuckDB's rows** for its
cell — `ground_truth.py` checks each against `read_parquet` over the same object
— and no two arms disagreed either.

V7 here is three real grid-zone parts in the bucket
(`_experiments/layout/2024/V7/t={31U,33U,23K}.parquet`: 8,038–8,500 rows, 4–5
groups of 2,000, zstd 18, sorted `_tile,datetime`, footers 39–48 KB). The RAILS
job that was to build all 1,011 spent its whole wall limit without reaching its
upload step, so `v7_parts_local.py` built the three the sweep reads directly off
the published year part — DuckDB rather than `gpio`, which changes the
GeoParquet metadata block and nothing a reader measures. The whole-year V7 facts
(1,011 objects, 5.49 GB, +1.1 %, fold cost) stand on part one's full RAILS
build.

RTT to the bucket: **before** median 0.507 s (0.388–0.992), **after** 0.947 s
(0.373–6.314). The path had a bad half-hour in the middle — Cloudflare 520s and
a 6 s outlier — which is why the spreads are wide; arms are interleaved cell by
cell, so it lands on all of them equally, and the reps-3–5 subset (288 cells,
quiet path) gives the same ordering with medians within 10 %.

**Cold** (fresh page) — median ms / requests / KiB, 15 runs per cell

| arm | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| `prod` — published + sidecar, **committed client (today)** | 2,871 / 9 / 248 | 3,060 / 9 / 248 | 2,928 / 9 / 248 | 2,713 / 9 / 248 |
| `prod-AB` — published + sidecar, candidate | 1,703 / 9 / 248 | 1,051 / 9 / 248 | 1,825 / 9 / 248 | 1,119 / 9 / 248 |
| `foot-old` — published, no sidecar, committed | 4,197 / 11 / 6,875 | 3,702 / 11 / 6,875 | 4,242 / 11 / 6,875 | 4,148 / 11 / 6,875 |
| `foot-AB` — published, no sidecar, candidate | 3,056 / 11 / 6,939 | 3,215 / 11 / 6,939 | 2,412 / 11 / 6,939 | 4,212 / 11 / 6,939 |
| `foot-AB-nosc` — published, `sidecars: false` | 2,279 / 10 / 6,939 | 1,913 / 10 / 6,939 | 2,349 / 10 / 6,939 | 2,188 / 10 / 6,939 |
| `v7-old` — V7, no sidecar, committed | 3,091 / 11 / 98 | 3,635 / 11 / 98 | 2,966 / 11 / 98 | 2,934 / 11 / 98 |
| `v7-AB` — V7, candidate, probe kept | 1,308 / 10 / 122 | 1,575 / 10 / 122 | 1,572 / 10 / 122 | 1,346 / 10 / 122 |
| **`v7-AB-nosc` — V7, candidate, `sidecars: false`** | **1,048** / 9 / **122** | **1,222** / 9 / **122** | **1,045** / 9 / **122** | **1,018** / 9 / **122** |

**Warm** (second identical search, metadata resolved)

| arm | 1 month | 3 months | whole year | 3 months + cloud ≤ 20 |
|---|---|---|---|---|
| `prod` (today) | 2,121 / 8 / 164 | 2,095 / 8 / 164 | 2,108 / 8 / 164 | 2,090 / 8 / 164 |
| `prod-AB` | 829 / 8 / 164 | 844 / 8 / 164 | 924 / 8 / 164 | 337 / 8 / 164 |
| `foot-old` | 2,098 / 8 / 164 | 2,512 / 8 / 164 | 2,410 / 8 / 164 | 2,320 / 8 / 164 |
| `foot-AB` | 749 / 8 / 164 | 793 / 8 / 164 | 841 / 8 / 164 | 784 / 8 / 164 |
| `foot-AB-nosc` | 921 / 8 / 164 | 949 / 8 / 164 | 720 / 8 / 164 | 858 / 8 / 164 |
| `v7-old` | 2,209 / 8 / 58 | 2,446 / 8 / 58 | 2,458 / 8 / 58 | 3,462 / 8 / 58 |
| `v7-AB` | 767 / 8 / 58 | 737 / 8 / 58 | 721 / 8 / 58 | 747 / 8 / 58 |
| **`v7-AB-nosc`** | **681** / 8 / **58** | **316** / 8 / **58** | **666** / 8 / **58** | **582** / 8 / **58** |

**The answer is yes.** V7 with no sidecar and the fixed client is the fastest
arm in every column, cold and warm: 1,018–1,222 ms against today's 2,713–3,060
(**2.4–2.8×**) and 316–681 ms warm against 2,090–2,121 (**3.1–6.6×**). It also
beats the sidecar arm *with the same client fix* (`prod-AB`, 1,051–1,825 ms
cold) in three of four shapes and ties the fourth, because it is now one
metadata request against one and the only difference left is bytes: 122 KiB
against 248, a 48 KB footer against an 84 KiB sidecar whose JSON costs 0.2–0.7 s
to revive.

Three readings worth keeping:

* **The client fix is worth more than the layout, and the layout is still worth
  having.** `prod` → `prod-AB` is −1.2 s cold and −1.3 s warm on the layout
  published today, with no rebuild. `prod-AB` → `v7-AB-nosc` is a further
  −0.1 to −0.8 s cold, −0.2 to −0.5 s warm, and −126 KiB.
* **The committed client is nearly insensitive to layout, which is the tell.**
  `prod`, `foot-old` and `v7-old` all sit at 2.1–2.5 s warm whether they read
  58 KiB or 164 KiB, because the eight reads are serialised and the wall time is
  eight lock waits. With `no-store` the arms separate by bytes, as they should:
  58 KiB (V7) beats 164 KiB (year file).
* **`foot-AB-nosc` is the one arm that barely improves** — 1,913–2,349 ms cold —
  and it is exactly the sidecar-free year file: 6.9 MB of metadata and a second
  of parse that no request-shaping can remove. Dropping the sidecar is a layout
  decision, not a client one.

## The gate: the real page, both collections

`check_app.py` loads `apps/explorer/index.html` itself in headless Chrome
(SwiftShader, because maplibre needs a WebGL context), in two served variants
that differ only in `search.js` — the working tree's against the committed blob
— waits for the page's own MGRS hit index, fires the map click a user's mouse
fires, and reads the ids off the rendered result cards. Both variants are also
checked against `ground_truth.py`'s DuckDB answer, which is what makes a case
checkable even where one client cannot finish it.

Collection 1 is searched in 2024 (one tile-major file per year).
`sentinel-2-l2a` is searched in **2016**, whose year is a single 4.4 MB file:
its post-2021 years are zone octants whose `s2:mgrs_tile` statistics overlap
across nearly every row group — 43 to 70 of 70 admitted for one tile, 300–490
range reads, 14 MB — so a search there takes minutes in the page with *either*
client and is no use as a gate. 2016 exercises exactly the same client path.

| collection | year | tile | candidate | matches DuckDB | committed client | wall (committed → candidate) |
|---|---|---|---|---|---|---|
| `sentinel-2-c1-l2a` | 2024 | 31UFT | 12 rows | yes | the same 12 rows | 10.5 s → 10.5 s |
| `sentinel-2-c1-l2a` | 2024 | 23KMR | 6 rows | yes | the same 6 rows | 9.4 s → 9.7 s |
| `sentinel-2-l2a` | 2016 | 33VVF | 13 rows | yes | the same 13 rows | 10.9 s → **6.1 s** |
| `sentinel-2-l2a` | 2016 | 34UCC | 12 rows | yes | the same 12 rows | 10.9 s → **6.9 s** |

All four pass: identical ids on both collections under both clients, and both
equal to DuckDB's. (The wall times here include the whole page — map, stats,
timeline, PMTiles — so they are not the per-search numbers of section C; the
L2A pair is where the page's own work is smallest and the search's share shows.)

**Where the committed client cannot finish at all** is the same collection's
2024 octant parts, and that is measured in the direct-client sweep rather than
the page, because the page cannot do it either way. `measure_search.py`'s
`l2a-old` arm — the committed client, `sentinel-2-l2a/year=2024/z21-31.parquet`,
272 chunk reads of one 208 MB object — throws `TypeError: Failed to fetch` on
**all three tiles**, with only its two metadata reads completed. The `l2a-AB`
arm, the candidate, completes and returns exactly DuckDB's rows. The same
failure appeared twice in 60 `foot-old` cells on Collection 1's own footer path.
Chrome gives up on requests queued too long behind the cache lock, and that is
what `no-store` removes.

## Recommendation, part two

1. **Ship the `search.js` change** (done in this commit): `cache: "no-store"`
   on every range read, one speculative 64 KiB tail read on the footer path, and
   a `sidecars` flag the caller sets per collection. Measured **−1.2 s cold and
   −1.3 s warm on the layout published today**, 2.6× on the warm phase, at zero
   cost in bytes, objects, CDN load or build time; and it turns a
   `sentinel-2-l2a` search on a 2024 octant part from a thrown `TypeError`
   into exactly DuckDB's rows.
   Nothing else in either part of this document is close.
2. **The sidecar can be dropped, but only on a small-footer layout.** On V7 it
   is now worth nothing — one request against one, 84 KiB of JSON against a
   48 KB footer — and V7 without it is the fastest arm measured. On the year
   file published today it is still worth 0.9–1.3 s cold, because 6.7 MB of
   footer and a second of thrift parse are not a request-shaping problem.
   **Keep publishing it for as long as Collection 1 is one file per year.**
3. **Repartition Collection 1 to V7 if the fold matters; it now pays for the
   search too.** The read win over today, with the fixed client on both sides,
   is **1.7–2.0 s cold and 1.4–1.8 s warm** per search, and 126 KiB fewer bytes.
   The costs are unchanged from part one: **+1.1–1.6 % bytes**, **~9,500 objects
   for 2015–2026** against 12, ~380–765 gpio core-minutes per year to build, and
   a client change in `COLLECTIONS[…].parts(year, tile)` to compute one regex.
   What it buys beyond latency is the thing today's layout cannot do at all: a
   **6-second, 1.3 GB fold unit** a GitHub runner runs, against the 83–85 GB a
   whole-year part peaks at — which would end the RAILS dependency for routine
   operation. Dropping the sidecar at the same time takes
   `make_search_sidecar.mjs` out of the fold entirely.
4. **Do not coalesce ranges**, and do not give each read its own URL. Measured
   worse than `no-store` on every part, and the second shifts load to the origin.
5. **Look at `sentinel-2-l2a` next.** Its parts are the worst case for this
   client — 272 reads, 7.3 MB, 34 admitted groups for one tile — and it is the
   collection where the layout question is now urgent rather than optional.

## Reproducing part two

```bash
# A: the four ways to issue the concurrent ranges, one real part at a time
python3 tools/rails/experiments/parallel_probe.py --reps 5 \
  --url https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-c1-l2a/year=2024/items.parquet

# the three V7 parts the sweep reads (the RAILS build is build_layout.sbatch)
python3 tools/rails/experiments/v7_parts_local.py --year 2024 \
  --prefixes 31U,33U,23K --out /tmp/v7 --upload

# B and C: the client-against-client sweep
python3 tools/rails/experiments/measure_search.py --year 2024 --reps 5 \
  --out /tmp/search-2024.jsonl
python3 tools/rails/experiments/measure_search.py --year 2024 --out /dev/null \
  --summarise /tmp/search-2024.jsonl
python3 tools/rails/experiments/ground_truth.py --jsonl /tmp/search-2024.jsonl

# the gate: the real page, both collections, both clients, against DuckDB
python3 tools/rails/experiments/check_app.py

# afterwards
AWS_PROFILE=source-coop python3 tools/rails/experiments/clean_experiments.py \
  --prefix _experiments --yes
```

The bucket's `_experiments/` prefix was emptied once these numbers were
recorded (3 objects, 0.03 GB), and every published path was re-checked
afterwards. **One thing is still outstanding**: the RAILS scratch of the job
that did not land. Its `ControlMaster` session expired mid-task and re-opening
it needs an interactive Duo prompt, so whoever next has a session there should
remove what Slurm job `194221` left on `/u`:

```bash
ssh rails 'rm -rf /u/cholmes/s2-c1/layout && rm -f ~/s2-catalog/logs/s2c1-layout-194221.out'
```

Nothing published depends on it; it is a partial V7 2024 build plus its
`.stage-V7`, `.gpio-V7` and `.duckdb-tmp-V7` working directories.

## Notes for whoever runs this next

* **`cache: "no-store"` is not `reload` and not a cache-busting query
  parameter.** It is the only one of the three that changes neither the request
  the CDN sees nor the cache key. If a future measurement shows the eight reads
  serialising again, check that nothing has re-added a default-cache `fetch` to
  the chunk path.
* **`chrome-headless-shell --disable-gpu` cannot run the explorer.** maplibre
  throws `Failed to initialize WebGL` inside the `Map` constructor, so the page
  never builds and nothing downstream of it exists to drive. `--use-gl=angle
  --use-angle=swiftshader --enable-unsafe-swiftshader` fixes it.
* **One browser process cannot hold eight loads of this page.** deck.gl, a
  multi-MB footer and a PMTiles index per load, and fetches start failing;
  `check_app.py` launches a fresh browser per case.
* **A measurement whose baseline can fail needs an external oracle.** Diffing
  the candidate against the committed client proves nothing where the committed
  client throws — which is exactly the case the change is worth most. That is
  what `ground_truth.py` is for.
* **Two harness bugs in `check_app.py` looked exactly like a regression**, and
  both are worth knowing before writing another page driver:
  * **Do not nudge the camera to hurry the hit index along.** `hitIndex` fills
    from the MGRS tileset as deck.gl decodes it, `mgrs.pmtiles` holds one z0
    tile, and the index keeps only the first zoom it sees — so a
    `jumpTo({zoom: 5})` meant to bring the tile under the click into view races
    the z0 request and can leave the index empty for good. Wait for
    `hitIndex.polys.length > 0` instead.
  * **`$("run").disabled` is not the signal that a click took.** The click
    handler sets it false and `runQuery` sets it true again in its own first
    synchronous statement, so a poller never sees the false; it keeps clicking
    every 250 ms for the whole search and a dozen overlapping searches pile up.
    The hint the handler writes (`Tile 31UFT.`) is the signal.

  Both failed for the candidate and not the baseline, purely because the two
  clients finish their requests at different moments, and neither had anything
  to do with `search.js`. A driver that fails asymmetrically between two
  variants is suspect until the asymmetry is explained.
* **The RAILS job for this part did not land.** Submitted at
  `--cpus-per-task=64 --mem=300g`, it waited 18 minutes for resources and then
  spent its whole 2-hour wall limit without reaching the upload step. Part one's
  note stands — ask for 16 cores and 2 hours, not 64 and 8 — and a measurement
  that needs three parts should build three parts, not 1,011.
* **`source-coop` credentials expire mid-task.** The `credential_process`
  profile went stale (`source-coop login` is interactive); the static
  `source-coop-uploader` profile in `~/.aws/credentials` still worked, against
  `s3://us-west-2.opendata.source.coop/…` — note `source.coop`, not
  `source-coop`, in that bucket name.
