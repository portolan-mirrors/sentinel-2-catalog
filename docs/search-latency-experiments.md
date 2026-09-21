# Scene-search latency experiments on 2025 data

Measured 2026-09-21. The question: the explorer's scene search takes ~30 s
cold; how far down can a tighter data layout drive it? All experiments use
`year=2025/z21-31.parquet` (875,583 rows, 4,759 tiles, 241 MB — the octant
holding the three test tiles of docs/query-performance.md: 31UFU, 30TVK,
23KKQ). The workload is the app's exact search: one tile, a 3-month window,
`eo:cloud_cover <= 20`, coverage ≥ 50 %, `ORDER BY cloud LIMIT 30`.

## Why the current search is slow

Confirmed against the live bucket (fresh DuckDB connection, app-shaped query):
**74–123 GETs, 2.1–2.3 MiB, 6.6–49.5 s wall** across the three tiles. The
mechanism is the one docs/query-performance.md established: on a path with
0.3–0.9 s time-to-first-byte per request, wall time is the *depth* of the
sequential request chain (HEAD → footer ×2 → per-admitted-group bloom filter +
filter columns → second pass for the projected columns), and the 2025 Hilbert
sort scatters one tile's month across 5–10 of the month's row groups, so the
chain is long *and* wide.

## Two structural findings that make rows tiny

1. **`thumbnail_url` is 100 % derivable from `id`** (0 exceptions in 875,583
   rows): `https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/
   {zone}/{band}/{square}/{year}/{month}/{id}/{tail}` where zone/band/square
   parse out of the tile in the id, year/month out of the date in the id, and
   `tail` is a 2-bit enum (`preview.jpg` 94 %, `thumbnail.jpg` 6 %,
   `thumbnail.jp2` 92 rows, null 78 rows). The COG directory the band mapper
   needs is the same path, so nothing else is lost.
2. **`id` itself is `S2{A-D}_{tile}_{YYYYMMDD}_{seq}_L2A`** (0 exceptions),
   with the date equal to the UTC date of `datetime`. So a search row is
   fully carried by: tile (implicit or sorted), datetime, cloud, nodata,
   bbox (4×float32), baseline (dict string), platform letter, seq, thumb enum.

A search row quantized this way (cloud/nodata as `USMALLINT` tenths of a
percent) costs **~27 bytes compressed**; keeping the raw `id` +
`thumbnail_url` strings instead costs 2.2× (29.3 MB vs 13.2 MB for the
octant-year).

## Layouts measured

Local range-supporting HTTP server with injected latency (0.4 s or 0.8 s per
request, 2.5 MB/s — the measured envelope of the real path), fresh DuckDB
connection per run. Server-side counters give GET/bytes.

| layout | file(s) | size | query path |
|---|---|---|---|
| V2-current | the live part, local copy | 241 MB | httpfs range reads |
| v3m | rebuild: sort `(_month, tile, _hilbert)`, month-aligned 6,144 groups (issue #9's plan) | 340 MB¹ | httpfs |
| tilesort-full | rebuild: sort `(tile, datetime)`, 6,144 groups | 285 MB¹ | httpfs |
| slim sidecar | search columns only, sort `(tile, datetime)`, rg 1k–20k | 13–15 MB | httpfs |
| per-tile files | `tiles/tile={tile}/…parquet`, slim columns | 4,765 files, 22.8 MB total, median 4.6 KB, max 29.5 KB | 1 GET whole file, query locally |
| tile-pack | the per-tile files concatenated + `{tile: [offset, length]}` index | 22.8 MB pack + 109 KB index (44.5 KB gz) | 1 range GET (+ index, cached), query locally |

¹ Written at DuckDB default zstd, not the production zstd-18; request counts
are what matter here, not size.

## Results (0.4 s/request; three tiles)

| layout | wall s | GET | MiB |
|---|---|---|---|
| V2-current | 6.2–6.9 | 74–122 | 2.2–2.3 |
| v3m (issue #9 rebuild) | 5.7–5.8 | 29 | 2.1–2.3 |
| tilesort-full | 5.6–5.7 | 11–20 | 1.5–1.8 |
| slim sidecar (rg 1k–20k) | 4.3–4.7 | 9–10 | 0.2–0.4 |
| per-tile file | **0.4** | **1** | **0.005–0.01** |
| tile-pack (index cached) | **0.4** | **1** | 0.005–0.007 |
| tile-pack (index cold) | 0.9 | 2 | 0.12 |

At 0.8 s/request (the path's bad half): V2 11.3 s, slim sidecar 8.3 s,
per-tile/pack 0.8 s. Row-group size 1k vs 20k barely moves the httpfs cases:
the chain depth, not bytes, is the cost. Every layout returns identical rows
(15/19/7 for the three tiles).

The floor for *any* DuckDB-httpfs layout is ~9 GETs, mostly sequential
(HEAD, footer ×2, bloom, filter columns, late-materialization second pass) —
about 4–8 s on this path no matter how the file is sorted or sized. The only
way out is to stop range-reading parquet internals per click: fetch a
tile-sized object whole and query it locally. Real-path spot check: a single
5 KB range GET from `data.source.coop` is 0.3–0.8 s on a warm connection
(2.4 s cold TLS). **Scene search lands under one second.**

## The tile-pack in detail

Per-tile parquet files answer in one GET but cost ~38k objects per year
globally. The pack removes that: concatenate the per-tile files (each remains
a complete, standalone parquet file) into one object per octant-year and
publish a `{tile: [offset, length]}` JSON index beside it. The client fetches
the index once per octant-year (44.5 KB gzipped; cacheable, or prefetchable on
tile click before the user hits search), then one range GET per search, and
registers the fragment with the DuckDB-WASM instance it already runs (or
parses it with hyparquet — at 5 KB either is instant). A byte-range-extracted
fragment was validated to parse and query correctly.

Global cost, extrapolated from this octant: ~180 MB and 16 objects
(8 packs + 8 indexes) per year, ~2 GB for 2016–2026 — beside a catalog whose
2025 parts alone are ~1.9 GB. The canonical stac-geoparquet parts stay
exactly as they are; the pack is a derived search sidecar, rebuilt from the
parts in seconds (the whole octant build: 2 s slim projection + pack
concatenation). The daily refresh would regenerate only the current year's
pack. A multi-year search window fans out to one range GET per year, in
parallel — still one round trip.

What the slim row keeps, meeting the product requirements: `datetime`,
`cloud10`/`nodata10` (cloud-cover and coverage filtering at 0.1 % precision),
bbox floats ("show on map"), baseline (BOA offset for the band mapper),
platform/seq/thumb (id and thumbnail/COG URL reconstruction — image preview).

## What this says about issue #9

The planned archive rebuild (month-aligned groups) is still right for the
*catalog* — it fixes the assets fetch and bulk analytics (29 vs 74–122 GETs).
But it does not fix the interactive search (5.7 s vs 6.5 s at 0.4 s RTT):
no parquet-over-httpfs layout can. If the search moves to the pack, the
rebuild's urgency drops to the assets-fetch and analytics cases, and the
tile-major sort `(tile, datetime)` measured here is worth considering as the
rebuild's sort instead — it beats the month-major plan for every tile-scoped
query (11–20 vs 29 GETs) at equal bytes.

## Artifacts

Scratchpad `exp/` (session-local, not committed): `build_variants.py`,
`build_full_sorts.py`, `build_pertile.py`, `build_pack.py`, `rangeserver.py`
(latency-injecting range server), `measure.py`, and the variant files. The
method mirrors docs/query-performance.md and reuses its three tiles.
