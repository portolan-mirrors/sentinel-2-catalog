# Query performance across the three published part layouts

Measured 2026-09-19 against the live bucket
(`https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a/`)
with DuckDB 1.5.3 (Python, `httpfs` + `spatial`), one fresh connection per
measurement so nothing is cached. The question: should the 2016–2025 parts be
rebuilt to the 2026 layout (6k-row groups, sorted `(_month, s2:mgrs_tile, _hilbert)`)?

## Vintages

| vintage | years | part measured | rows | row groups | rows/group | sort |
|---|---|---|---|---|---|---|
| V1 | 2016–2023 | `year=2020/z21-35.parquet` (4-part year) | 2,070,745 | 21 | ≤100,352 | `(_month, _hilbert)` |
| V1 | | `year=2023/z21-31.parquet` (8-part year) | 704,774 | 8 | ≤100,352 | `(_month, _hilbert)` |
| V2 | 2024–2025 | `year=2024/z21-31.parquet` | 719,185 | 118 | ≤6,144 | `(_month, _hilbert)` |
| V2 | | `year=2025/z21-31.parquet` | 875,583 | 143 | ≤6,144 | `(_month, _hilbert)` |
| V3 | 2026– | `year=2026/z21-31.parquet` (Jan–mid Sep) | 667,873 | 109 | ≤6,144 | `(_month, s2:mgrs_tile, _hilbert)` |

Tiles: `31UFU` (Netherlands), `30TVK` (Madrid), `23KKQ` (São Paulo) — three
UTM zones, both hemispheres, all inside the `z21-35` / `z21-31` part so every
vintage reads exactly one file. Window: months 4–6 (2026 has no data after
September). Item ids for the assets fetch: the first May scene of each tile in
each part.

## Headline: median of 3 cold runs, 3 tiles (9 runs per cell)

Wall = seconds for `EXPLAIN ANALYZE <query>` on a fresh connection. GET and
MiB come from the HTTPFS HTTP Stats block of that same run (they include the
HEAD, the two footer reads and any bloom-filter reads). Warm = the same query
re-run on the same connection.

| query | vintage | wall s (min–max) | GET | MiB in | warm s |
|---|---|---|---|---|---|
| **tile + 3 months + cloud ≤ 20, LIMIT 30** | V1-2020 | 12.6 (6.7–75.9) | 44 | 7.8 | 0.5 |
| | V1-2023 | 15.1 (8.4–76.3) | 21 | 5.1 | 0.8 |
| | V2-2024 | 14.0 (8.4–37.9) | 88 | 1.7 | 0.6 |
| | V2-2025 | 10.8 (5.6–19.9) | 100 | 1.8 | 0.3 |
| | V3-2026 | 13.5 (3.9–32.8) | 40 | 1.4 | 0.3 |
| **assets by id** (app shape: `_month = m AND id = …`) | V1-2020 | 8.4 (5.9–44.9) | 10 | 13.3 | 1.1 |
| | V1-2023 | 14.0 (6.2–125.8) | 4 | 14.0 | 1.2 |
| | V2-2024 | 6.1 (4.3–35.5) | 29 | 1.9 | 0.8 |
| | V2-2025 | 9.9 (4.1–11.9) | 25 | 2.1 | 0.3 |
| | V3-2026 | 6.8 (2.7–19.7) | 37 | 1.9 | 0.3 |
| **assets by id + tile filter** | V1-2020 | 12.2 (8.1–72.9) | 28 | 13.2 | 1.1 |
| | V1-2023 | 14.6 (9.6–45.2) | 8 | 14.1 | 1.1 |
| | V2-2024 | 10.6 (5.0–25.8) | 73 | 1.9 | 0.3 |
| | V2-2025 | 10.5 (5.8–19.0) | 85 | 2.0 | 0.6 |
| | V3-2026 | 7.7 (4.2–13.6) | 25 | 1.8 | 0.3 |
| **0.5° bbox, one month** (`ST_Intersects`) | V1-2020 | 12.7 (7.4–42.9) | 14 | 14.5 | 0.7 |
| | V1-2023 | 13.1 (9.4–65.3) | 9 | 5.8 | 0.9 |
| | V2-2024 | 13.8 (4.4–45.8) | 32 | 4.0 | 0.5 |
| | V2-2025 | 7.5 (4.4–27.6) | 39 | 3.9 | 0.3 |
| | V3-2026 | 6.5 (4.0–34.7) | 39 | 3.8 | 0.3 |
| **full-year tile history** (no month filter) | V1-2020 | 22.2 (10.3–35.6) | 92 | 24.6 | 0.7 |
| | V1-2023 | 12.8 (8.8–45.0) | 46 | 15.3 | 0.7 |
| | V2-2024 | 11.5 (7.5–19.2) | 129 | 2.9 | 0.3 |
| | V2-2025 | 9.6 (7.5–47.9) | 118 | 3.0 | 0.3 |
| | V3-2026 | 7.1 (4.7–19.0) | 46 | 2.3 | 0.5 |

GET and MiB are deterministic per (vintage, tile, query) and repeat exactly
across runs (the per-tile spread for V2 — e.g. 60/88/109 GETs for the
3-month query — is the Hilbert scatter, see below). Wall time is not: see
"Network conditions".

### Network conditions

Laptop, Copenhagen; the bucket is served through Cloudflare (edge `CPH`,
`cf-cache-status: DYNAMIC`, i.e. every range request goes to the origin).
`curl` to the part file during the run:

- TCP connect 10–30 ms; time-to-first-byte per 1-byte range request
  **0.3–0.9 s, typically 0.8 s**;
- 1 MiB range: 1.7–11.6 s (0.09–0.6 MB/s); 14 MiB range: ~4 s (3.6 MB/s).

Over the 225 default runs the wall-time median was 11.2 s, p10 5.8 s, p90
33.4 s, max 125.8 s (a 4-GET / 14 MB query that retried once). The ledger's
earlier numbers (5.0 s V1 vs 6.8 s V2 for the 3-month query) were taken on a
faster path; today's per-request latency is about 3× worse, which is why
every vintage lands at 6–15 s and the wall-time columns mostly cannot
separate vintages. The request and byte counts can, and they are what a
layout change controls. No ZSTD-decompression flake was seen: all 225 runs
returned rows.

## Mechanism: what parquet_metadata says each layout admits

From `parquet_metadata()` alone (no data read). "Admits" = row groups whose
`_month` and `s2:mgrs_tile` min/max statistics cannot exclude the predicate;
DuckDB reads at least the filter columns of every admitted group, then checks
the group's `s2:mgrs_tile` bloom filter (present in every vintage; one extra
GET per admitted group) before reading the rest.

| vintage | groups | footer KB | MB/group (all cols) | MB/group (assets) | MB/group (6 app cols) | groups in May | tile-May admits | tile 3-mo admits | tile-year admits | bbox-May admits (geo_bbox) |
|---|---|---|---|---|---|---|---|---|---|---|
| V1-2020 | 21 | 168 | 27.2 | 13.0 | 2.9 | 3 | 3 | 7 | 20–21 | 3 |
| V1-2023 | 8 | 66 | 24.9 | 12.2 | 2.8 | 1 | 1 | 3 | 7–8 | 1 |
| V2-2024 | 118 | 917 | 1.75 | 0.79 | 0.22 | 12 | 5–9 | 11–24 | 34–86 | 3 |
| V2-2025 | 143 | 1073 | 1.68 | 0.80 | 0.20 | 16 | 5–10 | 13–26 | 39–93 | 3 |
| V3-2026 | 109 | 813 | 1.67 | 0.78 | 0.21 | 16 | 3 | 6–7 | 14–17 | 4 |

Ranges are across the three tiles. What this shows:

- **V1 admits every group of the month** (a tile-month = 3 groups in a 4-part
  year, 1 in an 8-part year) and each group is 2.9 MB for the app's six
  columns, 13 MB for `assets`. Few requests, many bytes.
- **V2 scatters a tile across the month's groups**: Hilbert order puts each
  tile's ~12 monthly scenes into 5–10 of the month's 12–16 groups, and the
  stats of a group spanning the tile's range cannot exclude it. A 3-month
  window admits 11–26 groups, a year 34–93; each costs a bloom-filter read
  plus one request per column. Many requests, few bytes. This is the
  hypothesis in the ledger, confirmed.
- **V3 pins a tile-month to one group plus the two month-boundary groups**.
  Row groups are cut every 6,144 rows regardless of `_month`, so the group
  straddling months 4→5 and the one straddling 5→6 each span the whole tile
  range (`21DXJ…31XFL`) and are admitted by stats, then excluded by their
  bloom filter. Hence 3 admitted (1 real) per tile-month, 6–7 per 3-month
  window, 14–17 per partial year. Aligning row-group boundaries to `_month`
  in the build would cut this to 1 per tile-month and remove roughly a third
  of V3's remaining requests.
- **Footer size grows with group count**: 66–168 KB for V1, 0.8–1.1 MB for
  V2/V3. On V2/V3 the footer is ~half the bytes of every small query (the
  assets fetch moves 1.9 MB, of which 0.8 MB is footer).
- **Geometry statistics are not used for pruning.** `ST_Intersects(geometry,
  envelope)` is pushed into the scan as a filter, but DuckDB 1.5.3 read every
  row group's geometry column: the bbox query without a month filter on
  V3-2026 read 20.4 MB in 111 GETs (all 109 groups), and with `_month = 5` it
  read all 16 May groups (3.8 MB) though `geo_bbox` stats admit only 4. The
  `bbox` list column and `ST_XMin/XMax` bounds prune no better. So today the
  bbox query is bounded by the month filter only, and Hilbert order inside a
  month buys nothing for it. The V2→V3 sort change therefore cannot hurt it,
  and the measurement agrees (V2 32–39 GET / 4.0 MiB vs V3 39 GET / 3.8 MiB).

## Answers

**Is V3 faster than V1 for the tile queries?** In requests and bytes, yes;
in wall time on today's path, only clearly for the full-year history.

| query | V1-2020 → V3 | V1-2023 → V3 | V2 → V3 |
|---|---|---|---|
| tile + 3 months | GET 44 → 40 (−9%), MiB 7.8 → 1.4 (**−82%**), wall 12.6 → 13.5 s (noise) | GET 21 → 40 (+90%), MiB 5.1 → 1.4 (−73%), wall 15.1 → 13.5 s | GET 88–100 → 40 (**−55–60%**), MiB 1.7–1.8 → 1.4, wall 10.8–14.0 → 13.5 s (noise) |
| full-year history | GET 92 → 46 (**−50%**), MiB 24.6 → 2.3 (**−91%**), wall 22.2 → 7.1 s (**−68%**) | GET 46 → 46, MiB 15.3 → 2.3 (−85%), wall 12.8 → 7.1 s (−45%) | GET 118–129 → 46 (**−61–64%**), MiB 2.9–3.0 → 2.3, wall 9.6–11.5 → 7.1 s |
| assets by id | GET 10 → 37, MiB 13.3 → 1.9 (−86%), wall 8.4 → 6.8 s | GET 4 → 37, MiB 14.0 → 1.9, wall 14.0 → 6.8 s | GET 25–29 → 37, MiB same, wall same |

Why the 3-month query's wall time does not move: on a 0.8 s-per-request path
the cost is the *depth* of sequential requests, not their count or size.
Every vintage does HEAD → footer → (filter columns + bloom filter per admitted
group, groups in parallel) → (remaining columns for the matched groups, a
second pass because `LIMIT 30` triggers DuckDB's late materialization). That
chain is 6–8 round trips deep ≈ 5–7 s regardless of layout, and V1's extra
6 MB costs ~2–3 s at 2–3 MB/s in parallel with it. The full-year query is
where V1's 24.6 MB stops hiding behind latency.

**Does V3 hurt the bbox query?** No: V3 39 GET / 3.8 MiB / 6.5 s vs V2 32–39
GET / 3.9–4.0 MiB / 7.5–13.8 s — the same groups are read, because DuckDB
does not prune on geometry statistics (above). If a future DuckDB does, V3's
month-boundary groups still carry Hilbert order within each tile, and a 0.5°
box inside one tile lands in one tile's run of rows; expect it to admit about
the same groups as V2 (4 vs 3 for May by `geo_bbox`).

**Quick wins measured (no rebuild), 3-month query, one cold run per tile:**

| change | V1-2020 | V2-2024 | V3-2026 | verdict |
|---|---|---|---|---|
| `SET late_materialization_max_rows = 0` (single scan pass) | 42–44 GET, 6.5–6.9 MiB | 57–106 GET, 1.6 MiB | 34–37 GET, 1.4 MiB | −3 GET, −10% bytes; wall unchanged within noise |
| project 3 columns (`id, datetime, eo:cloud_cover`) instead of 6 | 35–38 GET, 3.4–3.8 MiB | 54–103 GET, 1.4 MiB | 31–34 GET, 1.2 MiB | −6 GET; halves V1 bytes; wall unchanged within noise |
| `SET httpfs_connection_caching = true` | 41–44 GET, 7.4–7.8 MiB | 60–109 GET, 1.6–1.7 MiB | 37–40 GET, 1.4 MiB | identical requests and bytes; wall unchanged within noise |
| add `s2:mgrs_tile` to the assets-by-id fetch | 10 → 28 GET | 29 → 73 GET | 37 → 25 GET | worse on V1/V2 (each admitted group costs a bloom read); keep the app's current shape until V1/V2 are rebuilt |

Nothing here is worth a code change today. Two things not measured but
implied by the numbers: (1) DuckDB 1.5's in-memory external file cache makes
a repeat query on the same connection 0.3–1.2 s with zero GETs, so a
long-lived connection is the only cheap speed-up there is (the explorer
keeps one; whether DuckDB-wasm caches the same way was not measured); (2) the two footer reads and the bloom-filter read per
group are ~1 s each on this path, which is why a query that touches one
6k-row group still takes 5 s cold.

## Recommendation

Rebuild V1 (2016–2023) and V2 (2024–2025) to the V3 layout, with one build
change: cut row groups on `_month` boundaries so a tile-month admits one
group. The gain is real but it is a bytes-and-requests gain, not a
wall-clock one on a high-latency path: full-year tile history 2–3× faster and
10× fewer bytes against 2019–2020; the 3-month app query goes from 5–8 MB to
1.4 MB per click with the same request count; V2's request count halves. The
years that benefit most are 2018–2020 (single-file and 4-part years with the
largest groups; 2019 also carries 1.2 KB/row geometries against 41 B/row
from 2020 on, so its geometry column alone is 2.2 GB in `2019/z21-35`, and
2018's 2.5 GB for 4.37M rows says the same of it). 2021–2023 (8-part, one group per month) are the least urgent
of V1.

**Rebuild cost** (single-threaded `gpio sort column` at zstd-18: 50–95 min per
1.3–2.4M-row part, and 2021's eight 1.08M-row octants at ~94 min each — i.e.
roughly 40–90 min per million rows):

| range | rows | today's parts | runner-hours at 40–90 min/M rows |
|---|---|---|---|
| 2016–2018 | 5.71M (2018 alone 4.37M in one file) | 1 + 1 + 1 | 3.8–8.6 |
| 2019–2020 | 15.23M | 4 + 4 | 10.2–22.8 |
| 2021–2023 | 17.16M | 8 × 3 | 11.4–25.7 |
| **V1 total** | **38.09M** | 35 | **25–57** |
| 2024–2025 (V2) | 9.43M | 8 + 8 | 6.3–14.1 |
| **V1 + V2** | **47.52M** | 51 | **32–71** |

Part-count change: the 6-hour job ceiling that forced 2021 into octants
applies here too — 2018 as one part is 2.9–6.6 h, the 2019–2020 quartiles
(1.5–2.2M rows) 1–3.4 h each — so the safe rebuild writes every year from
2017 on as eight zone parts (2016's 14,676 rows stay one file): 51 parts
become 1 + 8 × 9 = 73, and `apps/explorer/app.js` (`ZONE_SPLIT_FROM`,
`ZONE_SPLIT_8_FROM`), `catalog/sentinel-2-l2a/AGENTS.md` and the per-year
items all change with it. Keeping today's part layout and running the big
parts on a larger machine avoids the client changes; that is the user's
stated plan. Either way this is 32–71 runner-hours of single-threaded work
that parallelises per part, i.e. one long day on an 8-core box or a week of
free GitHub runners.

## Method (reproducible)

```python
import duckdb, re, time
B = "https://data.source.coop/portolan-mirrors/sentinel-2-catalog/sentinel-2-l2a"
APP = 'id, datetime, "eo:cloud_cover", thumbnail_url, "s2:mgrs_tile", "s2:nodata_pixel_percentage"'
f, t = f"{B}/year=2026/z21-31.parquet", "31UFU"
queries = {
  "tile3mo": f"""SELECT {APP} FROM read_parquet('{f}') WHERE "s2:mgrs_tile"='{t}'
                 AND _month BETWEEN 4 AND 6 AND "eo:cloud_cover"<=20 ORDER BY "eo:cloud_cover", id LIMIT 30""",
  "assets_id": f"""SELECT assets FROM read_parquet('{f}') WHERE _month=5 AND id='S2B_31UFU_20260501_0_L2A' LIMIT 1""",
  "bbox": f"""SELECT {APP} FROM read_parquet('{f}') WHERE _month=5
              AND ST_Intersects(geometry, ST_MakeEnvelope(5.226, 52.476, 5.726, 52.976))""",
  "year": f"""SELECT {APP} FROM read_parquet('{f}') WHERE "s2:mgrs_tile"='{t}' ORDER BY datetime""",
}
for name, q in queries.items():
    con = duckdb.connect(); con.execute("LOAD httpfs; LOAD spatial;")   # fresh connection = cold
    t0 = time.perf_counter(); txt = con.execute("EXPLAIN ANALYZE " + q).fetchall()[0][1]
    wall = time.perf_counter() - t0
    get = re.search(r"#GET:\s*(\d+)", txt).group(1); mib = re.search(r"in:\s*([\d.]+ \w+)", txt).group(1)
    print(name, round(wall, 1), "s", get, "GET", mib)
```

The bbox is a 0.5° square centred on the tile's `bbox`; each vintage's item
id is the first May scene of the tile (`ORDER BY datetime LIMIT 1`). The
admit counts come from `parquet_metadata(f)`: intersect the `row_group_id`s
whose `_month` `stats_min/max` bracket the month with those whose
`s2:mgrs_tile` `stats_min/max` bracket the tile; `geo_bbox` on the `geometry`
row gives the spatial equivalent. Three repetitions per cell, median
reported; every cold run was a new `duckdb.connect()`.
