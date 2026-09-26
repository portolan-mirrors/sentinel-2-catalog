// The scene search, off DuckDB and onto hyparquet (docs/
// search-latency-experiments.md, issue #9 amendment 2). DuckDB-WASM's httpfs
// runs a query as a mostly sequential chain — HEAD, footer, a bloom-filter
// read and per-column reads for every admitted row group, then a second
// late-materialization pass — which is 74-122 range GETs and 6-30 s on the
// measured path. This module reads the same parts with the request chain the
// layout actually needs: the parquet footer once per part per session, then
// per search the column chunks of the admitted row groups, all fetched in
// parallel. On the tile-major C1 parts a search is one admitted group and
// ~8 parallel GETs (~165 KiB), measured 1.1-1.6 s against the live bucket.
//
// Two things about *how* those requests are issued turned out to matter more
// than the layout they read (docs/c1-layout-experiments.md, sections A and B):
// every range read is `cache: "no-store"`, so Chrome does not serialise the
// concurrent reads of one part behind its cache lock (2.1 s -> 0.4 s for the
// same eight reads), and the footer path resolves in one speculative tail read
// rather than an 8-byte length read followed by the footer.
//
// hyparquet decodes the chunks; hyparquet-compressors carries the zstd the
// parts are written with. Both are small pure-JS ESM bundles, pinned like
// the page's other CDN imports.
import { parquetMetadata, parquetReadObjects } from "https://cdn.jsdelivr.net/npm/hyparquet@1.31.1/+esm";
import { compressors } from "https://cdn.jsdelivr.net/npm/hyparquet-compressors@1.1.2/+esm";

// The columns a search decodes — the card fields plus the filter columns.
// `assets` (half the bytes of a part) is never among them.
const SEARCH_COLUMNS = ["id", "datetime", "eo:cloud_cover",
  "s2:nodata_pixel_percentage", "thumbnail_url", "bbox", "s2:processing_baseline"];

// At most this many chunk fetches in flight per search. An HTTP/2 connection
// multiplexes them; the cap only keeps a many-group search (a live part with
// no tile order) from queueing hundreds of streams at once.
const MAX_IN_FLIGHT = 24;

// Every range read is `cache: "no-store"`, and that is the single biggest
// lever in this module (docs/c1-layout-experiments.md section A). Chrome
// serialises concurrent requests for one URL behind its HTTP cache lock —
// only one of them may write the entry — so the eight column-chunk reads a
// search issues to one part arrive in a staircase, ~260 ms apiece, 2.1 s for
// 160 KiB. `no-store` takes the request out of the cache entirely and the
// same eight reads of the same object finish in 0.40-0.47 s, measured:
// as good as giving each read its own URL, without making each read its own
// CDN cache key. It sends no extra request header, so the object, the bytes
// and the edge cache entry are exactly what they were.
const rangeGet = async (url, start, len, expectSize) => {
  const res = await fetch(url, { cache: "no-store",
    headers: { Range: `bytes=${start}-${start + len - 1}` } });
  // 200 means the server ignored the Range header; the whole part must never
  // be pulled to answer a search.
  if (res.status !== 206) throw new Error(`range read of ${url} got HTTP ${res.status}`);
  // Every 206 carries the file's true length in Content-Range, so checking
  // it against the caller's expectation is free. A mismatch means the part
  // was rebuilt after its sidecar: the error routes into searchPart's
  // footer retry before a byte is decoded.
  if (expectSize !== undefined) {
    const total = Number(res.headers.get("content-range")?.split("/")[1]);
    if (Number.isFinite(total) && total !== expectSize) {
      throw new Error(`the part is ${total} bytes but its sidecar says ${expectSize} — the sidecar is stale`);
    }
  }
  return res.arrayBuffer();
};

// The BigInt revival for sidecar numbers: hyparquet's own parse returns
// thrift i64 fields as BigInt, and its readers expect the same shapes back.
const big = (v) => (v === undefined || v === null ? undefined : BigInt(v));

// A part's sidecar (<stem>.idx.json, tools/make_search_sidecar.mjs): the
// slice of the footer the search uses, published beside the part. ~100 KB
// gzip-encoded on the wire against the 7.5 MB footer of a Collection 1
// year part, which is the whole cost of the first search on a year. The
// metadata rebuilt from it carries only the search columns, so hyparquet
// reads the part as if it were an eight-column file; the byte offsets are
// absolute, so the reads land exactly where the footer would send them.
async function sidecarMeta(url) {
  const res = await fetch(url.replace(/\.parquet$/, ".idx.json"));
  if (!res.ok) return null;
  const sc = await res.json();
  if (sc.v !== 1 || !Array.isArray(sc.groups)) return null;
  let row = 0;
  const rowGroups = [];
  const groups = [];
  for (const g of sc.groups) {
    const columns = g.columns.map((c) => ({
      file_offset: 0n,
      meta_data: { ...c,
        num_values: big(c.num_values),
        total_compressed_size: big(c.total_compressed_size),
        total_uncompressed_size: big(c.total_uncompressed_size),
        data_page_offset: big(c.data_page_offset),
        dictionary_page_offset: big(c.dictionary_page_offset),
      },
    }));
    rowGroups.push({ num_rows: big(g.num_rows), columns,
      total_byte_size: columns.reduce((a, c) => a + c.meta_data.total_compressed_size, 0n) });
    groups.push({ row0: row, row1: row + g.num_rows,
      tileMin: g.tile_min, tileMax: g.tile_max,
      chunks: g.columns.map((c) => ({ column: c.path_in_schema[0],
        off: Number(c.dictionary_page_offset ?? c.data_page_offset),
        len: Number(c.total_compressed_size) })) });
    row += g.num_rows;
  }
  const metadata = { version: 2, created_by: "sidecar", num_rows: big(sc.num_rows),
    schema: sc.schema, row_groups: rowGroups, metadata_length: 0 };
  return { url, size: sc.size, footerOff: sc.size, footer: new ArrayBuffer(0),
    metadata, groups };
}

// How much of a part's tail to read speculatively on the footer path. The
// 8-byte trailer at the very end names the footer's length, so reading only
// those 8 bytes costs a second request for the footer itself — two sequential
// round trips where one suffices. 64 KiB holds the trailer *and* the whole
// footer of every partitioned part measured (13-88 KB), and a footer that
// does not fit costs exactly the second request the old path always paid.
const TAIL_BYTES = 64 * 1024;

// One metadata fetch per part per session: the sidecar when the part has one,
// else one speculative tail read (its Content-Range names the file size, its
// last 8 bytes the footer length, and the footer is normally already inside
// it). `sidecars: false` from the caller — a collection that publishes none —
// drops the 404 probe, leaving a single request on the footer path too. The
// parsed metadata and the file size are then cached for every later search.
const metadataCache = new Map();
const noSidecar = new Set();
const partMeta = (url, sidecars = true) => {
  if (!metadataCache.has(url)) {
    if (!sidecars) noSidecar.add(url);
    metadataCache.set(url, (async () => {
      const sidecar = noSidecar.has(url) ? null
        : await sidecarMeta(url).catch(() => null);
      if (sidecar) return { ...sidecar, fromSidecar: true };
      const tail = await fetch(url, { cache: "no-store",
        headers: { Range: `bytes=-${TAIL_BYTES}` } });
      // 200 means the server ignored the Range header, as in rangeGet.
      if (tail.status !== 206) throw new Error(`range read of ${url} got HTTP ${tail.status}`);
      const size = Number(tail.headers.get("content-range")?.split("/")[1]);
      const tailBuf = await tail.arrayBuffer();
      if (!Number.isFinite(size) || tailBuf.byteLength < 8) throw new Error(`no usable Content-Range from ${url}`);
      const footerLen = new DataView(tailBuf).getUint32(tailBuf.byteLength - 8, true) + 8;
      if (footerLen > size) throw new Error(`${url} names a ${footerLen}-byte footer in ${size} bytes`);
      const footerOff = size - footerLen;
      // A part smaller than TAIL_BYTES comes back whole, so this covers it.
      const footer = footerLen <= tailBuf.byteLength
        ? tailBuf.slice(tailBuf.byteLength - footerLen)
        : await rangeGet(url, footerOff, footerLen);
      const metadata = parquetMetadata(footer);
      // Row offset and per-column chunk ranges per group, laid out once.
      let row = 0;
      const groups = metadata.row_groups.map((g) => {
        const chunks = [];
        for (const c of g.columns) {
          const m = c.meta_data;
          const off = Number(m.dictionary_page_offset ?? m.data_page_offset);
          chunks.push({ column: m.path_in_schema[0], off, len: Number(m.total_compressed_size),
            stats: m.statistics });
        }
        const out = { row0: row, row1: row + Number(g.num_rows), chunks };
        row += Number(g.num_rows);
        return out;
      });
      return { url, size, footerOff, footer, metadata, groups };
    })());
    // A failed footer read must not poison the cache for the next click.
    metadataCache.get(url).catch(() => metadataCache.delete(url));
  }
  return metadataCache.get(url);
};

const decodeStat = (v) => (typeof v === "string" ? v : v == null ? null : new TextDecoder().decode(v));

// The groups whose tile range cannot exclude `tile` — the sidecar carries
// the range per group, the footer path reads it off the tile column's
// statistics. A group without either (nothing guarantees a live part
// carries statistics) is admitted rather than skipped: correctness over
// bytes.
function admittedGroups(meta, tileColumn, tile) {
  return meta.groups.filter((g) => {
    let min, max;
    if (g.tileMin !== undefined) {
      min = g.tileMin; max = g.tileMax;
    } else {
      const chunk = g.chunks.find((c) => c.column === tileColumn);
      if (!chunk) return false;
      min = decodeStat(chunk.stats?.min_value);
      max = decodeStat(chunk.stats?.max_value);
    }
    if (min == null || max == null) return true;
    return min <= tile && tile <= max;
  });
}

// An AsyncBuffer over prefetched byte regions. Everything hyparquet asks for
// is already in a region; a miss falls through to the network so a decode
// never fails, and is counted so the plan can say it happened.
function regionBuffer(url, size, regions, tally, expectSize) {
  return {
    byteLength: size,
    expectSize,
    async slice(start, end) {
      for (const r of regions) {
        if (start >= r.off && end <= r.off + r.buf.byteLength) {
          return r.buf.slice(start - r.off, end - r.off);
        }
      }
      tally.misses += 1;
      const buf = await rangeGet(url, start, end - start, this.expectSize);
      tally.gets += 1;
      tally.bytes += buf.byteLength;
      return buf;
    },
  };
}

// The search over one part: admit groups, prefetch the needed chunks, decode
// each admitted group, keep the tile's rows. Returns raw decoded rows.
// A decode failure on sidecar-built metadata retries once on the footer
// path, so a stale or malformed sidecar degrades to the slow path instead
// of failing the search.
async function searchPart(url, tileColumn, tile, tally, sidecars) {
  const meta = await partMeta(url, sidecars);
  try {
    return await searchPartWith(meta, url, tileColumn, tile, tally);
  } catch (err) {
    if (!meta.fromSidecar) throw err;
    console.warn(`sidecar decode failed for ${url} — retrying via the footer: ${err.message}`);
    metadataCache.delete(url);
    noSidecar.add(url);
    return searchPartWith(await partMeta(url), url, tileColumn, tile, tally);
  }
}

async function searchPartWith(meta, url, tileColumn, tile, tally) {
  const groups = admittedGroups(meta, tileColumn, tile);
  if (!groups.length) return [];
  tally.parts += 1;
  tally.groups += groups.length;
  const columns = [tileColumn, ...SEARCH_COLUMNS];
  const jobs = groups.flatMap((g) => g.chunks.filter((c) => columns.includes(c.column)));
  const regions = [{ off: meta.footerOff, buf: meta.footer }];
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(MAX_IN_FLIGHT, jobs.length) }, async () => {
    while (next < jobs.length) {
      const job = jobs[next];
      next += 1;
      const buf = await rangeGet(url, job.off, job.len,
        meta.fromSidecar ? meta.size : undefined);
      tally.gets += 1;
      tally.bytes += buf.byteLength;
      regions.push({ off: job.off, buf });
    }
  }));
  const file = regionBuffer(url, meta.size, regions, tally,
    meta.fromSidecar ? meta.size : undefined);
  const parts = await Promise.all(groups.map((g) => parquetReadObjects({
    file, metadata: meta.metadata, compressors, columns, rowStart: g.row0, rowEnd: g.row1,
  })));
  return parts.flat().filter((r) => r[tileColumn] === tile);
}

// The raw read: every row of `tile` in the given parts, projected to the
// card fields, sorted by time. sceneRows applies no date, cloud or
// coverage filter and no limit; the page filters in memory so a slider
// drag costs no network read.
export async function sceneRows({ urls, tileColumn, tile, sidecars = true }) {
  const tally = { parts: 0, groups: 0, gets: 0, bytes: 0, misses: 0 };
  const t0 = performance.now();
  const raw = (await Promise.all(
    urls.map((u) => searchPart(u, tileColumn, tile, tally, sidecars)))).flat();
  const rows = raw.map((r) => {
    const t = r.datetime instanceof Date ? r.datetime.getTime() : Date.parse(r.datetime);
    const nodata = Number(r["s2:nodata_pixel_percentage"]);
    return {
      id: r.id,
      ts: new Date(t).toISOString().slice(0, 19) + "Z",
      day: new Date(t).toISOString().slice(0, 10),
      t,
      cloud: Number(r["eo:cloud_cover"]),
      cover: Number.isFinite(nodata) ? 100 - nodata : null,
      thumbnail_url: r.thumbnail_url,
      bbox: Array.from(r.bbox ?? []),
      baseline: r["s2:processing_baseline"],
    };
  }).sort((a, b) => a.t - b.t || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const secs = ((performance.now() - t0) / 1000).toFixed(1);
  const plan = `hyparquet range-read plan (no SQL engine, no API):\n`
    + `  ${tally.parts} part(s) held ${tile}, ${tally.groups} row group(s) admitted by their`
    + ` ${tileColumn} ranges\n`
    + `  ${tally.gets} parallel range GETs, ${(tally.bytes / 1024).toFixed(0)} KiB`
    + ` (footers cached per session), ${secs} s`
    + (tally.misses ? `\n  ${tally.misses} read(s) fell outside the prefetched chunks` : "");
  return { rows, plan };
}

// The full search, shaped exactly like the DuckDB query it replaces:
// tile, UTC day window, cloud ceiling, coverage floor; ORDER BY cloud, id;
// LIMIT 30. The harnesses and check_app.py pin this contract.
export async function sceneSearch({ urls, tileColumn, tile, d0, d1, cc, cov,
                                    sidecars = true }) {
  const { rows: all, plan } = await sceneRows({ urls, tileColumn, tile, sidecars });
  const lo = Date.parse(`${d0}T00:00:00Z`);
  const hi = Date.parse(`${d1}T23:59:59.999Z`);
  const rows = all
    .filter((r) => r.t >= lo && r.t <= hi && r.cloud <= cc
      && (cov <= 0 || (r.cover !== null && r.cover >= cov)))
    .sort((a, b) => a.cloud - b.cloud || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    .slice(0, 30)
    .map((r) => ({ id: r.id, ts: r.ts, cloud: r.cloud,
      thumbnail_url: r.thumbnail_url, bbox: r.bbox, baseline: r.baseline }));
  return { rows, plan };
}

// Warm a part before the first search needs it: resolve its metadata
// (sidecar or footer) in the background and swallow the failure — the
// search itself will surface it. app.js calls this for the parts of the
// window on screen, so the first click finds the metadata already cached.
export function warmPart(url, sidecars = true) {
  partMeta(url, sidecars).catch(() => {});
}

// The stats reads share the machinery above, so the whole page runs on one
// parquet reader and DuckDB-WASM is not loaded at all.

// A whole in-memory parquet file (a fetched timeline or month slice),
// decoded to row objects. `columns` narrows the decode.
export function readTable(buf, columns) {
  return parquetReadObjects({ file: buf, compressors, columns });
}

// The rows of one key from a key-sorted remote parquet file: footer (or
// sidecar) once per session via partMeta, groups admitted by the key
// column's ranges, the named columns' chunks fetched in parallel, rows
// filtered to the key. This is timelineFor's per-tile history read over
// stats/mgrs-monthly.parquet, and it works for any key-sorted table.
export async function keyedRows({ url, keyColumn, key, columns,
                                 sidecars = true }) {
  const meta = await partMeta(url, sidecars);
  const groups = admittedGroups(meta, keyColumn, key);
  if (!groups.length) return [];
  const wanted = [...new Set([keyColumn, ...columns])];
  const jobs = groups.flatMap((g) => g.chunks.filter((c) => wanted.includes(c.column)));
  const tally = { gets: 0, bytes: 0, misses: 0 };
  const regions = [{ off: meta.footerOff, buf: meta.footer }];
  await Promise.all(jobs.map(async (job) => {
    const buf = await rangeGet(url, job.off, job.len);
    regions.push({ off: job.off, buf });
  }));
  const file = regionBuffer(url, meta.size, regions, tally);
  const parts = await Promise.all(groups.map((g) => parquetReadObjects({
    file, metadata: meta.metadata, compressors, columns: wanted,
    rowStart: g.row0, rowEnd: g.row1,
  })));
  return parts.flat().filter((r) => r[keyColumn] === key);
}
