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

const rangeGet = async (url, start, len, expectSize) => {
  const res = await fetch(url, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
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

// One metadata fetch per part per session: the sidecar when the part has
// one, else the footer. On the footer path the 8-byte tail names the footer
// length, and its Content-Range names the file size; the footer follows as
// one exact suffix read. ~2 sequential requests, then the parsed metadata
// (and the file size) are cached for every later search.
const metadataCache = new Map();
const noSidecar = new Set();
const partMeta = (url) => {
  if (!metadataCache.has(url)) {
    metadataCache.set(url, (async () => {
      const sidecar = noSidecar.has(url) ? null
        : await sidecarMeta(url).catch(() => null);
      if (sidecar) return { ...sidecar, fromSidecar: true };
      const tail = await fetch(url, { headers: { Range: "bytes=-8" } });
      if (tail.status !== 206) throw new Error(`range read of ${url} got HTTP ${tail.status}`);
      const size = Number(tail.headers.get("content-range")?.split("/")[1]);
      const tailBuf = await tail.arrayBuffer();
      if (!Number.isFinite(size) || tailBuf.byteLength !== 8) throw new Error(`no usable Content-Range from ${url}`);
      const footerLen = new DataView(tailBuf).getUint32(0, true) + 8;
      const footer = await rangeGet(url, size - footerLen, footerLen);
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
      return { url, size, footerOff: size - footerLen, footer, metadata, groups };
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
async function searchPart(url, tileColumn, tile, tally) {
  const meta = await partMeta(url);
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

// The full search, shaped exactly like the DuckDB query it replaces:
// tile, UTC day window, cloud ceiling, coverage floor; ORDER BY cloud, id;
// LIMIT 30. Rows come back in the projection runQuery always handled
// ({id, ts, cloud, thumbnail_url, bbox, baseline}), plus a `plan` the page
// can print in place of the SQL.
export async function sceneSearch({ urls, tileColumn, tile, d0, d1, cc, cov }) {
  const tally = { parts: 0, groups: 0, gets: 0, bytes: 0, misses: 0 };
  const t0 = performance.now();
  const raw = (await Promise.all(urls.map((u) => searchPart(u, tileColumn, tile, tally)))).flat();
  const lo = new Date(`${d0}T00:00:00Z`);
  const hi = new Date(`${d1}T23:59:59.999Z`);
  const rows = raw
    .filter((r) => r.datetime >= lo && r.datetime <= hi
      && Number(r["eo:cloud_cover"]) <= cc
      && (cov <= 0 || 100 - Number(r["s2:nodata_pixel_percentage"]) >= cov))
    .sort((a, b) => Number(a["eo:cloud_cover"]) - Number(b["eo:cloud_cover"])
      || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    .slice(0, 30)
    .map((r) => ({
      id: r.id,
      ts: r.datetime.toISOString().slice(0, 19) + "Z",
      cloud: Number(r["eo:cloud_cover"]),
      thumbnail_url: r.thumbnail_url,
      bbox: Array.from(r.bbox ?? []),
      baseline: r["s2:processing_baseline"],
    }));
  const secs = ((performance.now() - t0) / 1000).toFixed(1);
  const plan = `hyparquet range-read plan (no SQL engine, no API):\n`
    + `  ${tally.parts} part(s) held ${tile}, ${tally.groups} row group(s) admitted by their`
    + ` ${tileColumn} ranges\n`
    + `  ${tally.gets} parallel range GETs, ${(tally.bytes / 1024).toFixed(0)} KiB`
    + ` (footers cached per session), ${secs} s`
    + (tally.misses ? `\n  ${tally.misses} read(s) fell outside the prefetched chunks` : "");
  return { rows, plan };
}

// Warm a part before the first search needs it: resolve its metadata
// (sidecar or footer) in the background and swallow the failure — the
// search itself will surface it. app.js calls this for the parts of the
// window on screen, so the first click finds the metadata already cached.
export function warmPart(url) {
  partMeta(url).catch(() => {});
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
export async function keyedRows({ url, keyColumn, key, columns }) {
  const meta = await partMeta(url);
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
