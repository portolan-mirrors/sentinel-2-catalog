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

const rangeGet = async (url, start, len) => {
  const res = await fetch(url, { headers: { Range: `bytes=${start}-${start + len - 1}` } });
  // 200 means the server ignored the Range header; the whole part must never
  // be pulled to answer a search.
  if (res.status !== 206) throw new Error(`range read of ${url} got HTTP ${res.status}`);
  return res.arrayBuffer();
};

// One footer fetch per part per session. The 8-byte tail names the footer
// length, and its Content-Range names the file size; the footer follows as
// one exact suffix read. ~2 sequential requests, then the parsed metadata
// (and the file size) are cached for every later search.
const metadataCache = new Map();
const partMeta = (url) => {
  if (!metadataCache.has(url)) {
    metadataCache.set(url, (async () => {
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

// The groups whose tile-column statistics cannot exclude `tile`. A group
// without statistics (nothing guarantees a live part carries them) is
// admitted rather than skipped: correctness over bytes.
function admittedGroups(meta, tileColumn, tile) {
  return meta.groups.filter((g) => {
    const chunk = g.chunks.find((c) => c.column === tileColumn);
    if (!chunk) return false;
    const min = decodeStat(chunk.stats?.min_value);
    const max = decodeStat(chunk.stats?.max_value);
    if (min == null || max == null) return true;
    return min <= tile && tile <= max;
  });
}

// An AsyncBuffer over prefetched byte regions. Everything hyparquet asks for
// is already in a region; a miss falls through to the network so a decode
// never fails, and is counted so the plan can say it happened.
function regionBuffer(url, size, regions, tally) {
  return {
    byteLength: size,
    async slice(start, end) {
      for (const r of regions) {
        if (start >= r.off && end <= r.off + r.buf.byteLength) {
          return r.buf.slice(start - r.off, end - r.off);
        }
      }
      tally.misses += 1;
      const buf = await rangeGet(url, start, end - start);
      tally.gets += 1;
      tally.bytes += buf.byteLength;
      return buf;
    },
  };
}

// The search over one part: admit groups, prefetch the needed chunks, decode
// each admitted group, keep the tile's rows. Returns raw decoded rows.
async function searchPart(url, tileColumn, tile, tally) {
  const meta = await partMeta(url);
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
      const buf = await rangeGet(url, job.off, job.len);
      tally.gets += 1;
      tally.bytes += buf.byteLength;
      regions.push({ off: job.off, buf });
    }
  }));
  const file = regionBuffer(url, meta.size, regions, tally);
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
    + `  ${tally.parts} part(s) held ${tile}, ${tally.groups} row group(s) admitted by the`
    + ` footer's ${tileColumn} statistics\n`
    + `  ${tally.gets} parallel range GETs, ${(tally.bytes / 1024).toFixed(0)} KiB`
    + ` (footers cached per session), ${secs} s`
    + (tally.misses ? `\n  ${tally.misses} read(s) fell outside the prefetched chunks` : "");
  return { rows, plan };
}
