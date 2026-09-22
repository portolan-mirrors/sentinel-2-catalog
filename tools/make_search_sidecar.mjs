// Build a search sidecar (<stem>.idx.json) for a published part.
//
// The explorer's scene search (apps/explorer/search.js) needs the parquet
// footer to decode a part's column chunks. A Collection 1 year part's footer
// is ~7.5 MB (825 row groups x 43 columns of thrift), and it is the whole
// cost of the first search on a year. The sidecar carries the slice of the
// footer the search actually uses: the schema subtree and the per-row-group
// chunk metadata of the search columns, plus each group's tile range. That
// is ~2 % of the footer's bytes. The client rebuilds hyparquet metadata from
// it and never fetches the footer; a part without a sidecar falls back to
// the footer path unchanged.
//
// The sidecar derives from the footer alone, so anyone can regenerate or
// audit it, and a rebuild of the part simply regenerates it.
//
// Usage:
//   npm install hyparquet            (once; no other dependency)
//   node tools/make_search_sidecar.mjs <part.parquet | https URL> [more...]
//
// Each output lands beside its input (local) or in the working directory
// (URL), named <stem>.idx.json.
import fs from "node:fs";
import path from "node:path";
import { parquetMetadata } from "hyparquet";

// The columns the search decodes, plus the tile column of either
// collection. A name absent from a part is skipped, so one list serves both.
const KEEP = new Set(["_tile", "s2:mgrs_tile", "id", "datetime",
  "eo:cloud_cover", "s2:nodata_pixel_percentage", "thumbnail_url", "bbox",
  "s2:processing_baseline"]);

async function footerOf(src) {
  if (/^https?:/.test(src)) {
    const tail = await fetch(src, { headers: { Range: "bytes=-8" } });
    if (tail.status !== 206) throw new Error(`${src}: HTTP ${tail.status} for the tail read`);
    const size = Number(tail.headers.get("content-range")?.split("/")[1]);
    const tailBuf = await tail.arrayBuffer();
    const footerLen = new DataView(tailBuf).getUint32(0, true) + 8;
    const res = await fetch(src, { headers: { Range: `bytes=${size - footerLen}-${size - 1}` } });
    if (res.status !== 206) throw new Error(`${src}: HTTP ${res.status} for the footer read`);
    return { footer: await res.arrayBuffer(), size };
  }
  const buf = fs.readFileSync(src);
  const size = buf.byteLength;
  const footerLen = buf.readUInt32LE(size - 8) + 8;
  const footer = buf.buffer.slice(buf.byteOffset + size - footerLen, buf.byteOffset + size);
  return { footer, size };
}

// The schema as a pruned element list: the root (its child count rewritten),
// then each kept top-level field with its whole subtree. Parquet lists
// schema elements depth-first, so a subtree is a contiguous run whose length
// is counted down through num_children.
function pruneSchema(schema) {
  const kept = [];
  let keptTop = 0;
  let i = 1;
  while (i < schema.length) {
    const run = subtreeLength(schema, i);
    if (KEEP.has(schema[i].name)) {
      kept.push(...schema.slice(i, i + run));
      keptTop += 1;
    }
    i += run;
  }
  return [{ ...schema[0], num_children: keptTop }, ...kept];
}

function subtreeLength(schema, at) {
  let need = 1;
  let n = 0;
  while (need > 0) {
    need -= 1;
    need += schema[at + n].num_children ?? 0;
    n += 1;
  }
  return n;
}

const plain = (v) => (typeof v === "bigint" ? Number(v) : v);

function sidecarOf(md, size, tileColumn) {
  const groups = md.row_groups.map((g) => {
    const tile = g.columns.find((c) => c.meta_data.path_in_schema[0] === tileColumn);
    const stats = tile.meta_data.statistics ?? {};
    return {
      num_rows: Number(g.num_rows),
      tile_min: stats.min_value ?? stats.min ?? null,
      tile_max: stats.max_value ?? stats.max ?? null,
      columns: g.columns
        .filter((c) => KEEP.has(c.meta_data.path_in_schema[0]))
        .map((c) => {
          const m = c.meta_data;
          const out = {
            type: m.type, encodings: m.encodings, path_in_schema: m.path_in_schema,
            codec: m.codec, num_values: plain(m.num_values),
            total_compressed_size: plain(m.total_compressed_size),
            total_uncompressed_size: plain(m.total_uncompressed_size),
            data_page_offset: plain(m.data_page_offset),
          };
          if (m.dictionary_page_offset !== undefined) {
            out.dictionary_page_offset = plain(m.dictionary_page_offset);
          }
          return out;
        }),
    };
  });
  return {
    v: 1, size, tile_column: tileColumn,
    num_rows: Number(md.num_rows),
    schema: pruneSchema(md.schema),
    groups,
  };
}

for (const src of process.argv.slice(2)) {
  const { footer, size } = await footerOf(src);
  const md = parquetMetadata(footer);
  const names = new Set(md.schema.map((s) => s.name));
  const tileColumn = names.has("_tile") ? "_tile" : "s2:mgrs_tile";
  const out = sidecarOf(md, size, tileColumn);
  const stem = path.basename(src).replace(/\.parquet$/, "");
  const dst = /^https?:/.test(src) ? `${stem}.idx.json` : src.replace(/\.parquet$/, ".idx.json");
  fs.writeFileSync(dst, JSON.stringify(out));
  console.log(`${dst}: ${md.row_groups.length} groups, `
    + `${(footer.byteLength / 1048576).toFixed(1)} MB footer -> `
    + `${(fs.statSync(dst).size / 1024).toFixed(0)} KB sidecar`);
}
