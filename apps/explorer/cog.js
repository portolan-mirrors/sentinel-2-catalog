// A Sentinel-2 visual (TCI) Cloud-Optimized GeoTIFF on the map, with no
// server in between: deck.gl's TileLayer asks for Web Mercator tiles, and each
// tile is filled by range-reading a window of the COG's best-matching overview
// (geotiff.js) and warping it from the scene's UTM grid into the tile.
//
// The warp is exact at the pixel level in the sense that matters here: every
// output pixel is placed by inverse-mapping its own lon/lat into the COG's
// pixel grid (proj4), through a bilinear interpolation of that mapping across
// a control grid of 16-pixel cells. UTM <-> lon/lat is smooth, so across a
// 16-pixel cell the interpolation error is far below one pixel; the visible
// approximation is the resampling itself (nearest neighbour out of an
// overview whose resolution is between 1x and 2x the tile's), which reads as
// a little aliasing on sharp edges, not as misplacement.
//
// The same warp draws the scene's preview (Task 27): the thumbnail JPEG is
// the TCI over the same UTM square at ~32x, so once the COG's headers give
// the geotransform it is one more overview level, warped whole into a single
// image over the scene's bounds before any tile has been read.
//
// A mid-resolution rung uses the same warp again (Task 25): one whole TCI
// overview level, near 1372 px, is a sharper preview than the thumbnail over
// the identical bounds — the scrub bar upgrades to it while the user flips,
// and a commit still loads the full tiles.
//
// Any other band goes the same way (Task 28): every band is its own COG in
// the scene directory (B02.tif, B08.tif, SCL.tif, ...), on its own 10, 20
// or 60 m grid. A composite reads one window per distinct band, warps each
// into a per-pixel sample plane of the tile, and paints the planes to RGBA
// through the stretch (bands.js). The planes are kept per scene, tile and
// band, so a stretch, curve, gamma or nodata change repaints without a
// single new byte, and a band change fetches only the bands not yet seen.
// The preview of a composite is each band's coarsest overview (the whole
// level, one read per band), warped over the scene bounds and painted the
// same way; those overviews also feed the histograms.
import { fromUrl } from "https://esm.sh/geotiff@3.0.5";
import proj4 from "https://esm.sh/proj4@2.22.0";
import { paintRGBA, paintTables, sampleStats, indexStats, bandsOf } from "./bands.js";
// deck.gl from the pinned dist bundle loaded by index.html (see app.js).
const { TileLayer, BitmapLayer } = window.deck;

const TILE = 256;   // output tile size in pixels
const CELL = 16;    // warp control-grid cell size in output pixels
const PREVIEW = 1024;  // the preview image's long side in pixels
// TCI nodata is 0,0,0 (GDAL_NODATA=0): the swath edge stays see-through.
const cogNodata = (data) => (k) => data[k] === 0 && data[k + 1] === 0 && data[k + 2] === 0;

// Open a COG and read what the warp needs from its base image: the UTM
// projection (from the EPSG geokey — 326NN north, 327NN south), the affine
// origin/resolution, and the overview pyramid. Only the headers are fetched,
// and in one range request: geotiff.js 3 reads exactly the bytes the parser
// asks for unless given a block size (measured: 12 sequential requests of
// 6 to 1024 bytes, ~4.7 s, for five IFDs that all sit in the first 5 KB), so
// the 2.x default of 64 KB blocks is asked for. Tile reads go through the
// same block cache, which merges a window's contiguous blocks into one range.
export async function openCog(href) {
  const tiff = await fromUrl(href, { allowFullFile: false, blockSize: 65536, cacheSize: 100 });
  const count = await tiff.getImageCount();
  const images = [];
  for (let i = 0; i < count; i++) images.push(await tiff.getImage(i));
  const base = images[0];
  const epsg = base.getGeoKeys()?.ProjectedCSTypeGeoKey;
  const series = Math.floor((epsg ?? 0) / 100);
  if (series !== 326 && series !== 327) {
    throw new Error(`EPSG:${epsg} is not a UTM/WGS84 code; only Sentinel-2 grids are supported`);
  }
  const zone = epsg % 100;
  const def = `+proj=utm +zone=${zone}${series === 327 ? " +south" : ""} +datum=WGS84 +units=m +no_defs`;
  const proj = proj4("EPSG:4326", def);
  const [ox, oy] = base.getOrigin();
  const [rx, ry] = base.getResolution();      // ry is negative (north-up)
  const w = base.getWidth(), h = base.getHeight();
  // Overviews carry no georeferencing tags of their own; each is the base
  // image scaled by the width ratio.
  const levels = images.map((image) => ({ image, scale: w / image.getWidth(),
    w: image.getWidth(), h: image.getHeight() }));
  // Lon/lat bounds, from the UTM box edges (sampled, since they curve).
  let west = Infinity, south = Infinity, east = -Infinity, north = -Infinity;
  const corner = (px, py) => {
    const [lon, lat] = proj.inverse([ox + px * rx, oy + py * ry]);
    west = Math.min(west, lon); east = Math.max(east, lon);
    south = Math.min(south, lat); north = Math.max(north, lat);
  };
  for (let i = 0; i <= 8; i++) {
    corner((w * i) / 8, 0); corner((w * i) / 8, h); corner(0, (h * i) / 8); corner(w, (h * i) / 8);
  }
  return { href, epsg, proj, ox, oy, rx, ry, w, h, levels,
    bands: base.getSamplesPerPixel(), bounds: [west, south, east, north] };
}

// The inverse mapping of a W x H output raster, linear in lon/lat over
// `bbox`, into the COG's base-pixel grid: one (nx+1) x (ny+1) control grid of
// base-pixel coordinates (about CELL output pixels apart) and its extent.
function controlGrid(cog, { west, south, east, north }, W, H) {
  const nx = Math.ceil(W / CELL), ny = Math.ceil(H / CELL), N = nx + 1;
  const gx = new Float64Array(N * (ny + 1)), gy = new Float64Array(N * (ny + 1));
  let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
  for (let j = 0; j <= ny; j++) {
    const lat = north - ((north - south) * j) / ny;
    for (let i = 0; i <= nx; i++) {
      const lon = west + ((east - west) * i) / nx;
      const [X, Y] = cog.proj.forward([lon, lat]);
      const px = (X - cog.ox) / cog.rx, py = (Y - cog.oy) / cog.ry;
      gx[j * N + i] = px; gy[j * N + i] = py;
      if (px < minx) minx = px; if (px > maxx) maxx = px;
      if (py < miny) miny = py; if (py > maxy) maxy = py;
    }
  }
  return { gx, gy, nx, ny, minx, miny, maxx, maxy };
}

// Walk a W x H output raster through a control grid over one source raster
// — an overview window, a whole overview, or the thumbnail, interchangeably:
// w x h samples whose pixel (0, 0) is (x0, y0) in a grid of `scale` base
// pixels per sample. Nearest neighbour; the placement is the grid's. For
// each output pixel that lands on a source sample, put(o, s) is called with
// the output pixel index and the source sample index. Every painter below
// (RGBA for the TCI and the thumbnail, one plane per band for composites)
// is this walk with a different put, so the placement cannot differ between
// them.
function warpEach(grid, { w, h, scale, x0, y0 }, W, H, put) {
  const { gx, gy, nx, ny } = grid, N = nx + 1;
  // The grid divides the output evenly, so a cell is CELL px only when the
  // side is a multiple of it (a tile always, the preview's short side not).
  const cw = W / nx, ch = H / ny;
  for (let y = 0; y < H; y++) {
    const fy = (y + 0.5) / ch, j = Math.min(ny - 1, Math.floor(fy)), t = fy - j;
    for (let x = 0; x < W; x++) {
      const fx = (x + 0.5) / cw, i = Math.min(nx - 1, Math.floor(fx)), u = fx - i;
      const a = j * N + i, b = a + 1, c = a + N, d = c + 1;
      const px = (gx[a] * (1 - u) + gx[b] * u) * (1 - t) + (gx[c] * (1 - u) + gx[d] * u) * t;
      const py = (gy[a] * (1 - u) + gy[b] * u) * (1 - t) + (gy[c] * (1 - u) + gy[d] * u) * t;
      const sx = Math.floor(px / scale) - x0, sy = Math.floor(py / scale) - y0;
      if (sx < 0 || sy < 0 || sx >= w || sy >= h) continue;
      put(y * W + x, sy * w + sx);
    }
  }
}

// Paint a W x H ImageData from one interleaved w x h x bands source (the TCI
// or the thumbnail); `nodata(k)` says whether the sample at byte offset k
// stays see-through.
function warp(grid, src, W, H) {
  const { data, bands, nodata } = src;
  const out = new Uint8ClampedArray(W * H * 4);
  warpEach(grid, src, W, H, (o, s) => {
    const k = s * bands;
    if (nodata(k)) return;
    o *= 4;
    out[o] = data[k]; out[o + 1] = data[k + 1]; out[o + 2] = data[k + 2]; out[o + 3] = 255;
  });
  return new ImageData(out, W, H);
}

// One band's samples placed into a W x H plane of floats; NaN where the
// output pixel falls outside the source (off the scene, or off a window
// clipped to the level). The file's nodata value (0 for every Sentinel-2
// band) is carried through as is: whether it is keyed out is the stretch's
// call (bands.js), so the Nodata control can change without a re-warp.
function warpPlane(grid, src, W, H) {
  const { data } = src;
  const plane = new Float32Array(W * H).fill(NaN);
  warpEach(grid, src, W, H, (o, s) => { plane[o] = data[s]; });
  return plane;
}

// The window of the overview whose pixels are closest to (but not coarser
// than) the grid's own — base-image pixels per output pixel, floored to a
// level — as a source raster for the painters above, or null when the grid
// misses the image.
async function readWindow(cog, grid, W, signal) {
  const { minx, miny, maxx, maxy } = grid;
  if (maxx <= 0 || maxy <= 0 || minx >= cog.w || miny >= cog.h) return null;
  const want = Math.max(maxx - minx, maxy - miny) / W;
  let lvl = cog.levels[0];
  for (const l of cog.levels) if (l.scale <= want) lvl = l;
  const s = lvl.scale;
  const x0 = Math.max(0, Math.floor(minx / s)), y0 = Math.max(0, Math.floor(miny / s));
  const x1 = Math.min(lvl.w, Math.ceil(maxx / s) + 1), y1 = Math.min(lvl.h, Math.ceil(maxy / s) + 1);
  if (x1 <= x0 || y1 <= y0) return null;
  const raster = await lvl.image.readRasters({ window: [x0, y0, x1, y1], interleave: true, signal });
  return { data: raster, w: raster.width, h: raster.height, scale: s, x0, y0 };
}

// One Web Mercator tile of the COG as ImageData, or null if the tile does not
// touch the image. The output raster is linear in lon/lat; over one 256-px
// tile that is within a pixel of BitmapLayer's Mercator-linear stretch, so no
// correction is asked for on the deck.gl side.
export async function readCogTile(cog, bbox, signal) {
  const grid = controlGrid(cog, bbox, TILE, TILE);
  const src = await readWindow(cog, grid, TILE, signal);
  if (!src) return null;
  return warp(grid, { ...src, bands: cog.bands, nodata: cogNodata(src.data) }, TILE, TILE);
}

// Which thumbnail pixels are the swath's nodata, as one flag per pixel. The
// thumbnail paints nodata in one flat colour — white on thumbnail.jpg,
// black on everything else: preview.jpg, and the thumbnail.jp2 that 22% of
// the 2018 rows carry (checked on 31UFU partial scenes of every year
// 2018-2026; 2024 has both .jpg names, and the name decides, not the year)
// — and compression smears a fringe of near-that-colour pixels along the
// swath edge. So: every pixel of exactly that colour, plus its immediate
// near-that-colour neighbours (grown once, from every exact match, so the
// growth is bounded to one pixel). Real dark water (black) and bright
// cloud (white) are lost only where they are exactly the flat colour or
// sit next to such a pixel.
function jpegNodataMask(data, w, h, white) {
  const exact = white ? (k) => data[k] >= 250 && data[k + 1] >= 250 && data[k + 2] >= 250
    : (k) => data[k] === 0 && data[k + 1] === 0 && data[k + 2] === 0;
  const near = white ? (k) => data[k] >= 235 && data[k + 1] >= 235 && data[k + 2] >= 235
    : (k) => data[k] <= 24 && data[k + 1] <= 24 && data[k + 2] <= 24;
  const seed = new Uint8Array(w * h), mask = new Uint8Array(w * h);
  for (let i = 0; i < w * h; i++) seed[i] = exact(i * 4) ? 1 : 0;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const i = y * w + x;
      if (seed[i]) { mask[i] = 1; continue; }
      if (!near(i * 4)) continue;
      for (let dy = -1; dy <= 1 && !mask[i]; dy++) {
        for (let dx = -1; dx <= 1; dx++) {
          const yy = y + dy, xx = x + dx;
          if (yy >= 0 && yy < h && xx >= 0 && xx < w && seed[yy * w + xx]) { mask[i] = 1; break; }
        }
      }
    }
  }
  return mask;
}

// The scene's thumbnail (an ImageBitmap of the JPEG next to the COG) warped
// whole over `cog.bounds` as one ImageData, PREVIEW px on the long side, for
// a BitmapLayer with `_imageCoordinateSystem: "lnglat"` — that spans a whole
// degree of latitude, where the Mercator-linear default would misplace the
// middle by a couple of pixels. The JPEG is treated as one more overview
// level of the COG, its pixel size the base's scaled by the width ratio;
// null if the thumbnail is not the COG's shape. `white` says the
// thumbnail's nodata colour (see jpegNodataMask).
// The preview's pixel size: PREVIEW on the long side by the scene's
// on-screen shape (longitude shrinks by cos(lat)).
// `long` is the long side asked for — PREVIEW for a thumbnail warp, the
// overview level's own width for the mid-resolution rung below.
function previewSize([west, south, east, north], long = PREVIEW) {
  const aspect = ((east - west) * Math.cos(((south + north) / 2) * Math.PI / 180)) / (north - south);
  return [Math.round(aspect >= 1 ? long : long * aspect),
    Math.round(aspect >= 1 ? long / aspect : long)];
}
export function previewImage(cog, bitmap, { white = false } = {}) {
  const [west, south, east, north] = cog.bounds;
  const [W, H] = previewSize(cog.bounds);
  // One scale serves both axes, so the thumbnail must have the COG's
  // shape (both are square; a thumbnail cut to another shape would be
  // stretched into the wrong place). More than a pixel off: no preview.
  if (Math.abs(bitmap.width * cog.h - bitmap.height * cog.w) > bitmap.width) return null;
  const canvas = document.createElement("canvas");
  canvas.width = bitmap.width; canvas.height = bitmap.height;
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  ctx.drawImage(bitmap, 0, 0);
  const { data } = ctx.getImageData(0, 0, bitmap.width, bitmap.height);
  const mask = jpegNodataMask(data, bitmap.width, bitmap.height, white);
  const grid = controlGrid(cog, { west, south, east, north }, W, H);
  return warp(grid, { data, w: bitmap.width, h: bitmap.height, bands: 4,
    scale: cog.w / bitmap.width, x0: 0, y0: 0, nodata: (k) => mask[k >> 2] === 1 }, W, H);
}

// The rung between the thumbnail preview and the full tiles (Task 25). The
// TCI's own overview pyramid is the ladder: each level is independently
// range-readable, so one of them read WHOLE — the same read sceneOverview
// makes per band, and the same warp previewImage ends with — is a sharper
// preview over the very same bounds, placed by the same control grid. The
// only difference from previewImage is the source: these are the COG's own
// pixels, so nodata is the TCI's black (cogNodata) and not a JPEG's smeared
// flat colour, and no shape check is needed because the level IS the image.
//
// The level: the smallest whose long side reaches `targetPx`, or the largest
// there is when none does. Budget — a 10,980 px TCI's pyramid is 5490 / 2745
// / 1372 / 686 / 343, so the default 1200 lands on the 1372 level: a few
// hundred KB of range reads (the headers are already in the block cache from
// the scrub preload's openCog) for a 1372 x 1372 RGBA result of ~7.5 MB.
// Asking for more than a rung's worth steps to the next level up and
// multiplies both numbers by four (1372 -> 2745), so a caller that raises
// targetPx must re-budget its own resident cap (app.js's SCRUB_HD_MAX).
const HD_TARGET = 1200;
function overviewAtLeast(cog, targetPx) {
  let best = null, largest = null;
  for (const l of cog.levels) {
    const long = Math.max(l.w, l.h);
    if (long >= targetPx && (!best || long < Math.max(best.w, best.h))) best = l;
    if (!largest || long > Math.max(largest.w, largest.h)) largest = l;
  }
  return best ?? largest;
}
export async function tciOverviewImage(cog, targetPx = HD_TARGET, signal) {
  const lvl = overviewAtLeast(cog, targetPx);
  if (!lvl) return null;
  const [west, south, east, north] = cog.bounds;
  // The output long side is the level's own, so the warp neither upsamples
  // the level nor throws away pixels it just paid for.
  const [W, H] = previewSize(cog.bounds, Math.max(lvl.w, lvl.h));
  const raster = await lvl.image.readRasters({ interleave: true, signal });
  const grid = controlGrid(cog, { west, south, east, north }, W, H);
  return warp(grid, { data: raster, w: raster.width, h: raster.height,
    bands: cog.bands, scale: lvl.scale, x0: 0, y0: 0, nodata: cogNodata(raster) }, W, H);
}

// The deck.gl layer for an opened COG: one TileLayer, clipped to the scene's
// footprint so no tile outside it is ever requested. `events` may carry the
// TileLayer's onViewportLoad / onTileError (see showOnMap in app.js).
export function cogTileLayer(cog, id = "cog", events = {}) {
  return new TileLayer({
    id,
    tileSize: TILE,
    minZoom: 4,
    maxZoom: 15,
    extent: cog.bounds,
    maxRequests: 6,
    refinementStrategy: "no-overlap",
    getTileData: ({ bbox, signal }) => readCogTile(cog, bbox, signal),
    renderSubLayers: (props) => {
      const { west, south, east, north } = props.tile.bbox;
      return props.data ? new BitmapLayer(props, {
        data: null, image: props.data, bounds: [west, south, east, north],
      }) : null;
    },
    ...events,
  });
}

// ---------------------------------------------------------------------------
// Bands (Task 28). A scene is a directory of band COGs; this is the per-scene
// memory of what has been opened, read and warped, so nothing is fetched
// twice while the scene is on the map: `cogs` band -> openCog promise,
// `overviews` band -> the coarsest overview read whole (preview and
// histogram), `previewPlanes` band -> that overview warped over the scene,
// `planes` "z/x/y/band" -> a tile's warped plane (bounded; the oldest go
// first). A band that fails to open keeps its rejection, so twenty tiles do
// not each retry a 404. All is dropped with the scene (app.js).
// ---------------------------------------------------------------------------
const PREVIEW_MIN = 256;   // the preview overview's long side, at least
const PLANE_CACHE = 400;   // tiles x bands kept per scene (~100 MB of Float32)
export const bandHref = (dir, band) => `${dir}/${band}.tif`;

export function openScene(id, dir) {
  return { id, dir, bounds: null, cogs: new Map(), overviews: new Map(),
    previewPlanes: new Map(), planes: new Map() };
}

// One band's COG, opened once per scene. The first to open sets the scene's
// bounds: every band covers the same 109.8 km square (10980 x 10 m, 5490 x
// 20 m, 1830 x 60 m), so any band's bounds are the scene's.
export function sceneCog(scene, band) {
  let p = scene.cogs.get(band);
  if (!p) {
    p = openCog(bandHref(scene.dir, band)).then((cog) => { scene.bounds ??= cog.bounds; return cog; });
    scene.cogs.set(band, p);
  }
  return p;
}

// One band's coarsest overview whose long side is at least PREVIEW_MIN px,
// read whole — one range read per band (a 10 m band's 16x level is 686 px;
// a 60 m band's 4x, 457) — with its histogram and percentiles. Cached.
export function sceneOverview(scene, band) {
  let p = scene.overviews.get(band);
  if (!p) {
    p = sceneCog(scene, band).then(async (cog) => {
      let lvl = cog.levels[0];
      for (const l of cog.levels) if (Math.max(l.w, l.h) >= PREVIEW_MIN) lvl = l;
      const raster = await lvl.image.readRasters({ interleave: true });
      // Stats on first use: the sort is for a histogram, and SCL has none.
      return { cog, band, data: raster, w: raster.width, h: raster.height, scale: lvl.scale,
        x0: 0, y0: 0, get stats() { return this._stats ??= sampleStats(raster, 0); } };
    });
    scene.overviews.set(band, p);
  }
  return p;
}

// The stats of an index over two overviews already read (null when a band
// is missing or the two levels differ in size, which the fixed pairs — both
// 10 m — never do).
export function sceneIndexStats(scene, a, b, offset) {
  const oa = scene.overviews.get(a)?.value, ob = scene.overviews.get(b)?.value;
  return indexStats(oa?.data, ob?.data, offset, 0);
}

// The band's overview warped over the scene's bounds at the preview size;
// null when that band failed. Sync: the overview must have been awaited
// (sceneOverview) and is looked up by its settled value.
function previewPlane(scene, band, W, H) {
  if (scene.previewPlanes.has(band)) return scene.previewPlanes.get(band);
  const ov = scene.overviews.get(band)?.value ?? null;
  const plane = ov ? warpPlane(controlGrid(ov.cog, boundsOf(scene.bounds), W, H), ov, W, H) : null;
  scene.previewPlanes.set(band, plane);
  return plane;
}
const boundsOf = ([west, south, east, north]) => ({ west, south, east, north });

// Await each band's overview, remembering the settled value (or the error)
// on the promise so the sync painters can look it up; returns the bands
// that failed with their errors.
export async function loadOverviews(scene, bands) {
  const failed = [];
  await Promise.all(bands.map(async (band) => {
    const p = sceneOverview(scene, band);
    try { p.value = await p; } catch (err) { p.error = err; failed.push([band, err]); }
  }));
  return failed;
}

// The preview of a composite: the bands' overviews warped and painted
// through the spec. Needs loadOverviews first; null if no band came.
export function bandPreviewImage(scene, spec) {
  if (!scene.bounds) return null;
  const [W, H] = previewSize(scene.bounds);
  const planes = {};
  let any = false;
  for (const band of bandsOf(spec)) { planes[band] = previewPlane(scene, band, W, H); any ||= !!planes[band]; }
  return any ? paintRGBA(planes, spec, W, H) : null;
}

// A tile's planes for the given bands, from the cache or one window read per
// band (in parallel). A band that cannot be opened is null (its channel
// paints black, the others show); a tile off the image is null throughout
// and the tile is skipped.
async function bandTilePlanes(scene, bands, { index, bbox, signal }) {
  const key = `${index.z}/${index.x}/${index.y}`;
  const planes = {};
  await Promise.all(bands.map(async (band) => {
    const ck = `${key}/${band}`;
    if (scene.planes.has(ck)) { planes[band] = scene.planes.get(ck); return; }
    let cog;
    try { cog = await sceneCog(scene, band); } catch { planes[band] = null; return; }
    const grid = controlGrid(cog, bbox, TILE, TILE);
    const src = await readWindow(cog, grid, TILE, signal);
    const plane = src ? warpPlane(grid, src, TILE, TILE) : null;
    if (scene.planes.size >= PLANE_CACHE) scene.planes.delete(scene.planes.keys().next().value);
    scene.planes.set(ck, plane);
    planes[band] = plane;
  }));
  if (!Object.values(planes).some(Boolean)) return null;
  return { planes, rgba: null, styleKey: null, byteLength: bands.length * TILE * TILE * 4 };
}

// The deck.gl layer for a composite of one scene. The layer id carries the
// band set, so a band change is a fresh tileset (its tiles come out of the
// plane cache where they were seen before) under a fresh preview; a style
// change keeps the id and passes a new styleKey through updateTriggers,
// which makes deck re-run renderSubLayers per tile without refetching
// (TileLayer nulls each tile's sublayers on any prop change that is not
// getTileData's). The RGBA is painted lazily per tile and kept on the tile
// data until the style changes.
export function bandTileLayer(scene, spec, styleKey, id, events = {}) {
  const bands = bandsOf(spec), tables = paintTables(spec);
  return new TileLayer({
    id,
    tileSize: TILE,
    minZoom: 4,
    maxZoom: 15,
    extent: scene.bounds,
    maxRequests: 6,
    refinementStrategy: "no-overlap",
    getTileData: (tile) => bandTilePlanes(scene, bands, tile),
    updateTriggers: { renderSubLayers: styleKey },
    renderSubLayers: (props) => {
      const d = props.data;
      if (!d) return null;
      if (d.styleKey !== styleKey) { d.rgba = paintRGBA(d.planes, spec, TILE, TILE, tables); d.styleKey = styleKey; }
      const { west, south, east, north } = props.tile.bbox;
      return new BitmapLayer(props, { data: null, image: d.rgba, bounds: [west, south, east, north] });
    },
    ...events,
  });
}

// The preview as a deck.gl layer: one bitmap over the scene's bounds, drawn
// beneath the tile layer until every tile in view has loaded. `cog` is
// anything with the bounds: an opened COG or a band scene.
export function previewLayer(image, cog, id = "cog-preview") {
  return new BitmapLayer({
    id,
    image,
    bounds: cog.bounds,
    _imageCoordinateSystem: "lnglat",
  });
}
