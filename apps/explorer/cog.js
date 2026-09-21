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
import { fromUrl } from "https://esm.sh/geotiff@3.0.5";
import proj4 from "https://esm.sh/proj4@2.22.0";
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

// Paint a W x H ImageData through a control grid from one source raster —
// an overview window or the thumbnail, interchangeably: `data` holds
// interleaved w x h x bands samples whose pixel (0, 0) is (x0, y0) in a grid
// of `scale` base pixels per sample, `nodata(k)` says whether the sample at
// byte offset k stays see-through. Nearest neighbour; the placement is the
// grid's.
function warp(grid, { data, w, h, bands, scale, x0, y0, nodata }, W, H) {
  const { gx, gy, nx, ny } = grid, N = nx + 1;
  // The grid divides the output evenly, so a cell is CELL px only when the
  // side is a multiple of it (a tile always, the preview's short side not).
  const cw = W / nx, ch = H / ny;
  const out = new Uint8ClampedArray(W * H * 4);
  for (let y = 0; y < H; y++) {
    const fy = (y + 0.5) / ch, j = Math.min(ny - 1, Math.floor(fy)), t = fy - j;
    for (let x = 0; x < W; x++) {
      const fx = (x + 0.5) / cw, i = Math.min(nx - 1, Math.floor(fx)), u = fx - i;
      const a = j * N + i, b = a + 1, c = a + N, d = c + 1;
      const px = (gx[a] * (1 - u) + gx[b] * u) * (1 - t) + (gx[c] * (1 - u) + gx[d] * u) * t;
      const py = (gy[a] * (1 - u) + gy[b] * u) * (1 - t) + (gy[c] * (1 - u) + gy[d] * u) * t;
      const sx = Math.floor(px / scale) - x0, sy = Math.floor(py / scale) - y0;
      if (sx < 0 || sy < 0 || sx >= w || sy >= h) continue;
      const k = (sy * w + sx) * bands, o = (y * W + x) * 4;
      if (nodata(k)) continue;
      out[o] = data[k]; out[o + 1] = data[k + 1]; out[o + 2] = data[k + 2]; out[o + 3] = 255;
    }
  }
  return new ImageData(out, W, H);
}

// One Web Mercator tile of the COG as ImageData, or null if the tile does not
// touch the image. The output raster is linear in lon/lat; over one 256-px
// tile that is within a pixel of BitmapLayer's Mercator-linear stretch, so no
// correction is asked for on the deck.gl side.
export async function readCogTile(cog, bbox, signal) {
  const grid = controlGrid(cog, bbox, TILE, TILE);
  const { minx, miny, maxx, maxy } = grid;
  if (maxx <= 0 || maxy <= 0 || minx >= cog.w || miny >= cog.h) return null;
  // The overview whose pixels are closest to (but not coarser than) the
  // tile's own: base-image pixels per output pixel, floored to a level.
  const want = Math.max(maxx - minx, maxy - miny) / TILE;
  let lvl = cog.levels[0];
  for (const l of cog.levels) if (l.scale <= want) lvl = l;
  const s = lvl.scale;
  const x0 = Math.max(0, Math.floor(minx / s)), y0 = Math.max(0, Math.floor(miny / s));
  const x1 = Math.min(lvl.w, Math.ceil(maxx / s) + 1), y1 = Math.min(lvl.h, Math.ceil(maxy / s) + 1);
  if (x1 <= x0 || y1 <= y0) return null;
  const raster = await lvl.image.readRasters({ window: [x0, y0, x1, y1], interleave: true, signal });
  return warp(grid, { data: raster, w: raster.width, h: raster.height, bands: cog.bands,
    scale: s, x0, y0, nodata: cogNodata(raster) }, TILE, TILE);
}

// Which thumbnail pixels are the swath's nodata, as one flag per pixel. The
// JPEG paints nodata in one flat colour — black on preview.jpg, white on
// thumbnail.jpg (checked on 31UFU partial scenes of every year 2018-2026;
// 2024 has both file names, and the name decides, not the year) — and
// compression smears a fringe of near-that-colour pixels along the swath
// edge. So: exactly that colour, grown by one pixel into near-that-colour
// neighbours. Real dark water (black era) and bright cloud (white era) are
// only lost where they are exactly the flat colour or touch the fringe;
// an isolated exact match is not grown from, so the loss stays at the edge.
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
// level of the COG, its pixel size the base's scaled by the width ratio.
// `white` says the JPEG's nodata colour (see jpegNodataMask).
export function previewImage(cog, bitmap, { white = false } = {}) {
  const [west, south, east, north] = cog.bounds;
  // Long side by the scene's on-screen shape: longitude shrinks by cos(lat).
  const aspect = ((east - west) * Math.cos(((south + north) / 2) * Math.PI / 180)) / (north - south);
  const W = Math.round(aspect >= 1 ? PREVIEW : PREVIEW * aspect);
  const H = Math.round(aspect >= 1 ? PREVIEW / aspect : PREVIEW);
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

// The preview as a deck.gl layer: one bitmap over the scene's bounds, drawn
// beneath the tile layer until every tile in view has loaded.
export function previewLayer(image, cog, id = "cog-preview") {
  return new BitmapLayer({
    id,
    image,
    bounds: cog.bounds,
    _imageCoordinateSystem: "lnglat",
  });
}
