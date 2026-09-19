// A Sentinel-2 visual (TCI) Cloud-Optimized GeoTIFF on the map, with no
// server in between: deck.gl's TileLayer asks for Web Mercator tiles, and each
// tile is filled by range-reading a window of the COG's best-matching overview
// (geotiff.js) and warping it from the scene's UTM grid into the tile.
//
// The warp is exact at the pixel level in the sense that matters here: every
// output pixel is placed by inverse-mapping its own lon/lat into the COG's
// pixel grid (proj4), through a bilinear interpolation of that mapping across
// a 16 x 16 control grid per tile. UTM <-> lon/lat is smooth, so across a
// 16-pixel cell the interpolation error is far below one pixel; the visible
// approximation is the resampling itself (nearest neighbour out of an
// overview whose resolution is between 1x and 2x the tile's), which reads as
// a little aliasing on sharp edges, not as misplacement.
import { fromUrl } from "https://esm.sh/geotiff@3.0.5";
import proj4 from "https://esm.sh/proj4@2.22.0";
// deck.gl from the pinned dist bundle loaded by index.html (see app.js).
const { TileLayer, BitmapLayer } = window.deck;

const TILE = 256;   // output tile size in pixels
const GRID = 16;    // warp control points per tile edge

// Open a COG and read what the warp needs from its base image: the UTM
// projection (from the EPSG geokey — 326NN north, 327NN south), the affine
// origin/resolution, and the overview pyramid. Only the headers are fetched.
export async function openCog(href) {
  const tiff = await fromUrl(href, { allowFullFile: false });
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

// One Web Mercator tile of the COG as ImageData, or null if the tile does not
// touch the image. The output raster is linear in lon/lat — which is exactly
// how BitmapLayer stretches an image across `bounds` — so no further
// correction is needed on the deck.gl side.
export async function readCogTile(cog, { west, south, east, north }, signal) {
  const N = GRID + 1;
  const gx = new Float64Array(N * N), gy = new Float64Array(N * N);
  let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
  for (let j = 0; j < N; j++) {
    const lat = north - ((north - south) * j) / GRID;
    for (let i = 0; i < N; i++) {
      const lon = west + ((east - west) * i) / GRID;
      const [X, Y] = cog.proj.forward([lon, lat]);
      const px = (X - cog.ox) / cog.rx, py = (Y - cog.oy) / cog.ry;
      gx[j * N + i] = px; gy[j * N + i] = py;
      if (px < minx) minx = px; if (px > maxx) maxx = px;
      if (py < miny) miny = py; if (py > maxy) maxy = py;
    }
  }
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
  const rw = raster.width, rh = raster.height, bands = cog.bands;
  const out = new Uint8ClampedArray(TILE * TILE * 4);
  const cell = TILE / GRID;
  for (let y = 0; y < TILE; y++) {
    const fy = (y + 0.5) / cell, j = Math.min(GRID - 1, Math.floor(fy)), t = fy - j;
    for (let x = 0; x < TILE; x++) {
      const fx = (x + 0.5) / cell, i = Math.min(GRID - 1, Math.floor(fx)), u = fx - i;
      const a = j * N + i, b = a + 1, c = a + N, d = c + 1;
      const px = (gx[a] * (1 - u) + gx[b] * u) * (1 - t) + (gx[c] * (1 - u) + gx[d] * u) * t;
      const py = (gy[a] * (1 - u) + gy[b] * u) * (1 - t) + (gy[c] * (1 - u) + gy[d] * u) * t;
      const sx = Math.floor(px / s) - x0, sy = Math.floor(py / s) - y0;
      if (sx < 0 || sy < 0 || sx >= rw || sy >= rh) continue;
      const k = (sy * rw + sx) * bands, o = (y * TILE + x) * 4;
      const r = raster[k], g = raster[k + 1], bl = raster[k + 2];
      // TCI nodata is 0,0,0 (GDAL_NODATA=0): the swath edge stays see-through.
      if (r === 0 && g === 0 && bl === 0) continue;
      out[o] = r; out[o + 1] = g; out[o + 2] = bl; out[o + 3] = 255;
    }
  }
  return new ImageData(out, TILE, TILE);
}

// The deck.gl layer for an opened COG: one TileLayer, clipped to the scene's
// footprint so no tile outside it is ever requested.
export function cogTileLayer(cog, id = "cog") {
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
  });
}
