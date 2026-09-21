// The band mapper's tables and pixel math (Task 28): which bands a Sentinel-2
// L2A scene has, the presets, the SCL palette, the index ramps, the
// histogram/percentile stats of an overview, and the painter that turns
// per-pixel sample planes (cog.js) into RGBA through a stretch. No I/O here.
//
// Sample values are the files' DN as stored: uint16 with DN/10000 =
// reflectance for the reflectance bands (0 = nodata; processing baselines
// >= 04.00, January 2022 on, add a BOA offset of 1000 so DN = 10000*rho +
// 1000), uint8 classes for SCL. Stretching works on DN as is — the handles
// show DN — and an index is (a - b) / (a + b) on DN, which cancels the
// 1/10000 scale but not the 1000 offset: on a >= 04.00 scene the raw-DN
// NDVI of a field at rho_nir 0.5, rho_red 0.1 is 0.5 instead of 0.67. So the
// offset is subtracted first when the row says which baseline it is
// (`s2:processing_baseline`, projected by the scene query); a row without
// it gets no correction and the panel says so.

// name -> label (ESA naming), central wavelength, native grid.
export const BANDS = {
  B01: { label: "Coastal aerosol", nm: 443, res: 60 },
  B02: { label: "Blue", nm: 490, res: 10 },
  B03: { label: "Green", nm: 560, res: 10 },
  B04: { label: "Red", nm: 665, res: 10 },
  B05: { label: "Red edge 1", nm: 705, res: 20 },
  B06: { label: "Red edge 2", nm: 740, res: 20 },
  B07: { label: "Red edge 3", nm: 783, res: 20 },
  B08: { label: "NIR", nm: 842, res: 10 },
  B8A: { label: "Narrow NIR", nm: 865, res: 20 },
  B09: { label: "Water vapour", nm: 945, res: 60 },
  B11: { label: "SWIR 1", nm: 1610, res: 20 },
  B12: { label: "SWIR 2", nm: 2190, res: 20 },
  AOT: { label: "Aerosol optical thickness", nm: null, res: 20 },
  WVP: { label: "Water vapour column", nm: null, res: 20 },
  SCL: { label: "Scene classification", nm: null, res: 20 },
};
export const bandTitle = (b) => {
  const d = BANDS[b];
  return d ? `${b} ${d.label}${d.nm ? ` ${d.nm} nm` : ""} · ${d.res} m` : b;
};

// The indices: (a - b) / (a + b), and the fixed diverging ramp each is drawn
// with over -1..1 (three stops; the handles narrow the range the ramp spans,
// the ramp itself does not change, and it is linear between the handles —
// the curve and gamma are for bands, where a nonlinear lift has no zero
// point to move). NDVI: brown -> pale -> green, the ColorBrewer BrBG ends.
// NDWI (McFeeters, green/NIR): brown -> pale -> blue, so water is blue and
// land brown.
export const INDICES = {
  ndvi: { label: "NDVI", a: "B08", b: "B04", ramp: ["#8c510a", "#f5f5f5", "#01665e"] },
  ndwi: { label: "NDWI", a: "B03", b: "B08", ramp: ["#a6611a", "#f5f5f5", "#0571b0"] },
};

// ESA's SCL classes and the palette its own products use.
export const SCL_CLASSES = [
  [0, "No data", "#000000"],
  [1, "Saturated / defective", "#ff0000"],
  [2, "Dark area", "#2f2f2f"],
  [3, "Cloud shadow", "#643200"],
  [4, "Vegetation", "#00a000"],
  [5, "Not vegetated", "#ffe65a"],
  [6, "Water", "#0000ff"],
  [7, "Unclassified", "#808080"],
  [8, "Cloud, medium probability", "#c0c0c0"],
  [9, "Cloud, high probability", "#ffffff"],
  [10, "Thin cirrus", "#64c8ff"],
  [11, "Snow / ice", "#ff96ff"],
];

// The presets. `kind` says how the channels are painted: tci is the visual
// COG as is (Task 27's path, already stretched by ESA), rgb three bands,
// gray one, index one ratio, scl the palette. custom and single take their
// bands from the selects.
export const PRESETS = {
  tci: { label: "True color (TCI)", kind: "tci", bands: ["TCI"] },
  fcir: { label: "False color IR", kind: "rgb", bands: ["B08", "B04", "B03"] },
  agri: { label: "Agriculture", kind: "rgb", bands: ["B11", "B08", "B02"] },
  swir: { label: "SWIR", kind: "rgb", bands: ["B12", "B8A", "B04"] },
  ndvi: { label: "NDVI", kind: "index", index: "ndvi" },
  ndwi: { label: "NDWI", kind: "index", index: "ndwi" },
  scl: { label: "SCL classes", kind: "scl", bands: ["SCL"] },
  single: { label: "Single band…", kind: "gray" },
  custom: { label: "Custom RGB", kind: "rgb" },
};

// The distinct bands a spec reads, in channel order.
export function bandsOf(spec) {
  if (spec.kind === "index") { const ix = INDICES[spec.index]; return [ix.a, ix.b]; }
  return [...new Set(spec.bands)];
}

// The stretch is a 1024-entry lookup from the normalised value t in 0..1 to
// a byte: the curve first (linear t; sqrt; log10(1 + 9t), which keeps 0 -> 0
// and 1 -> 1), then gamma as t^(1/gamma), so gamma above 1 brightens the
// mid-tones, as in most raster viewers.
const LUT_N = 1024;
export function makeLut(curve = "linear", gamma = 1) {
  const lut = new Uint8ClampedArray(LUT_N);
  const g = 1 / Math.max(0.05, Number(gamma) || 1);
  for (let i = 0; i < LUT_N; i++) {
    let t = i / (LUT_N - 1);
    if (curve === "sqrt") t = Math.sqrt(t);
    else if (curve === "log") t = Math.log10(1 + 9 * t);
    lut[i] = Math.round(255 * Math.pow(t, g));
  }
  return lut;
}

const hex = (c) => [parseInt(c.slice(1, 3), 16), parseInt(c.slice(3, 5), 16), parseInt(c.slice(5, 7), 16)];
// A 256-entry RGB table through the ramp's stops, evenly spaced.
export function rampTable(stops) {
  const rgb = stops.map(hex), out = new Uint8ClampedArray(256 * 3);
  for (let i = 0; i < 256; i++) {
    const f = (i / 255) * (rgb.length - 1), j = Math.min(rgb.length - 2, Math.floor(f)), t = f - j;
    for (let c = 0; c < 3; c++) out[i * 3 + c] = rgb[j][c] * (1 - t) + rgb[j + 1][c] * t;
  }
  return out;
}
const SCL_TABLE = (() => {
  const t = new Uint8ClampedArray(256 * 3);
  for (const [v, , c] of SCL_CLASSES) t.set(hex(c), v * 3);
  return t;
})();

// Histogram and percentiles of one band's overview: the file's nodata (0)
// is left out of the count, so an empty swath edge cannot pull the 2nd
// percentile to zero. 64 bins over the data's own min..max; p2/p98 by a
// numeric sort of the valid samples (a 686 x 686 overview sorts in tens of
// milliseconds).
export function sampleStats(data, nodata = 0) {
  const valid = new Float32Array(data.length);
  let n = 0;
  for (let i = 0; i < data.length; i++) { const v = data[i]; if (v !== nodata && Number.isFinite(v)) valid[n++] = v; }
  if (!n) return null;
  const sorted = valid.subarray(0, n).sort();
  const at = (q) => sorted[Math.min(n - 1, Math.max(0, Math.round(q * (n - 1))))];
  const min = sorted[0], max = sorted[n - 1];
  return { n, min, max, p2: at(0.02), p98: at(0.98), hist: histogram(sorted, min, max) };
}

// The same for an index over two overviews of the same size, on -1..1.
export function indexStats(a, b, offset = 0, nodata = 0) {
  if (!a || !b || a.length !== b.length) return null;
  const vals = new Float32Array(a.length);
  let n = 0;
  for (let i = 0; i < a.length; i++) {
    if (a[i] === nodata || b[i] === nodata) continue;
    const x = a[i] - offset, y = b[i] - offset, v = (x - y) / (x + y);
    if (Number.isFinite(v)) vals[n++] = Math.max(-1, Math.min(1, v));
  }
  if (!n) return null;
  const sorted = vals.subarray(0, n).sort();
  const at = (q) => sorted[Math.min(n - 1, Math.max(0, Math.round(q * (n - 1))))];
  return { n, min: -1, max: 1, p2: at(0.02), p98: at(0.98), hist: histogram(sorted, -1, 1) };
}

export const HIST_BINS = 64;
function histogram(values, lo, hi) {
  const bins = new Uint32Array(HIST_BINS), span = hi - lo || 1;
  for (let i = 0; i < values.length; i++) {
    bins[Math.min(HIST_BINS - 1, Math.floor(((values[i] - lo) / span) * HIST_BINS))]++;
  }
  return bins;
}

// Paint W x H RGBA from the planes a spec needs. `planes` maps band name ->
// Float32Array (W*H, NaN off the scene) or null for a band that could not be
// read (its channel paints black; the others still show). Channels are
// {band|index, min, max}; `nodata` is the sample value keyed out (0, the
// files' own) or null for none. Off-scene (NaN) is always transparent.
export function paintRGBA(planes, spec, W, H) {
  const out = new Uint8ClampedArray(W * H * 4);
  const lut = makeLut(spec.curve, spec.gamma);
  const nd = spec.nodata === null || spec.nodata === undefined ? NaN : Number(spec.nodata);
  const N = W * H;
  if (spec.kind === "scl") {
    const p = planes[spec.bands[0]];
    if (!p) return new ImageData(out, W, H);
    for (let i = 0; i < N; i++) {
      const v = p[i];
      if (v !== v || v === nd) continue;            // NaN or nodata: see-through
      const k = Math.min(255, Math.max(0, v | 0)) * 3, o = i * 4;
      out[o] = SCL_TABLE[k]; out[o + 1] = SCL_TABLE[k + 1]; out[o + 2] = SCL_TABLE[k + 2]; out[o + 3] = 255;
    }
    return new ImageData(out, W, H);
  }
  if (spec.kind === "index") {
    const ix = INDICES[spec.index], a = planes[ix.a], b = planes[ix.b];
    const ch = spec.channels[0], ramp = rampTable(ix.ramp), off = spec.offset || 0;
    const lo = ch.min, scale = 255 / ((ch.max - ch.min) || 1e-9);
    if (!a || !b) return new ImageData(out, W, H);
    for (let i = 0; i < N; i++) {
      const av = a[i], bv = b[i];
      if (av !== av || bv !== bv || av === nd || bv === nd) continue;
      const x = av - off, y = bv - off, v = (x - y) / (x + y);
      if (v !== v) continue;                       // 0/0 at a fully dark pixel
      let t = (v - lo) * scale;
      t = t < 0 ? 0 : t > 255 ? 255 : t;
      const k = (t | 0) * 3, o = i * 4;
      out[o] = ramp[k]; out[o + 1] = ramp[k + 1]; out[o + 2] = ramp[k + 2]; out[o + 3] = 255;
    }
    return new ImageData(out, W, H);
  }
  // gray (one channel to all three) or rgb.
  const chans = spec.kind === "gray" ? [0, 0, 0].map(() => spec.channels[0]) : spec.channels;
  const src = chans.map((c) => planes[c.band] ?? null);
  const lo = chans.map((c) => c.min), sc = chans.map((c) => (LUT_N - 1) / ((c.max - c.min) || 1e-9));
  for (let i = 0; i < N; i++) {
    const o = i * 4;
    let seen = false, drop = false;
    for (let c = 0; c < 3; c++) {
      const p = src[c];
      if (!p) { out[o + c] = 0; continue; }
      const v = p[i];
      if (v !== v || v === nd) { drop = true; break; }
      seen = true;
      let t = (v - lo[c]) * sc[c];
      t = t < 0 ? 0 : t > LUT_N - 1 ? LUT_N - 1 : t;
      out[o + c] = lut[t | 0];
    }
    if (seen && !drop) out[o + 3] = 255;
  }
  return new ImageData(out, W, H);
}
