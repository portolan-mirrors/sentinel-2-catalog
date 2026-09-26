// The derived pipeline of the search results: raw year rows in, the
// filtered and sorted view out. Pure functions, no DOM, no imports, so
// `node --test` runs them (results.test.mjs).
const cmpId = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

export const SORTS = {
  cloud: { label: "least cloud (clearest first)",
    cmp: (a, b) => a.cloud - b.cloud || cmpId(a, b) },
  coverage: { label: "most coverage (fullest first)",
    cmp: (a, b) => (b.cover ?? -1) - (a.cover ?? -1) || a.cloud - b.cloud || cmpId(a, b) },
  date: { label: "newest first",
    cmp: (a, b) => b.t - a.t || cmpId(a, b) },
};

// A null cover means "unknown", and the floor never excludes what it
// cannot judge — the same rule fillColor applies on the map.
export function filterRows(rows, f) {
  return rows.filter((r) => r.t >= f.t0 && r.t <= f.t1
    && r.cloud <= f.maxCloud
    && (f.minCoverage <= 0 || r.cover === null || r.cover >= f.minCoverage));
}

// hasOwn, not `SORTS[key] ?? SORTS.cloud`: "__proto__", "constructor" and
// "toString" all find something on the prototype chain, so the ?? never
// fires and `.cmp` comes back undefined — .sort(undefined) is a lexicographic
// sort by string, not the cloud order the fallback promises.
export function sortRows(rows, key) {
  const s = Object.hasOwn(SORTS, key) ? SORTS[key] : SORTS.cloud;
  return [...rows].sort(s.cmp);
}

export function viewOf(rows, f, key) {
  return sortRows(filterRows(rows, f), key);
}

export function indexOfId(view, id) {
  return view.findIndex((r) => r.id === id);
}

export function clampIndex(view, i) {
  return view.length ? Math.min(view.length - 1, Math.max(0, i)) : -1;
}

export function filterKeyOf(f, key, search) {
  return [search?.tile, search?.year, search?.at,
    f.t0, f.t1, f.maxCloud, f.minCoverage, key].join("|");
}
