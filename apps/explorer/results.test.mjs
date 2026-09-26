import test from "node:test";
import assert from "node:assert/strict";
import { SORTS, filterRows, sortRows, viewOf, indexOfId, clampIndex, filterKeyOf }
  from "./results.js";

const day = (d) => Date.parse(`${d}T12:00:00Z`);
const rows = [
  { id: "S2A_1", day: "2024-01-05", t: day("2024-01-05"), cloud: 40, cover: 100 },
  { id: "S2A_2", day: "2024-03-10", t: day("2024-03-10"), cloud: 5, cover: 30 },
  { id: "S2A_3", day: "2024-07-01", t: day("2024-07-01"), cloud: 5, cover: null },
  { id: "S2A_4", day: "2024-11-20", t: day("2024-11-20"), cloud: 80, cover: 90 },
];
const all = { maxCloud: 100, minCoverage: 0,
  t0: Date.parse("2024-01-01T00:00:00Z"), t1: Date.parse("2024-12-31T23:59:59.999Z") };

test("filterRows applies cloud, coverage and date window", () => {
  assert.equal(filterRows(rows, all).length, 4);
  assert.deepEqual(filterRows(rows, { ...all, maxCloud: 10 }).map((r) => r.id),
    ["S2A_2", "S2A_3"]);
  // a null cover is never excluded by the coverage floor
  assert.deepEqual(filterRows(rows, { ...all, minCoverage: 50 }).map((r) => r.id),
    ["S2A_1", "S2A_3", "S2A_4"]);
  assert.deepEqual(filterRows(rows, { ...all,
    t0: Date.parse("2024-03-01T00:00:00Z"),
    t1: Date.parse("2024-08-31T23:59:59.999Z") }).map((r) => r.id),
    ["S2A_2", "S2A_3"]);
});

test("sortRows: cloud ties break on id, coverage sorts nulls last, date is newest first", () => {
  assert.deepEqual(sortRows(rows, "cloud").map((r) => r.id),
    ["S2A_2", "S2A_3", "S2A_1", "S2A_4"]);
  assert.deepEqual(sortRows(rows, "coverage").map((r) => r.id),
    ["S2A_1", "S2A_4", "S2A_2", "S2A_3"]);
  assert.deepEqual(sortRows(rows, "date").map((r) => r.id),
    ["S2A_4", "S2A_3", "S2A_2", "S2A_1"]);
  assert.notEqual(sortRows(rows, "date"), rows); // never mutates its input
  assert.equal(rows[0].id, "S2A_1");
});

test("viewOf composes, indexOfId and clampIndex behave at the edges", () => {
  const view = viewOf(rows, { ...all, maxCloud: 10 }, "cloud");
  assert.deepEqual(view.map((r) => r.id), ["S2A_2", "S2A_3"]);
  assert.equal(indexOfId(view, "S2A_3"), 1);
  assert.equal(indexOfId(view, "S2A_1"), -1);
  assert.equal(clampIndex(view, 5), 1);
  assert.equal(clampIndex(view, -3), 0);
  assert.equal(clampIndex([], 0), -1);
});

test("filterKeyOf changes when any input changes", () => {
  const search = { tile: "31UFU", year: 2024, at: 1 };
  const a = filterKeyOf(all, "cloud", search);
  assert.notEqual(a, filterKeyOf({ ...all, maxCloud: 99 }, "cloud", search));
  assert.notEqual(a, filterKeyOf(all, "date", search));
  assert.notEqual(a, filterKeyOf(all, "cloud", { ...search, at: 2 }));
  assert.equal(typeof filterKeyOf(all, "cloud", null), "string");
});
