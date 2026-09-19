// A two-handle day-range slider, built from two native <input type=range>
// elements stacked on top of each other (see style.css .dayrange): no
// framework, keyboard-accessible, and the handles cannot cross. Values are
// whole days from `min`; the slider is bound two-way to a pair of
// <input type=date> fields.
const DAY = 86400000;
const iso = (d) => new Date(d).toISOString().slice(0, 10);
const utc = (s) => Date.parse(`${s}T00:00:00Z`);

// container: the .dayrange element to populate.
// from/to: the <input type=date> pair to keep in sync.
// min/max: ISO dates bounding the slider (inclusive).
// onChange(d0, d1): called with ISO dates after any change from either side.
export function dayRange({ container, from, to, min, max, onChange }) {
  // lo/hi/days are reassigned by rebound() when the caller re-scopes the
  // slider (Task 22: the window is bounded to whichever month is selected,
  // not the whole stats span), so they cannot be const.
  let lo = utc(min), hi = utc(max), days = Math.max(1, Math.round((hi - lo) / DAY));
  const a = document.createElement("input"), b = document.createElement("input");
  for (const r of [a, b]) {
    r.type = "range"; r.min = 0; r.max = days; r.step = 1;
    r.setAttribute("aria-label", r === a ? "Window start" : "Window end");
  }
  a.value = 0; b.value = days;
  const fill = document.createElement("div");
  fill.className = "fill";
  container.replaceChildren(fill, a, b);

  const clampDay = (n) => Math.min(days, Math.max(0, n));
  const dayOf = (s) => clampDay(Math.round((utc(s) - lo) / DAY));

  const paint = () => {
    const x0 = (100 * Number(a.value)) / days, x1 = (100 * Number(b.value)) / days;
    fill.style.left = `${x0}%`;
    fill.style.width = `${Math.max(0, x1 - x0)}%`;
    // The handle nearest the pointer must be the one that gets the drag:
    // when both sit at the same end, raise the one that can still move.
    a.style.zIndex = Number(a.value) >= days - 1 ? 3 : 1;
    b.style.zIndex = Number(b.value) <= 1 ? 3 : 2;
  };

  const fromSlider = () => {
    let d0 = Number(a.value), d1 = Number(b.value);
    if (d0 > d1) { d0 = d1; a.value = d0; }
    from.value = iso(lo + d0 * DAY);
    to.value = iso(lo + d1 * DAY);
    paint();
    onChange?.(from.value, to.value);
  };
  const fromDates = () => {
    if (!from.value || !to.value || Number.isNaN(utc(from.value)) || Number.isNaN(utc(to.value))) return;
    let d0 = dayOf(from.value), d1 = dayOf(to.value);
    if (d0 > d1) d1 = d0;
    a.value = d0; b.value = d1;
    // A date outside the bounds, or an end before the start, was clamped
    // above; the calendars take the clamped value so the two never diverge.
    from.value = iso(lo + d0 * DAY);
    to.value = iso(lo + d1 * DAY);
    paint();
    onChange?.(from.value, to.value);
  };
  a.addEventListener("input", () => { if (Number(a.value) > Number(b.value)) a.value = b.value; fromSlider(); });
  b.addEventListener("input", () => { if (Number(b.value) < Number(a.value)) b.value = a.value; fromSlider(); });
  from.addEventListener("change", fromDates);
  to.addEventListener("change", fromDates);
  from.min = min; from.max = max; to.min = min; to.max = max;
  fromDates();

  const setRange = (d0, d1) => { from.value = d0; to.value = d1; fromDates(); };
  // Re-scope the whole slider to a new [min, max] (Task 22: called on every
  // month change so the window can never reach outside the selected month)
  // and reset From/To to that full range, same as a fresh dayRange() would.
  const rebound = (newMin, newMax) => {
    lo = utc(newMin); hi = utc(newMax);
    days = Math.max(1, Math.round((hi - lo) / DAY));
    a.max = days; b.max = days;
    from.min = newMin; from.max = newMax; to.min = newMin; to.max = newMax;
    setRange(newMin, newMax);
  };
  return { set: setRange, rebound, days, min, max };
}
