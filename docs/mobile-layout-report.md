# Mobile layout: the map is the page — what was done

Answers `docs/mobile-layout-brief.md`. Branch `s2-layout-exp`.

## The change

Three files: `apps/explorer/index.html`, `style.css`, `app.js`. No new
dependency, no new library, no build step.

### One container, two layouts

`index.html` now wraps the two panels that are not the map:

    <div id="sheet">
      <button id="grip">…</button>
      <div id="sheetbody">
        <div id="imgpanel">…</div>     <!-- unchanged -->
        <aside id="panel">…</aside>    <!-- unchanged -->
      </div>
    </div>

Nothing inside either panel moved and no id changed, so every `$("…")`
lookup in `app.js`, `bands.js` and `cog.js` still finds what it found.

On a desktop `#sheet` and `#sheetbody` are `display: contents` and `#grip`
is `display: none`, so the two panels are laid out against the viewport
exactly as before: the sidebar at the left, the image card 16 px from the
map's lower right corner. This is why the wrapper could be added without
touching the desktop rules.

At the phone breakpoint — the same `@media (max-width: 760px)` the old
rules used — `#sheet` becomes the one bottom sheet:

* `#map` covers the whole viewport (`inset: 0`), and `body` cannot scroll.
* `#sheet` is fixed to the bottom, full width, rounded at the top, with the
  grab handle above a single scroller.
* `#imgpanel` is `position: static` inside that scroller, first, so the
  "Showing … Clear" bar and "Bands & stretch" render above the search
  controls. One sheet, never two.
* Three heights, all owned by CSS so they follow `dvh` when the iOS URL bar
  slides: `peek` = `calc(110px + env(safe-area-inset-bottom))`, `half` =
  `50dvh`, `full` = `88dvh`. `env(safe-area-inset-bottom)` also pads the
  sheet, so the home indicator does not sit on the controls.

### The handle

`app.js` gained one block. `setSnap(name)` clears any pixel height, writes
`data-snap` and — for `peek` — puts the scroller back at its top. The CSS
picks the height from `data-snap`; the JavaScript never computes one except
while a finger is down.

* A pointer drag on the handle sets a pixel height each `pointermove` and,
  on release, snaps to the nearest of the three stops.
* A press that moved less than 6 px is a tap and cycles peek → half → full
  → peek. The browser's follow-up `click` is swallowed once, so a keyboard
  Enter or Space on the handle — which sends a `click` with no pointer
  gesture — cycles too.
* The drag handler is bound to `#grip` only, and refuses a `pointerdown`
  whose target is a control. The sliders, selects and buttons all live
  below the handle in the scroller and keep their own pointer events, so a
  stretch-handle drag cannot be taken for a sheet drag, and a drag on the
  map still pans the map. Both are verified below.

### Showing a scene

`showOnMap()` calls `setSnap("peek")` right after it writes the cogbar, so
every way a scene reaches the map — the card's button, a band chip, and the
clearest scene a finished search puts up by itself — drops the sheet to
peek and leaves the "Showing …" line in view.

Two smaller edits follow from that:

* `runQuery()` skips its `scrollIntoView` of the results when the sheet is
  at peek. Without this the search's own smooth scroll lands *after* the
  snap and carries the "Showing …" line out of the one line peek shows.
  (Found in verification 2; the first build failed exactly here.)
* `hideImagePanel()` returns a peeking sheet to half: with no scene there
  is nothing to peek at.

`setSnap()` returns at once unless `#sheet` is a real box, so on a desktop
every call above is a no-op and `data-snap` is never written. That is what
keeps the desktop hero scroll in `runQuery()` working.

## Verification

Headless Chrome (Playwright's `chrome-headless-shell`, ANGLE + SwiftShader,
the launch line `tools/rails/experiments/check_app.py` uses), driven over
CDP against the real page served from this worktree, reading the real
catalog on Source Cooperative. Three viewports: 390x844, 428x926, 1440x900.

| # | Check | Result |
|---|-------|--------|
| 1 | On load at phone size the map is ≥ 50 % of the viewport | **pass** |
| 2 | Search, then show on map: peek, "Showing …" visible, scene drawn | **pass** |
| 3 | Full reaches the band controls and the stretch handles; they work | **pass** |
| 4 | The zoom buttons are hit-testable at every snap | **pass** |
| 5 | Desktop unchanged | **pass** |

**1 — the map on load.** `#map` is the full viewport at both phone sizes
(428x926 and 390x844) and the sheet opens at half, so the map above it is
463 px of 926 and 422 px of 844: 50.0 % of the viewport, the brief's own
`50dvh`, against the 9 vh strip the old two-panel stack left. The page
itself does not scroll (`document.scrollHeight` equals the viewport height
at both sizes).

**2 — search, then show on map.** One map click at 5.5°E 52.0°N named tile
31UFT; the search read 2 parts / 2 row groups and returned 14 scenes. The
sheet was at `peek` the moment the cards landed (the page shows the
clearest scene itself), sheet top 816 px of 926, so 88 % of the screen is
the scene. The cogbar reads "Showing S2A_T31UFT_20260915T103959_L2A on the
map — Full resolution — Clear"; the id's box is at y = 857, inside the
viewport, and `elementFromPoint` on it returns the cogbar, so the line is
visible and not covered. The scene is drawn: the status line says "on the
map at full resolution (True color (TCI))" and the screenshot shows the
COG. Scroll position inside the sheet is 0.

**3 — full reaches the controls.** Tapping the handle cycles to `full`
(sheet 814.9 px of 926, `data-snap="full"`). `#preset` is in the viewport
and `elementFromPoint` over it returns `#preset`. Changing it from `tci` to
`fcir` reloaded the bands for real — status: "B08, B04, B03 overviews
range-read straight from the COGs, reprojected and stretched in the
browser" — and the per-band histograms and stretch handles were built. A
real pointer drag on a stretch handle moved it from 80 to 466 while the
sheet height stayed 814.875 px and `data-snap` stayed `full`: the handle
took the drag, the sheet did not. The same test on `#maxcloud` inside the
sheet: 100 → 24, sheet height unchanged.

**4 — the zoom buttons.** At every stop, at both phone sizes,
`elementFromPoint` over the centre of `.maplibregl-ctrl-zoom-in` and
`.maplibregl-ctrl-zoom-out` returns the control, and a click on zoom-in
raises `map.getZoom()`. The controls sit at the map's top right and the
sheet's top edge is 816 px (peek), 463 px (half) and 111 px (full) at
428x926 — the buttons end at 68 px, clear at every stop. A drag across the
map at half moved the centre from 10°E 30°N to 31.8°E 54.6°N, so the map
still pans.

**5 — desktop.** At 1440x900: `#panel` is 360x900 at the left, `#map` is
inset 360 px, `#sheet` computes to `display: contents`, `#grip` to
`display: none`, `data-snap` is never written, and the image card is 360 px
wide, 16 px from the right and 16 px from the bottom — the same rule as
before the change. The page does not scroll.

Gates: `node --check` on all five explorer modules, `CI_LIGHT=1 python3
tests/run_all.py` (all gates passed), `CI_LIGHT=1 python3 -m pytest tests
-q` (217 passed).

## Screenshots

All under
`/private/tmp/claude-501/-Users-cholmes-repos-sentinel-2-catalog/2a289305-6a96-4630-8a13-26ed2c5e7ad3/scratchpad/shots/`.

The pair that matters, 428x926 on load:

| | |
|---|---|
| before | `before-428x926.png` |
| after | `after-428x926.png` |

The rest:

* `after-scene-428x926.png` — a scene on the map, sheet at peek with the
  "Showing …" line. This is the state the complaint was about.
* `after-scene-428x926-full.png` — the same sheet at full: the image
  controls, the band selects, the three histograms with their stretch
  handles, then the search controls below.
* `after-scene-428x926-peek.png`, `-half.png`, `-full.png` — the three
  stops with a scene shown.
* `after-390x844.png`, `after-390x844-imgpanel.png` — iPhone 14 size, the
  second with the image controls shown at the top of the sheet.
* `gestures-428x926-peek.png`, `-half.png`, `-full.png` — the three stops
  reached by tapping the handle.
* `after-desktop-1440x900.png`, `after-desktop-1440x900-imgpanel.png` — the
  desktop regression, the second with the image card at the lower right.
