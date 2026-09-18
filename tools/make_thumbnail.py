#!/usr/bin/env python3
"""Render the collection thumbnail: scene density on a world grid.

Portolan requires a geospatial collection to carry a thumbnail
(PORTO-CORE-067, rashid PTL-VIZ-001), and the useful thumbnail for an item
index is the one that shows where the items are. This paints one cell per
scene centre, log-scaled, so two days of staged data read as orbit swaths and
a full archive reads as global coverage. It is a picture of the data, not a
logo.

    python3 tools/make_thumbnail.py --data-dir ../s2-staging/publish/sentinel-2-l2a

Binning happens in DuckDB, so the memory cost is the grid and not the rows;
this runs the same against 31 thousand rows or 51 million. Centres come from
the `bbox` column rather than the geometry, which means no spatial extension
and no WKB to decode.

The PNG is written by hand from zlib and struct. That keeps the tool inside
the standard library, which is the same bargain every other tool here takes:
duckdb and nothing else.
"""
from __future__ import annotations

import argparse
import struct
import zlib
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent

WIDTH, HEIGHT = 1200, 600

# Equirectangular, dark ground, and a ramp that stays readable where the data
# is sparse. The first entry is the background: cells with no scenes.
BACKGROUND = (11, 20, 33)
RAMP = [
    (23, 60, 92),
    (30, 110, 140),
    (46, 168, 160),
    (129, 214, 130),
    (233, 227, 108),
    (255, 247, 214),
]

# A bbox that spans the antimeridian is published as [-180, …, 180, …], so its
# average longitude is 0 and its centre would paint a false meridian down the
# middle of the Atlantic. A Sentinel-2 scene is about one degree wide, so any
# bbox this wide is one of those. Earth Search also reports some dateline
# scenes the other way around, with west > east (e.g.
# [179.36, -41.61, -179.85, -40.61]) -- bbox[3] - bbox[1] is then negative,
# which slips under this same threshold, so west > east bboxes are rejected
# outright rather than relying on the width arithmetic alone.
MAX_SPAN_DEGREES = 10


def counts(con: duckdb.DuckDBPyConnection, sources: list[str]) -> dict[tuple[int, int], int]:
    rows = con.execute(
        f"""
        SELECT
          least(greatest(CAST(floor(((bbox[1] + bbox[3]) / 2 + 180)
                                    / 360 * {WIDTH}) AS INTEGER), 0), {WIDTH - 1}) AS ix,
          least(greatest(CAST(floor((90 - (bbox[2] + bbox[4]) / 2)
                                    / 180 * {HEIGHT}) AS INTEGER), 0), {HEIGHT - 1}) AS iy,
          count(*) AS n
        FROM read_parquet(?)
        WHERE bbox IS NOT NULL AND bbox[3] >= bbox[1]
          AND bbox[3] - bbox[1] < {MAX_SPAN_DEGREES}
        GROUP BY 1, 2
        """,
        [sources],
    ).fetchall()
    return {(ix, iy): n for ix, iy, n in rows}


def shade(n: int, top: int) -> tuple[int, int, int]:
    """A count to a colour, log-scaled so one scene is still visible."""
    from math import log1p

    position = log1p(n) / log1p(top) if top > 0 else 0.0
    # Gamma. Without it the whole image sits at the dark end whenever one
    # cell is far busier than the rest, which is every archive: the single
    # scene that is the point of an index would be invisible.
    scaled = position**0.5 * (len(RAMP) - 1)
    low = min(int(scaled), len(RAMP) - 2)
    weight = scaled - low
    a, b = RAMP[low], RAMP[low + 1]
    return tuple(round(a[i] + (b[i] - a[i]) * weight) for i in range(3))


def png(pixels: dict[tuple[int, int], tuple[int, int, int]]) -> bytes:
    """A truecolour PNG, written from the standard library."""
    raw = bytearray()
    for y in range(HEIGHT):
        raw.append(0)  # filter type 0: no per-row prediction
        for x in range(WIDTH):
            raw.extend(pixels.get((x, y), BACKGROUND))

    def chunk(kind: bytes, payload: bytes) -> bytes:
        return (struct.pack(">I", len(payload)) + kind + payload
                + struct.pack(">I", zlib.crc32(kind + payload) & 0xFFFFFFFF))

    header = struct.pack(">IIBBBBB", WIDTH, HEIGHT, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", header)
            + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
            + chunk(b"IEND", b""))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data-dir",
                    help="staged sentinel-2-l2a/ directory holding year=*/")
    ap.add_argument("--sources", nargs="*", default=[],
                    help="explicit parquet paths or URLs, instead of --data-dir")
    ap.add_argument("--out",
                    default=str(ROOT / "catalog" / "sentinel-2-l2a" / "thumbnail.png"))
    a = ap.parse_args()

    sources = list(a.sources)
    if a.data_dir:
        sources += [str(p) for p in sorted(Path(a.data_dir).resolve().glob("year=*/*.parquet"))]
    if not sources:
        raise SystemExit("no sources: pass --data-dir or --sources")

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    if any("://" in s for s in sources):
        con.execute("INSTALL httpfs; LOAD httpfs;")

    grid = counts(con, sources)
    if not grid:
        raise SystemExit("no rows with a usable bbox: nothing to draw")
    top = max(grid.values())
    pixels = {cell: shade(n, top) for cell, n in grid.items()}

    out = Path(a.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(png(pixels))
    print(f"wrote {out}: {len(grid):,} filled cell(s), peak {top:,} scenes, "
          f"{out.stat().st_size / 1000:,.0f} kB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
