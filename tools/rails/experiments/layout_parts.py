#!/usr/bin/env python3
"""The layout variants of docs/c1-layout-experiments.md, as part lists.

One module so the builder (build_layout.py) and the measurement harness
(make_harness.py) cannot drift: a variant is a name, a row-group size and a
list of parts, and a part is `(stem, predicate)` -- the file stem under the
variant's directory and the SQL that selects its rows out of a published
Collection 1 year.

The point of every partitioning here is *client-side pruning*: the explorer's
search (apps/explorer/search.js) fetches metadata for every URL it is handed,
so a partition only pays if the app can name the one part that holds a tile
before it asks for a byte. Each variant therefore records how a client picks
its parts (`prune`), in the same terms `apps/explorer/app.js` already has in
`COLLECTIONS[...].parts(year, tile)`:

  "none"   every part of the year is read (no pruning is possible)
  "tile"   the tile id alone names the part  (zones, octants, prefixes)
  "date"   the search window names the parts (months)
  "both"   tile and window together name the parts

`build_layout.py --variant V6/V7` discovers the actual key values in the
source year; the lists below are the fixed ones.

Nothing here touches the production builder: `s2_build.py` still writes one
`items.parquet` per Collection 1 year.
"""
from __future__ import annotations

import re

# The eight octants of tools/s2_build.ZONE_PARTS_8, repeated rather than
# imported so this file runs without the builder's dependencies.
ZONE_PARTS_8 = (
    ("z01-15", 1, 15),
    ("z16-20", 16, 20),
    ("z21-31", 21, 31),
    ("z32-35", 32, 35),
    ("z36-40", 36, 40),
    ("z41-46", 41, 46),
    ("z47-52", 47, 52),
    ("z53-60", 53, 60),
)

# The UTM zone of a Collection 1 scene, from the leading one or two digits
# of `_tile` ('1VCJ', '31UFU') -- tools/s2_build.zone_sql for `_tile`.
ZONE_SQL = r"""TRY_CAST(regexp_extract("_tile", '^(\d{1,2})', 1) AS INTEGER)"""
# The MGRS grid-zone prefix: the zone digits plus the latitude band letter
# ('31U', '1V'). Deterministic from the tile id, so a client computes it
# with a regex and never needs an index.
PREFIX_SQL = r"""regexp_extract("_tile", '^(\d{1,2}[A-Z])', 1)"""

# Published today, and the two row-group rewrites of it.
ROW_GROUP = 6000


def zone_of(tile: str) -> int:
    """The UTM zone of an MGRS tile id.

    Collection 1's `_tile` carries BOTH spellings of a single-digit zone --
    '1CCV' and '01CDS' both occur in year=2018 (75,381 rows unpadded against
    1,254,592 padded). Nothing here normalises them: a partition key derived
    from the tile *string* keeps every spelling self-consistent, which is all
    the client needs, since the search matches `_tile` by exact string too.
    """
    return int(re.match(r"^(\d{1,2})", tile).group(1))


def prefix_of(tile: str) -> str:
    """The grid-zone prefix: the zone digits as spelled, plus the latitude
    band letter ('31UFU' -> '31U', '1CCV' -> '1C', '01CDS' -> '01C')."""
    return re.match(r"^(\d{1,2}[A-Z])", tile).group(1)


def octant_of(tile: str) -> str:
    """The ZONE_PARTS_8 stem holding `tile`."""
    z = zone_of(tile)
    for label, lo, hi in ZONE_PARTS_8:
        if lo <= z <= hi:
            return label
    raise ValueError(f"no octant for {tile}")


def fixed_variants() -> dict[str, dict]:
    """The variants whose part lists do not depend on the year's data."""
    return {
        "V1": dict(row_group=20_000, prune="none",
                   parts=[("items", "TRUE")],
                   note="one file per year, 20,000-row groups"),
        "V2": dict(row_group=100_000, prune="none",
                   parts=[("items", "TRUE")],
                   note="one file per year, 100,000-row groups"),
        "V3": dict(row_group=ROW_GROUP, prune="date",
                   parts=[(f"m={m:02d}", f"month(datetime) = {m}")
                          for m in range(1, 13)],
                   note="12 files, one per calendar month"),
        "V4": dict(row_group=ROW_GROUP, prune="tile",
                   parts=[(label, f"{ZONE_SQL} BETWEEN {lo} AND {hi}")
                          for label, lo, hi in ZONE_PARTS_8],
                   note="8 files, the s2_build ZONE_PARTS_8 UTM-zone octants"),
        # Flat stems ("z21-31-m=05") rather than nested directories: a
        # partition value with a slash in it is what DuckDB PARTITION_BY
        # escapes, and the byte counts a search pays do not care whether the
        # key is one path segment or two.
        "V5": dict(row_group=ROW_GROUP, prune="both",
                   parts=[(f"{label}-m={m:02d}",
                           f"{ZONE_SQL} BETWEEN {lo} AND {hi} "
                           f"AND month(datetime) = {m}")
                          for label, lo, hi in ZONE_PARTS_8
                          for m in range(1, 13)],
                   note="96 files, octant x month"),
    }


# V6 (one file per UTM zone) and V7 (one file per MGRS grid-zone prefix)
# enumerate the keys actually present in the year, so an empty zone or an
# unpopulated band costs no object. build_layout.py fills these in.
DISCOVERED = {
    "V6": dict(row_group=ROW_GROUP, prune="tile", key_sql=ZONE_SQL,
               stem=lambda v: f"z={int(v):02d}",
               pred=lambda v: f"{ZONE_SQL} = {int(v)}",
               note="one file per UTM zone (<= 60)"),
    # V7, the chosen variant. Two reasons, both arithmetic on the measured
    # 2018 numbers (footer ~9.4 KB per row group of 57 columns; the eight
    # search columns cost ~27 B/row compressed):
    #
    # 1. The grid-zone prefix is the FINEST partition key a client can derive
    #    from a tile id with no index and no extra input -- one regex, works
    #    for every query shape including the whole-year one, where a month
    #    partition prunes nothing. 864 keys in 2018, median 1,770 rows.
    # 2. A part that small makes the footer a rounding error (1-4 row groups,
    #    ~10-40 KB) AND caps the per-search chunk read at the part's own row
    #    count. Row groups of 2,000 rather than 6,000 sit near the optimum of
    #    (footer 9.4 KB x R/g) + (chunks 27 B x g) for R in the 1-6,438 range
    #    these parts hold: g = sqrt(9.4e3 x R / 27) is 590-4,300.
    "V7": dict(row_group=2_000, prune="tile", key_sql=PREFIX_SQL,
               stem=lambda v: f"t={v}",
               pred=lambda v: f"{PREFIX_SQL} = '{v}'",
               note="one file per MGRS grid-zone prefix (zone + latitude "
                    "band, '31U'), 2,000-row groups: the finest partition "
                    "key a client can compute from the tile id alone"),
}

ALL = tuple(fixed_variants()) + tuple(DISCOVERED)
