# AGENTS.md — Sentinel-2 L2A STAC-GeoParquet Mirror

Guidance for AI agents and automated clients working with this catalog.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file. An agent acting on an
invented column name or an invented join key produces a confident wrong
answer, and nothing downstream catches it.

## What this catalog holds

Public root: `https://data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json`

One collection: `sentinel-2-l2a`, the AWS Earth Search Sentinel-2 L2A item
index as year-partitioned STAC-GeoParquet. Earth Search held 51.25 million
items when it was counted on 2026-09-15. A `stats` collection with MGRS-tile
aggregates follows.

Read [`sentinel-2-l2a/AGENTS.md`](sentinel-2-l2a/AGENTS.md) before you query.
It carries the schema, the query pattern, the NULLs to expect, and the
contract for the `assets` JSON-string column. Nothing here repeats it.

Coverage is partial before December 2018: nothing for 2015-2016, part of
2017-2018. That is Earth Search's record. Do not report a missing year as an
observation about Sentinel-2 itself.

This is a **mirror**. Earth Search, run by
[Element 84](https://element84.com/) on the AWS Registry of Open Data,
produces the item index this catalog republishes. Nothing is filtered,
reclassified or interpolated here.

## Structure

Assets and structural links resolve relative to the object that carries them.
Catalogs here carry no `self` link, so a client tracks its own location.
