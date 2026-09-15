# AGENTS.md — Sentinel-2 L2A STAC-GeoParquet Mirror

Guidance for AI agents and automated clients working with this catalog.

**One rule survives every edit to this file.** Every claim here is either
quoted from a source or measured from the data. If you cannot point at where a
fact came from, it does not belong in this file. An agent acting on an
invented column name or an invented join key produces a confident wrong
answer, and nothing downstream catches it.

## What this catalog holds

Public root: `https://data.source.coop/portolan-mirrors/sentinel-2-catalog/catalog.json`

**No collection is published yet.** This repository is scaffolding for a
catalog that will mirror the AWS Earth Search Sentinel-2 L2A item index —
about 28 million scenes, 2015-07-04 to today — as partitioned
STAC-GeoParquet, plus MGRS-tile aggregate stats. The
[design spec](https://github.com/portolan-mirrors/sentinel-2-catalog/blob/main/docs/superpowers/specs/2026-09-15-sentinel-2-catalog-design.md)
in this repository has the planned schema, partition layout, and column
meanings; once `sentinel-2-l2a` and `stats` publish, this section and
`sentinel-2-l2a/AGENTS.md` carry the verified facts.

This will be a **mirror**. Earth Search, run by
[Element 84](https://element84.com/) on the AWS Registry of Open Data,
produces the item index this catalog republishes.

## Structure

Assets and structural links resolve relative to the object that carries them.
Catalogs here carry no `self` link, so a client tracks its own location.
