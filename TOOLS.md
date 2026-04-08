# Tools Reference

This MCP server exposes **8 tools** for querying ACM (Autoriteit Consument en Markt) competition law data.

---

## `nl_comp_search_decisions`

Full-text search across ACM enforcement decisions.

**Input**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | Search query (e.g., `kartelafspraken`, `misbruik van machtspositie`) |
| `type` | string | no | Filter: `abuse_of_dominance`, `cartel`, `merger`, `sector_inquiry` |
| `sector` | string | no | Filter by sector ID (see `nl_comp_list_sectors`) |
| `outcome` | string | no | Filter: `prohibited`, `cleared`, `cleared_with_conditions`, `fine` |
| `limit` | number | no | Max results (default 20, max 100) |

**Output** — `{ results: Decision[], count: number, _meta }`

---

## `nl_comp_get_decision`

Get a specific ACM enforcement decision by case number.

**Input**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `case_number` | string | yes | ACM case number (e.g., `ACM/17/028447`) |

**Output** — `Decision | error`

---

## `nl_comp_search_mergers`

Search ACM merger control decisions.

**Input**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query` | string | yes | Search query (e.g., `telecom`, `energie`, `retail`) |
| `sector` | string | no | Filter by sector ID |
| `outcome` | string | no | Filter: `cleared`, `cleared_phase1`, `cleared_with_conditions`, `prohibited` |
| `limit` | number | no | Max results (default 20, max 100) |

**Output** — `{ results: Merger[], count: number, _meta }`

---

## `nl_comp_get_merger`

Get a specific ACM merger control decision by case number.

**Input**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `case_number` | string | yes | Merger case number (e.g., `M.7000`) |

**Output** — `Merger | error`

---

## `nl_comp_list_sectors`

List all sectors with ACM enforcement activity.

**Input** — none

**Output** — `{ sectors: Sector[], count: number, _meta }`

---

## `nl_comp_about`

Return server metadata: version, data source, coverage, tool list.

**Input** — none

**Output** — `{ name, version, description, data_source, tools[], _meta }`

---

## `nl_comp_list_sources`

List all data sources with provenance metadata.

**Input** — none

**Output** — `{ sources: Source[], _meta }`

Each source includes: `name`, `url`, `scope`, `license`, `last_ingested`.

---

## `nl_comp_check_data_freshness`

Check database staleness and record counts.

**Input** — none

**Output** — `{ status, last_ingested, record_counts, stale_threshold_days, _meta }`

`status` is `"fresh"`, `"stale"`, or `"unknown"`.

---

## Common `_meta` Block

All tool responses include a `_meta` block:

```json
{
  "_meta": {
    "disclaimer": "Data sourced from official ACM publications. Not legal advice — verify against primary sources.",
    "copyright": "© Autoriteit Consument en Markt (ACM). Data used for research purposes.",
    "source_url": "https://www.acm.nl/",
    "data_age": "Periodic updates; may lag official publications."
  }
}
```
