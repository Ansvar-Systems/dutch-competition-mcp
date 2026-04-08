# Corpus Coverage

This document describes the completeness of the ACM (Autoriteit Consument en Markt) dataset included in this MCP server.

## Summary

| Category | Count | Date Range | Notes |
|----------|-------|------------|-------|
| Enforcement decisions | TBD | TBD | Abuse of dominance, cartels, sector inquiries |
| Merger control cases | TBD | TBD | Phase 1, Phase 2, conditional clearances |
| Sectors covered | TBD | — | See `nl_comp_list_sectors` tool |

*Counts are populated at ingest time. Run `npm run ingest` to update.*

## Source

All data is sourced from official ACM publications at [acm.nl](https://www.acm.nl/).

## Known Gaps

- Decisions published only as press releases (no full decision PDF) may be omitted.
- Decisions older than the crawler start date are not included in the initial corpus.
- Non-public decisions (confidential versions) are not included.

## Freshness

Database freshness can be checked with the `nl_comp_check_data_freshness` tool, which returns the last ingest timestamp and record counts.

See `data/coverage.json` for machine-readable corpus statistics.
