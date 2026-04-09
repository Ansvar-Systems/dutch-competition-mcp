#!/usr/bin/env node

/**
 * ACM Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying Autoriteit Consument en Markt decisions, merger control
 * cases, and sector enforcement activity under Mededingingswet (Mw).
 *
 * Tool prefix: nl_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchMergers,
  getMerger,
  listSectors,
  getDbStats,
} from "./db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "dutch-competition-mcp";

// --- Tool definitions ---------------------------------------------------------

const TOOLS = [
  {
    name: "nl_comp_search_decisions",
    description:
      "Full-text search across ACM enforcement decisions (abuse of dominance, cartel, sector inquiries). Returns matching decisions with case number, parties, outcome, fine amount, and Mw articles cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'kartelafspraken', 'misbruik van machtspositie', 'concentratie')",
        },
        type: {
          type: "string",
          enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"],
          description: "Filter by decision type. Optional.",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'digital_economy', 'energy', 'food_retail'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"],
          description: "Filter by outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "nl_comp_get_decision",
    description:
      "Get a specific ACM decision by case number (e.g., 'ACM/17/028447', 'M.7000').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "ACM case number (e.g., 'ACM/17/028447', 'M.7000')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "nl_comp_search_mergers",
    description:
      "Search ACM merger control decisions. Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'Zilveren Kruis / De Friesland', 'UPC / Ziggo')",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'energy', 'food_retail', 'real_estate'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"],
          description: "Filter by merger outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "nl_comp_get_merger",
    description:
      "Get a specific ACM merger control decision by case number (e.g., 'ACM/17/028447', 'M.7000').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "ACM merger case number (e.g., 'ACM/17/028447', 'M.7000')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "nl_comp_list_sectors",
    description:
      "List all sectors with ACM enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "nl_comp_about",
    description:
      "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "nl_comp_list_sources",
    description:
      "List all data sources used by this server with provenance metadata: source name, URL, last_ingested date, scope, and license.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "nl_comp_check_data_freshness",
    description:
      "Check data freshness: returns staleness status of the SQLite database, last ingest timestamp, and record counts.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation --------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  case_number: z.string().min(1),
});

const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetMergerArgs = z.object({
  case_number: z.string().min(1),
});

// --- Helper ------------------------------------------------------------------

const _meta = {
  disclaimer: "Data sourced from official ACM publications. Not legal advice — verify against primary sources.",
  copyright: "© Autoriteit Consument en Markt (ACM). Data used for research purposes.",
  source_url: "https://www.acm.nl/",
  data_age: "Periodic updates; may lag official publications.",
};

function textContent(data: unknown) {
  const payload = typeof data === "object" && data !== null
    ? { ...(data as Record<string, unknown>), _meta }
    : { data, _meta };
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(payload, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// --- Server setup ------------------------------------------------------------

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "nl_comp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({
          query: parsed.query,
          type: parsed.type,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "nl_comp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.case_number);
        if (!decision) {
          return errorContent(`Decision not found: ${parsed.case_number}`);
        }
        return textContent(decision);
      }

      case "nl_comp_search_mergers": {
        const parsed = SearchMergersArgs.parse(args);
        const results = searchMergers({
          query: parsed.query,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "nl_comp_get_merger": {
        const parsed = GetMergerArgs.parse(args);
        const merger = getMerger(parsed.case_number);
        if (!merger) {
          return errorContent(`Merger case not found: ${parsed.case_number}`);
        }
        return textContent(merger);
      }

      case "nl_comp_list_sectors": {
        const sectors = listSectors();
        return textContent({ sectors, count: sectors.length });
      }

      case "nl_comp_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "ACM (Autoriteit Consument en Markt) MCP server. Provides access to Dutch competition law enforcement decisions and merger control cases under the Mededingingswet.",
          data_source: "ACM (https://www.acm.nl/)",
          coverage: {
            decisions: "Abuse of dominance, cartel enforcement, and sector inquiries",
            mergers: "Merger control decisions — Phase I and Phase II",
            sectors: "digitaal, energie, retail, automotive, financiele diensten, zorg, media, telecommunicatie",
          },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      case "nl_comp_list_sources": {
        return textContent({
          sources: [
            {
              name: "ACM (Autoriteit Consument en Markt)",
              url: "https://www.acm.nl/",
              scope: "Dutch competition law enforcement decisions, merger control cases, sector inquiries",
              license: "Public domain — official government publications",
              last_ingested: null,
            },
          ],
        });
      }

      case "nl_comp_check_data_freshness": {
        const stats = getDbStats();
        const staleDays = 30;
        const status = stats.last_ingested
          ? Math.floor((Date.now() - new Date(stats.last_ingested).getTime()) / 86_400_000) > staleDays
            ? "stale"
            : "fresh"
          : "unknown";
        return textContent({
          status,
          last_ingested: stats.last_ingested,
          record_counts: {
            decisions: stats.decisions,
            mergers: stats.mergers,
            sectors: stats.sectors,
          },
          stale_threshold_days: staleDays,
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// --- Main --------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
