/**
 * Ingestion crawler for the ACM (Autoriteit Consument & Markt) MCP server.
 *
 * Scrapes competition decisions, merger control decisions, and sector data
 * from acm.nl and populates the SQLite database.
 *
 * Data sources:
 *   - Sitemap XML pages (acm.nl/sitemap.xml?page=N) for URL discovery
 *   - Individual publication pages (acm.nl/nl/publicaties/...)
 *   - Concentration decisions (concentratiebesluiten)
 *   - Cartel/fine decisions (boetebesluiten, kartelzaken)
 *   - Abuse of dominance decisions
 *   - Sector inquiries
 *
 * Usage:
 *   npx tsx scripts/ingest-acm.ts
 *   npx tsx scripts/ingest-acm.ts --dry-run
 *   npx tsx scripts/ingest-acm.ts --resume
 *   npx tsx scripts/ingest-acm.ts --force
 *   npx tsx scripts/ingest-acm.ts --max-pages 5
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["ACM_DB_PATH"] ?? "data/acm-comp.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.acm.nl";
const SITEMAP_PAGES = 9;
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarACMCrawler/1.0 (+https://github.com/Ansvar-Systems/dutch-competition-mcp)";

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxSitemapPages = maxPagesArg ? parseInt(maxPagesArg, 10) : SITEMAP_PAGES;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  mw_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "nl,en;q=0.5",
        },
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`);
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(`  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`);
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Sitemap parsing — discover publication URLs
// ---------------------------------------------------------------------------

/**
 * URL patterns that indicate competition-related decisions.
 * We match against the URL slug to identify relevant pages.
 */
const DECISION_URL_PATTERNS = [
  "concentratiebesluit",
  "concentratiemelding",
  "boete",
  "kartel",
  "sanctie",
  "mededinging",
  "besluit",
  "fusie",
  "overname",
  "machtspositie",
  "marktwerking",
  "overtreding",
  "clementie",
];

function isRelevantPublicationUrl(url: string): boolean {
  // Must be a Dutch publication page
  if (!url.includes("/nl/publicaties/")) return false;

  // Exclude index/overview pages (no slug after /publicaties/)
  const pathAfterPublicaties = url.split("/nl/publicaties/")[1];
  if (!pathAfterPublicaties || pathAfterPublicaties.length < 5) return false;

  // Exclude subcategory listing pages
  if (pathAfterPublicaties === "marktonderzoeken") return false;
  if (pathAfterPublicaties.startsWith("openbare-stukken")) return false;

  // Match against decision-related URL patterns
  const slug = pathAfterPublicaties.toLowerCase();
  return DECISION_URL_PATTERNS.some((pattern) => slug.includes(pattern));
}

async function discoverUrlsFromSitemap(maxPages: number): Promise<string[]> {
  const urls: string[] = [];
  console.log(`\nDiscovering URLs from ${maxPages} sitemap pages...`);

  for (let page = 1; page <= maxPages; page++) {
    const sitemapUrl = `${BASE_URL}/sitemap.xml?page=${page}`;
    console.log(`  Fetching sitemap page ${page}/${maxPages}...`);

    const xml = await rateLimitedFetch(sitemapUrl);
    if (!xml) {
      console.warn(`  [WARN] Could not fetch sitemap page ${page}`);
      continue;
    }

    // Parse XML — cheerio handles XML adequately for sitemap extraction
    const $ = cheerio.load(xml, { xmlMode: true });
    $("url").each((_i, el) => {
      const loc = $(el).find("loc").text().trim();
      if (loc && isRelevantPublicationUrl(loc)) {
        urls.push(loc);
      }
    });

    console.log(`    Found ${urls.length} relevant URLs so far`);
  }

  // Deduplicate
  const unique = [...new Set(urls)];
  console.log(`\nDiscovered ${unique.length} relevant publication URLs (deduplicated)`);
  return unique;
}

// ---------------------------------------------------------------------------
// Page parsing — extract structured data from individual decision pages
// ---------------------------------------------------------------------------

/**
 * Extract metadata fields from the ACM publication page.
 *
 * ACM pages use labelled metadata fields:
 *   - Publicatiedatum (publication date)
 *   - Beslisdatum (decision date)
 *   - Publicatietype (publication type)
 *   - Zaak / Zaaknummer (case number)
 *   - Partijen (parties)
 *   - Trefwoorden (keywords)
 *   - Onderwerpen (subjects)
 *   - Boetebedrag (fine amount) — present on some fine decisions
 */
function extractMetadata($: cheerio.CheerioAPI): Record<string, string> {
  const meta: Record<string, string> = {};

  // ACM uses a definition-list style or labelled field pattern.
  // Try multiple selectors that match common Drupal/government site layouts.

  // Pattern 1: Definition list (dl/dt/dd)
  $("dl dt, .field--label, .field-label, .label").each((_i, el) => {
    const label = $(el).text().trim().replace(/:$/, "").toLowerCase();
    const valueEl =
      $(el).next("dd").length > 0
        ? $(el).next("dd")
        : $(el).next(".field--item, .field-item, .field__item").length > 0
          ? $(el).next(".field--item, .field-item, .field__item")
          : $(el).parent().find(".field--item, .field-item, .field__item").first();
    if (valueEl.length > 0) {
      meta[label] = valueEl.text().trim();
    }
  });

  // Pattern 2: Structured field wrappers (Drupal)
  $(
    ".field--name-field-publication-date, .field--name-field-decision-date, .field--name-field-case-number, .field--name-field-parties, .field--name-field-keywords, .field--name-field-subjects",
  ).each((_i, el) => {
    const label =
      $(el).find(".field--label, .field-label").text().trim().replace(/:$/, "").toLowerCase() || "";
    const value = $(el).find(".field--item, .field-item, .field__item").text().trim();
    if (label && value) {
      meta[label] = value;
    }
  });

  // Pattern 3: Table-based metadata (some older ACM pages)
  $("table.field-group-table tr, .publication-details tr").each((_i, el) => {
    const cells = $(el).find("td, th");
    if (cells.length >= 2) {
      const label = $(cells[0]).text().trim().replace(/:$/, "").toLowerCase();
      const value = $(cells[1]).text().trim();
      if (label && value) {
        meta[label] = value;
      }
    }
  });

  // Pattern 4: Inline text matching for metadata that appears in body text
  const fullText = $("article, .node, .content, main").text();

  // Case number from text (ACM/YY/NNNNNN pattern)
  if (!meta["zaak"] && !meta["zaaknummer"]) {
    const caseMatch = fullText.match(/ACM\/\d{2}\/\d{5,}/);
    if (caseMatch) {
      meta["zaaknummer"] = caseMatch[0];
    }
  }

  return meta;
}

/** Parse a Dutch date string (dd-mm-yyyy or d mmmm yyyy) to ISO format. */
function parseDutchDate(raw: string): string | null {
  if (!raw) return null;

  // Try dd-mm-yyyy
  const dashMatch = raw.match(/(\d{1,2})-(\d{1,2})-(\d{4})/);
  if (dashMatch) {
    const [, day, month, year] = dashMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Try "d mmmm yyyy" (Dutch month names)
  const dutchMonths: Record<string, string> = {
    januari: "01",
    februari: "02",
    maart: "03",
    april: "04",
    mei: "05",
    juni: "06",
    juli: "07",
    augustus: "08",
    september: "09",
    oktober: "10",
    november: "11",
    december: "12",
  };

  const textMatch = raw.match(/(\d{1,2})\s+(\w+)\s+(\d{4})/);
  if (textMatch) {
    const [, day, monthName, year] = textMatch;
    const monthNum = dutchMonths[monthName!.toLowerCase()];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // Try yyyy-mm-dd (already ISO)
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return isoMatch[0];
  }

  return null;
}

/** Extract a fine amount from text. Handles Dutch number formatting. */
function extractFineAmount(text: string): number | null {
  // Look for euro amounts: "€ 39.000.000" or "39 miljoen euro" or "2,2 miljoen euro"
  const patterns = [
    // "€ 1.234.567" or "EUR 1.234.567"
    /(?:€|EUR)\s*([\d.]+(?:,\d+)?)/gi,
    // "N miljoen euro" / "N miljard euro"
    /([\d.,]+)\s*miljoen\s*euro/gi,
    /([\d.,]+)\s*miljard\s*euro/gi,
    // "euro N" pattern
    /euro\s*([\d.]+(?:,\d+)?)/gi,
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1]) {
      let numStr = match[1];

      // Check for "miljoen" or "miljard" in the pattern source
      if (pattern.source.includes("miljard")) {
        // N,M miljard → multiply by 1_000_000_000
        numStr = numStr.replace(/\./g, "").replace(",", ".");
        return parseFloat(numStr) * 1_000_000_000;
      }
      if (pattern.source.includes("miljoen")) {
        // N,M miljoen → multiply by 1_000_000
        numStr = numStr.replace(/\./g, "").replace(",", ".");
        return parseFloat(numStr) * 1_000_000;
      }

      // Direct amount: Dutch uses dots as thousands separators, comma for decimal
      numStr = numStr.replace(/\./g, "").replace(",", ".");
      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 0) return val;
    }
  }

  return null;
}

/** Extract cited Mededingingswet / EU treaty articles from text. */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  // Art. N Mededingingswet / Mw
  const mwPattern = /Art(?:ikel)?\.?\s*(\d+)\s*(?:(?:lid\s*\d+\s*)?(?:van\s*de\s*)?)?Mededingingswet|Art(?:ikel)?\.?\s*(\d+)\s*Mw\b/gi;
  let m: RegExpExecArray | null;
  while ((m = mwPattern.exec(text)) !== null) {
    const artNum = m[1] ?? m[2];
    articles.add(`Art. ${artNum} Mededingingswet`);
  }

  // Art. 101/102 VWEU / TFEU
  const euPattern = /Art(?:ikel)?\.?\s*(101|102)\s*(?:VWEU|TFEU|VwEU)/gi;
  while ((m = euPattern.exec(text)) !== null) {
    articles.add(`Art. ${m[1]} VWEU`);
  }

  return [...articles];
}

/**
 * Classify a decision/publication based on its metadata and content.
 */
function classifyPublicationType(
  meta: Record<string, string>,
  title: string,
  bodyText: string,
): { isMerger: boolean; isDecision: boolean; type: string | null; outcome: string | null } {
  const titleLower = title.toLowerCase();
  const keywords = (meta["trefwoorden"] ?? "").toLowerCase();
  const subjects = (meta["onderwerpen"] ?? "").toLowerCase();
  const pubType = (meta["publicatietype"] ?? "").toLowerCase();
  const allText = `${titleLower} ${keywords} ${bodyText.toLowerCase().slice(0, 2000)}`;

  // Merger / concentration
  const isMerger =
    titleLower.includes("concentratiebesluit") ||
    titleLower.includes("concentratiemelding") ||
    titleLower.includes("vergunningaanvraag") ||
    titleLower.includes("mag") && (titleLower.includes("overnemen") || titleLower.includes("verkrijgen")) ||
    keywords.includes("fusies en overnames") ||
    keywords.includes("concentratie");

  // Decision type classification
  let type: string | null = null;
  if (keywords.includes("kartel") || allText.includes("kartelverbod") || allText.includes("kartelafspraken")) {
    type = "cartel";
  } else if (
    keywords.includes("misbruik machtspositie") ||
    allText.includes("misbruik van machtspositie") ||
    allText.includes("abuse of dominance") ||
    allText.includes("dominante positie")
  ) {
    type = "abuse_of_dominance";
  } else if (allText.includes("sectoronderzoek") || allText.includes("marktonderzoek")) {
    type = "sector_inquiry";
  } else if (keywords.includes("sanctie") || allText.includes("boetebesluit") || allText.includes("bestuurlijke boete")) {
    type = "sanction";
  } else if (allText.includes("toezegging") || allText.includes("commitment")) {
    type = "commitment_decision";
  } else if (isMerger) {
    type = "merger_control";
  } else if (pubType === "besluit") {
    type = "decision";
  }

  // Outcome classification
  let outcome: string | null = null;
  if (allText.includes("boete") && allText.includes("opgelegd") || allText.includes("beboet")) {
    outcome = "fine";
  } else if (allText.includes("goedgekeurd") || allText.includes("mag") && allText.includes("overnemen") && !allText.includes("niet")) {
    outcome = isMerger ? "cleared_phase1" : "cleared";
  } else if (allText.includes("niet overnemen") || allText.includes("verboden") || allText.includes("geweigerd")) {
    outcome = "blocked";
  } else if (allText.includes("toezegging") || allText.includes("voorwaarden") || allText.includes("onder voorwaarde")) {
    outcome = "cleared_with_conditions";
  } else if (allText.includes("gesloten") || allText.includes("afgerond") || allText.includes("afgesloten")) {
    outcome = "cleared";
  } else if (allText.includes("vergunning") && allText.includes("fase")) {
    outcome = allText.includes("fase 2") || allText.includes("fase II") ? "cleared_phase2" : "cleared_phase1";
  }

  const isDecision = !isMerger && (type !== null || pubType === "besluit");

  return { isMerger, isDecision, type, outcome };
}

/** Map Dutch subject/keyword terms to sector IDs. */
function classifySector(meta: Record<string, string>, title: string, bodyText: string): string | null {
  const text = `${meta["onderwerpen"] ?? ""} ${meta["trefwoorden"] ?? ""} ${title} ${bodyText.slice(0, 1500)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    { id: "digitaal", patterns: ["digitale economie", "online platform", "big tech", "app store", "digitale markt"] },
    { id: "energie", patterns: ["energie", "elektriciteit", "gas", "warmte", "netbeheer", "energieleveran"] },
    { id: "telecom", patterns: ["telecom", "breedband", "telefonie", "glasvezel", "frequentie", "kabel"] },
    { id: "zorg", patterns: ["zorg", "ziekenhuis", "farmac", "geneesmiddel", "medisch", "apotheek", "zorgverzeker"] },
    { id: "financiele_diensten", patterns: ["financ", "bank", "verzekering", "betaalverkeer", "betalingsverkeer", "pensioenfonds"] },
    { id: "retail", patterns: ["detailhandel", "supermarkt", "retail", "levensmiddel", "winkel"] },
    { id: "vervoer", patterns: ["vervoer", "transport", "luchtvaart", "spoor", "scheepvaart", "taxi", "ov"] },
    { id: "bouw", patterns: ["bouw", "vastgoed", "woningbouw", "aanbesteding bouw", "installatiebranche"] },
    { id: "agrarisch", patterns: ["agrarisch", "landbouw", "veeteelt", "zuivel", "eieren", "pluimvee", "tuinbouw"] },
    { id: "media", patterns: ["media", "uitgevers", "omroep", "persvrijheid"] },
    { id: "post", patterns: ["post", "pakketbezorging", "brief"] },
    { id: "water", patterns: ["drinkwater", "waterschap", "waterleidingbedrijf"] },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

/** Extract acquiring party and target from a merger title / body. */
function extractMergerParties(
  title: string,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  // Common ACM title pattern: "X mag Y overnemen" / "X wil Y overnemen"
  const titleMatch = title.match(
    /^(.+?)\s+(?:mag|wil)\s+(?:uitsluitende\s+zeggenschap\s+verkrijgen\s+over\s+)?(.+?)(?:\s+overnemen|\s+\(concentratie)/i,
  );
  if (titleMatch) {
    return {
      acquiring: titleMatch[1]!.trim(),
      target: titleMatch[2]!.trim(),
    };
  }

  // Fallback: look in the body for "X neemt Y over" patterns
  const bodyMatch = bodyText.match(
    /(.+?)\s+(?:heeft|wil|gaat)\s+(.+?)\s+(?:overnemen|overgenomen|verkrijgen)/i,
  );
  if (bodyMatch) {
    return {
      acquiring: bodyMatch[1]!.trim().slice(0, 200),
      target: bodyMatch[2]!.trim().slice(0, 200),
    };
  }

  return { acquiring: null, target: null };
}

/** Generate a case number when none is found in the metadata. */
function generateCaseNumber(url: string, title: string): string {
  // Use the URL slug as a fallback identifier
  const slug = url.split("/nl/publicaties/").pop() ?? "";
  const shortSlug = slug.slice(0, 60).replace(/-+$/, "");
  return `ACM-WEB/${shortSlug}`;
}

/**
 * Parse a single ACM publication page and return a decision or merger record.
 */
function parsePage(
  html: string,
  url: string,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // Extract title
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    $("title").text().trim().replace(/ \| ACM$/, "") ||
    "";

  if (!title) {
    return { decision: null, merger: null };
  }

  // Extract metadata fields
  const meta = extractMetadata($);

  // Extract body text
  const bodySelectors = [
    "article .field--name-body",
    "article .body",
    ".node__content .field--name-body",
    ".content-area .field--type-text-with-summary",
    "article .text-long",
    ".publication-body",
    "main article",
    ".node--type-publication .content",
  ];

  let bodyText = "";
  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0) {
      bodyText = el.text().trim();
      break;
    }
  }

  // Fallback: grab all paragraph text from main content
  if (!bodyText) {
    const paragraphs: string[] = [];
    $("article p, main p, .content p").each((_i, el) => {
      const text = $(el).text().trim();
      if (text.length > 30) paragraphs.push(text);
    });
    bodyText = paragraphs.join("\n\n");
  }

  // If still empty, use the full page text minus nav/footer
  if (!bodyText) {
    $("nav, footer, header, .menu, .breadcrumb, script, style").remove();
    bodyText = $("main, article, .content").text().trim();
  }

  if (!bodyText || bodyText.length < 50) {
    return { decision: null, merger: null };
  }

  // Extract structured fields
  const caseNumber =
    meta["zaaknummer"] ?? meta["zaak"] ?? meta["case number"] ?? generateCaseNumber(url, title);

  const rawDate = meta["beslisdatum"] ?? meta["publicatiedatum"] ?? meta["datum"] ?? "";
  const date = parseDutchDate(rawDate);

  const { isMerger, isDecision, type, outcome } = classifyPublicationType(meta, title, bodyText);
  const sector = classifySector(meta, title, bodyText);

  // Build the full_text field: combine summary and body
  const summary = bodyText.slice(0, 500).replace(/\s+/g, " ").trim();
  const fullText = bodyText;

  if (isMerger) {
    const { acquiring, target } = extractMergerParties(title, bodyText);
    const parties = meta["partijen"] ?? null;

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date,
        sector,
        acquiring_party: acquiring ?? parties?.split(/[,;]/)[0]?.trim() ?? null,
        target: target ?? null,
        summary,
        full_text: fullText,
        outcome: outcome ?? "pending",
        turnover: null, // Not reliably extractable from HTML
      },
    };
  }

  if (isDecision) {
    const parties = meta["partijen"] ?? null;
    const fineAmount = extractFineAmount(fullText);
    const articles = extractLegalArticles(fullText);

    return {
      decision: {
        case_number: caseNumber,
        title,
        date,
        type,
        sector,
        parties: parties ? JSON.stringify(parties.split(/[,;]/).map((p) => p.trim()).filter(Boolean)) : null,
        summary,
        full_text: fullText,
        outcome: outcome ?? (fineAmount ? "fine" : "pending"),
        fine_amount: fineAmount,
        mw_articles: articles.length > 0 ? JSON.stringify(articles) : null,
        status: "final",
      },
      merger: null,
    };
  }

  // If we cannot clearly classify, treat as a decision (broader category)
  const fineAmount = extractFineAmount(fullText);
  const articles = extractLegalArticles(fullText);

  return {
    decision: {
      case_number: caseNumber,
      title,
      date,
      type: type ?? "decision",
      sector,
      parties: meta["partijen"]
        ? JSON.stringify(
            meta["partijen"]
              .split(/[,;]/)
              .map((p) => p.trim())
              .filter(Boolean),
          )
        : null,
      summary,
      full_text: fullText,
      outcome: outcome ?? (fineAmount ? "fine" : "pending"),
      fine_amount: fineAmount,
      mw_articles: articles.length > 0 ? JSON.stringify(articles) : null,
      status: "final",
    },
    merger: null,
  };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, mw_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, mw_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      mw_articles = excluded.mw_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return { insertDecision, upsertDecision, insertMerger, upsertMerger, upsertSector };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== ACM Competition Decisions Crawler ===");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${dryRun}`);
  console.log(`  Resume:     ${resume}`);
  console.log(`  Force:      ${force}`);
  console.log(`  Max pages:  ${maxSitemapPages}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // Step 1: Discover URLs from sitemap
  const allUrls = await discoverUrlsFromSitemap(maxSitemapPages);

  // Filter already-processed URLs (for --resume)
  const urlsToProcess = resume
    ? allUrls.filter((u) => !processedSet.has(u))
    : allUrls;

  console.log(`\nURLs to process: ${urlsToProcess.length}`);
  if (resume && allUrls.length !== urlsToProcess.length) {
    console.log(`  Skipping ${allUrls.length - urlsToProcess.length} already-processed URLs`);
  }

  if (urlsToProcess.length === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // Step 2: Initialize database (unless dry run)
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // Step 3: Process each URL
  let decisionsIngested = state.decisionsIngested;
  let mergersIngested = state.mergersIngested;
  let errors = 0;
  let skipped = 0;

  const sectorCounts: SectorAccumulator = {};

  for (let i = 0; i < urlsToProcess.length; i++) {
    const url = urlsToProcess[i]!;
    const progress = `[${i + 1}/${urlsToProcess.length}]`;

    console.log(`${progress} Fetching: ${url}`);

    const html = await rateLimitedFetch(url);
    if (!html) {
      console.log(`  SKIP — could not fetch`);
      state.errors.push(`fetch_failed: ${url}`);
      errors++;
      continue;
    }

    try {
      const { decision, merger } = parsePage(html, url);

      if (decision) {
        if (dryRun) {
          console.log(`  DECISION: ${decision.case_number} — ${decision.title.slice(0, 80)}`);
          console.log(`    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}`);
        } else {
          const stmt = force ? stmts!.upsertDecision : stmts!.insertDecision;
          stmt.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.mw_articles,
            decision.status,
          );
          console.log(`  INSERTED decision: ${decision.case_number}`);
        }

        decisionsIngested++;

        // Track sector counts
        if (decision.sector) {
          if (!sectorCounts[decision.sector]) {
            sectorCounts[decision.sector] = {
              name: decision.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[decision.sector]!.decisionCount++;
        }
      } else if (merger) {
        if (dryRun) {
          console.log(`  MERGER: ${merger.case_number} — ${merger.title.slice(0, 80)}`);
          console.log(`    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 40)}`);
        } else {
          const stmt = force ? stmts!.upsertMerger : stmts!.insertMerger;
          stmt.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          console.log(`  INSERTED merger: ${merger.case_number}`);
        }

        mergersIngested++;

        // Track sector counts
        if (merger.sector) {
          if (!sectorCounts[merger.sector]) {
            sectorCounts[merger.sector] = {
              name: merger.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[merger.sector]!.mergerCount++;
        }
      } else {
        console.log(`  SKIP — could not parse structured data`);
        skipped++;
      }

      // Mark URL as processed
      processedSet.add(url);
      state.processedUrls.push(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`parse_error: ${url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 URLs)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} URLs`);
    }
  }

  // Step 4: Update sector counts
  if (!dryRun && db && stmts) {
    const sectorMeta: Record<string, { name: string; name_en: string }> = {
      digitaal: { name: "Digitale economie", name_en: "Digital Economy" },
      energie: { name: "Energie", name_en: "Energy" },
      telecom: { name: "Telecommunicatie", name_en: "Telecommunications" },
      zorg: { name: "Gezondheidszorg", name_en: "Healthcare" },
      financiele_diensten: { name: "Financiele diensten", name_en: "Financial Services" },
      retail: { name: "Detailhandel", name_en: "Retail" },
      vervoer: { name: "Vervoer", name_en: "Transport" },
      bouw: { name: "Bouw", name_en: "Construction" },
      agrarisch: { name: "Agrarisch", name_en: "Agriculture" },
      media: { name: "Media", name_en: "Media" },
      post: { name: "Post en pakketbezorging", name_en: "Postal Services" },
      water: { name: "Drinkwater", name_en: "Water" },
    };

    // Count decisions and mergers per sector from the database
    const decisionSectorCounts = db
      .prepare("SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector")
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare("SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector")
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<string, { decisions: number; mergers: number }> = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector]) finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector]) finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const meta = sectorMeta[id];
        stmts!.upsertSector.run(
          id,
          meta?.name ?? id,
          meta?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(`\nUpdated ${Object.keys(finalSectorCounts).length} sector records`);
  }

  // Step 5: Final state save
  state.decisionsIngested = decisionsIngested;
  state.mergersIngested = mergersIngested;
  saveState(state);

  // Step 6: Summary
  if (!dryRun && db) {
    const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
    const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
    const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${decisionsIngested - state.decisionsIngested + decisionsIngested}`);
    console.log(`  New mergers:      ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log(`\nDone.`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
