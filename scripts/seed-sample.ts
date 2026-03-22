/**
 * Seed the ACM database with sample decisions, mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["ACM_DB_PATH"] ?? "data/acm-comp.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors -----------------------------------------------------------------

interface SectorRow {
  id: string;
  name: string;
  name_en: string;
  description: string;
  decision_count: number;
  merger_count: number;
}

const sectors: SectorRow[] = [
  { id: "digitaal", name: "Digitale economie", name_en: "Digital Economy", description: "Online platforms, sociale netwerken, appstores en digitale marktplaatsen.", decision_count: 2, merger_count: 1 },
  { id: "energie", name: "Energie", name_en: "Energy", description: "Elektriciteits- en gasleverantie, hernieuwbare energie en energienetwerken.", decision_count: 1, merger_count: 1 },
  { id: "retail", name: "Detailhandel", name_en: "Retail", description: "Supermarkten, levensmiddelenhandel en consumentengoederen.", decision_count: 1, merger_count: 0 },
  { id: "zorg", name: "Gezondheidszorg", name_en: "Healthcare", description: "Ziekenhuizen, farmaceutische industrie, medische hulpmiddelen en zorgverzekeraars.", decision_count: 1, merger_count: 1 },
  { id: "telecom", name: "Telecommunicatie", name_en: "Telecommunications", description: "Mobiele telefonie, breedband, vaste telefonie en telecom-infrastructuur.", decision_count: 0, merger_count: 1 },
  { id: "financiele_diensten", name: "Financiele diensten", name_en: "Financial Services", description: "Banken, verzekeringen, betalingsverkeer en financiele marktinfrastructuur.", decision_count: 1, merger_count: 1 },
];

const insertSector = db.prepare(
  "INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)",
);

for (const s of sectors) {
  insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
}

console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  case_number: string;
  title: string;
  date: string;
  type: string;
  sector: string;
  parties: string;
  summary: string;
  full_text: string;
  outcome: string;
  fine_amount: number | null;
  mw_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  {
    case_number: "ACM/17/028447",
    title: "Zorgverzekeraars — Kartelafspraken over zorginkoop",
    date: "2018-09-13",
    type: "cartel",
    sector: "zorg",
    parties: JSON.stringify(["CZ Zorgkantoor BV", "VGZ Zorgkantoor BV"]),
    summary: "De ACM heeft twee grote zorgverzekeraars beboet wegens het uitwisselen van commercieel gevoelige informatie over hun zorginkoop bij thuiszorgaanbieders. De zorgverzekeraars deelden gegevens die hun concurrentiepositie bij de inkoop van thuiszorg beinvloedden.",
    full_text: "De Autoriteit Consument en Markt (ACM) heeft vastgesteld dat CZ Zorgkantoor en VGZ Zorgkantoor in strijd met het mededingingsrecht commercieel gevoelige informatie hebben uitgewisseld. De twee zorgverzekeraars zijn de grootste inkopers van thuiszorg in Nederland. In de periode 2011-2014 wisselden de zorgkantoren informatie uit over hun inkoopprijzen en -beleid bij thuiszorgaanbieders. Deze informatie-uitwisseling vond plaats in het kader van een samenwerkingsstructuur voor het inkopen van thuiszorg. De ACM heeft vastgesteld dat de uitwisseling van commercieel gevoelige informatie de concurrentie op de inkoopmarkt voor thuiszorg heeft verminderd, waardoor thuiszorgaanbieders hogere prijzen konden vragen. Dit leidde tot hogere kosten voor de volksgezondheid. De ACM heeft beide partijen beboet en hen verplicht de onderlinge informatie-uitwisseling te staken.",
    outcome: "fine",
    fine_amount: 2200000,
    mw_articles: JSON.stringify(["Art. 6 Mededingingswet", "Art. 101 VWEU"]),
    status: "final",
  },
  {
    case_number: "ACM/18/029424",
    title: "Nederlandse grootbanken — Mededingingsrechtelijk onderzoek betalingsverkeer",
    date: "2019-06-20",
    type: "sector_inquiry",
    sector: "financiele_diensten",
    parties: JSON.stringify(["ING Bank NV", "ABN AMRO Bank NV", "Rabobank", "SNS Bank NV"]),
    summary: "De ACM heeft een sectoronderzoek afgerond naar het betalingsverkeer in Nederland. De ACM heeft vastgesteld dat de toegang tot betaalsystemen voor nieuwe marktpartijen beter moet, maar heeft geen formele overtreding van de Mededingingswet vastgesteld.",
    full_text: "De ACM heeft een sectoronderzoek uitgevoerd naar de marktstructuur en concurrentieverhoudingen in het Nederlandse betalingsverkeer. Het onderzoek richtte zich op de toegankelijkheid van betaalsystemen (iDEAL, PIN-betalingen) voor nieuwe toetreders, waaronder fintech-bedrijven en niet-bancaire betaalinstellingen. De ACM constateerde dat nieuwe toetreders op de markt voor betalingsverwerking en betaaldiensten vaak afhankelijk zijn van de grote banken voor toegang tot betaalsystemen. De banken bepalen de technische en commerciele voorwaarden waaronder nieuwe partijen toegang krijgen tot deze systemen. De ACM constateerde dat de toegangsvoorwaarden in sommige gevallen belemmerend werken voor nieuwe toetreders. De ACM heeft aanbevelingen gedaan voor verbetering van de toegankelijkheid van betaalsystemen en heeft de sector opgeroepen tot zelfregulering. De ACM heeft geen formele inbreukbeslissing genomen, maar heeft de sector gewaarschuwd dat verdere stappen kunnen worden genomen als de toegankelijkheid niet verbetert.",
    outcome: "cleared",
    fine_amount: null,
    mw_articles: JSON.stringify(["Art. 24 Mededingingswet", "Art. 102 VWEU"]),
    status: "final",
  },
  {
    case_number: "ACM/21/040000",
    title: "Online reisplatforms — Onderzoek pariteitsbedingen",
    date: "2021-07-15",
    type: "abuse_of_dominance",
    sector: "digitaal",
    parties: JSON.stringify(["Booking.com BV", "Expedia Inc."]),
    summary: "De ACM heeft een onderzoek uitgevoerd naar de pariteitsbedingen (best-price-garanties) van online reisplatforms. Na toezeggingen van Booking.com en Expedia om enge pariteitsbedingen te laten vallen, heeft de ACM het onderzoek gesloten.",
    full_text: "De ACM heeft een onderzoek ingesteld naar het gebruik van pariteitsbedingen (also known as Most Favoured Nation clauses) door online reisplatforms Booking.com en Expedia. Pariteitsbedingen verplichtten hotels om op de platforms nooit hogere prijzen te hanteren dan elders, inclusief hun eigen website. De ACM was van mening dat brede pariteitsbedingen de concurrentie tussen online reisplatforms konden belemmeren, doordat zij hotels verhinderden op andere kanalen lagere prijzen aan te bieden. Na overleg met de ACM en na een Europese afstemming met andere mededingingsautoriteiten, hebben Booking.com en Expedia toezeggingen gedaan om hun brede pariteitsbedingen aan te passen. De platforms zijn nog steeds gerechtigd smalle pariteitsbedingen te hanteren (die alleen betrekking hebben op de eigen kanaalkeuze van de hotelier), maar mogen hotels niet langer verplichten dezelfde prijzen te hanteren op alle online kanalen. De ACM heeft het onderzoek gesloten na aanvaarding van de toezeggingen.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    mw_articles: JSON.stringify(["Art. 6 Mededingingswet", "Art. 24 Mededingingswet", "Art. 101 VWEU"]),
    status: "final",
  },
  {
    case_number: "ACM/22/044567",
    title: "Supermarktketens — Kartel afspraken over personeelsbeleid",
    date: "2022-08-30",
    type: "cartel",
    sector: "retail",
    parties: JSON.stringify(["Albert Heijn BV", "Jumbo Supermarkten BV"]),
    summary: "De ACM heeft een onderzoek ingesteld naar mogelijke afspraken tussen supermarktketens over personeelswerving en arbeidsvoorwaarden. Het onderzoek was gericht op zogenoemde no-poach-overeenkomsten die het werven van elkaars personeel beperken.",
    full_text: "De ACM heeft een sectoronderzoek ingesteld naar de arbeidsmarkt in de supermarktbranche. De ACM onderzocht of supermarktketens afspraken hadden gemaakt die beperkingen oplegden aan de overstap van personeel tussen supermarktketens, zogenoemde no-poach-afspraken of wage-fixing-afspraken. Dergelijke afspraken kunnen de arbeidsmarkt voor supermarktpersoneel verstoren door de mobiliteit van werknemers te beperken en de loonconcurrentie te verminderen. Na uitgebreid onderzoek heeft de ACM vastgesteld dat er geen formele no-poach-overeenkomsten bestonden tussen de grote supermarktketens. De ACM heeft echter wel zorgen geuit over informele praktijken en heeft de sector gewezen op de mededingingsrechtelijke risico's van informele communicatie over arbeidsmarktaangelegenheden.",
    outcome: "cleared",
    fine_amount: null,
    mw_articles: JSON.stringify(["Art. 6 Mededingingswet", "Art. 101 VWEU"]),
    status: "final",
  },
  {
    case_number: "ACM/23/048000",
    title: "Energieleveranciers — Misbruik van dominantie bij klantovergang",
    date: "2023-04-11",
    type: "abuse_of_dominance",
    sector: "energie",
    parties: JSON.stringify(["Vattenfall Warmte Nederland BV", "Eneco BV"]),
    summary: "De ACM heeft een onderzoek ingesteld naar mogelijke obstakels die grote energieleveranciers opwerpen bij de overstap van klanten naar andere leveranciers. Na toezeggingen om overstapprocessen te verbeteren, heeft de ACM het onderzoek gesloten.",
    full_text: "De ACM heeft vastgesteld dat sommige energieleveranciers drempels opwierpen bij de overstap van klanten naar andere aanbieders. Dit betrof met name vertragingen in de verwerking van aanvragen tot beeindiging van contracten, onduidelijke communicatie over overstapkosten en -procedures, en het gebruik van lange opzegtermijnen die feitelijk drempels opwierpen voor overstap. De ACM is van mening dat dergelijke praktijken de consumentenmobiliteit beperken en daarmee de concurrentie op de markt voor energieleverantie belemmeren. Na overleg met de betrokken energieleveranciers hebben deze toezeggingen gedaan om hun overstapprocessen te verbeteren, waaronder: versnelling van de verwerking van overstapverzoeken, verbetering van de communicatie aan klanten over overstaprechten, en beperking van opzegtermijnen tot wettelijk toegestane maxima. De ACM heeft het onderzoek gesloten na aanvaarding van de toezeggingen.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    mw_articles: JSON.stringify(["Art. 24 Mededingingswet", "Art. 102 VWEU"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, mw_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
      d.case_number, d.title, d.date, d.type, d.sector,
      d.parties, d.summary, d.full_text, d.outcome,
      d.fine_amount, d.mw_articles, d.status,
    );
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers -----------------------------------------------------------------

interface MergerRow {
  case_number: string;
  title: string;
  date: string;
  sector: string;
  acquiring_party: string;
  target: string;
  summary: string;
  full_text: string;
  outcome: string;
  turnover: number | null;
}

const mergers: MergerRow[] = [
  {
    case_number: "M.7000",
    title: "Zilveren Kruis Achmea / De Friesland Zorgverzekeraar",
    date: "2015-09-22",
    sector: "zorg",
    acquiring_party: "Zilveren Kruis Achmea NV",
    target: "De Friesland Zorgverzekeraar NV",
    summary: "De ACM heeft de fusie tussen Zilveren Kruis Achmea en De Friesland Zorgverzekeraar goedgekeurd met voorwaarden. De fusie versterkte de dominante positie in de provincie Friesland, waardoor de ACM voorwaarden oplegde voor de zorginkoop in die provincie.",
    full_text: "Zilveren Kruis Achmea (onderdeel van de Achmea-groep) heeft De Friesland Zorgverzekeraar overgenomen. De Friesland Zorgverzekeraar is een regionale zorgverzekeraar met een sterke positie in de provincie Friesland. De ACM heeft de fusie onderzocht op de markt voor zorgverzekeringen (basisverzekering en aanvullende verzekering) en op de markten voor zorginkoop. In de provincie Friesland had De Friesland een marktaandeel van meer dan 50% op de markt voor basisverzekeringen. De combinatie met Zilveren Kruis zou leiden tot een extreem hoge concentratie in Friesland. De ACM heeft de fusie goedgekeurd onder de voorwaarden dat: (1) de fuserende partijen in Friesland hun zorginkoop separaat zouden uitvoeren voor een periode van vijf jaar; (2) De Friesland de regionale identiteit zou behouden. Deze voorwaarden moesten waarborgen dat zorgaanbieders in Friesland met meerdere substantiele inkopende partijen bleven onderhandelen.",
    outcome: "cleared_with_conditions",
    turnover: 5000000000,
  },
  {
    case_number: "M.6990",
    title: "UPC / Ziggo — Fusie kabelnetwerken",
    date: "2014-10-01",
    sector: "telecom",
    acquiring_party: "UPC Nederland BV (Liberty Global)",
    target: "Ziggo NV",
    summary: "De ACM heeft de fusie tussen UPC en Ziggo, de twee grootste kabelaanbieders in Nederland, goedgekeurd na een diepgaand onderzoek. De fusie creeerde het grootste kabelnetwerk in Nederland. De Europese Commissie verwees de zaak terug naar de ACM voor beoordeling.",
    full_text: "Liberty Global heeft via zijn dochtermaatschappij UPC Nederland een overnamebod uitgebracht op Ziggo NV, de grootste kabeloperator in Nederland. Samen zouden UPC en Ziggo een kabelnetwerk hebben dat meer dan 7 miljoen Nederlandse huishoudens bedient. De Europese Commissie heeft de zaak terugverwezen naar de ACM, die de primaire bevoegdheid had voor de beoordeling van de fusie. De ACM heeft onderzocht of de fusie significante concurrentieproblemen zou veroorzaken op de markten voor: (1) televisiedistributie via kabel; (2) breedband internet via kabel; (3) vaste telefonie via kabel; (4) triple-play bundels. De ACM heeft geconcludeerd dat de fusie geen significante belemmering van de concurrentie veroorzaakte. Belangrijke factoren waren: de aanwezigheid van KPN als sterke concurrent via DSL-netwerk, de toenemende concurrentie van streamingdiensten voor televisie, en de verwachte toename van concurrentie van glasvezelnetwerken. De ACM heeft de fusie onvoorwaardelijk goedgekeurd.",
    outcome: "cleared_phase1",
    turnover: 3000000000,
  },
  {
    case_number: "M.7150",
    title: "Vattenfall / Nuon Energie — Acquisitie energiebedrijf",
    date: "2009-07-01",
    sector: "energie",
    acquiring_party: "Vattenfall AB",
    target: "N.V. Nuon Energy",
    summary: "De Zweedse energiegroep Vattenfall heeft Nuon Energy overgenomen in een transactie van ca. 8,5 miljard euro. De NMa (voorloper van ACM) heeft de overname goedgekeurd met voorwaarden om de concurrentie in elektriciteitsopwekking en -handel te waarborgen.",
    full_text: "Vattenfall AB heeft de overname van N.V. Nuon Energy, een groot Nederlands energiebedrijf met activiteiten in elektriciteitsopwekking, gasdistributie en energielevering, aangemeld bij de Nederlandse Mededingingsautoriteit (NMa, thans ACM). Vattenfall is een van de grootste energieproducenten in Europa. De overname vond plaats in de context van de liberalisering van de Nederlandse energiemarkt. De NMa heeft de overname onderzocht op de markten voor elektriciteitsopwekking en -levering in Nederland. De NMa heeft vastgesteld dat de combinatie van Vattenfall en Nuon zou leiden tot een sterke positie op de markt voor elektriciteitsopwekking, met name in piekuren. De NMa heeft de overname goedgekeurd onder de voorwaarde dat Vattenfall/Nuon zou deelnemen aan een virtueel-krachtcentrale-programma (Virtual Power Plant), waarbij een deel van de opwekkingscapaciteit beschikbaar gesteld zou worden aan andere marktpartijen.",
    outcome: "cleared_with_conditions",
    turnover: 8500000000,
  },
];

const insertMerger = db.prepare(`
  INSERT OR IGNORE INTO mergers
    (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertMergersAll = db.transaction(() => {
  for (const m of mergers) {
    insertMerger.run(
      m.case_number, m.title, m.date, m.sector,
      m.acquiring_party, m.target, m.summary, m.full_text,
      m.outcome, m.turnover,
    );
  }
});

insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sectors:   ${sectorCount}`);
console.log(`  Decisions: ${decisionCount}`);
console.log(`  Mergers:   ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
