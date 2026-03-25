#!/usr/bin/env python3
"""
event_extractor.py — Dodo Digest first-pass candidate event builder.

Converts feed.json into candidate event records (events.json) optimised
for a later editorial pass that handles semantic merging, final
classification, translation, scoring, and newsletter writing.

This script is deterministic and uses only the Python standard library.
It does NOT attempt final event reasoning — it preserves evidence and
provides best-effort heuristic guesses that the editorial pass can override.

Usage:
    python event_extractor.py [--input feed.json] [--output events.json]
"""

import json
import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_CATEGORIES = {
    "local", "regional", "global", "finance", "weather", "utilities"
}

# ---------------------------------------------------------------------------
# Data-record detection (prices, rates, weather bulletins, outage schedules)
# ---------------------------------------------------------------------------

DATA_TICKER_SOURCE_NAMES = {
    "Brent Crude Oil", "Bitcoin Price", "Gold Price",
    "MUR Exchange Rates", "Stock Exchange of Mauritius",
    "Mauritius Met Services", "CEB Power Outages",
}

DATA_TICKER_TITLE_PATTERNS = [
    r"(?i)\b(brent crude|bitcoin|gold)\s+price\b",
    r"(?i)\bexchange rates?\b",
    r"(?i)\bSEMDEX\b",
    r"(?i)\bweather bulletin\b",
    r"(?i)\bpower outage\b",
    r"(?i)\bmétéo\b.*\bprévisions\b",
]


def is_data_record(title: str, source: str) -> bool:
    """Detect structured data items that are not narrative news stories."""
    if source in DATA_TICKER_SOURCE_NAMES:
        return True
    for pattern in DATA_TICKER_TITLE_PATTERNS:
        if re.search(pattern, title):
            return True
    return False


# ---------------------------------------------------------------------------
# Event-type keyword rules (first-pass heuristic)
# ---------------------------------------------------------------------------
# Order matters: first match wins.  Patterns are case-insensitive unless
# prefixed with "CASESENSITIVE:" (used for short acronyms like UN).
# Both English and French terms are included because ~30% of feed items
# are in French.

EVENT_TYPE_RULES: list[tuple[str, list[str]]] = [
    ("obituary", [
        r"\bpassed away\b", r"\bdied\b", r"\bdeath of\b", r"\bdecès\b",
        r"\bobituari", r"\bfuneral\b", r"\bmourning\b", r"\bhomage\b",
        r"\bhommage\b", r"\bdécédé\b", r"\bin memoriam\b",
        r"\bdernier hommage\b", r"\bdisparition\b", r"\bsuccombe\b",
    ]),
    ("weather", [
        r"\bcyclone\b", r"\bflood", r"\bstorm\b", r"\bweather\b",
        r"\brainfall\b", r"\btempête\b", r"\bmétéo\b", r"\btornado\b",
        r"\bdrought\b", r"\btsunami\b", r"\bforte[s]? pluie",
        r"\bavis de\b.*\b(pluie|vent|cyclone)\b",
        r"\bheavy rain\b", r"\bwarning\b.*\b(rain|wind|wave)\b",
        r"\balerte\b.*\b(pluie|vent|cyclonique)\b",
        r"\bprévisions\b.*\b(météo|temps)\b",
    ]),
    ("utilities", [
        r"\bwater (cut|supply|shortage)\b", r"\bpower (cut|outage)\b",
        r"\bCEB\b", r"\bCWA\b", r"\belectricit", r"\bload.?shedding\b",
        r"\bcoupure d.eau\b", r"\bcoupure\b.*\bélectr",
        r"\bwater disruption\b", r"\bno water\b",
        r"\bpanne d.électricité\b", r"\bhuile lourde\b",
    ]),
    ("courts", [
        r"\bcourt\b", r"\btribunal\b", r"\bjudg[e]?ment\b", r"\bverdict\b",
        r"\bsentenc", r"\bprocès\b", r"\bacquitt", r"\bconvict",
        r"\bmagistrat", r"\bjudiciary\b", r"\bjuge\b", r"\bcondamn",
        r"\bICPC\b", r"\bICAC\b",
        r"\bprivy council\b", r"\bjudicial review\b",
        r"\bcharged with\b", r"\bpleaded guilty\b", r"\bfound guilty\b",
        r"\binculpé\b", r"\bcour suprême\b", r"\bcondamnation\b",
        r"\bdétention\b", r"\bremise en liberté\b",
        r"\bcaution\b.*\b(bail|liberté)\b",
        r"\bplainte constitutionnelle\b", r"\bcompar[ua]", r"\bpoursuivi\b",
    ]),
    ("crime", [
        r"\barrest", r"\bmurder\b", r"\bkill", r"\bstabb", r"\brobb",
        r"\bdrug[s]?\b", r"\bseiz(ed|ure)\b", r"\bfraud\b", r"\btheft\b",
        r"\bpolice\b", r"\bsuspect\b", r"\binquest\b", r"\bcrime\b",
        r"\bheroin\b", r"\bcocaine\b", r"\bcannabis\b",
        r"\bGCCB\b", r"\bADSU\b", r"\bFCC\b",
        r"\binvestigation\b", r"\braid\b", r"\bdetain",
        r"\bsuspected of\b", r"\btaken into custody\b",
        r"\bblanchiment\b",
        r"\bmeurtre\b", r"\bdrogue\b", r"\bsaisie\b", r"\bagress",
        r"\bsynthétique\b", r"\btrafic\b",
        r"\bcadavre\b", r"\bcorps sans vie\b",
        r"\bcouteau\b", r"\bmalfaiteur\b",
        r"\bvol\b", r"\bvolés?\b", r"\bdérobés?\b", r"\bcambriol",
        r"\barrêté\b", r"\binterpellation\b",
        r"\bsubstances? suspectes?\b",
        r"\bmenace\b.*\b(couteau|arme|mort)\b",
        r"\benquête\b.*\b(police|meurtre|vol|drogue|blanchiment|FCC)\b",
        r"\babus\b",
    ]),
    ("politics", [
        r"\bparliament\b", r"\bgovernment\b", r"\bminister\b",
        r"\bprime minister\b", r"\belection", r"\bopposition\b", r"\bvote\b",
        r"\bbudget\b", r"\blegislat", r"\bcabinet\b", r"\bconstituency\b",
        r"\bpolitiq", r"\bgouvernement\b", r"\bassemblée\b", r"\bministre\b",
        r"\bparlement\b",
        r"\bMSM\b", r"\bPTr\b", r"\bMMM\b", r"\bPMSD\b",
        r"\breform alliance\b",
        r"\bgovernment policy\b", r"\bpublic policy\b", r"\bpublic sector\b",
        r"\bfonction publique\b", r"\bprojet de loi\b",
        r"\bparliamentary\b", r"\bpremier ministre\b",
        r"\bPNQ\b", r"\bPrivate Notice Question\b",
        r"\bdémission\b", r"\bdéputé\b", r"\bélu\b",
        r"\bbureau politique\b", r"\bmilitant\b",
    ]),
    ("energy", [
        r"\bfuel\b", r"\bpetrol\b", r"\bdiesel\b", r"\bgas price\b",
        r"\benergy\b", r"\bsolar\b", r"\brenewable\b", r"\boil price\b",
        r"\bcarburant[s]?\b", r"\bessence\b", r"\bgazole\b",
        r"\bprix.{0,15}(essence|diesel|carburant)\b",
        r"\bfuel.{0,10}(price|hike|increase|subsid)\b",
        r"\bpétrole\b", r"\bpétrolier\b",
    ]),
    ("economy", [
        r"\binflation\b", r"\bGDP\b", r"\beconom", r"\brupee\b",
        r"\bexchange rate\b", r"\bcentral bank\b", r"\bBank of Mauritius\b",
        r"\bBoM\b", r"\btax\b", r"\bVAT\b",
        r"\bcroissance\b", r"\btaux\b.*\b(change|intérêt)\b",
        r"\binterest rate\b", r"\bcost of living\b",
        r"\bcoût de la vie\b", r"\bpouvoir d.achat\b",
        r"\bmonetary policy\b", r"\brepo rate\b", r"\bkey rate\b",
        r"\bMoody.s\b", r"\bjunk status\b", r"\bcredit rating\b",
        r"\béconomie bleue\b",
    ]),
    ("business", [
        r"\bcompan", r"\bcorporat", r"\bstartup\b", r"\bmerger\b",
        r"\bacquisition\b", r"\bprofit\b", r"\brevenue\b", r"\bIPO\b",
        r"\bstock\b", r"\bmarket\b", r"\binvestment\b", r"\bbusiness\b",
        r"\bentreprise\b", r"\bsociété\b",
        r"\bquarterly results\b", r"\bearnings\b",
        r"\brésultats financiers\b", r"\bchiffre d.affaires\b",
        r"\btouris\w*\b",
    ]),
    ("health", [
        r"\bhealth\b", r"\bhospital\b", r"\bdisease\b", r"\bvaccin",
        r"\bepidemic\b", r"\bsanté\b", r"\bhôpital\b", r"\bCOVID\b",
        r"\bdengue\b", r"\bchikungunya\b", r"\bdoctor\b", r"\bmedic",
        r"\bpharma\b", r"\bmaladi",
        r"\bpatient\b", r"\bsurgery\b", r"\bdiagnos",
        r"\bministry of health\b", r"\bministère de la santé\b",
        r"\btuberculose\b", r"\bTB\b",
        r"\bpersonnel soignant\b", r"\bmédecin\b", r"\binfirmier\b",
        r"\bhospitalier\b", r"\bchirurgi\b",
    ]),
    ("transport", [
        r"\btransport\b", r"\bairport\b", r"\bflight\b",
        r"\bairline\b", r"\bport authority\b", r"\bshipping\b", r"\btraffic\b",
        r"\broad accident\b", r"\bhighway\b", r"\baéroport\b",
        r"\baccident\b.*\broute\b",
        r"\bAir Mauritius\b", r"\bMRT\b",
        r"\blight rail\b", r"\baccident de la route\b",
        r"\bcar crash\b", r"\bplane crash\b",
    ]),
    ("sports", [
        r"\bfootball\b", r"\bcricket\b", r"\bolympic\b", r"\bathlet",
        r"\bfifa\b", r"\btournament\b", r"\bchampion",
        r"\bsport\b", r"\brugby\b", r"\bbadminton\b", r"\bjeux\b",
        r"\bgoal\b.*\b(scored|minute)\b", r"\bqualif",
        r"\bcoupe\b", r"\bligue\b",
        r"\bcyclisme\b", r"\bvolley\b", r"\bjudo\b",
        r"\bWorld Cup\b",
    ]),
    ("technology", [
        r"\btech\b", r"\bartificial intellig", r"\bcyber\b",
        r"\bdigital\b", r"\binternet\b", r"\bsoftware\b",
        r"\btechnolog", r"\binnovation\b", r"\bnumérique\b",
        r"\bdata breach\b", r"\bhack\b",
        r"\bbroadband\b", r"\bfibre\b",
    ]),
    ("culture", [
        r"\bcultur", r"\bfestival\b", r"\bmusic\b", r"\bfilm\b",
        r"\bheritage\b", r"\bmuseum\b", r"\bsega\b", r"\bpatrimoine\b",
        r"\bcreole\b", r"\bkréol\b", r"\bliterature\b",
        r"\bgastronomie\b", r"\bculinaire\b",
    ]),
    ("human_interest", [
        r"\bcommunity\b", r"\bcharity\b", r"\bvolunteer\b", r"\brescue\b",
        r"\bhero\b", r"\bfeel.?good\b", r"\binspir", r"\bsolidari",
        r"\bretrouvé\b.*\bsain\b",
    ]),
    ("international", [
        r"CASESENSITIVE:\bUN\b", r"\bUnited Nations\b",
        r"\bEU\b", r"\bNATO\b", r"\bSADC\b",
        r"\bAfrican Union\b", r"\binternational\b", r"\bglobal\b",
        r"\bdiplomat", r"\bsanction\b",
        r"\bforeign affairs\b", r"\bgeopoliti",
    ]),
]

# Mauritius-relevance signals (for heuristic scoring)
MAURITIUS_STRONG_SIGNALS = [
    r"\bmauritius\b", r"\bmauricien", r"\bport.?louis\b", r"\bcurepipe\b",
    r"\bquatre.?bornes\b", r"\bvacoas\b", r"\bphoenix\b", r"\brose.?hill\b",
    r"\bmoka\b", r"\bflacq\b", r"\bpamplemousses\b", r"\brigaud\b",
    r"\bmahebourg\b", r"\bplaine.?wilhems\b", r"\brodrigues\b",
    r"\bagalega\b", r"\bchagos\b", r"\btrou.?aux.?biches\b",
    r"\bflic.?en.?flac\b", r"\ble.?morne\b", r"\baapravasi\b",
    r"\bmaurice\b", r"\bîle maurice\b",
    r"\bbeau.?bassin\b", r"\btamarin\b", r"\bgoodlands\b",
    r"\btriolet\b", r"\bsouillac\b", r"\bchamarel\b",
    r"\bébène\b", r"\bbaie.?du.?tombeau\b",
    r"\bmontagne.?longue\b", r"\broche.?bois\b", r"\bbagatelle\b",
    r"\bbeaux.?songes\b", r"\bmont.?roches\b",
    r"\bBank of Mauritius\b", r"\bBoM\b", r"\bMRA\b",
    r"\bCEB\b", r"\bCWA\b", r"\bMBC\b", r"\bICPC\b", r"\bICAC\b",
    r"\bFSC\b", r"\bSEM\b", r"\bNHDC\b", r"\bFCC\b",
    r"\bMauritian\b", r"\bSEMDEX\b",
    r"\bMSM\b", r"\bPTr\b", r"\bMMM\b", r"\bPMSD\b",
    r"\bReform Alliance\b", r"\bAlliance Lepep\b",
]

MAURITIUS_MODERATE_SIGNALS = [
    r"\bindian ocean\b", r"\bocéan indien\b", r"\breunion\b",
    r"\bmadagascar\b", r"\bseychelles\b", r"\bcomoros\b",
    r"\bafrican\b", r"\bafrica\b", r"\bDiego Garcia\b",
]

# Language detection helpers
FR_INDICATORS = re.compile(r"[éèêëàâùûüôçîïæœ]", re.IGNORECASE)
CREOLE_MARKERS = [
    r"\binn\b", r"\bfinn\b", r"\bpou\b", r"\bzot\b", r"\bnou\b",
    r"\beki\b", r"\bena\b", r"\blinn\b", r"\bdan\b", r"\bban\b",
    r"\bdimoune\b", r"\bmorisien\b", r"\bkreol\b",
]

# Entity dictionaries
KNOWN_ORGS: set[str] = {
    "CEB", "CWA", "MRA", "ICAC", "ICPC", "MBC", "SEM", "FSC",
    "NHDC", "STC", "MPA", "MEXA", "EDB", "BOI", "MCCI", "PPC", "FCC",
    "Mauritius Revenue Authority", "State Trading Corporation",
    "Bank of Mauritius", "Air Mauritius", "Mauritius Telecom",
    "National Assembly", "Supreme Court", "Privy Council",
    "Tourism Authority", "Mauritius Sports Council",
    "MSM", "PTr", "MMM", "PMSD", "Reform Alliance", "Alliance Lepep",
    "ADSU", "GCCB", "SST", "CCID", "CID",
    "WHO", "EU", "SADC", "IMF", "UNESCO", "FIFA", "IOC",
    "NATO", "BRICS", "African Union", "COMESA",
    "General Assembly", "High Court", "World Bank", "United Nations",
}

KNOWN_PLACES: set[str] = {
    "Mauritius", "Port Louis", "Port-Louis", "Curepipe",
    "Quatre Bornes", "Vacoas", "Phoenix", "Rose Hill", "Rose-Hill",
    "Moka", "Flacq", "Pamplemousses", "Mahebourg",
    "Beau Bassin", "Beau-Bassin", "Tamarin", "Flic en Flac", "Goodlands",
    "Triolet", "Souillac", "Chamarel", "Le Morne",
    "Rivière Noire", "Grand Baie", "Trou aux Biches",
    "Ebène", "Baie du Tombeau", "Plaine Wilhems",
    "Terre Rouge", "Riche Terre", "Pailles", "Floréal",
    "Centre de Flacq", "Rivière du Rempart",
    "Montagne Longue", "Roche Bois", "Roche-Bois",
    "Bagatelle", "Beaux Songes", "Mont Roches", "Mont-Roches",
    "Chemin Grenier", "Chemin-Grenier", "Plaine Champagne",
    "Nouvelle France", "Nouvelle-France", "Pereybère",
    "Rodrigues", "Agalega", "Chagos", "Diego Garcia",
    "Reunion", "Réunion", "Madagascar", "Seychelles", "Comoros",
    "India", "China", "France", "UK", "USA", "Africa",
    "South Africa", "Dubai", "Singapore", "London", "Paris",
}

TOPIC_KEYWORDS: dict[str, list[str]] = {
    "inflation": [r"\binflation\b"],
    "diesel": [r"\bdiesel\b"],
    "fuel prices": [r"\bfuel\b", r"\bpetrol\b", r"\bcarburant\b",
                    r"\bessence\b", r"\bpétrol"],
    "parliament": [r"\bparliament\b", r"\bparlement\b",
                    r"\bassemblée nationale\b", r"\bPNQ\b"],
    "elections": [r"\belection\b", r"\bélection\b"],
    "airport": [r"\bairport\b", r"\baéroport\b"],
    "prisons": [r"\bprison\b"],
    "tourism": [r"\btouris\b"],
    "drugs": [r"\bdrug\b", r"\bdrogue\b", r"\bheroin\b",
              r"\bcocaine\b", r"\bcannabis\b", r"\bnarcotics\b"],
    "environment": [r"\benvironment\b", r"\bpollution\b", r"\bwaste\b",
                     r"\benvironnement\b"],
    "education": [r"\bschool\b", r"\beducation\b", r"\buniversit\b",
                   r"\bexam\b"],
    "housing": [r"\bhousing\b", r"\blogement\b", r"\bNHDC\b"],
    "water supply": [r"\bwater (supply|cut|shortage)\b", r"\bCWA\b",
                      r"\bcoupure d.eau\b"],
    "electricity": [r"\belectricit\b", r"\bCEB\b", r"\bpower cut\b",
                     r"\bhuile lourde\b"],
    "public safety": [r"\bsafety\b", r"\bsécurité\b"],
    "corruption": [r"\bcorruption\b", r"\bICAC\b", r"\bbribe\b",
                    r"\bblanchiment\b"],
    "trade": [r"\btrade\b"],
    "rupee": [r"\brupee\b", r"\broupie\b", r"\bMUR\b"],
    "middle east": [r"\bmiddle east\b", r"\bisrael\b", r"\bpalestine\b",
                     r"\bgaza\b", r"\biran\b", r"\bmoyen.orient\b"],
    "climate": [r"\bclimate\b", r"\bclimatique\b"],
    "COVID": [r"\bcovid\b"],
    "dengue": [r"\bdengue\b"],
    "cost of living": [r"\bcost of living\b", r"\bcoût de la vie\b",
                        r"\bpouvoir d.achat\b"],
    "audit": [r"\baudit\b", r"\brapport de l.audit\b"],
}

# Specific topics where 2+ events sharing the topic are likely merge candidates
MERGE_SENSITIVE_TOPICS = {
    "diesel", "fuel prices", "electricity", "cost of living",
    "audit", "prisons", "drugs", "corruption",
}

# Broad topics where we need 3+ events before flagging merge
MERGE_BROAD_TOPICS = {"middle east", "climate"}


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return value if isinstance(value, str) else str(value)


def clean_text(text: str) -> str:
    """Normalise unicode and collapse whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip()


def detect_language(text: str) -> str:
    """Heuristic language detection → 'en', 'fr', or 'mfe'."""
    if not text or len(text.strip()) < 5:
        return "en"
    lower = text.lower()
    if sum(1 for p in CREOLE_MARKERS if re.search(p, lower)) >= 2:
        return "mfe"
    has_diacritics = bool(FR_INDICATORS.search(text))
    fr_markers = [
        r"\ble\b", r"\bla\b", r"\bles\b", r"\bdu\b", r"\bdes\b",
        r"\best\b", r"\bsont\b", r"\bdans\b", r"\bune?\b", r"\bpour\b",
        r"\bavec\b", r"\bqui\b", r"\bselon\b", r"\bplus\b", r"\bmais\b",
        r"\baux\b", r"\bpar\b", r"\bces\b", r"\bcette\b",
    ]
    fr_hits = sum(1 for p in fr_markers if re.search(p, lower))
    if has_diacritics and fr_hits >= 1:
        return "fr"
    if fr_hits >= 3:
        return "fr"
    return "en"


# ---------------------------------------------------------------------------
# Multi-source summary parsing
# ---------------------------------------------------------------------------

_SOURCE_BLOCK_RE = re.compile(
    r"\[([^\]]+)\]\s*(.*?)(?=\n\n\[|\Z)", re.DOTALL
)


def parse_multi_source_summary(raw_summary: str) -> list[dict]:
    """
    Parse '[Source] text\\n\\n[Source] text' blocks from a raw summary.
    Returns list of {source, text} dicts.  Empty list if not multi-source.
    """
    if not raw_summary or "[" not in raw_summary:
        return []
    matches = _SOURCE_BLOCK_RE.findall(raw_summary)
    if len(matches) < 2:
        return []
    return [
        {"source": s.strip(), "text": clean_text(t)}
        for s, t in matches if clean_text(t)
    ]


# ---------------------------------------------------------------------------
# Classification (first-pass heuristic)
# ---------------------------------------------------------------------------

def classify_event_type(title: str, summary: str, category: str,
                        source: str) -> tuple[str, str]:
    """
    Returns (event_type_guess, event_type_confidence).
    Confidence: "keyword" if matched by pattern, "category_fallback" if not.
    """
    if is_data_record(title, source):
        return "data", "source_match"

    combined = f"{title} {summary}"
    for event_type, patterns in EVENT_TYPE_RULES:
        for pattern in patterns:
            if pattern.startswith("CASESENSITIVE:"):
                if re.search(pattern[14:], combined):
                    return event_type, "keyword"
            elif re.search(pattern, combined, re.IGNORECASE):
                return event_type, "keyword"

    fallback_map = {
        "finance": "economy", "weather": "weather", "utilities": "utilities",
        "global": "international", "regional": "international",
    }
    if category in fallback_map:
        return fallback_map[category], "category_fallback"
    return "other", "none"


# ---------------------------------------------------------------------------
# Scoring helpers (all first-pass guesses)
# ---------------------------------------------------------------------------

def guess_mauritius_relevance(title: str, summary: str,
                              category: str) -> int:
    """Heuristic 0-10 Mauritius relevance guess."""
    combined = f"{title} {summary}".lower()
    score = {"local": 8, "utilities": 8, "finance": 5,
             "regional": 4, "global": 2}.get(category, 0)

    strong = sum(1 for p in MAURITIUS_STRONG_SIGNALS
                 if re.search(p, combined, re.IGNORECASE))
    if strong >= 2:
        score = max(score, 9)
    elif strong == 1:
        score = max(score, 7)

    moderate = sum(1 for p in MAURITIUS_MODERATE_SIGNALS
                   if re.search(p, combined, re.IGNORECASE))
    if moderate >= 1:
        score = max(score, 4)

    return min(10, max(0, score))


def guess_editorial_priority(cluster_size: int, source_count: int,
                             event_type: str, mu_relevance: int,
                             summary_length: int) -> int:
    """Heuristic 0-10 editorial priority guess."""
    s = mu_relevance * 0.5
    if source_count >= 3:
        s += 3.0
    elif source_count >= 2:
        s += 2.0
    if cluster_size >= 5:
        s += 2.0
    elif cluster_size >= 3:
        s += 1.5
    elif cluster_size >= 2:
        s += 1.0
    if event_type in {"politics", "economy", "crime", "health",
                       "energy", "weather", "utilities", "courts"}:
        s += 1.5
    if summary_length > 200:
        s += 0.5
    if summary_length < 30:
        s -= 1.5
    if event_type == "data":
        s -= 2.0
    return min(10, max(0, round(s)))


def assess_confidence(cluster_size: int, source_count: int,
                      summary: str, needs_translation: bool) -> str:
    """
    Candidate-quality confidence: how much can the Claude pass trust
    this candidate without heavy re-examination?
    """
    slen = len(summary)
    truncated = summary.rstrip().endswith("...")

    # High: multi-source coherent cluster with substantial text
    if source_count >= 2 and cluster_size >= 2 and slen > 80:
        return "high"
    # High: rich singleton
    if slen > 300 and not truncated and not needs_translation:
        return "high"
    # Low: very thin or title-only
    if slen < 40 or summary.startswith("Report:"):
        return "low"
    if truncated and slen < 100:
        return "low"
    # Low: thin + needs translation = doubly uncertain
    if needs_translation and slen < 80:
        return "low"
    # Same-source bundle with moderate text
    if cluster_size > 1 and source_count == 1:
        return "medium"
    return "medium"


# ---------------------------------------------------------------------------
# Entity and topic extraction
# ---------------------------------------------------------------------------

def extract_entities(text: str) -> dict[str, list[str]]:
    """Dictionary-based extraction for orgs and places.  People/products
    are left empty for the Claude pass."""
    entities: dict[str, list[str]] = {
        "people": [], "organizations": [], "places": [], "products": [],
    }
    if not text:
        return entities

    for org in sorted(KNOWN_ORGS, key=len, reverse=True):
        if len(org) <= 3:
            if re.search(rf"\b{re.escape(org)}\b", text):
                entities["organizations"].append(org)
        elif org.lower() in text.lower():
            entities["organizations"].append(org)

    for place in sorted(KNOWN_PLACES, key=len, reverse=True):
        if place.lower() in text.lower():
            entities["places"].append(place)

    # Dedup normalising hyphens
    for key in entities:
        seen: set[str] = set()
        deduped = []
        for item in entities[key]:
            norm = item.lower().replace("-", " ")
            if norm not in seen:
                seen.add(norm)
                deduped.append(item)
        entities[key] = deduped
    return entities


def extract_topics(title: str, summary: str) -> list[str]:
    combined = f"{title} {summary}".lower()
    topics = []
    for topic, patterns in TOPIC_KEYWORDS.items():
        if any(re.search(p, combined, re.IGNORECASE) for p in patterns):
            topics.append(topic)
    return topics


# ---------------------------------------------------------------------------
# Headline / summary construction
# ---------------------------------------------------------------------------

def clean_headline(raw: str) -> str:
    t = clean_text(raw)
    if not t:
        return ""
    t = re.sub(r"^\s*[-–—:]\s*", "", t)
    t = re.sub(r"\s*[-–—:]\s*$", "", t)
    t = re.sub(r"\s+[|–—]\s+[A-Za-zÀ-ÿ']+(?:\s+[A-Za-zÀ-ÿ']+){0,3}\s*$",
               "", t)
    t = re.sub(r"\s+-\s+[A-Za-zÀ-ÿ']+(?:\s+[A-Za-zÀ-ÿ']+){0,3}\s*$",
               "", t)
    return t.strip().rstrip(".")


def build_headline(item: dict) -> tuple[str, str]:
    """Returns (headline, headline_original).  Prefers English when available."""
    lead = item.get("lead", {})
    titles = item.get("titles", [])

    for t in titles:
        tc = clean_text(safe_str(t))
        if tc and detect_language(tc) == "en":
            return clean_headline(tc), ""

    lead_t = clean_text(safe_str(lead.get("title", "")))
    if lead_t:
        h = clean_headline(lead_t)
        lang = detect_language(lead_t)
        return h, (h if lang != "en" else "")

    return "Untitled event", ""


def build_summary(item: dict) -> tuple[str, str]:
    """Returns (summary, summary_original)."""
    lead = item.get("lead", {})
    s = clean_text(safe_str(lead.get("summary", "")))
    t = clean_text(safe_str(lead.get("title", "")))

    if not s and not t:
        return "Insufficient information for event summary.", ""
    if s:
        lang = detect_language(s)
        return s, (s if lang != "en" else "")
    fallback = f"Report: {t}."
    lang = detect_language(t)
    return fallback, (fallback if lang != "en" else "")


# ---------------------------------------------------------------------------
# Articles list (best-effort reconstruction)
# ---------------------------------------------------------------------------

def build_articles(item: dict, lead_title: str, lead_source: str,
                   lead_url: str, lead_summary_clean: str,
                   lead_lang: str) -> list[dict]:
    """Build articles list from lead + multi-source blocks + cluster titles."""
    articles = []
    lead = item.get("lead", {})
    raw_summary = safe_str(lead.get("summary", ""))
    titles = item.get("titles", [])
    urls = item.get("urls", [])
    sources = item.get("sources", [])

    source_blocks = parse_multi_source_summary(raw_summary)

    if source_blocks:
        articles.append({
            "title": lead_title, "source": source_blocks[0]["source"],
            "url": lead_url, "summary": source_blocks[0]["text"],
            "language": detect_language(source_blocks[0]["text"]),
        })
        for block in source_blocks[1:]:
            articles.append({
                "title": "", "source": block["source"],
                "url": "", "summary": block["text"],
                "language": detect_language(block["text"]),
            })
    elif lead_title or lead_url:
        articles.append({
            "title": lead_title, "source": lead_source,
            "url": lead_url, "summary": lead_summary_clean,
            "language": lead_lang,
        })

    if len(titles) > 1:
        for i, title in enumerate(titles):
            tc = clean_text(safe_str(title))
            if tc == lead_title:
                continue
            articles.append({
                "title": tc,
                "source": safe_str(sources[i]) if i < len(sources) else "",
                "url": safe_str(urls[i]) if i < len(urls) else "",
                "summary": "",
                "language": detect_language(tc),
            })

    return articles


# ---------------------------------------------------------------------------
# Evidence block (compact raw preservation)
# ---------------------------------------------------------------------------

def build_evidence(item: dict) -> dict:
    """Compact preservation of raw feed-item fields for Claude pass."""
    lead = item.get("lead", {})
    return {
        "lead_title": safe_str(lead.get("title", "")),
        "lead_summary": safe_str(lead.get("summary", "")),
        "lead_source": safe_str(lead.get("source", "")),
        "lead_url": safe_str(lead.get("url", "")),
        "titles": item.get("titles", []),
        "sources": item.get("sources", []),
        "urls": item.get("urls", []),
        "languages": item.get("languages", []),
        "cluster_size": item.get("cluster_size", 1),
        "source_count": item.get("source_count", 1),
    }


# ---------------------------------------------------------------------------
# Notes (free-text for human/Claude context)
# ---------------------------------------------------------------------------

def build_notes(item: dict, detected_langs: set[str],
                is_data: bool) -> str:
    parts: list[str] = []
    cs = item.get("cluster_size", 1)
    sc = item.get("source_count", 1)

    if is_data:
        parts.append("data record; not a narrative news story")
    if cs == 1:
        parts.append("singleton")
    if cs > 1 and sc == 1:
        parts.append("same-source bundle only")

    non_en = detected_langs - {"en"}
    if non_en:
        names = {"fr": "French", "mfe": "Mauritian Creole"}
        parts.append(
            f"contains {', '.join(names.get(l, l) for l in sorted(non_en))};"
            " needs translation"
        )

    summary = safe_str(item.get("lead", {}).get("summary", ""))
    if len(summary) < 30:
        parts.append("thin summary; needs editorial enrichment")

    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Merge-hint detection (second pass over all candidates)
# ---------------------------------------------------------------------------

def detect_merge_hints(events: list[dict]) -> None:
    """
    Second pass: populate structured merge_hints on each candidate.
    Uses shared topics and shared entities to find likely-same-story groups.
    """
    # Build topic→indices and org→indices maps
    topic_map: dict[str, list[int]] = {}
    org_map: dict[str, list[int]] = {}

    for i, evt in enumerate(events):
        for topic in evt.get("topics", []):
            topic_map.setdefault(topic, []).append(i)
        for org in evt.get("key_entities", {}).get("organizations", []):
            org_map.setdefault(org, []).append(i)

    # Identify merge groups
    merge_groups: dict[str, list[int]] = {}

    for topic, indices in topic_map.items():
        if topic in MERGE_SENSITIVE_TOPICS and len(indices) >= 2:
            merge_groups[topic] = indices
        elif topic not in MERGE_BROAD_TOPICS and len(indices) >= 3:
            merge_groups[topic] = indices

    for org, indices in org_map.items():
        if len(indices) >= 2:
            merge_groups[f"org:{org}"] = indices

    # Write structured hints
    for group_key, indices in merge_groups.items():
        event_ids = [events[i]["event_id"] for i in indices]
        is_topic = not group_key.startswith("org:")
        topic_name = group_key if is_topic else None
        entity_name = group_key[4:] if not is_topic else None

        for i in indices:
            evt = events[i]
            hints = evt["merge_hints"]
            other_ids = [eid for eid in event_ids
                         if eid != evt["event_id"]]
            for oid in other_ids:
                if oid not in hints["related_event_ids"]:
                    hints["related_event_ids"].append(oid)
            if topic_name and topic_name not in hints["shared_topics"]:
                hints["shared_topics"].append(topic_name)
            if entity_name and entity_name not in hints["shared_entities"]:
                hints["shared_entities"].append(entity_name)


# ---------------------------------------------------------------------------
# Main transform: feed item → candidate event
# ---------------------------------------------------------------------------

def safe_get_lead(item: dict) -> dict:
    lead = item.get("lead")
    return lead if isinstance(lead, dict) else {}


def transform_item(item: dict, index: int) -> dict:
    """Convert one feed item into a candidate event record."""
    event_id = f"evt_{index + 1:04d}"
    item_id = safe_str(item.get("id", f"unknown_{index}"))
    cluster_time = safe_str(item.get("cluster_time", ""))
    category = safe_str(item.get("category", "local"))
    if category not in VALID_CATEGORIES:
        category = "local"

    lead = safe_get_lead(item)
    lead_title = clean_text(safe_str(lead.get("title", "")))
    lead_summary = clean_text(safe_str(lead.get("summary", "")))
    lead_source = safe_str(lead.get("source", ""))
    lead_url = safe_str(lead.get("url", ""))

    cluster_size = item.get("cluster_size", 1)
    if not isinstance(cluster_size, int):
        cluster_size = 1
    source_count = item.get("source_count", 1)
    if not isinstance(source_count, int):
        source_count = 1
    sources = item.get("sources", []) if isinstance(
        item.get("sources"), list) else []
    titles = item.get("titles", []) if isinstance(
        item.get("titles"), list) else []

    # Language
    detected: set[str] = set()
    lead_lang = detect_language(lead_title)
    detected.add(lead_lang)
    if lead_summary:
        detected.add(detect_language(lead_summary))
    for t in titles:
        detected.add(detect_language(clean_text(safe_str(t))))
    primary_language = "en" if "en" in detected else lead_lang

    # Classification
    etype_guess, etype_conf = classify_event_type(
        lead_title, lead_summary, category, lead_source)
    is_data = (etype_guess == "data")
    candidate_kind = "data_record" if is_data else "news_event"

    # Headline & summary
    headline, headline_original = build_headline(item)
    summary, summary_original = build_summary(item)
    needs_translation = bool(headline_original or summary_original)

    # Entities & topics
    combined_text = " ".join(
        [lead_title, lead_summary] +
        [clean_text(safe_str(t)) for t in titles]
    )
    key_entities = extract_entities(combined_text)
    topics = extract_topics(lead_title, lead_summary)

    # Scoring
    mu_rel = guess_mauritius_relevance(lead_title, lead_summary, category)
    ed_pri = guess_editorial_priority(
        cluster_size, source_count, etype_guess, mu_rel, len(lead_summary))
    confidence = assess_confidence(
        cluster_size, source_count, lead_summary, needs_translation)

    # Articles
    articles = build_articles(
        item, lead_title, lead_source, lead_url, lead_summary, lead_lang)

    # Evidence, notes, merge_hints stub (populated in second pass)
    evidence = build_evidence(item)
    notes = build_notes(item, detected, is_data)
    merge_hints: dict[str, Any] = {
        "related_event_ids": [],
        "shared_topics": [],
        "shared_entities": [],
    }

    return {
        "event_id": event_id,
        "source_item_ids": [item_id],
        "candidate_kind": candidate_kind,
        "cluster_time": cluster_time,
        "category": category,
        "event_type_guess": etype_guess,
        "event_type_confidence": etype_conf,
        "language": primary_language,
        "headline": headline,
        "headline_original": headline_original,
        "summary": summary,
        "summary_original": summary_original,
        "key_entities": key_entities,
        "topics": topics,
        "source_count": source_count,
        "cluster_size": cluster_size,
        "source_names": sources,
        "article_count": len(articles),
        "articles": articles,
        "mauritius_relevance_guess": mu_rel,
        "editorial_priority_guess": ed_pri,
        "confidence": confidence,
        "merge_hints": merge_hints,
        "notes": notes,
        "evidence": evidence,
    }


# ---------------------------------------------------------------------------
# Malformed-item placeholder
# ---------------------------------------------------------------------------

def malformed_candidate(index: int) -> dict:
    return {
        "event_id": f"evt_{index + 1:04d}",
        "source_item_ids": [f"malformed_{index}"],
        "candidate_kind": "news_event",
        "cluster_time": "",
        "category": "local",
        "event_type_guess": "other",
        "event_type_confidence": "none",
        "language": "en",
        "headline": "Malformed feed item",
        "headline_original": "",
        "summary": "This feed item could not be parsed.",
        "summary_original": "",
        "key_entities": {"people": [], "organizations": [],
                         "places": [], "products": []},
        "topics": [],
        "source_count": 0,
        "cluster_size": 0,
        "source_names": [],
        "article_count": 0,
        "articles": [],
        "mauritius_relevance_guess": 0,
        "editorial_priority_guess": 0,
        "confidence": "low",
        "merge_hints": {"related_event_ids": [], "shared_topics": [],
                        "shared_entities": []},
        "notes": "malformed feed item; could not parse",
        "evidence": {},
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(input_path: str = "feed.json", output_path: str = "events.json"):
    path = Path(input_path)
    if not path.exists():
        print(f"Error: {input_path} not found.", file=sys.stderr)
        sys.exit(1)
    try:
        with open(path, "r", encoding="utf-8") as f:
            feed = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"Error reading {input_path}: {e}", file=sys.stderr)
        sys.exit(1)

    items = feed.get("items", [])
    if not isinstance(items, list):
        print("Error: feed.json 'items' is not a list.", file=sys.stderr)
        sys.exit(1)

    # Pass 1: build candidate events
    events: list[dict] = []
    lang_counts: dict[str, int] = {}
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            events.append(malformed_candidate(i))
            continue
        evt = transform_item(item, i)
        events.append(evt)
        lang = evt["language"]
        lang_counts[lang] = lang_counts.get(lang, 0) + 1

    # Pass 2: populate merge hints
    detect_merge_hints(events)

    # Write output
    output = {
        "generated_from": input_path,
        "event_count": len(events),
        "language_distribution": lang_counts,
        "events": events,
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"Wrote {output_path} with {len(events)} candidate events")
    lang_names = {"en": "English", "fr": "French", "mfe": "Creole"}
    if lang_counts:
        print("  Languages: " + ", ".join(
            f"{lang_names.get(k, k)}: {v}"
            for k, v in sorted(lang_counts.items(), key=lambda x: -x[1])))
    trans = sum(1 for e in events
                if e["headline_original"] or e["summary_original"])
    if trans:
        print(f"  {trans} candidates need translation")
    kinds = Counter(e["candidate_kind"] for e in events)
    print(f"  Kinds: {dict(kinds)}")
    types = Counter(e["event_type_guess"] for e in events)
    print(f"  Types: {dict(types.most_common())}")
    confs = Counter(e["confidence"] for e in events)
    print(f"  Confidence: {dict(confs)}")
    merges = sum(1 for e in events if e["merge_hints"]["related_event_ids"])
    if merges:
        print(f"  {merges} candidates have merge hints")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Dodo Digest first-pass event extractor: "
                    "feed.json → events.json")
    parser.add_argument("--input", default="feed.json")
    parser.add_argument("--output", default="events.json")
    args = parser.parse_args()
    run(args.input, args.output)
