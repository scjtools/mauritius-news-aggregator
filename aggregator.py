import feedparser
import requests
import yaml
import json
import hashlib
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}
MAX_AGE_HOURS = 24
MAX_SUMMARY_CHARS = 500
SCRAPE_SLEEP_SECONDS = 2


def load_sources(path="sources/sources.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def is_recent(dt):
    if dt is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
    return dt >= cutoff


def parse_date(entry):
    for field in ("published_parsed", "updated_parsed"):
        t = getattr(entry, field, None)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def item_id(title, url):
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()


def try_parse_date_from_html(tag):
    """Best-effort date extraction from an article card element."""
    time_el = tag.find("time", attrs={"datetime": True})
    if time_el:
        try:
            dt_str = time_el["datetime"][:19]
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(dt_str[:len(fmt)], fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        except Exception:
            pass
    for el in tag.find_all(attrs={"data-date": True}):
        try:
            dt = datetime.fromisoformat(el["data-date"])
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            pass
    return None


def clean_title(title, source_name):
    """Strip trailing source attribution suffixes added by Google News aggregation."""
    if "Google News" in source_name:
        # Titles come as "Real title - Source Name" — strip the last " - Source" part
        title = re.sub(r'\s*[-–]\s*[^-–]{3,50}$', '', title).strip()
    return title


# ── RSS sources ──────────────────────────────────────────────────────────────

def fetch_rss(source):
    items = []
    max_items = source.get("max_items", None)  # None = no cap
    exclude_url_patterns   = source.get("exclude_url_patterns", [])
    exclude_title_prefixes = source.get("exclude_title_prefixes", [])
    try:
        feed = feedparser.parse(source["url"])
        for entry in feed.entries:
            dt = parse_date(entry)
            if not is_recent(dt):
                continue
            raw_title = entry.get("title", "").strip()
            url = entry.get("link", "")
            # Optional per-source content filters
            if any(pat in url for pat in exclude_url_patterns):
                continue
            if any(raw_title.startswith(pfx) for pfx in exclude_title_prefixes):
                continue
            title = clean_title(raw_title, source["name"])
            summary = BeautifulSoup(
                getattr(entry, "summary", "") or "", "html.parser"
            ).get_text()[:MAX_SUMMARY_CHARS]
            items.append({
                "id":           item_id(title, entry.get("link", "")),
                "title":        title,
                "url":          url,
                "summary":      summary.strip(),
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
                "date_verified": dt is not None,
            })
        # Cap AFTER recency filter so max_items means "top N of the last 24h"
        # Feed order is newest-first, so head-truncation gives the most recent items
        if max_items:
            items = items[:max_items]
    except Exception as e:
        print(f"[RSS ERROR] {source['name']}: {e}")
    return items


# ── Homepage scraper (no article fetching, polite sleep) ─────────────────────

def scrape_homepage(source):
    """
    Fetches a single listing page and extracts article links with whatever
    context is visible on the listing itself (title, teaser, date).
    Does NOT fetch individual article pages to avoid block risk.
    Dates are best-effort; if unavailable, published = now() and Stage 2
    should re-verify article age before including in the newsletter.
    """
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        seen = set()

        # Try semantic article/card containers first
        candidates = soup.find_all(
            ["article", "div"],
            class_=re.compile(r"(article|post|news|item|card|story|entry)", re.I)
        )
        use_link_fallback = not candidates
        if use_link_fallback:
            candidates = soup.find_all("a", href=True)

        for tag in candidates:
            if use_link_fallback:
                title = tag.get_text(" ", strip=True)
                url = tag.get("href", "")
            else:
                link_tag = tag.find("a", href=True)
                if not link_tag:
                    continue
                title = link_tag.get_text(" ", strip=True)
                for h in tag.find_all(["h1", "h2", "h3"]):
                    ht = h.get_text(" ", strip=True)
                    if len(ht) >= 20:
                        title = ht
                        break
                url = link_tag.get("href", "")

            if len(title) < 20 or len(title) > 200:
                continue
            if not url.startswith("http"):
                base = source["url"].rstrip("/")
                url = base + ("" if url.startswith("/") else "/") + url.lstrip("/")
            if url in seen:
                continue
            seen.add(url)

            # Extract teaser text from the card itself
            summary = ""
            if not use_link_fallback:
                for p in tag.find_all("p"):
                    pt = p.get_text(" ", strip=True)
                    if len(pt) > 40:
                        summary = pt[:MAX_SUMMARY_CHARS]
                        break
                if not summary:
                    for teaser_el in tag.find_all(
                        class_=re.compile(r"(teaser|excerpt|description|summary|intro|lead)", re.I)
                    ):
                        tt = teaser_el.get_text(" ", strip=True)
                        if len(tt) > 40:
                            summary = tt[:MAX_SUMMARY_CHARS]
                            break

            dt = try_parse_date_from_html(tag) if not use_link_fallback else None
            published = dt.isoformat() if dt else datetime.now(timezone.utc).isoformat()

            items.append({
                "id":           item_id(title, url),
                "title":        title,
                "url":          url,
                "summary":      summary,
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    published,
                "date_verified": dt is not None,  # False = scrape time used, age unconfirmed
            })

            if len(items) >= source.get("max_items", 20):
                break

        time.sleep(SCRAPE_SLEEP_SECONDS)

    except Exception as e:
        print(f"[SCRAPE ERROR] {source['name']}: {e}")
    return items


# ── L'Express scraper ────────────────────────────────────────────────────────

# French month names for parsing L'Express date strings like "19 mars 2026"
FR_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
}

def parse_lexpress_date(date_text, time_text):
    """Parse L'Express date like '19 mars 2026' + time like '14:32' into UTC datetime."""
    try:
        parts = date_text.strip().split()
        if len(parts) == 3:
            day = int(parts[0])
            month = FR_MONTHS.get(parts[1].lower(), 0)
            year = int(parts[2])
            if month:
                h, m = (int(x) for x in time_text.strip().split(":")) if ":" in time_text else (0, 0)
                # L'Express times are in MUT (UTC+4)
                mu_tz = timezone(timedelta(hours=4))
                dt = datetime(year, month, day, h, m, tzinfo=mu_tz)
                return dt.astimezone(timezone.utc)
    except Exception:
        pass
    return None


def scrape_lexpress(source):
    """
    Dedicated scraper for lexpress.mu section pages.
    Articles are server-rendered in <a href="/s/..."> tags.
    Extracts title from h2/h3, date from adjacent span elements.
    """
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        seen = set()
        base_url = "https://lexpress.mu"

        # All article links have href starting with /s/
        for a_tag in soup.find_all("a", href=re.compile(r"^/s/")):
            href = a_tag.get("href", "")
            url = base_url + href
            if url in seen:
                continue

            # Get title from h2 or h3 inside the link
            title = ""
            for h in a_tag.find_all(["h2", "h3", "h4"]):
                t = h.get_text(" ", strip=True)
                if len(t) >= 15:
                    title = t
                    break
            if not title:
                title = a_tag.get_text(" ", strip=True)[:200]
            if len(title) < 15:
                continue

            seen.add(url)

            # Extract date and time from span elements within the link
            spans = [s.get_text(strip=True) for s in a_tag.find_all("span")]
            date_text = ""
            time_text = ""
            for span in spans:
                # Date spans look like "19 mars 2026"
                if re.match(r"^\d{1,2}\s+\w+\s+\d{4}$", span):
                    date_text = span
                # Time spans look like "14:32"
                elif re.match(r"^\d{1,2}:\d{2}$", span):
                    time_text = span

            dt = parse_lexpress_date(date_text, time_text) if date_text else None

            # Age filter — skip if older than 24h
            if dt and not is_recent(dt):
                continue

            published = dt.isoformat() if dt else datetime.now(timezone.utc).isoformat()

            # Teaser: grab any <p> tag inside the link
            summary = ""
            for p in a_tag.find_all("p"):
                pt = p.get_text(" ", strip=True)
                if len(pt) > 40:
                    summary = pt[:MAX_SUMMARY_CHARS]
                    break

            items.append({
                "id":        item_id(title, url),
                "title":     title,
                "url":       url,
                "summary":   summary,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": published,
            })

            if len(items) >= 20:
                break

        time.sleep(SCRAPE_SLEEP_SECONDS)

    except Exception as e:
        print(f"[LEXPRESS ERROR] {source['name']}: {e}")
    return items


# ── mega.mu scraper (L'Express aggregator with full summaries) ────────────────

def scrape_megamu(source):
    """
    Scrapes live.mega.mu which republishes L'Express articles with full summaries.
    Paginates through pages 1..max_pages, stopping early if articles go beyond 24h.
    Each item links back to the original lexpress.mu article via the redirect.
    """
    items = []
    max_pages = source.get("max_pages", 5)
    seen = set()

    # Date pattern: "19 Mar 2026" or "18 mars 2026" — mega.mu uses English short month
    EN_MONTHS = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    def parse_megamu_date(text):
        # Format: "19 Mar 2026, Lexpress.mu"
        m = re.match(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", text.strip())
        if m:
            day, mon, year = int(m.group(1)), m.group(2).lower()[:3], int(m.group(3))
            month = EN_MONTHS.get(mon)
            if month:
                mu_tz = timezone(timedelta(hours=4))
                return datetime(year, month, day, 12, 0, tzinfo=mu_tz).astimezone(timezone.utc)
        return None

    try:
        for page in range(1, max_pages + 1):
            url = source["url"] if page == 1 else f"{source['url']}?page={page}"
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Each article is an <h3> inside the main content, with date text nearby
            articles = soup.find_all("h3")
            found_on_page = 0
            page_went_stale = False

            for h3 in articles:
                a_tag = h3.find("a", href=True)
                if not a_tag:
                    continue

                title = h3.get_text(" ", strip=True)
                if len(title) < 15:
                    continue

                redirect_url = a_tag["href"]
                if not redirect_url.startswith("http"):
                    redirect_url = "https://live.mega.mu" + redirect_url

                if redirect_url in seen:
                    continue
                seen.add(redirect_url)

                # Summary is in the next sibling paragraph
                summary = ""
                parent = h3.find_parent()
                if parent:
                    p = parent.find("p")
                    if p:
                        summary = p.get_text(" ", strip=True)[:MAX_SUMMARY_CHARS]

                # Date text appears after the summary, format "19 Mar 2026, Lexpress.mu"
                dt = None
                if parent:
                    for text_node in parent.find_all(string=re.compile(r"\d{1,2}\s+\w{3}\s+\d{4}")):
                        dt = parse_megamu_date(text_node)
                        if dt:
                            break

                if dt and not is_recent(dt):
                    page_went_stale = True
                    continue

                published = dt.isoformat() if dt else datetime.now(timezone.utc).isoformat()

                items.append({
                    "id":           item_id(title, redirect_url),
                    "title":        title,
                    "url":          redirect_url,
                    "summary":      summary,
                    "source":       source["name"],
                    "language":     source["language"],
                    "category":     source["category"],
                    "published":    published,
                    "date_verified": dt is not None,
                })
                found_on_page += 1

            time.sleep(SCRAPE_SLEEP_SECONDS)

            # If entire page was stale, no need to go deeper
            if page_went_stale and found_on_page == 0:
                break

    except Exception as e:
        print(f"[MEGAMU ERROR] {source['name']}: {e}")

    return items


# ── Met Service bulletin scraper ─────────────────────────────────────────────

def scrape_bulletin(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        paragraphs = []
        seen_para = set()
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 40 and text not in seen_para:
                paragraphs.append(text)
                seen_para.add(text)

        if paragraphs:
            # No character limit — capture the full bulletin
            bulletin_text = " ".join(paragraphs)
            # Strip the boilerplate intro if present
            bulletin_text = re.sub(
                r"^Welcome to Mauritius Meteorological Services\s*", "", bulletin_text
            ).strip()
            now = datetime.now(timezone.utc)
            items.append({
                "id":           item_id("Met bulletin", now.strftime("%Y-%m-%d")),
                "title":        f"Mauritius weather bulletin – {now.strftime('%d %B %Y')}",
                "url":          source["url"],
                "summary":      bulletin_text,
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })
        time.sleep(SCRAPE_SLEEP_SECONDS)
    except Exception as e:
        print(f"[BULLETIN ERROR] {source['name']}: {e}")
    return items


# ── SEMDEX scraper ───────────────────────────────────────────────────────────

def scrape_semdex(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        text = soup.get_text(" ", strip=True)
        match = re.search(r"SEMDEX[^\d]*(\d[\d,]+\.?\d*)", text)
        semdex_value = match.group(1).replace(",", "") if match else None

        if semdex_value:
            now = datetime.now(timezone.utc)
            items.append({
                "id":           item_id("SEMDEX", now.strftime("%Y-%m-%d")),
                "title":        f"SEMDEX – {now.strftime('%d %B %Y')}",
                "url":          source["url"],
                "summary":      f"SEMDEX closed at {semdex_value}",
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })
        else:
            print(f"[SEMDEX WARNING] Could not extract index value from page")
        time.sleep(SCRAPE_SLEEP_SECONDS)
    except Exception as e:
        print(f"[SEMDEX ERROR] {source['name']}: {e}")
    return items


# ── CEB Power Outages ─────────────────────────────────────────────────────────

def fetch_power_outages(source):
    """
    Fetches MrSunshyne's CEB power outage dataset from GitHub.
    JSON structure: {"today": [...], "future": [...]}
    Each outage has: locality, streets, district, from (ISO UTC), to (ISO UTC), date (French string)
    We emit one feed item per outage, for both today and future entries.
    """
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        now = datetime.now(timezone.utc)
        all_outages = data.get("today", []) + data.get("future", [])

        for outage in all_outages:
            locality = outage.get("locality", "").title()
            streets = outage.get("streets", "")
            district = outage.get("district", "").title()
            from_str = outage.get("from", "")
            to_str = outage.get("to", "")
            outage_id = outage.get("id", "")

            # Parse ISO timestamps
            try:
                from_dt = datetime.fromisoformat(from_str.replace("Z", "+00:00"))
                to_dt = datetime.fromisoformat(to_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

            # Convert UTC times to Mauritius time (UTC+4) for display
            mu_offset = timedelta(hours=4)
            from_mu = from_dt + mu_offset
            to_mu = to_dt + mu_offset

            date_display = from_mu.strftime("%d %B %Y")
            time_display = f"{from_mu.strftime('%H:%M')} to {to_mu.strftime('%H:%M')}"

            title = f"CEB power outage – {locality}, {district} on {date_display}"
            summary = f"{locality} ({district}): {time_display}."
            if streets:
                summary += f" Areas affected: {streets[:300]}"

            items.append({
                "id":           item_id("CEB outage", outage_id or f"{locality}{from_str}"),
                "title":        title,
                "url":          "https://github.com/MrSunshyne/mauritius-dataset-electricity",
                "summary":      summary[:MAX_SUMMARY_CHARS],
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    from_dt.isoformat(),
                "date_verified": True,
            })

    except Exception as e:
        print(f"[POWER OUTAGES ERROR] {source['name']}: {e}")
    return items


# ── Public holidays ───────────────────────────────────────────────────────────

def fetch_public_holidays(source):
    """
    Reads data/public_holidays.yaml and emits an item when a holiday
    is today or within the next 3 days.
    Uses Mauritius time (UTC+4) to determine today's date.
    YAML structure: list of year blocks, each with a 'holidays' list.
    """
    items = []
    try:
        with open(source["url"]) as f:
            # safe_load_all handles multiple YAML documents (separated by ---)
            # but our file uses repeated top-level keys, so we load all docs
            raw = f.read()

        # Parse all YAML documents in the file (each year block separated by ---)
        all_holidays = []
        for doc in yaml.safe_load_all(raw):
            if doc and "holidays" in doc:
                all_holidays.extend(doc["holidays"])

        # Use Mauritius time (UTC+4) to determine today's date
        mu_tz = timezone(timedelta(hours=4))
        today = datetime.now(mu_tz).date()
        lookahead_days = 3
        upcoming = []

        for holiday in all_holidays:
            try:
                hdate = date.fromisoformat(holiday["date"])
            except (ValueError, KeyError):
                continue
            days_away = (hdate - today).days
            if 0 <= days_away <= lookahead_days:
                upcoming.append((days_away, holiday["name"], holiday["date"]))

        if upcoming:
            now = datetime.now(timezone.utc)
            parts = []
            for days_away, name, hdate in sorted(upcoming):
                if days_away == 0:
                    parts.append(f"Today is {name}")
                elif days_away == 1:
                    parts.append(f"Tomorrow: {name}")
                else:
                    parts.append(f"In {days_away} days ({hdate}): {name}")

            items.append({
                "id":           item_id("public holiday", today.isoformat()),
                "title":        f"Upcoming public holiday – {upcoming[0][1]}",
                "url":          "https://govmu.org",
                "summary":      " | ".join(parts),
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })

    except Exception as e:
        print(f"[PUBLIC HOLIDAYS ERROR] {source['name']}: {e}")
    return items


# ── Deduplicate ───────────────────────────────────────────────────────────────

def deduplicate(items):
    """
    Two-pass deduplication:
    1. By URL — catches same article with different titles (e.g. BBC updating headline)
    2. By ID (title+url hash) — catches same title at different URLs
    URL-based pass runs first and wins.
    """
    seen_urls = {}
    for item in items:
        url = item.get("url", "")
        if url and url not in seen_urls:
            seen_urls[url] = item

    seen_ids = {}
    for item in seen_urls.values():
        if item["id"] not in seen_ids:
            seen_ids[item["id"]] = item

    return list(seen_ids.values())


# ── Build RSS output ──────────────────────────────────────────────────────────

def build_rss(items):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "Mauritius News Aggregator"
    SubElement(channel, "link").text = "https://github.com"
    SubElement(channel, "description").text = "Aggregated Mauritius news – last 24 hours"
    SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    for item in sorted(items, key=lambda x: x["published"], reverse=True):
        entry = SubElement(channel, "item")
        SubElement(entry, "title").text = item["title"]
        SubElement(entry, "link").text = item["url"]
        SubElement(entry, "description").text = item["summary"]
        SubElement(entry, "pubDate").text = item["published"]
        SubElement(entry, "guid").text = item["id"]
        SubElement(entry, "source").text = item["source"]
        SubElement(entry, "category").text = item["category"]
        SubElement(entry, "language").text = item["language"]
        # date_verified=false means pubDate is scrape time, not article publish time
        # Stage 2 should treat these items' ages as unconfirmed
        SubElement(entry, "date_verified").text = str(item.get("date_verified", True)).lower()

    raw = tostring(rss, encoding="unicode")
    return minidom.parseString(raw).toprettyxml(indent="  ")


# ── Exchange rates (FloatRates MUR) ──────────────────────────────────────────

def fetch_exchange_rates(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        target_currencies = source.get("currencies", [])
        rates = {}
        for item in root.findall("item"):
            title = item.findtext("title", "")
            for currency in target_currencies:
                if title.endswith(currency):
                    parts = title.split("=")
                    if len(parts) == 2:
                        # parts[1] is e.g. "0.02150 USD" — strip the currency suffix
                        rate_value = parts[1].strip()
                        if rate_value.endswith(currency):
                            rate_value = rate_value[:-len(currency)].strip()
                        try:
                            # FloatRates gives 1 MUR = X foreign; invert to get 1 foreign = Y MUR
                            rates[currency] = 1.0 / float(rate_value)
                        except (ValueError, ZeroDivisionError):
                            pass
        if rates:
            now = datetime.now(timezone.utc)
            # Format: "MUR/USD 46.51 | MUR/EUR 53.44"
            summary = " | ".join(
                f"MUR/{c} {rates[c]:.2f}"
                for c in target_currencies if c in rates
            )
            items.append({
                "id":           item_id("MUR rates", now.strftime("%Y-%m-%d")),
                "title":        f"MUR exchange rates – {now.strftime('%d %B %Y')}",
                "url":          source["url"],
                "summary":      summary,
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })
    except Exception as e:
        print(f"[RATES ERROR] {source['name']}: {e}")
    return items


# ── Gold API (gold, bitcoin, oil) ─────────────────────────────────────────────

def fetch_gold_api(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        price = data.get("price")
        symbol = data.get("symbol", "")
        currency = data.get("currency", "USD")

        if price is not None:
            now = datetime.now(timezone.utc)
            label = {"XAU": "Gold", "BTC": "Bitcoin", "USOIL": "WTI Crude Oil"}.get(symbol, symbol)
            items.append({
                "id":           item_id(f"{symbol} price", now.strftime("%Y-%m-%d")),
                "title":        f"{label} price – {now.strftime('%d %B %Y')}",
                "url":          source["url"],
                "summary":      f"{label} ({symbol}): {currency} {price:,.2f}",
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })
    except Exception as e:
        print(f"[GOLD API ERROR] {source['name']}: {e}")
    return items


# ── Yahoo Finance commodity price ────────────────────────────────────────────

def fetch_yahoo_finance(source):
    """
    Fetches a commodity price from Yahoo Finance's unofficial chart API.
    Used for WTI crude oil (CL=F) and others with no free dedicated API.
    """
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        meta = result.get("meta", {})
        price = meta.get("regularMarketPrice")
        currency = meta.get("currency", "USD")
        symbol = meta.get("symbol", "")
        label = source.get("label", symbol)

        if price is not None:
            now = datetime.now(timezone.utc)
            items.append({
                "id":        item_id(f"{symbol} price", now.strftime("%Y-%m-%d")),
                "title":     f"{label} price – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   f"{label} ({symbol}): {currency} {price:,.2f} per barrel",
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })
    except Exception as e:
        print(f"[YAHOO FINANCE ERROR] {source['name']}: {e}")
    return items


# ── OilPriceAPI demo (Brent crude, no key, 20 req/hr) ────────────────────────

def fetch_oilprice_demo(source):
    """
    Fetches Brent crude price from oilpriceapi.com's no-auth demo endpoint.
    Returns latest available price. 20 requests/hour limit — well within our 2/day.
    Response: {"data": [{"price": 97.66, "formatted": "$97.66", "currency": "USD",
               "code": "BRENT_CRUDE_USD", ...}, ...]}
    """
    items = []
    try:
        headers = {**HEADERS, "Content-Type": "application/json"}
        r = requests.get(source["url"], headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()

        # Demo endpoint: {"data": {"prices": [...], "meta": {...}}}
        price_list = data.get("data", {}).get("prices", [])
        price_data = next((p for p in price_list if "BRENT" in p.get("code", "")), price_list[0] if price_list else {})
        price = price_data.get("price")
        formatted = price_data.get("formatted", "")
        currency = price_data.get("currency", "USD")
        code = price_data.get("code", "BRENT_CRUDE_USD")
        label = "Brent Crude Oil" if "BRENT" in code else "WTI Crude Oil"

        if price is not None:
            now = datetime.now(timezone.utc)
            items.append({
                "id":           item_id("Brent crude", now.strftime("%Y-%m-%d")),
                "title":        f"{label} price – {now.strftime('%d %B %Y')}",
                "url":          "https://www.oilpriceapi.com",
                "summary":      f"{label}: {formatted if formatted else f'{currency} {price:,.2f}'} per barrel",
                "source":       source["name"],
                "language":     source["language"],
                "category":     source["category"],
                "published":    now.isoformat(),
                "date_verified": True,
            })
        else:
            print(f"[OILPRICE WARNING] No price found in response: {data}")
    except Exception as e:
        print(f"[OILPRICE ERROR] {source['name']}: {e}")
    return items


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sources = load_sources()
    all_items = []

    print("Fetching RSS feeds...")
    for source in sources.get("rss_feeds", []):
        items = fetch_rss(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Scraping pages...")
    for source in sources.get("scrapers", []):
        scrape_type = source.get("type")
        if scrape_type == "bulletin":
            items = scrape_bulletin(source)
        elif scrape_type == "semdex":
            items = scrape_semdex(source)
        elif scrape_type == "power_outages":
            items = fetch_power_outages(source)
        elif scrape_type == "public_holidays":
            items = fetch_public_holidays(source)
        elif scrape_type == "lexpress":
            items = scrape_lexpress(source)
        elif scrape_type == "megamu":
            items = scrape_megamu(source)
        else:
            items = scrape_homepage(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Fetching exchange rates...")
    for source in sources.get("exchange_rates", []):
        rate_type = source.get("type")
        if rate_type == "gold_api":
            items = fetch_gold_api(source)
        elif rate_type == "yahoo_finance":
            items = fetch_yahoo_finance(source)
        elif rate_type == "oilprice_demo":
            items = fetch_oilprice_demo(source)
        else:
            items = fetch_exchange_rates(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    all_items = deduplicate(all_items)
    print(f"\nTotal unique items: {len(all_items)}")

    rss_output = build_rss(all_items)
    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_output)
    print("Written to feed.xml")

    build_candidates(all_items)



# ═════════════════════════════════════════════════════════════════════════════
# Stage 2 — candidates.json generation
# Runs in the same process immediately after feed.xml is written.
# No network calls. No LLM. Pure Python.
#
# Clustering strategy (two passes):
#
#   Pass 1 — Full-title Jaccard ≥ 0.75 (same-language near-duplicates)
#     Catches the same article republished verbatim across sources.
#     Strict threshold — only merges when titles are almost identical.
#     Language-agnostic: works on whatever language both titles share.
#
#   Pass 2 — Proper-noun Jaccard ≥ 0.60 (cross-language same-story)
#     Catches "Bérenger resigns as DPM" vs "Bérenger démissionne comme VPM".
#     Proper nouns (capitalised tokens in the original title) are language-
#     invariant — "Bérenger", "Ramgoolam", "Goodlands", "SEMDEX" appear the
#     same in EN, FR, and creole. Multi-word proper nouns like "La Case Noyale"
#     are extracted as a unit to avoid treating "La" alone as a named entity.
#     Requires ≥ 2 shared proper nouns to merge (prevents false positives on
#     stories that merely mention the same person in passing).
#
#   Option C hint — similar_to
#     Items that share ≥ 2 proper nouns but scored below the Pass 2 merge
#     threshold (i.e. likely the same event, different angle or timepoint)
#     get a lightweight "similar_to" hint: [{"title": "...", "source": "..."}].
#     This saves Claude from independently reasoning about obvious pairs and
#     costs ~10 tokens per hint vs potentially much more in Claude's thinking.
#
# In all cases:
#   - Cluster representative = most recent item
#   - Older items travel as "related" array with full metadata for Stage 3
#   - No scoring, no source ranking — Claude decides what's newsworthy
# ═════════════════════════════════════════════════════════════════════════════

# Categories routed to data digest (never editorial)
_DATA_CATS = {"weather", "finance", "utilities", "government"}

# Cross-source corroboration caps for regional and international buckets.
# After clustering, only the top N clusters ranked by distinct-source count
# are passed to Stage 2. Local is never capped — all local news is kept.
_REGIONAL_CAP = 15
_INTL_CAP     = 15

# Source tier weights for corroboration scoring.
# Tier-2 editorial outlets that make independent coverage decisions but
# rarely share stories with other sources get a bonus score so they
# are not unfairly penalised versus wire aggregators in the cap ranking.
# Score = distinct_sources + max(tier_bonus across all sources in cluster).
_SOURCE_TIER_BONUS = {
    "Business Insider Africa": 1,
    "TechCabal":               1,
    "BBC Africa":              1,
}

# Common words that appear capitalised but are NOT proper nouns.
# Covers EN titles, FR titles, and common creole/abbreviation patterns.
_CAP_STOPWORDS = {
    # English articles / prepositions / conjunctions
    "The", "A", "An", "In", "Of", "To", "And", "For", "Is", "On", "At",
    "With", "By", "As", "It", "Its", "From", "This", "That", "Are", "Was",
    "Were", "Has", "Have", "Been", "Be", "Will", "Would", "Could", "After",
    "But", "Not", "No", "New", "How", "Why", "Who", "What", "When", "Where",
    # French articles / prepositions / conjunctions
    "Le", "La", "Les", "Du", "Des", "Un", "Une", "En", "Et", "Au", "Aux",
    "Sur", "Par", "Ou", "Que", "Qui", "Se", "Sa", "Son", "Ses", "Dans",
    "Est", "Pas", "Plus", "Lui", "Ils", "Elles", "Nous", "Vous", "Mon",
    "Ton", "Leur", "Leurs", "Ce", "Ces", "Cet", "Cette", "Si", "Même",
    "Après", "Avant", "Lors", "Sans", "Sous", "Entre", "Vers", "Chez",
    "Avec", "Pour", "Dont", "Mais", "Car", "Donc", "Puis",
    # Generic title-case words that are not unique identifiers
    "Watch", "Report", "Update", "Live", "Breaking", "News", "Day",
    "Year", "Week", "Today", "First", "Last", "More", "Less",
    # Political / professional roles — generic, appear across many stories
    "Prime", "Deputy", "Minister", "President", "Director", "General",
    "Chief", "Officer", "Secretary", "Leader", "Head", "Vice", "Senior",
    "Former", "Ex", "Acting", "Interim", "National", "Federal", "State",
    "Premier", "Ministre", "Directeur", "Président", "Secrétaire",
    # Common news verbs / nouns that get title-cased mid-headline
    "Call", "Calls", "Says", "Said", "Told", "Warns", "Urges",
    "Resigns", "Resign", "Quits", "Quit", "Joins", "Leaves", "Returns",
    "Murder", "Meurtre", "Affaire", "Case", "Inquiry", "Trial", "Court",
    "Following", "Amid", "Over", "Against", "Between",
    # Titles used in bylines / attributions
    "Dr", "Mr", "Mrs", "Ms", "Prof",
}


def _parse_published(s: str) -> datetime:
    """Parse ISO 8601 pubdate string into UTC-aware datetime."""
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(s)
    except Exception:
        pass
    return datetime.now(timezone.utc)


def _title_words(title: str) -> frozenset:
    """
    Stopword-stripped lowercase word-set for Pass 1 (same-language) matching.
    Bilingual stopwords — EN + FR.
    """
    stop = {
        "the","a","an","in","of","to","and","for","is","on","at","with",
        "by","as","it","its","from","this","that","are","was","were","has",
        "have","been","be","will","would","could","de","la","le","les","du",
        "des","un","une","en","et","au","aux","sur","par","ou","que","qui",
        "se","sa","son","ses","dans","est","pas","plus","lui","ils","also",
        "after","mais","car","donc","puis","lors","sans","sous","entre",
        "vers","chez","avec","pour","dont","ces","cet","cette","même",
    }
    tokens = re.sub(r"[^\w\s]", " ", title.lower()).split()
    return frozenset(t for t in tokens if t not in stop and len(t) > 2)


def _proper_nouns(title: str) -> frozenset:
    """
    Extract proper nouns from a title for Pass 2 (cross-language) matching.

    Strategy: scan the *original* (cased) title for runs of capitalised tokens.
    A capitalised token is one whose first character is uppercase.
    Consecutive capitalised tokens are joined as a single named entity
    (handles "La Case Noyale", "Deputy Prime Minister", "Bank of Mauritius").
    Tokens in _CAP_STOPWORDS are treated as connecting words, not entities,
    so "Bérenger au PMO" yields {"Bérenger", "PMO"} not {"Bérenger", "Au", "PMO"}.

    Numbers and all-caps abbreviations (SEMDEX, MMM, CEB) are always included.
    Minimum length 2 characters.
    """
    # Tokenise preserving original case; strip punctuation per token
    raw_tokens = re.sub(r"[«»""''`]", " ", title).split()
    tokens = [re.sub(r"^[^\w]+|[^\w]+$", "", t) for t in raw_tokens]
    tokens = [t for t in tokens if t]

    entities: set = set()
    run: list = []

    def flush_run():
        if run:
            entity = " ".join(run)
            if len(entity) >= 2:
                entities.add(entity)
            run.clear()

    for tok in tokens:
        clean = re.sub(r"[^\w\-']", "", tok)
        if not clean:
            flush_run()
            continue

        is_cap   = clean[0].isupper()
        is_abbr  = clean.isupper() and len(clean) >= 2   # CEB, MMM, DPM
        is_num   = bool(re.match(r"^\d", clean))          # Rs 75, 2026
        is_stop  = clean in _CAP_STOPWORDS

        if is_abbr or is_num:
            # Always a standalone entity — flush any open run first
            flush_run()
            entities.add(clean)
        elif is_cap and not is_stop:
            run.append(clean)
        else:
            flush_run()

    flush_run()

    # Normalise multi-word proper nouns: also register each component word
    # individually so "Paul Bérenger" and "Bérenger" both yield "Bérenger".
    # This handles the very common pattern where French titles use full names
    # and English titles use surnames only.
    extras: set = set()
    for entity in entities:
        parts = entity.split()
        if len(parts) > 1:
            for part in parts:
                if len(part) >= 2 and part not in _CAP_STOPWORDS:
                    extras.add(part)
    entities.update(extras)

    return frozenset(entities)


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a and not b:
        return 1.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _shared_proper_nouns(a: frozenset, b: frozenset) -> int:
    """Count shared proper nouns between two title fingerprints."""
    return len(a & b)


def _cluster_editorial(items: list) -> list:
    """
    Two-pass multilingual clustering of editorial items.

    Pass 1: Full-title Jaccard ≥ 0.75 — same-language near-duplicates.
    Pass 2: Proper-noun Jaccard ≥ 0.60, requiring ≥ 2 shared names —
            cross-language same-story detection.

    After both passes, surviving items are checked for shared proper nouns
    (≥ 2) with other survivors. Those pairs get a lightweight "similar_to"
    hint to guide Claude without the cost of Claude reasoning it out itself.

    Cluster representative = most recent item.
    Older cluster members travel as "related" with full metadata.
    """
    # Sort newest-first — first item encountered becomes the representative
    items = sorted(
        items,
        key=lambda x: _parse_published(x.get("published", "")),
        reverse=True,
    )

    # ── Pass 1: same-language, full-title Jaccard ≥ 0.75 ─────────────────────
    kept_p1: list  = []
    fps_p1:  list  = []   # word-set fingerprints

    for item in items:
        fp     = _title_words(item["title"])
        merged = False
        for i, efp in enumerate(fps_p1):
            if _jaccard(fp, efp) >= 0.75:
                kept_p1[i].setdefault("related", [])
                kept_p1[i]["related"].append({
                    "title":     item["title"],
                    "source":    item["source"],
                    "url":       item["url"],
                    "published": item.get("published", ""),
                    "summary":   item.get("summary", ""),
                })
                merged = True
                break
        if not merged:
            kept_p1.append(item)
            fps_p1.append(fp)

    # ── Pass 2: cross-language, proper-noun Jaccard ≥ 0.60, ≥ 2 shared ───────
    kept_p2: list  = []
    pns_p2:  list  = []   # proper-noun fingerprints

    for item in kept_p1:
        pn     = _proper_nouns(item["title"])
        merged = False
        for i, epn in enumerate(pns_p2):
            shared = _shared_proper_nouns(pn, epn)
            if shared >= 2 and _jaccard(pn, epn) >= 0.60:
                kept_p2[i].setdefault("related", [])
                kept_p2[i]["related"].append({
                    "title":     item["title"],
                    "source":    item["source"],
                    "url":       item["url"],
                    "published": item.get("published", ""),
                    "summary":   item.get("summary", ""),
                })
                merged = True
                break
        if not merged:
            kept_p2.append(item)
            pns_p2.append(pn)

    # ── Option C: similar_to hints for near-miss pairs ────────────────────────
    # Items that share ≥ 2 proper nouns but didn't merge (different angle /
    # different timepoint) get a lightweight hint so Claude can relate them
    # without spending tokens re-deriving the connection.
    # Only applied within the same category to avoid cross-category noise.
    n = len(kept_p2)
    for i in range(n):
        for j in range(i + 1, n):
            if kept_p2[i].get("category") != kept_p2[j].get("category"):
                continue
            pn_i = _proper_nouns(kept_p2[i]["title"])
            pn_j = _proper_nouns(kept_p2[j]["title"])
            if _shared_proper_nouns(pn_i, pn_j) >= 2:
                # Mutual hints — minimal payload to keep token cost low
                hint_i = {"title": kept_p2[j]["title"], "source": kept_p2[j]["source"]}
                hint_j = {"title": kept_p2[i]["title"], "source": kept_p2[i]["source"]}
                kept_p2[i].setdefault("similar_to", [])
                kept_p2[j].setdefault("similar_to", [])
                if hint_i not in kept_p2[i]["similar_to"]:
                    kept_p2[i]["similar_to"].append(hint_i)
                if hint_j not in kept_p2[j]["similar_to"]:
                    kept_p2[j]["similar_to"].append(hint_j)

    return kept_p2


def _clean_weather(text: str) -> str:
    """
    Remove boilerplate and exact duplications from the Met Services bulletin.
    Keeps ALL factual content — temperatures, wind, sea state, tides, sunrise/sunset.
    Stage 3 writes the actual summary sentence; we just clean the raw input.
    """
    if not text:
        return text

    # The bulletin often contains itself twice — deduplicate by splitting on
    # the header marker and keeping only the first occurrence.
    marker = "Weather news for Mauritius issued"
    parts = text.split(marker)
    if len(parts) > 2:      # marker appears 3+ times — keep just the first block
        text = (marker + parts[1]).strip()
    elif len(parts) == 2:   # marker appears twice — normal duplicate case
        text = (marker + parts[1]).strip()
    # len(parts) == 1 means marker not found at all — leave text unchanged

    # Strip navigation/footer boilerplate
    for pattern in [
        r"About Us\s*\|.*?©.*?Mauritius Meteorological Services\.?",
        r"© Copyright.*?$",
    ]:
        text = re.sub(pattern, "", text, flags=re.DOTALL | re.IGNORECASE).strip()

    # Collapse multiple whitespace/newlines
    text = re.sub(r"\s{2,}", " ", text).strip()

    return text


def _build_data_digest(data_items: list) -> dict:
    """
    Structures all data-category items into a clean digest dict for Stage 3.
    All parsing is regex-based; no LLM involved.
    """
    digest = {
        "weather":     None,   # cleaned bulletin text — full, all facts preserved
        "semdex":      None,   # string value e.g. "2226.88"
        "exchange":    {},     # {"MUR/USD": "46.51", "MUR/EUR": "53.76", ...}
        "commodities": {},     # {"Gold (XAU)": "USD 4,707.90", "BTC": ..., "Brent Crude": ...}
        "ceb_outages": [],     # list of {title, detail, url}
        "holidays":    [],     # list of {title, detail, url}
    }

    for item in data_items:
        cat     = item.get("category", "")
        source  = item.get("source", "")
        title   = item.get("title", "")
        summary = item.get("summary", "")
        url     = item.get("url", "")

        if cat == "weather":
            digest["weather"] = _clean_weather(summary)

        elif cat == "finance":
            if "SEMDEX" in title:
                m = re.search(r"([\d,]+\.?\d+)", summary)
                if m:
                    digest["semdex"] = m.group(1).replace(",", "")

            elif "exchange" in title.lower() or "MUR" in title:
                # "MUR/USD 46.51 | MUR/EUR 53.76"
                for pair, val in re.findall(r"([\w/]+)\s+([\d.]+)", summary):
                    if "/" in pair:
                        digest["exchange"][pair] = val

            elif "Bitcoin" in title or "BTC" in title:
                m = re.search(r"USD\s*([\d,]+\.?\d*)", summary)
                if m:
                    digest["commodities"]["BTC (Bitcoin)"] = f"USD {m.group(1)}"

            elif "Gold" in title:
                m = re.search(r"USD\s*([\d,]+\.?\d*)", summary)
                if m:
                    digest["commodities"]["Gold (XAU)"] = f"USD {m.group(1)}"

            elif "Brent" in title or "Oil" in title or "WTI" in title:
                m = re.search(r"USD\s*([\d,]+\.?\d*)", summary)
                if m:
                    label = "Brent Crude" if "Brent" in title else "WTI Crude"
                    digest["commodities"][label] = f"USD {m.group(1)}/bbl"

        elif cat == "utilities":
            digest["ceb_outages"].append({
                "title":  title,
                "detail": summary,
                "url":    url,
            })

        elif cat == "government":
            digest["holidays"].append({
                "title":  title,
                "detail": summary,
                "url":    url,
            })

    return digest


def _corroboration_score(item: dict) -> int:
    """
    Score a cluster by editorial significance.
    Base score = number of distinct sources that covered the story.
    Tier bonus = added for editorial outlets that make independent coverage
    decisions but rarely share stories with other sources (e.g. BI Africa,
    TechCabal). Without the bonus these would be unfairly ranked below
    single-source AllAfrica wire briefs that happen to share a topic.
    """
    sources = {item.get("source", "")}
    for rel in item.get("related", []):
        sources.add(rel.get("source", ""))
    sources.discard("")
    tier_bonus = max(
        (_SOURCE_TIER_BONUS.get(s, 0) for s in sources),
        default=0,
    )
    return len(sources) + tier_bonus


def build_candidates(all_items: list, output_path: str = "candidates.json") -> None:
    """
    Produces candidates.json for Stage 2b (human + Claude editorial selection).

    Output structure:
      {
        "meta":        { run info, item counts },
        "editorial":   {
          "local":          [ items... ],   // reverse-chronological
          "regional":        [ items... ],  // Indian Ocean + Africa combined
          "international":  [ items... ]
        },
        "data_digest": { weather, semdex, exchange, commodities, ceb_outages, holidays }
      }

    Items within each section are in reverse-chronological order (newest first).
    No scoring or ranking is applied — that's Claude's job in Stage 2b.
    Each item may have a "related" array containing older near-duplicate items
    that were clustered under it; these carry context for the Stage 3 writer.
    """
    from collections import defaultdict

    now = datetime.now(timezone.utc)

    editorial_raw = []
    data_items    = []

    for item in all_items:
        cat = item.get("category", "")

        if cat in _DATA_CATS:
            data_items.append(item)

        else:
            editorial_raw.append(item)

    # Two-pass multilingual clustering (Pass 1: same-language, Pass 2: cross-language)
    # Plus similar_to hints for related-but-distinct items
    editorial_clustered = _cluster_editorial(editorial_raw)
    n_merged = len(editorial_raw) - len(editorial_clustered)

    # Bucket by category, preserve reverse-chronological order within each
    by_cat: dict = defaultdict(list)
    for item in editorial_clustered:
        by_cat[item["category"]].append(item)

    # regional and international: rank by cross-source corroboration score
    # (distinct sources per cluster), then cap. This sheds single-source
    # wire briefs while keeping stories that multiple outlets covered.
    # Local is never capped — all Mauritius news passes through.
    regional_all = sorted(
        by_cat.get("regional", []),
        key=_corroboration_score,
        reverse=True,
    )
    intl_all = sorted(
        by_cat.get("international", []),
        key=_corroboration_score,
        reverse=True,
    )
    regional      = regional_all[:_REGIONAL_CAP]
    international = intl_all[:_INTL_CAP]
    n_regional_dropped = len(regional_all) - len(regional)
    n_intl_dropped     = len(intl_all) - len(international)

    editorial = {
        "local":         by_cat.get("local", []),
        "regional":      regional,
        "international": international,
    }

    data_digest = _build_data_digest(data_items)

    output = {
        "meta": {
            "generated_at":    now.isoformat(),
            "feed_build_date": now.strftime("%a, %d %b %Y %H:%M:%S +0000"),
            "item_counts": {
                "feed_total":            len(all_items),
                "editorial_raw":         len(editorial_raw),
                "after_clustering":      len(editorial_clustered),
                "clusters_merged":       n_merged,
                "data_items":            len(data_items),
                "local":                 len(editorial["local"]),
                "regional":              len(editorial["regional"]),
                "regional_dropped":      n_regional_dropped,
                "international":         len(editorial["international"]),
                "international_dropped": n_intl_dropped,
            },
            "slot_targets": {
                "local":           "10–12",
                "regional":        "up to 4",
                "international":   "up to 4",
            },
            "notes": (
                "Local: all items kept. Regional and international: pre-filtered to top "
                f"{_REGIONAL_CAP} / {_INTL_CAP} clusters ranked by cross-source corroboration "
                "(distinct sources per cluster). "
                "Clustering: Pass 1 merges near-identical same-language titles (Jaccard≥0.75); "
                "Pass 2 merges cross-language titles sharing ≥2 proper nouns (Jaccard≥0.60). "
                "Merged items appear as 'related' arrays. "
                "Items sharing ≥2 proper nouns but not merged carry a 'similar_to' hint."
            ),
        },
        "editorial":   editorial,
        "data_digest": data_digest,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written to {output_path}")
    print(f"  local:          {len(editorial['local'])} candidates")
    print(f"  regional:       {len(editorial['regional'])} candidates "
          f"({n_regional_dropped} dropped by corroboration filter)")
    print(f"  international:  {len(editorial['international'])} candidates "
          f"({n_intl_dropped} dropped by corroboration filter)")
    print(f"  clusters merged:{n_merged} (from {len(editorial_raw)} raw editorial items)")
    print(f"  data digest:    weather={'yes' if data_digest['weather'] else 'no'}, "
          f"semdex={data_digest['semdex']}, "
          f"ceb={len(data_digest['ceb_outages'])} outage(s), "
          f"holidays={len(data_digest['holidays'])}")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
