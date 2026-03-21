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
    except Exception as e:
        print(f"[RSS ERROR] {source['name']}: {e}")
    return items


# ── Homepage scraper (no article fetching, polite sleep) ─────────────────────

def scrape_homepage(source):
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
                if url.startswith("/"):
                    # Absolute path — use scheme+domain only, not the full page URL
                    from urllib.parse import urlparse
                    parsed = urlparse(source["url"])
                    base = f"{parsed.scheme}://{parsed.netloc}"
                else:
                    # Relative path — use full page URL as base
                    base = source["url"].rstrip("/")
                url = base + "/" + url.lstrip("/")
            if url in seen:
                continue

            # Subdomain exclusion: skip URLs that don't belong to the source's own domain
            from urllib.parse import urlparse as _urlparse
            source_host = _urlparse(source["url"]).netloc
            url_host = _urlparse(url).netloc
            if url_host and source_host and not (url_host == source_host or url_host.endswith("." + source_host)):
                continue

            # Category allowlist (Defimedia-style: checks .article-status.article-category label)
            category_allowlist = source.get("category_allowlist")
            if category_allowlist and not use_link_fallback:
                cat_el = tag.find(
                    class_=lambda c: c and "article-category" in c
                )
                if cat_el is None or cat_el.get_text(strip=True) not in category_allowlist:
                    continue

            # URL path allowlist (Le Mauricien-style: checks URL path substrings)
            url_path_allowlist = source.get("url_path_allowlist")
            if url_path_allowlist:
                if not any(seg in url for seg in url_path_allowlist):
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

            # Fallback: scan HTML comments in the card for JSON-LD datePublished
            # (used by Business Insider Africa which embeds dates in commented-out scripts)
            if not dt and not use_link_fallback:
                import json as _json
                from bs4 import Comment
                for comment in tag.find_all(string=lambda t: isinstance(t, Comment)):
                    try:
                        comment_soup = BeautifulSoup(comment, "html.parser")
                        for script in comment_soup.find_all("script", type="application/ld+json"):
                            data = _json.loads(script.string or "")
                            raw_date = None
                            if isinstance(data, dict):
                                raw_date = data.get("datePublished") or data.get("dateCreated")
                            elif isinstance(data, list):
                                for d in data:
                                    if isinstance(d, dict):
                                        raw_date = d.get("datePublished") or d.get("dateCreated")
                                        if raw_date:
                                            break
                            if raw_date:
                                from dateutil import parser as _dp
                                pub_dt = _dp.parse(raw_date)
                                if pub_dt.tzinfo is None:
                                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                                dt = pub_dt
                                break
                    except Exception:
                        pass
                    if dt:
                        break

            if dt and not is_recent(dt):
                seen.discard(url)
                continue

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

        time.sleep(SCRAPE_SLEEP_SECONDS)

    except Exception as e:
        print(f"[SCRAPE ERROR] {source['name']}: {e}")
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


# ── Le Mauricien scraper ─────────────────────────────────────────────────────

# Le Mauricien date strings appear as "21 Mar 2026 12h00" next to each headline
_LM_MONTHS = {
    "jan": 1, "fév": 2, "mar": 3, "avr": 4, "mai": 5, "jun": 6,
    "juil": 7, "aoû": 8, "sep": 9, "oct": 10, "nov": 11, "déc": 12,
    # fallbacks for ASCII variants
    "fev": 2, "aou": 8,
}

def _parse_lm_date(text: str):
    """Parse Le Mauricien date strings like '21 Mar 2026 12h00' into UTC datetime."""
    m = re.search(
        r"(\d{1,2})\s+(\w+)\s+(\d{4})\s+(\d{1,2})h(\d{2})",
        text, re.IGNORECASE
    )
    if m:
        day, mon, year = int(m.group(1)), m.group(2).lower()[:3], int(m.group(3))
        hour, minute = int(m.group(4)), int(m.group(5))
        month = _LM_MONTHS.get(mon)
        if month:
            mu_tz = timezone(timedelta(hours=4))  # MUT = UTC+4
            return datetime(year, month, day, hour, minute, tzinfo=mu_tz).astimezone(timezone.utc)
    # Fallback: date only, no time
    m2 = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text, re.IGNORECASE)
    if m2:
        day, mon, year = int(m2.group(1)), m2.group(2).lower()[:3], int(m2.group(3))
        month = _LM_MONTHS.get(mon)
        if month:
            mu_tz = timezone(timedelta(hours=4))
            return datetime(year, month, day, 12, 0, tzinfo=mu_tz).astimezone(timezone.utc)
    return None


def scrape_lemauricien(source):
    """
    Dedicated scraper for lemauricien.com.
    - Extracts article links, titles, teasers, and dates from the homepage.
    - Dates are parsed from inline text (e.g. '21 Mar 2026 12h00'), giving
      date_verified=True without needing to fetch individual article pages.
    - Articles without a teaser are marked date_verified per date result but
      summary is left empty for enrichment to fill in.
    - Only keeps articles whose URL matches url_path_allowlist (set in sources.yaml).
    """
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        url_path_allowlist = source.get("url_path_allowlist", [])
        source_host = source["url"].rstrip("/").split("//")[-1]

        seen = set()

        # All article links on lemauricien.com are <a href="..."> wrapping a title
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")

            # Resolve URL
            if href.startswith("http"):
                url = href
            elif href.startswith("/"):
                url = f"https://www.lemauricien.com{href}"
            else:
                continue

            # Domain check
            if source_host not in url:
                continue

            # Path allowlist
            if url_path_allowlist and not any(seg in url for seg in url_path_allowlist):
                continue

            if url in seen:
                continue

            # Title: prefer h2/h3/h4 inside the link, fall back to link text
            title = ""
            for h in a_tag.find_all(["h1", "h2", "h3", "h4"]):
                t = h.get_text(" ", strip=True)
                if len(t) >= 20:
                    title = t
                    break
            if not title:
                title = a_tag.get_text(" ", strip=True)
            if len(title) < 20 or len(title) > 200:
                continue

            seen.add(url)

            # Teaser: look for a <p> sibling or parent container paragraph
            summary = ""
            parent = a_tag.find_parent()
            if parent:
                for p in parent.find_all("p"):
                    pt = p.get_text(" ", strip=True)
                    if len(pt) > 40:
                        summary = pt[:MAX_SUMMARY_CHARS]
                        break

            # Date: scan the parent container for a date string
            dt = None
            if parent:
                parent_text = parent.get_text(" ", strip=True)
                dt = _parse_lm_date(parent_text)
                # Also check grandparent in case date sits outside the direct parent
                if not dt:
                    gp = parent.find_parent()
                    if gp:
                        dt = _parse_lm_date(gp.get_text(" ", strip=True))

            if dt and not is_recent(dt):
                continue

            published = dt.isoformat() if dt else datetime.now(timezone.utc).isoformat()

            items.append({
                "id":            item_id(title, url),
                "title":         title,
                "url":           url,
                "summary":       summary,
                "source":        source["name"],
                "language":      source["language"],
                "category":      source["category"],
                "published":     published,
                "date_verified": dt is not None,
            })

        time.sleep(SCRAPE_SLEEP_SECONDS)

    except Exception as e:
        print(f"[LEMAURICIEN ERROR] {source['name']}: {e}")
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


# ── Market data (exchange rates, commodities, crypto) ────────────────────────

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
            label = {"XAU": "Gold", "BTC": "Bitcoin"}.get(symbol, symbol)
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

def _parse_defimedia_date(soup) -> str | None:
    """
    Extract publication date from Defimedia article pages.
    Date appears as: <div class="published-date"><span>Publié le: </span>21 mars 2026 à 10:31</div>
    Returns an ISO datetime string or None.
    """
    _DEFI_MONTHS = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
        "fev": 2, "fév": 2, "aou": 8, "aoû": 8,
    }
    date_div = soup.find("div", class_="published-date")
    if not date_div:
        return None
    text = date_div.get_text(" ", strip=True)
    # Remove "Publié le:" prefix
    text = re.sub(r"Publi[ée]\s+le\s*:\s*", "", text, flags=re.IGNORECASE).strip()
    # Match "21 mars 2026 à 10:31" or "21 mars 2026"
    m = re.search(
        r"(\d{1,2})\s+(\w+)\s+(\d{4})(?:\s+[àa]\s+(\d{1,2}):(\d{2}))?",
        text, re.IGNORECASE
    )
    if m:
        day = int(m.group(1))
        mon = m.group(2).lower()
        year = int(m.group(3))
        hour = int(m.group(4)) if m.group(4) else 12
        minute = int(m.group(5)) if m.group(5) else 0
        month = _DEFI_MONTHS.get(mon[:4]) or _DEFI_MONTHS.get(mon[:3])
        if month:
            mu_tz = timezone(timedelta(hours=4))  # MUT = UTC+4
            dt = datetime(year, month, day, hour, minute, tzinfo=mu_tz)
            return dt.astimezone(timezone.utc).isoformat()
    return None


def _fetch_article_meta(url: str, extra_headers: dict = None, session=None) -> dict:
    """
    Fetch a single article page and extract:
    - article:published_time (or og:article:published_time / datePublished)
    - og:description or meta description (for summary enrichment)

    Returns dict with keys: published (ISO str or None), summary (str or None)
    Implements exponential backoff on 429/503.
    extra_headers: merged on top of HEADERS (e.g. Referer for Defimedia).
    session: if provided, used instead of requests.get. May be a curl_cffi session
             (for TLS fingerprint impersonation) or a requests.Session (cookies).
    """
    import time as _time

    is_defimedia = "defimedia.info" in url
    headers = {**HEADERS, **(extra_headers or {})}
    delays = [2, 10, 30]  # seconds between retries
    for attempt, delay in enumerate([0] + delays):
        if delay:
            _time.sleep(delay)
        try:
            if session is not None:
                r = session.get(url, timeout=20)
            else:
                r = requests.get(url, headers=headers, timeout=20)

            if is_defimedia:
                print(f"      [Defimedia fetch] status={r.status_code} url={url[:80]}")

            if r.status_code in (429, 503):
                continue  # retry with next delay
            if r.status_code != 200:
                return {"published": None, "summary": None}

            # Force UTF-8 for sources known to serve UTF-8 with wrong content-type headers
            if "lemauricien.com" in url or is_defimedia:
                r.encoding = "utf-8"

            soup = BeautifulSoup(r.text, "html.parser")

            # ── Published date ────────────────────────────────────────────────
            published = None

            # Defimedia-specific: date in <div class="published-date">
            if is_defimedia:
                published = _parse_defimedia_date(soup)

            # Standard meta tags
            if not published:
                for attr, name in [
                    ("property", "article:published_time"),
                    ("property", "og:article:published_time"),
                    ("name",     "article:published_time"),
                    ("name",     "datePublished"),
                    ("itemprop", "datePublished"),
                ]:
                    tag = soup.find("meta", attrs={attr: name})
                    if tag and tag.get("content"):
                        published = tag["content"].strip()
                        break

            # Fallback: JSON-LD
            if not published:
                import json as _json
                for script in soup.find_all("script", type="application/ld+json"):
                    try:
                        data = _json.loads(script.string or "")
                        if isinstance(data, dict):
                            published = data.get("datePublished") or data.get("dateCreated")
                        elif isinstance(data, list):
                            for d in data:
                                if isinstance(d, dict):
                                    published = d.get("datePublished") or d.get("dateCreated")
                                    if published:
                                        break
                        if published:
                            break
                    except Exception:
                        pass

            # ── Summary ───────────────────────────────────────────────────────
            summary = None
            for attr, name in [
                ("property", "og:description"),
                ("name",     "description"),
                ("name",     "twitter:description"),
            ]:
                tag = soup.find("meta", attrs={attr: name})
                if tag and tag.get("content") and len(tag["content"].strip()) > 40:
                    summary = tag["content"].strip()[:MAX_SUMMARY_CHARS]
                    break

            return {"published": published, "summary": summary}

        except Exception as e:
            if is_defimedia:
                print(f"      [Defimedia fetch error] attempt={attempt} url={url[:80]} err={e}")
            continue

    return {"published": None, "summary": None}


# Sources that use curl_cffi browser TLS impersonation for enrichment
_ENRICH_SESSION_SOURCES = {"Defimedia"}

# Per-source extra headers for enrichment fetches
_ENRICH_EXTRA_HEADERS = {
    "Defimedia": {"Referer": "https://defimedia.info"},
    "Le Mauricien": {"Referer": "https://www.lemauricien.com"},
    "Business Insider Africa": {"Referer": "https://africa.businessinsider.com"},
}

# One session per blocking source, initialised lazily during enrichment
_ENRICH_SESSIONS: dict = {}


def _get_enrich_session(source_name: str, homepage_url: str):
    """
    Return a session for a blocking source, creating it on first call.
    For Defimedia: uses curl_cffi with Chrome TLS impersonation to bypass
    bot detection that rejects standard Python requests TLS fingerprints.
    The session GETs the homepage first to establish cookies.
    """
    if source_name not in _ENRICH_SESSIONS:
        try:
            from curl_cffi.requests import Session as CurlSession
            session = CurlSession(impersonate="chrome")
            session.headers.update({
                **HEADERS,
                **_ENRICH_EXTRA_HEADERS.get(source_name, {}),
            })
            try:
                session.get(homepage_url, timeout=15)
                time.sleep(SCRAPE_SLEEP_SECONDS)
            except Exception:
                pass
        except ImportError:
            # curl_cffi not available — fall back to plain requests.Session
            print(f"[WARN] curl_cffi not installed, falling back to requests.Session for {source_name}")
            session = requests.Session()
            session.headers.update({
                **HEADERS,
                **_ENRICH_EXTRA_HEADERS.get(source_name, {}),
            })
            try:
                session.get(homepage_url, timeout=15)
                time.sleep(SCRAPE_SLEEP_SECONDS)
            except Exception:
                pass
        _ENRICH_SESSIONS[source_name] = session
    return _ENRICH_SESSIONS[source_name]


def enrich_articles(items: list) -> list:
    """
    Enrichment pass for local articles:

    Two modes per item:
    A) date_verified=False — fetch article page to get real date + summary.
       If real date is older than 24h: drop. If fresh: update date + summary.
       If fetch fails: keep as-is (Stage 2 handles conservatively).
    B) date_verified=True but summary is empty — fetch article page for
       summary only. Date is not touched.

    No cap on number of items enriched.
    Per-source stats logged for diagnostics.
    Source-specific Referer headers applied to reduce block rate.
    """
    from collections import defaultdict
    from dateutil import parser as _dp

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    # Mode A: unverified date — local and regional sources
    date_targets = [
        i for i, item in enumerate(items)
        if not item.get("date_verified", True)
        and item.get("category") in ("local", "regional")
        and item.get("url", "").startswith("http")
    ]

    # Mode B: verified date but no summary — local and regional sources
    summary_targets = [
        i for i, item in enumerate(items)
        if item.get("date_verified", True)
        and not item.get("summary", "").strip()
        and item.get("category") in ("local", "regional")
        and item.get("url", "").startswith("http")
        and i not in set(date_targets)
    ]

    print(f"  Enriching: {len(date_targets)} need date+summary, "
          f"{len(summary_targets)} need summary only")

    # Per-source counters
    stats = defaultdict(lambda: {"enriched": 0, "dropped": 0, "failed": 0, "summary_only": 0})
    indices_to_drop = []

    # ── Mode A: date + summary ────────────────────────────────────────────────
    for idx in date_targets:
        item = items[idx]
        source_name = item.get("source", "")
        extra_headers = _ENRICH_EXTRA_HEADERS.get(source_name, {})
        session = None
        if source_name in _ENRICH_SESSION_SOURCES:
            session = _get_enrich_session(source_name, item["url"].split("/")[0] + "//" + item["url"].split("/")[2])
        meta = _fetch_article_meta(item["url"], extra_headers, session)
        time.sleep(SCRAPE_SLEEP_SECONDS)

        if meta["published"]:
            try:
                pub_dt = _dp.parse(meta["published"])
                if pub_dt.tzinfo is None:
                    pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    indices_to_drop.append(idx)
                    stats[source_name]["dropped"] += 1
                else:
                    items[idx]["published"] = pub_dt.isoformat()
                    items[idx]["date_verified"] = True
                    if meta["summary"] and not items[idx].get("summary"):
                        items[idx]["summary"] = meta["summary"]
                    stats[source_name]["enriched"] += 1
            except Exception:
                stats[source_name]["failed"] += 1
        else:
            stats[source_name]["failed"] += 1

    # ── Mode B: summary only ──────────────────────────────────────────────────
    for idx in summary_targets:
        item = items[idx]
        source_name = item.get("source", "")
        extra_headers = _ENRICH_EXTRA_HEADERS.get(source_name, {})
        session = None
        if source_name in _ENRICH_SESSION_SOURCES:
            session = _get_enrich_session(source_name, item["url"].split("/")[0] + "//" + item["url"].split("/")[2])
        meta = _fetch_article_meta(item["url"], extra_headers, session)
        time.sleep(SCRAPE_SLEEP_SECONDS)

        if meta["summary"]:
            items[idx]["summary"] = meta["summary"]
            stats[source_name]["summary_only"] += 1
        else:
            stats[source_name]["failed"] += 1

    # Drop stale items (in reverse order to preserve indices)
    for idx in sorted(indices_to_drop, reverse=True):
        items.pop(idx)

    # Per-source summary
    for src, s in sorted(stats.items()):
        print(f"    [{src}] enriched={s['enriched']}, dropped={s['dropped']}, "
              f"summary_only={s['summary_only']}, failed={s['failed']}")

    total_dropped = sum(s["dropped"] for s in stats.values())
    print(f"  Total stale dropped: {total_dropped}")
    return items

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
        elif scrape_type == "megamu":
            items = scrape_megamu(source)
        elif scrape_type == "lemauricien":
            items = scrape_lemauricien(source)
        else:
            items = scrape_homepage(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Fetching market data...")
    for source in sources.get("market_data", []):
        rate_type = source.get("type")
        if rate_type == "gold_api":
            items = fetch_gold_api(source)
        elif rate_type == "oilprice_demo":
            items = fetch_oilprice_demo(source)
        else:
            items = fetch_exchange_rates(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    all_items = deduplicate(all_items)
    print(f"\nTotal unique items: {len(all_items)}")

    print("Enriching unverified local articles...")
    all_items = enrich_articles(all_items)
    print(f"  Total after enrichment: {len(all_items)}")

    rss_output = build_rss(all_items)
    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_output)
    print("Written to feed.xml")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
