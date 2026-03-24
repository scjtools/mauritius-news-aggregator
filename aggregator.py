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
import os
import warnings
import logging

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

from cluster import deduplicate_items, cluster_and_collapse, deduplicate_bulletin_text

logging.basicConfig(level=logging.INFO, format="%(message)s")

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
        title = re.sub(r"\s*[-–]\s*[^-–]{3,50}$", "", title).strip()
    return title


# ── RSS sources ──────────────────────────────────────────────────────────────

def fetch_rss(source):
    items = []
    exclude_url_patterns = source.get("exclude_url_patterns", [])
    exclude_title_prefixes = source.get("exclude_title_prefixes", [])
    try:
        if "lemauricien.com" in source["url"]:
            _rss_raw = requests.get(source["url"], headers=HEADERS, timeout=15)
            _rss_raw.encoding = "utf-8"
            feed = feedparser.parse(_rss_raw.text)
        else:
            feed = feedparser.parse(source["url"])

        for entry in feed.entries:
            dt = parse_date(entry)
            if not is_recent(dt):
                continue

            raw_title = entry.get("title", "").strip()
            url = entry.get("link", "")

            if any(pat in url for pat in exclude_url_patterns):
                continue
            if any(raw_title.startswith(pfx) for pfx in exclude_title_prefixes):
                continue

            title = clean_title(raw_title, source["name"])
            summary = BeautifulSoup(
                getattr(entry, "summary", "") or "", "html.parser"
            ).get_text()[:MAX_SUMMARY_CHARS]

            if not summary.strip() and source.get("summary_fallback_title"):
                summary = title

            items.append({
                "id": item_id(title, entry.get("link", "")),
                "title": title,
                "url": url,
                "summary": summary.strip(),
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
                "date_verified": dt is not None,
            })
    except Exception as e:
        print(f"[RSS ERROR] {source['name']}: {e}")
    return items


# ── Homepage scraper ─────────────────────────────────────────────────────────

def scrape_homepage(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        seen = set()

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
                    from urllib.parse import urlparse
                    parsed = urlparse(source["url"])
                    base = f"{parsed.scheme}://{parsed.netloc}"
                else:
                    base = source["url"].rstrip("/")
                url = base + "/" + url.lstrip("/")

            if url in seen:
                continue

            from urllib.parse import urlparse as _urlparse
            source_host = _urlparse(source["url"]).netloc
            url_host = _urlparse(url).netloc
            if url_host and source_host and not (url_host == source_host or url_host.endswith("." + source_host)):
                continue

            category_allowlist = source.get("category_allowlist")
            if category_allowlist and not use_link_fallback:
                cat_el = tag.find(class_=lambda c: c and "article-category" in c)
                if cat_el is None or cat_el.get_text(strip=True) not in category_allowlist:
                    continue

            url_path_allowlist = source.get("url_path_allowlist")
            if url_path_allowlist and not any(seg in url for seg in url_path_allowlist):
                continue

            seen.add(url)

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
                "id": item_id(title, url),
                "title": title,
                "url": url,
                "summary": summary,
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": published,
                "date_verified": dt is not None,
            })

        time.sleep(SCRAPE_SLEEP_SECONDS)

    except Exception as e:
        print(f"[SCRAPE ERROR] {source['name']}: {e}")
    return items


# ── mega.mu scraper ───────────────────────────────────────────────────────────

def scrape_megamu(source):
    items = []
    max_pages = source.get("max_pages", 5)
    seen = set()

    EN_MONTHS = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    def parse_megamu_date(text):
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

                summary = ""
                parent = h3.find_parent()
                if parent:
                    p = parent.find("p")
                    if p:
                        summary = p.get_text(" ", strip=True)[:MAX_SUMMARY_CHARS]

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
                    "id": item_id(title, redirect_url),
                    "title": title,
                    "url": redirect_url,
                    "summary": summary,
                    "source": source["name"],
                    "language": source["language"],
                    "category": source["category"],
                    "published": published,
                    "date_verified": dt is not None,
                })
                found_on_page += 1

            time.sleep(SCRAPE_SLEEP_SECONDS)

            if page_went_stale and found_on_page == 0:
                break

    except Exception as e:
        print(f"[MEGAMU ERROR] {source['name']}: {e}")

    return items


# ── Le Mauricien scraper ─────────────────────────────────────────────────────

_LM_MONTHS = {
    "jan": 1, "fév": 2, "mar": 3, "avr": 4, "mai": 5, "jun": 6,
    "juil": 7, "aoû": 8, "sep": 9, "oct": 10, "nov": 11, "déc": 12,
    "fev": 2, "aou": 8,
}


def _parse_lm_date(text: str):
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})\s+(\d{1,2})h(\d{2})", text, re.IGNORECASE)
    if m:
        day, mon, year = int(m.group(1)), m.group(2).lower()[:3], int(m.group(3))
        hour, minute = int(m.group(4)), int(m.group(5))
        month = _LM_MONTHS.get(mon)
        if month:
            mu_tz = timezone(timedelta(hours=4))
            return datetime(year, month, day, hour, minute, tzinfo=mu_tz).astimezone(timezone.utc)

    m2 = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text, re.IGNORECASE)
    if m2:
        day, mon, year = int(m2.group(1)), m2.group(2).lower()[:3], int(m2.group(3))
        month = _LM_MONTHS.get(mon)
        if month:
            mu_tz = timezone(timedelta(hours=4))
            return datetime(year, month, day, 12, 0, tzinfo=mu_tz).astimezone(timezone.utc)

    return None


def scrape_lemauricien(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        url_path_allowlist = source.get("url_path_allowlist", [])
        source_host = source["url"].rstrip("/").split("//")[-1]
        seen = set()

        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")

            if href.startswith("http"):
                url = href
            elif href.startswith("/"):
                url = f"https://www.lemauricien.com{href}"
            else:
                continue

            if source_host not in url:
                continue
            if url_path_allowlist and not any(seg in url for seg in url_path_allowlist):
                continue
            if url in seen:
                continue

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

            summary = ""
            if parent := a_tag.find_parent():
                for p in parent.find_all("p"):
                    pt = p.get_text(" ", strip=True)
                    if len(pt) > 40:
                        summary = pt[:MAX_SUMMARY_CHARS]
                        break

            dt = None
            if parent := a_tag.find_parent():
                parent_text = parent.get_text(" ", strip=True)
                dt = _parse_lm_date(parent_text)
                if not dt:
                    gp = parent.find_parent()
                    if gp:
                        dt = _parse_lm_date(gp.get_text(" ", strip=True))

            if dt and not is_recent(dt):
                continue

            published = dt.isoformat() if dt else datetime.now(timezone.utc).isoformat()

            items.append({
                "id": item_id(title, url),
                "title": title,
                "url": url,
                "summary": summary,
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": published,
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
            bulletin_text = " ".join(paragraphs)
            bulletin_text = re.sub(r"^Welcome to Mauritius Meteorological Services\s*", "", bulletin_text).strip()
            bulletin_text = re.sub(r"\s*About Us\|Publications\|.+$", "", bulletin_text, flags=re.DOTALL).strip()
            bulletin_text = deduplicate_bulletin_text(bulletin_text)

            now = datetime.now(timezone.utc)
            items.append({
                "id": item_id("Met bulletin", now.strftime("%Y-%m-%d")),
                "title": f"Mauritius weather bulletin – {now.strftime('%d %B %Y')}",
                "url": source["url"],
                "summary": bulletin_text,
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
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
                "id": item_id("SEMDEX", now.strftime("%Y-%m-%d")),
                "title": f"SEMDEX – {now.strftime('%d %B %Y')}",
                "url": source["url"],
                "summary": f"SEMDEX closed at {semdex_value}",
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
                "date_verified": True,
            })
        else:
            print("[SEMDEX WARNING] Could not extract index value from page")

        time.sleep(SCRAPE_SLEEP_SECONDS)
    except Exception as e:
        print(f"[SEMDEX ERROR] {source['name']}: {e}")
    return items


# ── CEB Power Outages ────────────────────────────────────────────────────────

def fetch_power_outages(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        all_outages = data.get("today", []) + data.get("future", [])

        for outage in all_outages:
            locality = outage.get("locality", "").title()
            streets = outage.get("streets", "")
            district = outage.get("district", "").title()
            from_str = outage.get("from", "")
            to_str = outage.get("to", "")
            outage_id = outage.get("id", "")

            try:
                from_dt = datetime.fromisoformat(from_str.replace("Z", "+00:00"))
                to_dt = datetime.fromisoformat(to_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

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
                "id": item_id("CEB outage", outage_id or f"{locality}{from_str}"),
                "title": title,
                "url": "https://github.com/MrSunshyne/mauritius-dataset-electricity",
                "summary": summary[:MAX_SUMMARY_CHARS],
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": from_dt.isoformat(),
                "date_verified": True,
            })

    except Exception as e:
        print(f"[POWER OUTAGES ERROR] {source['name']}: {e}")
    return items


# ── Public holidays ──────────────────────────────────────────────────────────

def fetch_public_holidays(source):
    items = []
    try:
        with open(source["url"]) as f:
            raw = f.read()

        all_holidays = []
        for doc in yaml.safe_load_all(raw):
            if doc and "holidays" in doc:
                all_holidays.extend(doc["holidays"])

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
                "id": item_id("public holiday", today.isoformat()),
                "title": f"Upcoming public holiday – {upcoming[0][1]}",
                "url": "https://govmu.org",
                "summary": " | ".join(parts),
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
                "date_verified": True,
            })

    except Exception as e:
        print(f"[PUBLIC HOLIDAYS ERROR] {source['name']}: {e}")
    return items


# ── Deduplicate (hash-based) ────────────────────────────────────────────────

def deduplicate(items):
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


# ── Build RSS output ─────────────────────────────────────────────────────────

def _list_text(value):
    if not value:
        return ""
    if isinstance(value, list):
        return "\n".join(str(v) for v in value if str(v).strip())
    return str(value)


def _sort_key(item):
    return item.get("cluster_time") or item.get("published") or ""


def _clean_xml_text(value):
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"\bURL:\s*https?://\S+", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"https?://\S+", lambda m: m.group(0) if False else " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _set_text(parent, tag, value):
    text = _clean_xml_text(value)
    if text:
        SubElement(parent, tag).text = text


def build_rss(items):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "Mauritius News Aggregator"
    SubElement(channel, "link").text = "https://github.com/scjtools/mauritius-news-aggregator/blob/main/feed.xml"
    SubElement(channel, "description").text = "Aggregated Mauritius news – last 24 hours"
    SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    for item in sorted(items, key=_sort_key, reverse=True):
        entry = SubElement(channel, "item")
        cluster_size = int(item.get("cluster_size", 1) or 1)

        _set_text(entry, "title", item.get("title", ""))
        _set_text(entry, "link", item.get("url", ""))
        _set_text(entry, "description", item.get("summary", ""))
        _set_text(entry, "source", item.get("source", ""))
        _set_text(entry, "category", item.get("category", ""))
        _set_text(entry, "cluster_time", item.get("cluster_time", item.get("published", "")))
        _set_text(entry, "cluster_size", str(cluster_size))
        _set_text(entry, "source_count", str(item.get("source_count", 1)))

        # Only keep cluster_id for actual multi-item clusters
        if cluster_size > 1:
            _set_text(entry, "cluster_id", item.get("cluster_id", ""))

            sources_text = _list_text(item.get("sources", []))
            urls_text = _list_text(item.get("urls", []))
            titles_text = _list_text(item.get("titles", []))
            languages_text = _list_text(item.get("languages", []))

            if sources_text:
                SubElement(entry, "sources").text = sources_text
            if urls_text:
                SubElement(entry, "urls").text = urls_text
            if titles_text:
                SubElement(entry, "titles").text = titles_text
            if languages_text:
                SubElement(entry, "languages").text = languages_text

    raw = tostring(rss, encoding="unicode")
    return minidom.parseString(raw).toprettyxml(indent="  ")


# ── Market data ──────────────────────────────────────────────────────────────

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
                        rate_value = parts[1].strip()
                        if rate_value.endswith(currency):
                            rate_value = rate_value[:-len(currency)].strip()
                        try:
                            rates[currency] = 1.0 / float(rate_value)
                        except (ValueError, ZeroDivisionError):
                            pass

        if rates:
            now = datetime.now(timezone.utc)
            summary = " | ".join(
                f"MUR/{c} {rates[c]:.2f}"
                for c in target_currencies if c in rates
            )
            items.append({
                "id": item_id("MUR rates", now.strftime("%Y-%m-%d")),
                "title": f"MUR exchange rates – {now.strftime('%d %B %Y')}",
                "url": source["url"],
                "summary": summary,
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
                "date_verified": True,
            })
    except Exception as e:
        print(f"[RATES ERROR] {source['name']}: {e}")
    return items


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
                "id": item_id(f"{symbol} price", now.strftime("%Y-%m-%d")),
                "title": f"{label} price – {now.strftime('%d %B %Y')}",
                "url": source["url"],
                "summary": f"{label} ({symbol}): {currency} {price:,.2f}",
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
                "date_verified": True,
            })
    except Exception as e:
        print(f"[GOLD API ERROR] {source['name']}: {e}")
    return items


def fetch_oilprice_demo(source):
    items = []
    try:
        headers = {**HEADERS, "Content-Type": "application/json"}
        r = requests.get(source["url"], headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()

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
                "id": item_id("Brent crude", now.strftime("%Y-%m-%d")),
                "title": f"{label} price – {now.strftime('%d %B %Y')}",
                "url": "https://www.oilpriceapi.com",
                "summary": f"{label}: {formatted if formatted else f'{currency} {price:,.2f}'} per barrel",
                "source": source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": now.isoformat(),
                "date_verified": True,
            })
        else:
            print(f"[OILPRICE WARNING] No price found in response: {data}")
    except Exception as e:
        print(f"[OILPRICE ERROR] {source['name']}: {e}")
    return items


# ── Article enrichment ───────────────────────────────────────────────────────

def _parse_defimedia_date(soup) -> str | None:
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
    text = re.sub(r"Publi[ée]\s+le\s*:\s*", "", text, flags=re.IGNORECASE).strip()
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
        month = _DEFI_MONTHS.get(mon) or _DEFI_MONTHS.get(mon[:4]) or _DEFI_MONTHS.get(mon[:3])
        if month:
            mu_tz = timezone(timedelta(hours=4))
            dt = datetime(year, month, day, hour, minute, tzinfo=mu_tz)
            return dt.astimezone(timezone.utc).isoformat()

    return None


def _fetch_article_meta(url: str, extra_headers: dict = None, session=None) -> dict:
    import time as _time

    is_defimedia = "defimedia.info" in url
    headers = {**HEADERS, **(extra_headers or {})}
    delays = [2, 10, 30]

    for attempt, delay in enumerate([0] + delays):
        if delay:
            _time.sleep(delay)
        try:
            if session is not None:
                r = session.get(url, timeout=20)
            else:
                r = requests.get(url, headers=headers, timeout=20)

            if r.status_code in (429, 503):
                continue
            if r.status_code != 200:
                return {"published": None, "summary": None}

            if "lemauricien.com" in url or is_defimedia:
                r.encoding = "utf-8"

            soup = BeautifulSoup(r.text, "html.parser")
            published = None

            if is_defimedia:
                published = _parse_defimedia_date(soup)

            if not published:
                for attr, name in [
                    ("property", "article:published_time"),
                    ("property", "og:article:published_time"),
                    ("name", "article:published_time"),
                    ("name", "datePublished"),
                    ("itemprop", "datePublished"),
                ]:
                    tag = soup.find("meta", attrs={attr: name})
                    if tag and tag.get("content"):
                        published = tag["content"].strip()
                        break

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

            summary = None
            for attr, name in [
                ("property", "og:description"),
                ("name", "description"),
                ("name", "twitter:description"),
            ]:
                tag = soup.find("meta", attrs={attr: name})
                if tag and tag.get("content") and len(tag["content"].strip()) > 40:
                    summary = tag["content"].strip()[:MAX_SUMMARY_CHARS]
                    break

            if not summary and "businessinsider.com" in url:
                for div in soup.find_all("div", class_="ringCommonDetail"):
                    p = div.find("p")
                    if p:
                        text = p.get_text(strip=True)
                        if len(text) > 40:
                            summary = text[:MAX_SUMMARY_CHARS]
                            break

            return {"published": published, "summary": summary}

        except Exception as e:
            if is_defimedia:
                print(f"      [Defimedia fetch error] attempt={attempt} url={url[:80]} err={e}")
            continue

    return {"published": None, "summary": None}


_ENRICH_SESSION_SOURCES = {"Defimedia"}

_ENRICH_EXTRA_HEADERS = {
    "Defimedia": {"Referer": "https://defimedia.info"},
    "Le Mauricien": {"Referer": "https://www.lemauricien.com"},
    "Business Insider Africa": {"Referer": "https://africa.businessinsider.com"},
}

_ENRICH_SESSIONS: dict = {}


def _get_enrich_session(source_name: str, homepage_url: str):
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
    from collections import defaultdict
    from dateutil import parser as _dp

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    date_targets = [
        i for i, item in enumerate(items)
        if not item.get("date_verified", True)
        and item.get("category") in ("local", "regional")
        and item.get("url", "").startswith("http")
    ]

    summary_targets = [
        i for i, item in enumerate(items)
        if item.get("date_verified", True)
        and not item.get("summary", "").strip()
        and item.get("category") in ("local", "regional")
        and item.get("url", "").startswith("http")
        and i not in set(date_targets)
    ]

    print(f"  Enriching: {len(date_targets)} need date+summary, {len(summary_targets)} need summary only")

    stats = defaultdict(lambda: {"enriched": 0, "dropped": 0, "failed": 0, "summary_only": 0})
    indices_to_drop = []

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

    for idx in sorted(indices_to_drop, reverse=True):
        items.pop(idx)

    for src, s in sorted(stats.items()):
        print(f"    [{src}] enriched={s['enriched']}, dropped={s['dropped']}, summary_only={s['summary_only']}, failed={s['failed']}")

    total_dropped = sum(s["dropped"] for s in stats.values())
    print(f"  Total stale dropped: {total_dropped}")
    return items


# ── Manual inject ────────────────────────────────────────────────────────────

def load_injected_items(path="inject.yaml"):
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        raw = data.get("items", []) or []
        if not raw:
            return []

        items = []
        now = datetime.now(timezone.utc)
        for entry in raw:
            url = (entry.get("url") or "").strip()
            title = (entry.get("title") or "").strip()
            description = (entry.get("description") or "").strip()
            category = (entry.get("category") or "global").strip()
            language = (entry.get("language") or "en").strip()
            if not url:
                continue
            if not title:
                try:
                    meta = _fetch_article_meta(url)
                    title = meta.get("title") or url
                    if not description:
                        description = meta.get("summary") or ""
                except Exception:
                    title = url

            items.append({
                "id": item_id(title, url),
                "title": title,
                "url": url,
                "summary": description,
                "source": "Injected",
                "language": language,
                "category": category,
                "published": now.isoformat(),
                "date_verified": True,
            })

        with open(path, "w") as f:
            f.write("items: []\n")

        return items
    except Exception as e:
        print(f"[INJECT ERROR] {e}")
        return []


# ── Main ─────────────────────────────────────────────────────────────────────

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

    _CACHE_PATH = "market_cache.json"
    try:
        with open(_CACHE_PATH) as _f:
            _prev_cache = json.load(_f)
    except Exception:
        _prev_cache = {}

    _new_cache = {}
    for item in all_items:
        if item.get("category") != "finance":
            continue

        src = item.get("source", "")
        summary = item.get("summary", "")

        cache_key = None
        current_val = None

        if src == "Stock Exchange of Mauritius":
            m = re.search(r"SEMDEX closed at ([\d,.]+)", summary)
            if m:
                cache_key = "SEMDEX"
                current_val = float(m.group(1).replace(",", ""))

        elif src == "MUR Exchange Rates":
            for pair in summary.split(" | "):
                m = re.match(r"(MUR/[A-Z]+) ([\d.]+)", pair.strip())
                if m:
                    k, v = m.group(1), float(m.group(2))
                    _new_cache[k] = v
                    if k in _prev_cache:
                        delta = v - _prev_cache[k]
                        arrow = "▲" if delta > 0 else "▼" if delta < 0 else "→"
                        item["summary"] += f" ({arrow} {abs(delta):.2f} from last)"

        elif src in ("Gold Price", "Bitcoin Price", "Brent Crude Oil"):
            m = re.search(r"[A-Z]+ [\d,]+\.?\d*", summary)
            if m:
                val_str = m.group(0).split()[-1].replace(",", "")
                try:
                    current_val = float(val_str)
                    cache_key = {"Gold Price": "XAU", "Bitcoin Price": "BTC", "Brent Crude Oil": "BRENT"}[src]
                except ValueError:
                    pass

        if cache_key and current_val is not None:
            _new_cache[cache_key] = current_val
            if cache_key in _prev_cache:
                prev = _prev_cache[cache_key]
                delta = current_val - prev
                pct = (delta / prev * 100) if prev else 0
                arrow = "▲" if delta > 0 else "▼" if delta < 0 else "→"
                if cache_key == "SEMDEX":
                    item["summary"] += f" ({arrow} {abs(delta):.2f} pts, {'+' if delta >= 0 else ''}{pct:.2f}% from last close {prev:,.2f})"
                else:
                    item["summary"] += f" ({arrow} {'+' if delta >= 0 else ''}{pct:.2f}% from last)"

    for k, v in _prev_cache.items():
        if k not in _new_cache:
            _new_cache[k] = v

    try:
        with open(_CACHE_PATH, "w") as _f:
            json.dump(_new_cache, _f, indent=2)
    except Exception as e:
        print(f"[CACHE WRITE ERROR] {e}")

    injected = load_injected_items()
    if injected:
        print(f"  Injected items: {len(injected)}")
        all_items.extend(injected)

    all_items = deduplicate(all_items)
    print(f"\nAfter hash dedup: {len(all_items)} items")

    print("Enriching unverified local articles...")
    all_items = enrich_articles(all_items)
    print(f"  Total after enrichment: {len(all_items)}")

    before = len(all_items)
    all_items = deduplicate_items(all_items)
    print(f"After semantic dedup: {len(all_items)} items ({before - len(all_items)} removed)")

    all_items = cluster_and_collapse(all_items)
    multi = sum(1 for i in all_items if i.get("cluster_size", 1) > 1)
    print(f"After cluster+collapse: {len(all_items)} items ({multi} multi-source clusters)")

    rss_output = build_rss(all_items)
    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_output)
    print("Written to feed.xml")


if __name__ == "__main__":
    main()
