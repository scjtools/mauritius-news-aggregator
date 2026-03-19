import feedparser
import requests
import yaml
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


# ── RSS sources ──────────────────────────────────────────────────────────────

def fetch_rss(source):
    items = []
    try:
        feed = feedparser.parse(source["url"])
        for entry in feed.entries:
            dt = parse_date(entry)
            if not is_recent(dt):
                continue
            summary = BeautifulSoup(
                getattr(entry, "summary", "") or "", "html.parser"
            ).get_text()[:MAX_SUMMARY_CHARS]
            items.append({
                "id":        item_id(entry.get("title", ""), entry.get("link", "")),
                "title":     entry.get("title", "").strip(),
                "url":       entry.get("link", ""),
                "summary":   summary.strip(),
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
            })
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
        print(f"[SCRAPE ERROR] {source['name']}: {e}")
    return items


# ── Met Service bulletin scraper ─────────────────────────────────────────────

def scrape_bulletin(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        paragraphs = []
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 40:
                paragraphs.append(text)

        if paragraphs:
            bulletin_text = " ".join(paragraphs[:4])[:MAX_SUMMARY_CHARS]
            now = datetime.now(timezone.utc)
            items.append({
                "id":        item_id("Met bulletin", now.strftime("%Y-%m-%d")),
                "title":     f"Mauritius weather bulletin – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   bulletin_text,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
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
                "id":        item_id("SEMDEX", now.strftime("%Y-%m-%d")),
                "title":     f"SEMDEX – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   f"SEMDEX closed at {semdex_value}",
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
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
                "id":        item_id("CEB outage", outage_id or f"{locality}{from_str}"),
                "title":     title,
                "url":       "https://github.com/MrSunshyne/mauritius-dataset-electricity",
                "summary":   summary[:MAX_SUMMARY_CHARS],
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": from_dt.isoformat(),
            })

    except Exception as e:
        print(f"[POWER OUTAGES ERROR] {source['name']}: {e}")
    return items


# ── Public holidays ───────────────────────────────────────────────────────────

def fetch_public_holidays(source):
    """
    Reads data/public_holidays.yaml and emits an item when a holiday
    is today or within the next 3 days.
    """
    items = []
    try:
        with open(source["url"]) as f:
            data = yaml.safe_load(f)

        today = date.today()
        lookahead_days = 3
        upcoming = []

        for holiday in data.get("holidays", []):
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
                "id":        item_id("public holiday", today.isoformat()),
                "title":     f"Upcoming public holiday – {upcoming[0][1]}",
                "url":       "https://govmu.org",
                "summary":   " | ".join(parts),
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })

    except Exception as e:
        print(f"[PUBLIC HOLIDAYS ERROR] {source['name']}: {e}")
    return items


# ── Deduplicate ───────────────────────────────────────────────────────────────

def deduplicate(items):
    seen = {}
    for item in items:
        if item["id"] not in seen:
            seen[item["id"]] = item
    return list(seen.values())


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
                        rates[currency] = parts[1].strip()
        if rates:
            now = datetime.now(timezone.utc)
            summary = " | ".join(f"1 MUR = {rates[c]} {c}" for c in target_currencies if c in rates)
            items.append({
                "id":        item_id("MUR rates", now.strftime("%Y-%m-%d")),
                "title":     f"MUR exchange rates – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   summary,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
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
                "id":        item_id(f"{symbol} price", now.strftime("%Y-%m-%d")),
                "title":     f"{label} price – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   f"{label} ({symbol}): {currency} {price:,.2f}",
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })
    except Exception as e:
        print(f"[GOLD API ERROR] {source['name']}: {e}")
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
        else:
            items = scrape_homepage(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Fetching exchange rates...")
    for source in sources.get("exchange_rates", []):
        rate_type = source.get("type")
        if rate_type == "gold_api":
            items = fetch_gold_api(source)
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


if __name__ == "__main__":
    main()
